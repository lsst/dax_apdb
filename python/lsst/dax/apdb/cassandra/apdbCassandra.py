# This file is part of dax_apdb.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ["ApdbCassandraConfig", "ApdbCassandra"]

import dataclasses
import json
import logging
from collections.abc import Iterable, Iterator, Mapping, Set
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pandas

# If cassandra-driver is not there the module can still be imported
# but ApdbCassandra cannot be instantiated.
try:
    import cassandra
    import cassandra.query
    from cassandra.auth import AuthProvider, PlainTextAuthProvider
    from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
    from cassandra.policies import AddressTranslator, RoundRobinPolicy, WhiteListRoundRobinPolicy

    CASSANDRA_IMPORTED = True
except ImportError:
    CASSANDRA_IMPORTED = False

import astropy.time
import felis.datamodel
from lsst import sphgeom
from lsst.pex.config import ChoiceField, Field, ListField
from lsst.utils.db_auth import DbAuth, DbAuthNotFoundError
from lsst.utils.iteration import chunk_iterable

from ..apdb import Apdb, ApdbConfig
from ..apdbConfigFreezer import ApdbConfigFreezer
from ..apdbReplica import ReplicaChunk
from ..apdbSchema import ApdbTables
from ..monitor import MonAgent
from ..pixelization import Pixelization
from ..schema_model import Table
from ..timer import Timer
from ..versionTuple import IncompatibleVersionError, VersionTuple
from .apdbCassandraReplica import ApdbCassandraReplica
from .apdbCassandraSchema import ApdbCassandraSchema, ExtraTables
from .apdbMetadataCassandra import ApdbMetadataCassandra
from .cassandra_utils import (
    PreparedStatementCache,
    literal,
    pandas_dataframe_factory,
    quote_id,
    raw_data_factory,
    select_concurrent,
)

if TYPE_CHECKING:
    from ..apdbMetadata import ApdbMetadata

_LOG = logging.getLogger(__name__)

_MON = MonAgent(__name__)

VERSION = VersionTuple(0, 1, 0)
"""Version for the code controlling non-replication tables. This needs to be
updated following compatibility rules when schema produced by this code
changes.
"""

# Copied from daf_butler.
DB_AUTH_ENVVAR = "LSST_DB_AUTH"
"""Default name of the environmental variable that will be used to locate DB
credentials configuration file. """

DB_AUTH_PATH = "~/.lsst/db-auth.yaml"
"""Default path at which it is expected that DB credentials are found."""


class CassandraMissingError(Exception):
    def __init__(self) -> None:
        super().__init__("cassandra-driver module cannot be imported")


class ApdbCassandraConfig(ApdbConfig):
    """Configuration class for Cassandra-based APDB implementation."""

    contact_points = ListField[str](
        doc="The list of contact points to try connecting for cluster discovery.", default=["127.0.0.1"]
    )
    private_ips = ListField[str](doc="List of internal IP addresses for contact_points.", default=[])
    port = Field[int](doc="Port number to connect to.", default=9042)
    keyspace = Field[str](doc="Default keyspace for operations.", default="apdb")
    username = Field[str](
        doc=f"Cassandra user name, if empty then {DB_AUTH_PATH} has to provide it with password.",
        default="",
    )
    read_consistency = Field[str](
        doc="Name for consistency level of read operations, default: QUORUM, can be ONE.", default="QUORUM"
    )
    write_consistency = Field[str](
        doc="Name for consistency level of write operations, default: QUORUM, can be ONE.", default="QUORUM"
    )
    read_timeout = Field[float](doc="Timeout in seconds for read operations.", default=120.0)
    write_timeout = Field[float](doc="Timeout in seconds for write operations.", default=60.0)
    remove_timeout = Field[float](doc="Timeout in seconds for remove operations.", default=600.0)
    read_concurrency = Field[int](doc="Concurrency level for read operations.", default=500)
    protocol_version = Field[int](
        doc="Cassandra protocol version to use, default is V4",
        default=cassandra.ProtocolVersion.V4 if CASSANDRA_IMPORTED else 0,
    )
    dia_object_columns = ListField[str](
        doc="List of columns to read from DiaObject[Last], by default read all columns", default=[]
    )
    prefix = Field[str](doc="Prefix to add to table names", default="")
    part_pixelization = ChoiceField[str](
        allowed=dict(htm="HTM pixelization", q3c="Q3C pixelization", mq3c="MQ3C pixelization"),
        doc="Pixelization used for partitioning index.",
        default="mq3c",
    )
    part_pix_level = Field[int](doc="Pixelization level used for partitioning index.", default=10)
    part_pix_max_ranges = Field[int](doc="Max number of ranges in pixelization envelope", default=64)
    ra_dec_columns = ListField[str](default=["ra", "dec"], doc="Names of ra/dec columns in DiaObject table")
    timer = Field[bool](doc="If True then print/log timing information", default=False)
    time_partition_tables = Field[bool](
        doc="Use per-partition tables for sources instead of partitioning by time", default=False
    )
    time_partition_days = Field[int](
        doc=(
            "Time partitioning granularity in days, this value must not be changed after database is "
            "initialized"
        ),
        default=30,
    )
    time_partition_start = Field[str](
        doc=(
            "Starting time for per-partition tables, in yyyy-mm-ddThh:mm:ss format, in TAI. "
            "This is used only when time_partition_tables is True."
        ),
        default="2018-12-01T00:00:00",
    )
    time_partition_end = Field[str](
        doc=(
            "Ending time for per-partition tables, in yyyy-mm-ddThh:mm:ss format, in TAI. "
            "This is used only when time_partition_tables is True."
        ),
        default="2030-01-01T00:00:00",
    )
    query_per_time_part = Field[bool](
        default=False,
        doc=(
            "If True then build separate query for each time partition, otherwise build one single query. "
            "This is only used when time_partition_tables is False in schema config."
        ),
    )
    query_per_spatial_part = Field[bool](
        default=False,
        doc="If True then build one query per spatial partition, otherwise build single query.",
    )
    use_insert_id_skips_diaobjects = Field[bool](
        default=False,
        doc=(
            "If True then do not store DiaObjects when use_insert_id is True "
            "(DiaObjectsChunks has the same data)."
        ),
    )


@dataclasses.dataclass
class _FrozenApdbCassandraConfig:
    """Part of the configuration that is saved in metadata table and read back.

    The attributes are a subset of attributes in `ApdbCassandraConfig` class.

    Parameters
    ----------
    config : `ApdbSqlConfig`
        Configuration used to copy initial values of attributes.
    """

    use_insert_id: bool
    part_pixelization: str
    part_pix_level: int
    ra_dec_columns: list[str]
    time_partition_tables: bool
    time_partition_days: int
    use_insert_id_skips_diaobjects: bool

    def __init__(self, config: ApdbCassandraConfig):
        self.use_insert_id = config.use_insert_id
        self.part_pixelization = config.part_pixelization
        self.part_pix_level = config.part_pix_level
        self.ra_dec_columns = list(config.ra_dec_columns)
        self.time_partition_tables = config.time_partition_tables
        self.time_partition_days = config.time_partition_days
        self.use_insert_id_skips_diaobjects = config.use_insert_id_skips_diaobjects

    def to_json(self) -> str:
        """Convert this instance to JSON representation."""
        return json.dumps(dataclasses.asdict(self))

    def update(self, json_str: str) -> None:
        """Update attribute values from a JSON string.

        Parameters
        ----------
        json_str : str
            String containing JSON representation of configuration.
        """
        data = json.loads(json_str)
        if not isinstance(data, dict):
            raise TypeError(f"JSON string must be convertible to object: {json_str!r}")
        allowed_names = {field.name for field in dataclasses.fields(self)}
        for key, value in data.items():
            if key not in allowed_names:
                raise ValueError(f"JSON object contains unknown key: {key}")
            setattr(self, key, value)


if CASSANDRA_IMPORTED:

    class _AddressTranslator(AddressTranslator):
        """Translate internal IP address to external.

        Only used for docker-based setup, not viable long-term solution.
        """

        def __init__(self, public_ips: list[str], private_ips: list[str]):
            self._map = dict((k, v) for k, v in zip(private_ips, public_ips))

        def translate(self, private_ip: str) -> str:
            return self._map.get(private_ip, private_ip)


class ApdbCassandra(Apdb):
    """Implementation of APDB database on to of Apache Cassandra.

    The implementation is configured via standard ``pex_config`` mechanism
    using `ApdbCassandraConfig` configuration class. For an example of
    different configurations check config/ folder.

    Parameters
    ----------
    config : `ApdbCassandraConfig`
        Configuration object.
    """

    metadataSchemaVersionKey = "version:schema"
    """Name of the metadata key to store schema version number."""

    metadataCodeVersionKey = "version:ApdbCassandra"
    """Name of the metadata key to store code version number."""

    metadataReplicaVersionKey = "version:ApdbCassandraReplica"
    """Name of the metadata key to store replica code version number."""

    metadataConfigKey = "config:apdb-cassandra.json"
    """Name of the metadata key to store code version number."""

    _frozen_parameters = (
        "use_insert_id",
        "part_pixelization",
        "part_pix_level",
        "ra_dec_columns",
        "time_partition_tables",
        "time_partition_days",
        "use_insert_id_skips_diaobjects",
    )
    """Names of the config parameters to be frozen in metadata table."""

    partition_zero_epoch = astropy.time.Time(0, format="unix_tai")
    """Start time for partition 0, this should never be changed."""

    def __init__(self, config: ApdbCassandraConfig):
        if not CASSANDRA_IMPORTED:
            raise CassandraMissingError()

        self._keyspace = config.keyspace

        self._cluster, self._session = self._make_session(config)

        meta_table_name = ApdbTables.metadata.table_name(config.prefix)
        self._metadata = ApdbMetadataCassandra(
            self._session, meta_table_name, config.keyspace, "read_tuples", "write"
        )

        # Read frozen config from metadata.
        config_json = self._metadata.get(self.metadataConfigKey)
        if config_json is not None:
            # Update config from metadata.
            freezer = ApdbConfigFreezer[ApdbCassandraConfig](self._frozen_parameters)
            self.config = freezer.update(config, config_json)
        else:
            self.config = config
        self.config.validate()

        self._pixelization = Pixelization(
            self.config.part_pixelization,
            self.config.part_pix_level,
            config.part_pix_max_ranges,
        )

        self._schema = ApdbCassandraSchema(
            session=self._session,
            keyspace=self._keyspace,
            schema_file=self.config.schema_file,
            schema_name=self.config.schema_name,
            prefix=self.config.prefix,
            time_partition_tables=self.config.time_partition_tables,
            enable_replica=self.config.use_insert_id,
        )
        self._partition_zero_epoch_mjd = float(self.partition_zero_epoch.mjd)

        if self._metadata.table_exists():
            self._versionCheck(self._metadata)

        # Cache for prepared statements
        self._preparer = PreparedStatementCache(self._session)

        _LOG.debug("ApdbCassandra Configuration:")
        for key, value in self.config.items():
            _LOG.debug("    %s: %s", key, value)

        self._timer_args: list[MonAgent | logging.Logger] = [_MON]
        if self.config.timer:
            self._timer_args.append(_LOG)

    def __del__(self) -> None:
        if hasattr(self, "_cluster"):
            self._cluster.shutdown()

    def _timer(self, name: str, *, tags: Mapping[str, str | int] | None = None) -> Timer:
        """Create `Timer` instance given its name."""
        return Timer(name, *self._timer_args, tags=tags)

    @classmethod
    def _make_session(cls, config: ApdbCassandraConfig) -> tuple[Cluster, Session]:
        """Make Cassandra session."""
        addressTranslator: AddressTranslator | None = None
        if config.private_ips:
            addressTranslator = _AddressTranslator(list(config.contact_points), list(config.private_ips))

        cluster = Cluster(
            execution_profiles=cls._makeProfiles(config),
            contact_points=config.contact_points,
            port=config.port,
            address_translator=addressTranslator,
            protocol_version=config.protocol_version,
            auth_provider=cls._make_auth_provider(config),
        )
        session = cluster.connect()
        # Disable result paging
        session.default_fetch_size = None

        return cluster, session

    @classmethod
    def _make_auth_provider(cls, config: ApdbCassandraConfig) -> AuthProvider | None:
        """Make Cassandra authentication provider instance."""
        try:
            dbauth = DbAuth(DB_AUTH_PATH, DB_AUTH_ENVVAR)
        except DbAuthNotFoundError:
            # Credentials file doesn't exist, use anonymous login.
            return None

        empty_username = True
        # Try every contact point in turn.
        for hostname in config.contact_points:
            try:
                username, password = dbauth.getAuth(
                    "cassandra", config.username, hostname, config.port, config.keyspace
                )
                if not username:
                    # Password without user name, try next hostname, but give
                    # warning later if no better match is found.
                    empty_username = True
                else:
                    return PlainTextAuthProvider(username=username, password=password)
            except DbAuthNotFoundError:
                pass

        if empty_username:
            _LOG.warning(
                f"Credentials file ({DB_AUTH_PATH} or ${DB_AUTH_ENVVAR}) provided password but not "
                f"user name, anonymous Cassandra logon will be attempted."
            )

        return None

    def _versionCheck(self, metadata: ApdbMetadataCassandra) -> None:
        """Check schema version compatibility."""

        def _get_version(key: str, default: VersionTuple) -> VersionTuple:
            """Retrieve version number from given metadata key."""
            if metadata.table_exists():
                version_str = metadata.get(key)
                if version_str is None:
                    # Should not happen with existing metadata table.
                    raise RuntimeError(f"Version key {key!r} does not exist in metadata table.")
                return VersionTuple.fromString(version_str)
            return default

        # For old databases where metadata table does not exist we assume that
        # version of both code and schema is 0.1.0.
        initial_version = VersionTuple(0, 1, 0)
        db_schema_version = _get_version(self.metadataSchemaVersionKey, initial_version)
        db_code_version = _get_version(self.metadataCodeVersionKey, initial_version)

        # For now there is no way to make read-only APDB instances, assume that
        # any access can do updates.
        if not self._schema.schemaVersion().checkCompatibility(db_schema_version):
            raise IncompatibleVersionError(
                f"Configured schema version {self._schema.schemaVersion()} "
                f"is not compatible with database version {db_schema_version}"
            )
        if not self.apdbImplementationVersion().checkCompatibility(db_code_version):
            raise IncompatibleVersionError(
                f"Current code version {self.apdbImplementationVersion()} "
                f"is not compatible with database version {db_code_version}"
            )

        # Check replica code version only if replica is enabled.
        if self._schema.has_replica_chunks:
            db_replica_version = _get_version(self.metadataReplicaVersionKey, initial_version)
            code_replica_version = ApdbCassandraReplica.apdbReplicaImplementationVersion()
            if not code_replica_version.checkCompatibility(db_replica_version):
                raise IncompatibleVersionError(
                    f"Current replication code version {code_replica_version} "
                    f"is not compatible with database version {db_replica_version}"
                )

    @classmethod
    def apdbImplementationVersion(cls) -> VersionTuple:
        """Return version number for current APDB implementation.

        Returns
        -------
        version : `VersionTuple`
            Version of the code defined in implementation class.
        """
        return VERSION

    def tableDef(self, table: ApdbTables) -> Table | None:
        # docstring is inherited from a base class
        return self._schema.tableSchemas.get(table)

    @classmethod
    def init_database(
        cls,
        hosts: list[str],
        keyspace: str,
        *,
        schema_file: str | None = None,
        schema_name: str | None = None,
        read_sources_months: int | None = None,
        read_forced_sources_months: int | None = None,
        use_insert_id: bool = False,
        use_insert_id_skips_diaobjects: bool = False,
        port: int | None = None,
        username: str | None = None,
        prefix: str | None = None,
        part_pixelization: str | None = None,
        part_pix_level: int | None = None,
        time_partition_tables: bool = True,
        time_partition_start: str | None = None,
        time_partition_end: str | None = None,
        read_consistency: str | None = None,
        write_consistency: str | None = None,
        read_timeout: int | None = None,
        write_timeout: int | None = None,
        ra_dec_columns: list[str] | None = None,
        replication_factor: int | None = None,
        drop: bool = False,
    ) -> ApdbCassandraConfig:
        """Initialize new APDB instance and make configuration object for it.

        Parameters
        ----------
        hosts : `list` [`str`]
            List of host names or IP addresses for Cassandra cluster.
        keyspace : `str`
            Name of the keyspace for APDB tables.
        schema_file : `str`, optional
            Location of (YAML) configuration file with APDB schema. If not
            specified then default location will be used.
        schema_name : `str`, optional
            Name of the schema in YAML configuration file. If not specified
            then default name will be used.
        read_sources_months : `int`, optional
            Number of months of history to read from DiaSource.
        read_forced_sources_months : `int`, optional
            Number of months of history to read from DiaForcedSource.
        use_insert_id : `bool`, optional
            If True, make additional tables used for replication to PPDB.
        use_insert_id_skips_diaobjects : `bool`, optional
            If `True` then do not fill regular ``DiaObject`` table when
            ``use_insert_id`` is `True`.
        port : `int`, optional
            Port number to use for Cassandra connections.
        username : `str`, optional
            User name for Cassandra connections.
        prefix : `str`, optional
            Optional prefix for all table names.
        part_pixelization : `str`, optional
            Name of the MOC pixelization used for partitioning.
        part_pix_level : `int`, optional
            Pixelization level.
        time_partition_tables : `bool`, optional
            Create per-partition tables.
        time_partition_start : `str`, optional
            Starting time for per-partition tables, in yyyy-mm-ddThh:mm:ss
            format, in TAI.
        time_partition_end : `str`, optional
            Ending time for per-partition tables, in yyyy-mm-ddThh:mm:ss
            format, in TAI.
        read_consistency : `str`, optional
            Name of the consistency level for read operations.
        write_consistency : `str`, optional
            Name of the consistency level for write operations.
        read_timeout : `int`, optional
            Read timeout in seconds.
        write_timeout : `int`, optional
            Write timeout in seconds.
        ra_dec_columns : `list` [`str`], optional
            Names of ra/dec columns in DiaObject table.
        replication_factor : `int`, optional
            Replication factor used when creating new keyspace, if keyspace
            already exists its replication factor is not changed.
        drop : `bool`, optional
            If `True` then drop existing tables before re-creating the schema.

        Returns
        -------
        config : `ApdbCassandraConfig`
            Resulting configuration object for a created APDB instance.
        """
        config = ApdbCassandraConfig(
            contact_points=hosts,
            keyspace=keyspace,
            use_insert_id=use_insert_id,
            use_insert_id_skips_diaobjects=use_insert_id_skips_diaobjects,
            time_partition_tables=time_partition_tables,
        )
        if schema_file is not None:
            config.schema_file = schema_file
        if schema_name is not None:
            config.schema_name = schema_name
        if read_sources_months is not None:
            config.read_sources_months = read_sources_months
        if read_forced_sources_months is not None:
            config.read_forced_sources_months = read_forced_sources_months
        if port is not None:
            config.port = port
        if username is not None:
            config.username = username
        if prefix is not None:
            config.prefix = prefix
        if part_pixelization is not None:
            config.part_pixelization = part_pixelization
        if part_pix_level is not None:
            config.part_pix_level = part_pix_level
        if time_partition_start is not None:
            config.time_partition_start = time_partition_start
        if time_partition_end is not None:
            config.time_partition_end = time_partition_end
        if read_consistency is not None:
            config.read_consistency = read_consistency
        if write_consistency is not None:
            config.write_consistency = write_consistency
        if read_timeout is not None:
            config.read_timeout = read_timeout
        if write_timeout is not None:
            config.write_timeout = write_timeout
        if ra_dec_columns is not None:
            config.ra_dec_columns = ra_dec_columns

        cls._makeSchema(config, drop=drop, replication_factor=replication_factor)

        return config

    def get_replica(self) -> ApdbCassandraReplica:
        """Return `ApdbReplica` instance for this database."""
        # Note that this instance has to stay alive while replica exists, so
        # we pass reference to self.
        return ApdbCassandraReplica(self, self._schema, self._session)

    @classmethod
    def _makeSchema(
        cls, config: ApdbConfig, *, drop: bool = False, replication_factor: int | None = None
    ) -> None:
        # docstring is inherited from a base class

        if not isinstance(config, ApdbCassandraConfig):
            raise TypeError(f"Unexpected type of configuration object: {type(config)}")

        cluster, session = cls._make_session(config)

        schema = ApdbCassandraSchema(
            session=session,
            keyspace=config.keyspace,
            schema_file=config.schema_file,
            schema_name=config.schema_name,
            prefix=config.prefix,
            time_partition_tables=config.time_partition_tables,
            enable_replica=config.use_insert_id,
        )

        # Ask schema to create all tables.
        if config.time_partition_tables:
            time_partition_start = astropy.time.Time(config.time_partition_start, format="isot", scale="tai")
            time_partition_end = astropy.time.Time(config.time_partition_end, format="isot", scale="tai")
            part_epoch = float(cls.partition_zero_epoch.mjd)
            part_days = config.time_partition_days
            part_range = (
                cls._time_partition_cls(time_partition_start, part_epoch, part_days),
                cls._time_partition_cls(time_partition_end, part_epoch, part_days) + 1,
            )
            schema.makeSchema(drop=drop, part_range=part_range, replication_factor=replication_factor)
        else:
            schema.makeSchema(drop=drop, replication_factor=replication_factor)

        meta_table_name = ApdbTables.metadata.table_name(config.prefix)
        metadata = ApdbMetadataCassandra(session, meta_table_name, config.keyspace, "read_tuples", "write")

        # Fill version numbers, overrides if they existed before.
        if metadata.table_exists():
            metadata.set(cls.metadataSchemaVersionKey, str(schema.schemaVersion()), force=True)
            metadata.set(cls.metadataCodeVersionKey, str(cls.apdbImplementationVersion()), force=True)

            if config.use_insert_id:
                # Only store replica code version if replica is enabled.
                metadata.set(
                    cls.metadataReplicaVersionKey,
                    str(ApdbCassandraReplica.apdbReplicaImplementationVersion()),
                    force=True,
                )

            # Store frozen part of a configuration in metadata.
            freezer = ApdbConfigFreezer[ApdbCassandraConfig](cls._frozen_parameters)
            metadata.set(cls.metadataConfigKey, freezer.to_json(config), force=True)

        cluster.shutdown()

    def getDiaObjects(self, region: sphgeom.Region) -> pandas.DataFrame:
        # docstring is inherited from a base class

        sp_where = self._spatial_where(region)
        _LOG.debug("getDiaObjects: #partitions: %s", len(sp_where))

        # We need to exclude extra partitioning columns from result.
        column_names = self._schema.apdbColumnNames(ApdbTables.DiaObjectLast)
        what = ",".join(quote_id(column) for column in column_names)

        table_name = self._schema.tableName(ApdbTables.DiaObjectLast)
        query = f'SELECT {what} from "{self._keyspace}"."{table_name}"'
        statements: list[tuple] = []
        for where, params in sp_where:
            full_query = f"{query} WHERE {where}"
            if params:
                statement = self._preparer.prepare(full_query)
            else:
                # If there are no params then it is likely that query has a
                # bunch of literals rendered already, no point trying to
                # prepare it because it's not reusable.
                statement = cassandra.query.SimpleStatement(full_query)
            statements.append((statement, params))
        _LOG.debug("getDiaObjects: #queries: %s", len(statements))

        with _MON.context_tags({"table": "DiaObject"}):
            _MON.add_record(
                "select_query_stats", values={"num_sp_part": len(sp_where), "num_queries": len(statements)}
            )
            with self._timer("select_time"):
                objects = cast(
                    pandas.DataFrame,
                    select_concurrent(
                        self._session, statements, "read_pandas_multi", self.config.read_concurrency
                    ),
                )

        _LOG.debug("found %s DiaObjects", objects.shape[0])
        return objects

    def getDiaSources(
        self, region: sphgeom.Region, object_ids: Iterable[int] | None, visit_time: astropy.time.Time
    ) -> pandas.DataFrame | None:
        # docstring is inherited from a base class
        months = self.config.read_sources_months
        if months == 0:
            return None
        mjd_end = visit_time.mjd
        mjd_start = mjd_end - months * 30

        return self._getSources(region, object_ids, mjd_start, mjd_end, ApdbTables.DiaSource)

    def getDiaForcedSources(
        self, region: sphgeom.Region, object_ids: Iterable[int] | None, visit_time: astropy.time.Time
    ) -> pandas.DataFrame | None:
        # docstring is inherited from a base class
        months = self.config.read_forced_sources_months
        if months == 0:
            return None
        mjd_end = visit_time.mjd
        mjd_start = mjd_end - months * 30

        return self._getSources(region, object_ids, mjd_start, mjd_end, ApdbTables.DiaForcedSource)

    def containsVisitDetector(self, visit: int, detector: int) -> bool:
        # docstring is inherited from a base class
        # The order of checks corresponds to order in store(), on potential
        # store failure earlier tables have higher probability containing
        # stored records. With per-partition tables there will be many tables
        # in the list, but it is unlikely that we'll use that setup in
        # production.
        existing_tables = self._schema.existing_tables(ApdbTables.DiaSource, ApdbTables.DiaForcedSource)
        tables_to_check = existing_tables[ApdbTables.DiaSource][:]
        if self.config.use_insert_id:
            tables_to_check.append(self._schema.tableName(ExtraTables.DiaSourceChunks))
        tables_to_check.extend(existing_tables[ApdbTables.DiaForcedSource])
        if self.config.use_insert_id:
            tables_to_check.append(self._schema.tableName(ExtraTables.DiaForcedSourceChunks))

        # I do not want to run concurrent queries as they are all full-scan
        # queries, so we do one by one.
        for table_name in tables_to_check:
            # Try to find a single record with given visit/detector. This is a
            # full scan query so ALLOW FILTERING is needed. It will probably
            # guess PER PARTITION LIMIT itself, but let's help it.
            query = (
                f'SELECT * from "{self._keyspace}"."{table_name}" '
                "WHERE visit = ? AND detector = ? "
                "PER PARTITION LIMIT 1 LIMIT 1 ALLOW FILTERING"
            )
            with self._timer("contains_visit_detector_time", tags={"table": table_name}):
                result = self._session.execute(self._preparer.prepare(query), (visit, detector))
                if result.one() is not None:
                    # There is a result.
                    return True
        return False

    def getSSObjects(self) -> pandas.DataFrame:
        # docstring is inherited from a base class
        tableName = self._schema.tableName(ApdbTables.SSObject)
        query = f'SELECT * from "{self._keyspace}"."{tableName}"'

        objects = None
        with self._timer("select_time", tags={"table": "SSObject"}):
            result = self._session.execute(query, execution_profile="read_pandas")
            objects = result._current_rows

        _LOG.debug("found %s SSObjects", objects.shape[0])
        return objects

    def store(
        self,
        visit_time: astropy.time.Time,
        objects: pandas.DataFrame,
        sources: pandas.DataFrame | None = None,
        forced_sources: pandas.DataFrame | None = None,
    ) -> None:
        # docstring is inherited from a base class

        replica_chunk: ReplicaChunk | None = None
        if self._schema.has_replica_chunks:
            replica_chunk = ReplicaChunk.make_replica_chunk(visit_time, self.config.replica_chunk_seconds)
            self._storeReplicaChunk(replica_chunk, visit_time)

        # fill region partition column for DiaObjects
        objects = self._add_obj_part(objects)
        self._storeDiaObjects(objects, visit_time, replica_chunk)

        if sources is not None:
            # copy apdb_part column from DiaObjects to DiaSources
            sources = self._add_src_part(sources, objects)
            self._storeDiaSources(ApdbTables.DiaSource, sources, visit_time, replica_chunk)
            self._storeDiaSourcesPartitions(sources, visit_time, replica_chunk)

        if forced_sources is not None:
            forced_sources = self._add_fsrc_part(forced_sources, objects)
            self._storeDiaSources(ApdbTables.DiaForcedSource, forced_sources, visit_time, replica_chunk)

    def storeSSObjects(self, objects: pandas.DataFrame) -> None:
        # docstring is inherited from a base class
        self._storeObjectsPandas(objects, ApdbTables.SSObject)

    def reassignDiaSources(self, idMap: Mapping[int, int]) -> None:
        # docstring is inherited from a base class

        # To update a record we need to know its exact primary key (including
        # partition key) so we start by querying for diaSourceId to find the
        # primary keys.

        table_name = self._schema.tableName(ExtraTables.DiaSourceToPartition)
        # split it into 1k IDs per query
        selects: list[tuple] = []
        for ids in chunk_iterable(idMap.keys(), 1_000):
            ids_str = ",".join(str(item) for item in ids)
            selects.append(
                (
                    (
                        'SELECT "diaSourceId", "apdb_part", "apdb_time_part", "apdb_replica_chunk" '
                        f'FROM "{self._keyspace}"."{table_name}" WHERE "diaSourceId" IN ({ids_str})'
                    ),
                    {},
                )
            )

        # No need for DataFrame here, read data as tuples.
        result = cast(
            list[tuple[int, int, int, int | None]],
            select_concurrent(self._session, selects, "read_tuples", self.config.read_concurrency),
        )

        # Make mapping from source ID to its partition.
        id2partitions: dict[int, tuple[int, int]] = {}
        id2chunk_id: dict[int, int] = {}
        for row in result:
            id2partitions[row[0]] = row[1:3]
            if row[3] is not None:
                id2chunk_id[row[0]] = row[3]

        # make sure we know partitions for each ID
        if set(id2partitions) != set(idMap):
            missing = ",".join(str(item) for item in set(idMap) - set(id2partitions))
            raise ValueError(f"Following DiaSource IDs do not exist in the database: {missing}")

        # Reassign in standard tables
        queries = cassandra.query.BatchStatement()
        table_name = self._schema.tableName(ApdbTables.DiaSource)
        for diaSourceId, ssObjectId in idMap.items():
            apdb_part, apdb_time_part = id2partitions[diaSourceId]
            values: tuple
            if self.config.time_partition_tables:
                query = (
                    f'UPDATE "{self._keyspace}"."{table_name}_{apdb_time_part}"'
                    ' SET "ssObjectId" = ?, "diaObjectId" = NULL'
                    ' WHERE "apdb_part" = ? AND "diaSourceId" = ?'
                )
                values = (ssObjectId, apdb_part, diaSourceId)
            else:
                query = (
                    f'UPDATE "{self._keyspace}"."{table_name}"'
                    ' SET "ssObjectId" = ?, "diaObjectId" = NULL'
                    ' WHERE "apdb_part" = ? AND "apdb_time_part" = ? AND "diaSourceId" = ?'
                )
                values = (ssObjectId, apdb_part, apdb_time_part, diaSourceId)
            queries.add(self._preparer.prepare(query), values)

        # Reassign in replica tables, only if replication is enabled
        if id2chunk_id:
            # Filter out chunks that have been deleted already. There is a
            # potential race with concurrent removal of chunks, but it
            # should be handled by WHERE in UPDATE.
            known_ids = set()
            if replica_chunks := self.get_replica().getReplicaChunks():
                known_ids = set(replica_chunk.id for replica_chunk in replica_chunks)
            id2chunk_id = {key: value for key, value in id2chunk_id.items() if value in known_ids}
            if id2chunk_id:
                table_name = self._schema.tableName(ExtraTables.DiaSourceChunks)
                for diaSourceId, ssObjectId in idMap.items():
                    if replica_chunk := id2chunk_id.get(diaSourceId):
                        query = (
                            f'UPDATE "{self._keyspace}"."{table_name}" '
                            ' SET "ssObjectId" = ?, "diaObjectId" = NULL '
                            'WHERE "apdb_replica_chunk" = ? AND "diaSourceId" = ?'
                        )
                        values = (ssObjectId, replica_chunk, diaSourceId)
                        queries.add(self._preparer.prepare(query), values)

        _LOG.debug("%s: will update %d records", table_name, len(idMap))
        with self._timer("update_time", tags={"table": table_name}):
            self._session.execute(queries, execution_profile="write")

    def dailyJob(self) -> None:
        # docstring is inherited from a base class
        pass

    def countUnassociatedObjects(self) -> int:
        # docstring is inherited from a base class

        # It's too inefficient to implement it for Cassandra in current schema.
        raise NotImplementedError()

    @property
    def metadata(self) -> ApdbMetadata:
        # docstring is inherited from a base class
        if self._metadata is None:
            raise RuntimeError("Database schema was not initialized.")
        return self._metadata

    @classmethod
    def _makeProfiles(cls, config: ApdbCassandraConfig) -> Mapping[Any, ExecutionProfile]:
        """Make all execution profiles used in the code."""
        if config.private_ips:
            loadBalancePolicy = WhiteListRoundRobinPolicy(hosts=config.contact_points)
        else:
            loadBalancePolicy = RoundRobinPolicy()

        read_tuples_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.read_consistency),
            request_timeout=config.read_timeout,
            row_factory=cassandra.query.tuple_factory,
            load_balancing_policy=loadBalancePolicy,
        )
        read_pandas_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.read_consistency),
            request_timeout=config.read_timeout,
            row_factory=pandas_dataframe_factory,
            load_balancing_policy=loadBalancePolicy,
        )
        read_raw_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.read_consistency),
            request_timeout=config.read_timeout,
            row_factory=raw_data_factory,
            load_balancing_policy=loadBalancePolicy,
        )
        # Profile to use with select_concurrent to return pandas data frame
        read_pandas_multi_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.read_consistency),
            request_timeout=config.read_timeout,
            row_factory=pandas_dataframe_factory,
            load_balancing_policy=loadBalancePolicy,
        )
        # Profile to use with select_concurrent to return raw data (columns and
        # rows)
        read_raw_multi_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.read_consistency),
            request_timeout=config.read_timeout,
            row_factory=raw_data_factory,
            load_balancing_policy=loadBalancePolicy,
        )
        write_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.write_consistency),
            request_timeout=config.write_timeout,
            load_balancing_policy=loadBalancePolicy,
        )
        # To replace default DCAwareRoundRobinPolicy
        default_profile = ExecutionProfile(
            load_balancing_policy=loadBalancePolicy,
        )
        return {
            "read_tuples": read_tuples_profile,
            "read_pandas": read_pandas_profile,
            "read_raw": read_raw_profile,
            "read_pandas_multi": read_pandas_multi_profile,
            "read_raw_multi": read_raw_multi_profile,
            "write": write_profile,
            EXEC_PROFILE_DEFAULT: default_profile,
        }

    def _getSources(
        self,
        region: sphgeom.Region,
        object_ids: Iterable[int] | None,
        mjd_start: float,
        mjd_end: float,
        table_name: ApdbTables,
    ) -> pandas.DataFrame:
        """Return catalog of DiaSource instances given set of DiaObject IDs.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Spherical region.
        object_ids :
            Collection of DiaObject IDs
        mjd_start : `float`
            Lower bound of time interval.
        mjd_end : `float`
            Upper bound of time interval.
        table_name : `ApdbTables`
            Name of the table.

        Returns
        -------
        catalog : `pandas.DataFrame`, or `None`
            Catalog containing DiaSource records. Empty catalog is returned if
            ``object_ids`` is empty.
        """
        object_id_set: Set[int] = set()
        if object_ids is not None:
            object_id_set = set(object_ids)
            if len(object_id_set) == 0:
                return self._make_empty_catalog(table_name)

        sp_where = self._spatial_where(region)
        tables, temporal_where = self._temporal_where(table_name, mjd_start, mjd_end)

        # We need to exclude extra partitioning columns from result.
        column_names = self._schema.apdbColumnNames(table_name)
        what = ",".join(quote_id(column) for column in column_names)

        # Build all queries
        statements: list[tuple] = []
        for table in tables:
            prefix = f'SELECT {what} from "{self._keyspace}"."{table}"'
            statements += list(self._combine_where(prefix, sp_where, temporal_where))
        _LOG.debug("_getSources %s: #queries: %s", table_name, len(statements))

        with _MON.context_tags({"table": table_name.name}):
            _MON.add_record(
                "select_query_stats", values={"num_sp_part": len(sp_where), "num_queries": len(statements)}
            )
            with self._timer("select_time"):
                catalog = cast(
                    pandas.DataFrame,
                    select_concurrent(
                        self._session, statements, "read_pandas_multi", self.config.read_concurrency
                    ),
                )

        # filter by given object IDs
        if len(object_id_set) > 0:
            catalog = cast(pandas.DataFrame, catalog[catalog["diaObjectId"].isin(object_id_set)])

        # precise filtering on midpointMjdTai
        catalog = cast(pandas.DataFrame, catalog[catalog["midpointMjdTai"] > mjd_start])

        _LOG.debug("found %d %ss", catalog.shape[0], table_name.name)
        return catalog

    def _storeReplicaChunk(self, replica_chunk: ReplicaChunk, visit_time: astropy.time.Time) -> None:
        # Cassandra timestamp uses milliseconds since epoch
        timestamp = int(replica_chunk.last_update_time.unix_tai * 1000)

        # everything goes into a single partition
        partition = 0

        table_name = self._schema.tableName(ExtraTables.ApdbReplicaChunks)
        query = (
            f'INSERT INTO "{self._keyspace}"."{table_name}" '
            "(partition, apdb_replica_chunk, last_update_time, unique_id) "
            "VALUES (?, ?, ?, ?)"
        )

        self._session.execute(
            self._preparer.prepare(query),
            (partition, replica_chunk.id, timestamp, replica_chunk.unique_id),
            timeout=self.config.write_timeout,
            execution_profile="write",
        )

    def _storeDiaObjects(
        self, objs: pandas.DataFrame, visit_time: astropy.time.Time, replica_chunk: ReplicaChunk | None
    ) -> None:
        """Store catalog of DiaObjects from current visit.

        Parameters
        ----------
        objs : `pandas.DataFrame`
            Catalog with DiaObject records
        visit_time : `astropy.time.Time`
            Time of the current visit.
        replica_chunk : `ReplicaChunk` or `None`
            Replica chunk identifier if replication is configured.
        """
        if len(objs) == 0:
            _LOG.debug("No objects to write to database.")
            return

        visit_time_dt = visit_time.datetime
        extra_columns = dict(lastNonForcedSource=visit_time_dt)
        self._storeObjectsPandas(objs, ApdbTables.DiaObjectLast, extra_columns=extra_columns)

        extra_columns["validityStart"] = visit_time_dt
        time_part: int | None = self._time_partition(visit_time)
        if not self.config.time_partition_tables:
            extra_columns["apdb_time_part"] = time_part
            time_part = None

        # Only store DiaObects if not doing replication or explicitly
        # configured to always store them.
        if replica_chunk is None or not self.config.use_insert_id_skips_diaobjects:
            self._storeObjectsPandas(
                objs, ApdbTables.DiaObject, extra_columns=extra_columns, time_part=time_part
            )

        if replica_chunk is not None:
            extra_columns = dict(apdb_replica_chunk=replica_chunk.id, validityStart=visit_time_dt)
            self._storeObjectsPandas(objs, ExtraTables.DiaObjectChunks, extra_columns=extra_columns)

    def _storeDiaSources(
        self,
        table_name: ApdbTables,
        sources: pandas.DataFrame,
        visit_time: astropy.time.Time,
        replica_chunk: ReplicaChunk | None,
    ) -> None:
        """Store catalog of DIASources or DIAForcedSources from current visit.

        Parameters
        ----------
        table_name : `ApdbTables`
            Table where to store the data.
        sources : `pandas.DataFrame`
            Catalog containing DiaSource records
        visit_time : `astropy.time.Time`
            Time of the current visit.
        replica_chunk : `ReplicaChunk` or `None`
            Replica chunk identifier if replication is configured.
        """
        time_part: int | None = self._time_partition(visit_time)
        extra_columns: dict[str, Any] = {}
        if not self.config.time_partition_tables:
            extra_columns["apdb_time_part"] = time_part
            time_part = None

        self._storeObjectsPandas(sources, table_name, extra_columns=extra_columns, time_part=time_part)

        if replica_chunk is not None:
            extra_columns = dict(apdb_replica_chunk=replica_chunk.id)
            if table_name is ApdbTables.DiaSource:
                extra_table = ExtraTables.DiaSourceChunks
            else:
                extra_table = ExtraTables.DiaForcedSourceChunks
            self._storeObjectsPandas(sources, extra_table, extra_columns=extra_columns)

    def _storeDiaSourcesPartitions(
        self, sources: pandas.DataFrame, visit_time: astropy.time.Time, replica_chunk: ReplicaChunk | None
    ) -> None:
        """Store mapping of diaSourceId to its partitioning values.

        Parameters
        ----------
        sources : `pandas.DataFrame`
            Catalog containing DiaSource records
        visit_time : `astropy.time.Time`
            Time of the current visit.
        """
        id_map = cast(pandas.DataFrame, sources[["diaSourceId", "apdb_part"]])
        extra_columns = {
            "apdb_time_part": self._time_partition(visit_time),
            "apdb_replica_chunk": replica_chunk.id if replica_chunk is not None else None,
        }

        self._storeObjectsPandas(
            id_map, ExtraTables.DiaSourceToPartition, extra_columns=extra_columns, time_part=None
        )

    def _storeObjectsPandas(
        self,
        records: pandas.DataFrame,
        table_name: ApdbTables | ExtraTables,
        extra_columns: Mapping | None = None,
        time_part: int | None = None,
    ) -> None:
        """Store generic objects.

        Takes Pandas catalog and stores a bunch of records in a table.

        Parameters
        ----------
        records : `pandas.DataFrame`
            Catalog containing object records
        table_name : `ApdbTables`
            Name of the table as defined in APDB schema.
        extra_columns : `dict`, optional
            Mapping (column_name, column_value) which gives fixed values for
            columns in each row, overrides values in ``records`` if matching
            columns exist there.
        time_part : `int`, optional
            If not `None` then insert into a per-partition table.

        Notes
        -----
        If Pandas catalog contains additional columns not defined in table
        schema they are ignored. Catalog does not have to contain all columns
        defined in a table, but partition and clustering keys must be present
        in a catalog or ``extra_columns``.
        """
        # use extra columns if specified
        if extra_columns is None:
            extra_columns = {}
        extra_fields = list(extra_columns.keys())

        # Fields that will come from dataframe.
        df_fields = [column for column in records.columns if column not in extra_fields]

        column_map = self._schema.getColumnMap(table_name)
        # list of columns (as in felis schema)
        fields = [column_map[field].name for field in df_fields if field in column_map]
        fields += extra_fields

        # check that all partitioning and clustering columns are defined
        required_columns = self._schema.partitionColumns(table_name) + self._schema.clusteringColumns(
            table_name
        )
        missing_columns = [column for column in required_columns if column not in fields]
        if missing_columns:
            raise ValueError(f"Primary key columns are missing from catalog: {missing_columns}")

        qfields = [quote_id(field) for field in fields]
        qfields_str = ",".join(qfields)

        with self._timer("insert_build_time", tags={"table": table_name.name}):
            table = self._schema.tableName(table_name)
            if time_part is not None:
                table = f"{table}_{time_part}"

            holders = ",".join(["?"] * len(qfields))
            query = f'INSERT INTO "{self._keyspace}"."{table}" ({qfields_str}) VALUES ({holders})'
            statement = self._preparer.prepare(query)
            queries = cassandra.query.BatchStatement()
            for rec in records.itertuples(index=False):
                values = []
                for field in df_fields:
                    if field not in column_map:
                        continue
                    value = getattr(rec, field)
                    if column_map[field].datatype is felis.datamodel.DataType.timestamp:
                        if isinstance(value, pandas.Timestamp):
                            value = literal(value.to_pydatetime())
                        else:
                            # Assume it's seconds since epoch, Cassandra
                            # datetime is in milliseconds
                            value = int(value * 1000)
                    values.append(literal(value))
                for field in extra_fields:
                    value = extra_columns[field]
                    values.append(literal(value))
                queries.add(statement, values)

        _LOG.debug("%s: will store %d records", self._schema.tableName(table_name), records.shape[0])
        with self._timer("insert_time", tags={"table": table_name.name}):
            self._session.execute(queries, timeout=self.config.write_timeout, execution_profile="write")

    def _add_obj_part(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """Calculate spatial partition for each record and add it to a
        DataFrame.

        Notes
        -----
        This overrides any existing column in a DataFrame with the same name
        (apdb_part). Original DataFrame is not changed, copy of a DataFrame is
        returned.
        """
        # calculate HTM index for every DiaObject
        apdb_part = np.zeros(df.shape[0], dtype=np.int64)
        ra_col, dec_col = self.config.ra_dec_columns
        for i, (ra, dec) in enumerate(zip(df[ra_col], df[dec_col])):
            uv3d = sphgeom.UnitVector3d(sphgeom.LonLat.fromDegrees(ra, dec))
            idx = self._pixelization.pixel(uv3d)
            apdb_part[i] = idx
        df = df.copy()
        df["apdb_part"] = apdb_part
        return df

    def _add_src_part(self, sources: pandas.DataFrame, objs: pandas.DataFrame) -> pandas.DataFrame:
        """Add apdb_part column to DiaSource catalog.

        Notes
        -----
        This method copies apdb_part value from a matching DiaObject record.
        DiaObject catalog needs to have a apdb_part column filled by
        ``_add_obj_part`` method and DiaSource records need to be
        associated to DiaObjects via ``diaObjectId`` column.

        This overrides any existing column in a DataFrame with the same name
        (apdb_part). Original DataFrame is not changed, copy of a DataFrame is
        returned.
        """
        pixel_id_map: dict[int, int] = {
            diaObjectId: apdb_part for diaObjectId, apdb_part in zip(objs["diaObjectId"], objs["apdb_part"])
        }
        apdb_part = np.zeros(sources.shape[0], dtype=np.int64)
        ra_col, dec_col = self.config.ra_dec_columns
        for i, (diaObjId, ra, dec) in enumerate(
            zip(sources["diaObjectId"], sources[ra_col], sources[dec_col])
        ):
            if diaObjId == 0:
                # DiaSources associated with SolarSystemObjects do not have an
                # associated DiaObject hence we skip them and set partition
                # based on its own ra/dec
                uv3d = sphgeom.UnitVector3d(sphgeom.LonLat.fromDegrees(ra, dec))
                idx = self._pixelization.pixel(uv3d)
                apdb_part[i] = idx
            else:
                apdb_part[i] = pixel_id_map[diaObjId]
        sources = sources.copy()
        sources["apdb_part"] = apdb_part
        return sources

    def _add_fsrc_part(self, sources: pandas.DataFrame, objs: pandas.DataFrame) -> pandas.DataFrame:
        """Add apdb_part column to DiaForcedSource catalog.

        Notes
        -----
        This method copies apdb_part value from a matching DiaObject record.
        DiaObject catalog needs to have a apdb_part column filled by
        ``_add_obj_part`` method and DiaSource records need to be
        associated to DiaObjects via ``diaObjectId`` column.

        This overrides any existing column in a DataFrame with the same name
        (apdb_part). Original DataFrame is not changed, copy of a DataFrame is
        returned.
        """
        pixel_id_map: dict[int, int] = {
            diaObjectId: apdb_part for diaObjectId, apdb_part in zip(objs["diaObjectId"], objs["apdb_part"])
        }
        apdb_part = np.zeros(sources.shape[0], dtype=np.int64)
        for i, diaObjId in enumerate(sources["diaObjectId"]):
            apdb_part[i] = pixel_id_map[diaObjId]
        sources = sources.copy()
        sources["apdb_part"] = apdb_part
        return sources

    @classmethod
    def _time_partition_cls(cls, time: float | astropy.time.Time, epoch_mjd: float, part_days: int) -> int:
        """Calculate time partition number for a given time.

        Parameters
        ----------
        time : `float` or `astropy.time.Time`
            Time for which to calculate partition number. Can be float to mean
            MJD or `astropy.time.Time`
        epoch_mjd : `float`
            Epoch time for partition 0.
        part_days : `int`
            Number of days per partition.

        Returns
        -------
        partition : `int`
            Partition number for a given time.
        """
        if isinstance(time, astropy.time.Time):
            mjd = float(time.mjd)
        else:
            mjd = time
        days_since_epoch = mjd - epoch_mjd
        partition = int(days_since_epoch) // part_days
        return partition

    def _time_partition(self, time: float | astropy.time.Time) -> int:
        """Calculate time partition number for a given time.

        Parameters
        ----------
        time : `float` or `astropy.time.Time`
            Time for which to calculate partition number. Can be float to mean
            MJD or `astropy.time.Time`

        Returns
        -------
        partition : `int`
            Partition number for a given time.
        """
        if isinstance(time, astropy.time.Time):
            mjd = float(time.mjd)
        else:
            mjd = time
        days_since_epoch = mjd - self._partition_zero_epoch_mjd
        partition = int(days_since_epoch) // self.config.time_partition_days
        return partition

    def _make_empty_catalog(self, table_name: ApdbTables) -> pandas.DataFrame:
        """Make an empty catalog for a table with a given name.

        Parameters
        ----------
        table_name : `ApdbTables`
            Name of the table.

        Returns
        -------
        catalog : `pandas.DataFrame`
            An empty catalog.
        """
        table = self._schema.tableSchemas[table_name]

        data = {
            columnDef.name: pandas.Series(dtype=self._schema.column_dtype(columnDef.datatype))
            for columnDef in table.columns
        }
        return pandas.DataFrame(data)

    def _combine_where(
        self,
        prefix: str,
        where1: list[tuple[str, tuple]],
        where2: list[tuple[str, tuple]],
        suffix: str | None = None,
    ) -> Iterator[tuple[cassandra.query.Statement, tuple]]:
        """Make cartesian product of two parts of WHERE clause into a series
        of statements to execute.

        Parameters
        ----------
        prefix : `str`
            Initial statement prefix that comes before WHERE clause, e.g.
            "SELECT * from Table"
        """
        # If lists are empty use special sentinels.
        if not where1:
            where1 = [("", ())]
        if not where2:
            where2 = [("", ())]

        for expr1, params1 in where1:
            for expr2, params2 in where2:
                full_query = prefix
                wheres = []
                if expr1:
                    wheres.append(expr1)
                if expr2:
                    wheres.append(expr2)
                if wheres:
                    full_query += " WHERE " + " AND ".join(wheres)
                if suffix:
                    full_query += " " + suffix
                params = params1 + params2
                if params:
                    statement = self._preparer.prepare(full_query)
                else:
                    # If there are no params then it is likely that query
                    # has a bunch of literals rendered already, no point
                    # trying to prepare it.
                    statement = cassandra.query.SimpleStatement(full_query)
                yield (statement, params)

    def _spatial_where(
        self, region: sphgeom.Region | None, use_ranges: bool = False
    ) -> list[tuple[str, tuple]]:
        """Generate expressions for spatial part of WHERE clause.

        Parameters
        ----------
        region : `sphgeom.Region`
            Spatial region for query results.
        use_ranges : `bool`
            If True then use pixel ranges ("apdb_part >= p1 AND apdb_part <=
            p2") instead of exact list of pixels. Should be set to True for
            large regions covering very many pixels.

        Returns
        -------
        expressions : `list` [ `tuple` ]
            Empty list is returned if ``region`` is `None`, otherwise a list
            of one or more (expression, parameters) tuples
        """
        if region is None:
            return []
        if use_ranges:
            pixel_ranges = self._pixelization.envelope(region)
            expressions: list[tuple[str, tuple]] = []
            for lower, upper in pixel_ranges:
                upper -= 1
                if lower == upper:
                    expressions.append(('"apdb_part" = ?', (lower,)))
                else:
                    expressions.append(('"apdb_part" >= ? AND "apdb_part" <= ?', (lower, upper)))
            return expressions
        else:
            pixels = self._pixelization.pixels(region)
            if self.config.query_per_spatial_part:
                return [('"apdb_part" = ?', (pixel,)) for pixel in pixels]
            else:
                pixels_str = ",".join([str(pix) for pix in pixels])
                return [(f'"apdb_part" IN ({pixels_str})', ())]

    def _temporal_where(
        self,
        table: ApdbTables,
        start_time: float | astropy.time.Time,
        end_time: float | astropy.time.Time,
        query_per_time_part: bool | None = None,
    ) -> tuple[list[str], list[tuple[str, tuple]]]:
        """Generate table names and expressions for temporal part of WHERE
        clauses.

        Parameters
        ----------
        table : `ApdbTables`
            Table to select from.
        start_time : `astropy.time.Time` or `float`
            Starting Datetime of MJD value of the time range.
        end_time : `astropy.time.Time` or `float`
            Starting Datetime of MJD value of the time range.
        query_per_time_part : `bool`, optional
            If None then use ``query_per_time_part`` from configuration.

        Returns
        -------
        tables : `list` [ `str` ]
            List of the table names to query.
        expressions : `list` [ `tuple` ]
            A list of zero or more (expression, parameters) tuples.
        """
        tables: list[str]
        temporal_where: list[tuple[str, tuple]] = []
        table_name = self._schema.tableName(table)
        time_part_start = self._time_partition(start_time)
        time_part_end = self._time_partition(end_time)
        time_parts = list(range(time_part_start, time_part_end + 1))
        if self.config.time_partition_tables:
            tables = [f"{table_name}_{part}" for part in time_parts]
        else:
            tables = [table_name]
            if query_per_time_part is None:
                query_per_time_part = self.config.query_per_time_part
            if query_per_time_part:
                temporal_where = [('"apdb_time_part" = ?', (time_part,)) for time_part in time_parts]
            else:
                time_part_list = ",".join([str(part) for part in time_parts])
                temporal_where = [(f'"apdb_time_part" IN ({time_part_list})', ())]

        return tables, temporal_where
