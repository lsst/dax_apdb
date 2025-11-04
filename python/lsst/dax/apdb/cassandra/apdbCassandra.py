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

__all__ = ["ApdbCassandra"]

import datetime
import logging
import random
import uuid
import warnings
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Set
from typing import TYPE_CHECKING, Any, cast

import numpy as np
import pandas

# If cassandra-driver is not there the module can still be imported
# but ApdbCassandra cannot be instantiated.
try:
    import cassandra
    import cassandra.query
    from cassandra.query import UNSET_VALUE

    CASSANDRA_IMPORTED = True
except ImportError:
    CASSANDRA_IMPORTED = False

import astropy.time
import felis.datamodel

from lsst import sphgeom
from lsst.utils.iteration import chunk_iterable

from ..apdb import Apdb, ApdbConfig
from ..apdbConfigFreezer import ApdbConfigFreezer
from ..apdbReplica import ApdbTableData, ReplicaChunk
from ..apdbSchema import ApdbSchema, ApdbTables
from ..monitor import MonAgent
from ..recordIds import DiaObjectId
from ..schema_model import Table
from ..timer import Timer
from ..versionTuple import VersionTuple
from .apdbCassandraAdmin import ApdbCassandraAdmin
from .apdbCassandraReplica import ApdbCassandraReplica
from .apdbCassandraSchema import ApdbCassandraSchema, CreateTableOptions, ExtraTables
from .apdbMetadataCassandra import ApdbMetadataCassandra
from .cassandra_utils import (
    execute_concurrent,
    literal,
    quote_id,
    select_concurrent,
)
from .config import ApdbCassandraConfig, ApdbCassandraConnectionConfig, ApdbCassandraTimePartitionRange
from .connectionContext import ConnectionContext, DbVersions
from .exceptions import CassandraMissingError
from .partitioner import Partitioner
from .sessionFactory import SessionContext, SessionFactory

if TYPE_CHECKING:
    from ..apdbMetadata import ApdbMetadata
    from ..apdbUpdateRecord import ApdbUpdateRecord

_LOG = logging.getLogger(__name__)

_MON = MonAgent(__name__)

VERSION = VersionTuple(1, 3, 0)
"""Version for the code controlling non-replication tables. This needs to be
updated following compatibility rules when schema produced by this code
changes.
"""


class ApdbCassandra(Apdb):
    """Implementation of APDB database with Apache Cassandra backend.

    Parameters
    ----------
    config : `ApdbCassandraConfig`
        Configuration object.
    """

    def __init__(self, config: ApdbCassandraConfig):
        if not CASSANDRA_IMPORTED:
            raise CassandraMissingError()

        self._config = config
        self._keyspace = config.keyspace
        self._schema = ApdbSchema(config.schema_file, config.ss_schema_file)

        self._session_factory = SessionFactory(config)
        self._connection_context: ConnectionContext | None = None

    @property
    def _context(self) -> ConnectionContext:
        """Establish connection if not established and return context."""
        if self._connection_context is None:
            current_versions = DbVersions(
                schema_version=self.schema.schemaVersion(),
                code_version=self.apdbImplementationVersion(),
                replica_version=ApdbCassandraReplica.apdbReplicaImplementationVersion(),
            )
            _LOG.debug("Current versions: %s", current_versions)

            session = self._session_factory.session()
            self._connection_context = ConnectionContext(
                session, self._config, self.schema.tableSchemas, current_versions
            )

            if _LOG.isEnabledFor(logging.DEBUG):
                _LOG.debug("ApdbCassandra Configuration: %s", self._connection_context.config.model_dump())

        return self._connection_context

    def _timer(self, name: str, *, tags: Mapping[str, str | int] | None = None) -> Timer:
        """Create `Timer` instance given its name."""
        return Timer(name, _MON, tags=tags)

    @classmethod
    def apdbImplementationVersion(cls) -> VersionTuple:
        """Return version number for current APDB implementation.

        Returns
        -------
        version : `VersionTuple`
            Version of the code defined in implementation class.
        """
        return VERSION

    def getConfig(self) -> ApdbCassandraConfig:
        # docstring is inherited from a base class
        return self._context.config

    def tableDef(self, table: ApdbTables) -> Table | None:
        # docstring is inherited from a base class
        return self.schema.tableSchemas.get(table)

    @classmethod
    def init_database(
        cls,
        hosts: tuple[str, ...],
        keyspace: str,
        *,
        schema_file: str | None = None,
        ss_schema_file: str | None = None,
        read_sources_months: int | None = None,
        read_forced_sources_months: int | None = None,
        enable_replica: bool = False,
        replica_skips_diaobjects: bool = False,
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
        ra_dec_columns: tuple[str, str] | None = None,
        replication_factor: int | None = None,
        drop: bool = False,
        table_options: CreateTableOptions | None = None,
    ) -> ApdbCassandraConfig:
        """Initialize new APDB instance and make configuration object for it.

        Parameters
        ----------
        hosts : `tuple` [`str`, ...]
            List of host names or IP addresses for Cassandra cluster.
        keyspace : `str`
            Name of the keyspace for APDB tables.
        schema_file : `str`, optional
            Location of (YAML) configuration file with APDB schema. If not
            specified then default location will be used.
        ss_schema_file : `str`, optional
            Location of (YAML) configuration file with SSO schema. If not
            specified then default location will be used.
        read_sources_months : `int`, optional
            Number of months of history to read from DiaSource.
        read_forced_sources_months : `int`, optional
            Number of months of history to read from DiaForcedSource.
        enable_replica : `bool`, optional
            If True, make additional tables used for replication to PPDB.
        replica_skips_diaobjects : `bool`, optional
            If `True` then do not fill regular ``DiaObject`` table when
            ``enable_replica`` is `True`.
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
        ra_dec_columns : `tuple` [`str`, `str`], optional
            Names of ra/dec columns in DiaObject table.
        replication_factor : `int`, optional
            Replication factor used when creating new keyspace, if keyspace
            already exists its replication factor is not changed.
        drop : `bool`, optional
            If `True` then drop existing tables before re-creating the schema.
        table_options : `CreateTableOptions`, optional
            Options used when creating Cassandra tables.

        Returns
        -------
        config : `ApdbCassandraConfig`
            Resulting configuration object for a created APDB instance.
        """
        # Some non-standard defaults for connection parameters, these can be
        # changed later in generated config. Check Cassandra driver
        # documentation for what these parameters do. These parameters are not
        # used during database initialization, but they will be saved with
        # generated config.
        connection_config = ApdbCassandraConnectionConfig(
            extra_parameters={
                "idle_heartbeat_interval": 0,
                "idle_heartbeat_timeout": 30,
                "control_connection_timeout": 100,
            },
        )
        config = ApdbCassandraConfig(
            contact_points=hosts,
            keyspace=keyspace,
            enable_replica=enable_replica,
            replica_skips_diaobjects=replica_skips_diaobjects,
            connection_config=connection_config,
        )
        config.partitioning.time_partition_tables = time_partition_tables
        if schema_file is not None:
            config.schema_file = schema_file
        if ss_schema_file is not None:
            config.ss_schema_file = ss_schema_file
        if read_sources_months is not None:
            config.read_sources_months = read_sources_months
        if read_forced_sources_months is not None:
            config.read_forced_sources_months = read_forced_sources_months
        if port is not None:
            config.connection_config.port = port
        if username is not None:
            config.connection_config.username = username
        if prefix is not None:
            config.prefix = prefix
        if part_pixelization is not None:
            config.partitioning.part_pixelization = part_pixelization
        if part_pix_level is not None:
            config.partitioning.part_pix_level = part_pix_level
        if time_partition_start is not None:
            config.partitioning.time_partition_start = time_partition_start
        if time_partition_end is not None:
            config.partitioning.time_partition_end = time_partition_end
        if read_consistency is not None:
            config.connection_config.read_consistency = read_consistency
        if write_consistency is not None:
            config.connection_config.write_consistency = write_consistency
        if read_timeout is not None:
            config.connection_config.read_timeout = read_timeout
        if write_timeout is not None:
            config.connection_config.write_timeout = write_timeout
        if ra_dec_columns is not None:
            config.ra_dec_columns = ra_dec_columns

        cls._makeSchema(config, drop=drop, replication_factor=replication_factor, table_options=table_options)

        return config

    def get_replica(self) -> ApdbCassandraReplica:
        """Return `ApdbReplica` instance for this database."""
        # Note that this instance has to stay alive while replica exists, so
        # we pass reference to self.
        return ApdbCassandraReplica(self)

    @classmethod
    def _makeSchema(
        cls,
        config: ApdbConfig,
        *,
        drop: bool = False,
        replication_factor: int | None = None,
        table_options: CreateTableOptions | None = None,
    ) -> None:
        # docstring is inherited from a base class

        if not isinstance(config, ApdbCassandraConfig):
            raise TypeError(f"Unexpected type of configuration object: {type(config)}")

        simple_schema = ApdbSchema(config.schema_file, config.ss_schema_file)

        with SessionContext(config) as session:
            schema = ApdbCassandraSchema(
                session=session,
                keyspace=config.keyspace,
                table_schemas=simple_schema.tableSchemas,
                prefix=config.prefix,
                time_partition_tables=config.partitioning.time_partition_tables,
                enable_replica=config.enable_replica,
                replica_skips_diaobjects=config.replica_skips_diaobjects,
            )

            # Ask schema to create all tables.
            part_range_config: ApdbCassandraTimePartitionRange | None = None
            if config.partitioning.time_partition_tables:
                partitioner = Partitioner(config)
                time_partition_start = astropy.time.Time(
                    config.partitioning.time_partition_start, format="isot", scale="tai"
                )
                time_partition_end = astropy.time.Time(
                    config.partitioning.time_partition_end, format="isot", scale="tai"
                )
                part_range_config = ApdbCassandraTimePartitionRange(
                    start=partitioner.time_partition(time_partition_start),
                    end=partitioner.time_partition(time_partition_end),
                )
                schema.makeSchema(
                    drop=drop,
                    part_range=part_range_config,
                    replication_factor=replication_factor,
                    table_options=table_options,
                )
            else:
                schema.makeSchema(
                    drop=drop, replication_factor=replication_factor, table_options=table_options
                )

            meta_table_name = ApdbTables.metadata.table_name(config.prefix)
            metadata = ApdbMetadataCassandra(
                session, meta_table_name, config.keyspace, "read_tuples", "write"
            )

            # Fill version numbers, overrides if they existed before.
            metadata.set(
                ConnectionContext.metadataSchemaVersionKey, str(simple_schema.schemaVersion()), force=True
            )
            metadata.set(
                ConnectionContext.metadataCodeVersionKey, str(cls.apdbImplementationVersion()), force=True
            )

            if config.enable_replica:
                # Only store replica code version if replica is enabled.
                metadata.set(
                    ConnectionContext.metadataReplicaVersionKey,
                    str(ApdbCassandraReplica.apdbReplicaImplementationVersion()),
                    force=True,
                )

            # Store frozen part of a configuration in metadata.
            freezer = ApdbConfigFreezer[ApdbCassandraConfig](ConnectionContext.frozen_parameters)
            metadata.set(ConnectionContext.metadataConfigKey, freezer.to_json(config), force=True)

            # Store time partition range.
            if part_range_config:
                part_range_config.save_to_meta(metadata)

    def getDiaObjects(self, region: sphgeom.Region) -> pandas.DataFrame:
        # docstring is inherited from a base class
        context = self._context
        config = context.config

        sp_where, num_sp_part = context.partitioner.spatial_where(region, for_prepare=True)
        _LOG.debug("getDiaObjects: #partitions: %s", len(sp_where))

        # We need to exclude extra partitioning columns from result.
        column_names = context.schema.apdbColumnNames(ApdbTables.DiaObjectLast)
        what = ",".join(quote_id(column) for column in column_names)

        table_name = context.schema.tableName(ApdbTables.DiaObjectLast)
        query = f'SELECT {what} from "{self._keyspace}"."{table_name}"'
        statements: list[tuple] = []
        for where, params in sp_where:
            full_query = f"{query} WHERE {where}"
            if params:
                statement = context.preparer.prepare(full_query)
            else:
                # If there are no params then it is likely that query has a
                # bunch of literals rendered already, no point trying to
                # prepare it because it's not reusable.
                statement = cassandra.query.SimpleStatement(full_query)
            statements.append((statement, params))
        _LOG.debug("getDiaObjects: #queries: %s", len(statements))

        with _MON.context_tags({"table": "DiaObject"}):
            _MON.add_record(
                "select_query_stats", values={"num_sp_part": num_sp_part, "num_queries": len(statements)}
            )
            with self._timer("select_time") as timer:
                objects = cast(
                    pandas.DataFrame,
                    select_concurrent(
                        context.session,
                        statements,
                        "read_pandas_multi",
                        config.connection_config.read_concurrency,
                    ),
                )
                timer.add_values(row_count=len(objects))

        _LOG.debug("found %s DiaObjects", objects.shape[0])
        return objects

    def getDiaSources(
        self,
        region: sphgeom.Region,
        object_ids: Iterable[int] | None,
        visit_time: astropy.time.Time,
        start_time: astropy.time.Time | None = None,
    ) -> pandas.DataFrame | None:
        # docstring is inherited from a base class
        context = self._context
        config = context.config

        months = config.read_sources_months
        if start_time is None and months == 0:
            return None

        mjd_end = float(visit_time.tai.mjd)
        if start_time is None:
            mjd_start = mjd_end - months * 30
        else:
            mjd_start = float(start_time.tai.mjd)

        return self._getSources(region, object_ids, mjd_start, mjd_end, ApdbTables.DiaSource)

    def getDiaForcedSources(
        self,
        region: sphgeom.Region,
        object_ids: Iterable[int] | None,
        visit_time: astropy.time.Time,
        start_time: astropy.time.Time | None = None,
    ) -> pandas.DataFrame | None:
        # docstring is inherited from a base class
        context = self._context
        config = context.config

        months = config.read_forced_sources_months
        if start_time is None and months == 0:
            return None

        mjd_end = float(visit_time.tai.mjd)
        if start_time is None:
            mjd_start = mjd_end - months * 30
        else:
            mjd_start = float(start_time.tai.mjd)

        return self._getSources(region, object_ids, mjd_start, mjd_end, ApdbTables.DiaForcedSource)

    def getDiaObjectsForDedup(self, since: astropy.time.Time) -> pandas.DataFrame:
        # docstring is inherited from a base class
        context = self._context
        config = context.config

        if not context.has_dedup_table:
            raise TypeError("DiaObjectDedup table does not exist in this APDB instance.")

        column_names = context.schema.apdbColumnNames(ExtraTables.DiaObjectDedup)
        what = ",".join(quote_id(column) for column in column_names)

        if self._schema.has_mjd_timestamps:
            validity_start_column = "validityStartMjdTai"
            timestamp = float(since.tai.mjd)
        else:
            validity_start_column = "validityStart"
            timestamp = since.datetime

        table_name = context.schema.tableName(ExtraTables.DiaObjectDedup)
        query = (
            f'SELECT {what} FROM "{self._keyspace}"."{table_name}" '
            f'WHERE dedup_part = ? AND "{validity_start_column}" >= ? '
            "ALLOW FILTERING"
        )
        statement = context.preparer.prepare(query)

        num_part = config.partitioning.num_part_dedup
        statements = []
        for dedup_part in range(num_part):
            statements.append((statement, (dedup_part, timestamp)))

        with self._timer("select_time", tags={"table": "DiaObjectDedup"}) as timer:
            objects = cast(
                pandas.DataFrame,
                select_concurrent(
                    context.session,
                    statements,
                    "read_pandas_multi_dedup",
                    config.connection_config.read_concurrency,
                ),
            )
            timer.add_values(row_count=len(objects))

        _LOG.debug("found %s DiaObjectDedup records", objects.shape[0])
        return objects

    def getDiaSourcesForDiaObjects(
        self, objects: list[DiaObjectId], start_time: astropy.time.Time, max_dist_arcsec: float = 1.0
    ) -> pandas.DataFrame:
        # docstring is inherited from a base class
        context = self._context
        config = context.config

        # Which tables to query and temporal constraints.
        end_time = astropy.time.Time.now()
        tables, temporal_where = context.partitioner.temporal_where(
            ApdbTables.DiaSource,
            start_time,
            end_time,
            for_prepare=False,
            partitons_range=context.time_partitions_range,
            query_per_time_part=False,
        )
        if not tables:
            warnings.warn(
                f"Query time range ({start_time.isot} - {end_time.isot}) does not overlap database "
                "time partitions."
            )

        # Group DiaObjects by partition.
        partitioner = context.partitioner
        partitioned_object_ids: dict[int, list[int]] = defaultdict(list)
        for obj_id in objects:
            # Make a small circle with center at DiaSource position.
            lon_lat = sphgeom.LonLat.fromDegrees(obj_id.ra, obj_id.dec)
            center = sphgeom.UnitVector3d(lon_lat)
            region = sphgeom.Circle(center, sphgeom.Angle.fromDegrees(max_dist_arcsec / 3600.0))

            pixel_ranges = partitioner.pixelization.envelope(region)
            for lower, upper in pixel_ranges:
                for pixel in range(lower, upper + 1):
                    partitioned_object_ids[pixel].append(obj_id.diaObjectId)

        # Columns to return.
        column_names = context.schema.apdbColumnNames(ApdbTables.DiaSource)
        what = ",".join(quote_id(column) for column in column_names)

        # Make a bunch of queries.
        statements = []
        suffix = "ALLOW FILTERING"
        for apdb_part, diaObjectIds in partitioned_object_ids.items():
            spatial_where = [(f"apdb_part = {apdb_part}", ())]
            for table in tables:
                prefix = f'SELECT {what} FROM "{self._keyspace}"."{table}"'
                for id_chunk in chunk_iterable(diaObjectIds, 10_000):
                    chunk_str = ",".join(str(diaObjectId) for diaObjectId in id_chunk)
                    id_where = (f'"diaObjectId" IN ({chunk_str})', ())
                    statements += list(
                        self._combine_where(prefix, spatial_where, temporal_where, id_where, suffix=suffix)
                    )

        _LOG.debug("getDiaSourcesForDiaObjects #queries: %s", len(statements))

        with self._timer("select_time", tags={"table": "DiaSource"}) as timer:
            catalog = cast(
                pandas.DataFrame,
                select_concurrent(
                    context.session,
                    statements,
                    "read_pandas_multi",
                    config.connection_config.read_concurrency,
                ),
            )
            timer.add_values(row_count_from_db=len(catalog))

            # precise filtering on midpointMjdTai
            catalog = cast(pandas.DataFrame, catalog[catalog["midpointMjdTai"] >= start_time.tai.mjd])

            timer.add_values(row_count=len(catalog))

        _LOG.debug("found %d DiaSources", len(catalog))
        return catalog

    def containsVisitDetector(
        self,
        visit: int,
        detector: int,
        region: sphgeom.Region,
        visit_time: astropy.time.Time,
    ) -> bool:
        # docstring is inherited from a base class
        context = self._context
        config = context.config

        # If ApdbDetectorVisit table exists just check it.
        if context.has_visit_detector_table:
            table_name = context.schema.tableName(ExtraTables.ApdbVisitDetector)
            query = (
                f'SELECT count(*) FROM "{self._keyspace}"."{table_name}" WHERE visit = %s AND detector = %s'
            )
            with self._timer("contains_visit_detector_time"):
                result = context.session.execute(query, (visit, detector))
                return bool(result.one()[0])

        # The order of checks corresponds to order in store(), on potential
        # store failure earlier tables have higher probability containing
        # stored records. With per-partition tables there will be many tables
        # in the list, but it is unlikely that we'll use that setup in
        # production.
        sp_where, _ = context.partitioner.spatial_where(region, use_ranges=True, for_prepare=True)
        visit_detector_where = ("visit = ? AND detector = ?", (visit, detector))

        # Sources are partitioned on their midPointMjdTai. To avoid precision
        # issues add some fuzzines to visit time.
        mjd_start = float(visit_time.tai.mjd) - 1.0 / 24
        mjd_end = float(visit_time.tai.mjd) + 1.0 / 24

        statements: list[tuple] = []
        for table_type in ApdbTables.DiaSource, ApdbTables.DiaForcedSource:
            tables, temporal_where = context.partitioner.temporal_where(
                table_type, mjd_start, mjd_end, query_per_time_part=True, for_prepare=True
            )
            for table in tables:
                prefix = f'SELECT apdb_part FROM "{self._keyspace}"."{table}"'
                # Needs ALLOW FILTERING as there is no PK constraint.
                suffix = "PER PARTITION LIMIT 1 LIMIT 1 ALLOW FILTERING"
                statements += list(
                    self._combine_where(prefix, sp_where, temporal_where, visit_detector_where, suffix)
                )

        with self._timer("contains_visit_detector_time"):
            result = cast(
                list[tuple[int] | None],
                select_concurrent(
                    context.session,
                    statements,
                    "read_tuples",
                    config.connection_config.read_concurrency,
                ),
            )
        return bool(result)

    def store(
        self,
        visit_time: astropy.time.Time,
        objects: pandas.DataFrame,
        sources: pandas.DataFrame | None = None,
        forced_sources: pandas.DataFrame | None = None,
    ) -> None:
        # docstring is inherited from a base class
        context = self._context
        config = context.config

        if context.has_visit_detector_table:
            # Store visit/detector in a special table, this has to be done
            # before all other writes so if there is a failure at any point
            # later we still have a record for attempted write.
            visit_detector: set[tuple[int, int]] = set()
            for df in sources, forced_sources:
                if df is not None and not df.empty:
                    df = df[["visit", "detector"]]
                    for visit, detector in df.itertuples(index=False):
                        visit_detector.add((visit, detector))

            if visit_detector:
                # Typically there is only one entry, do not bother with
                # concurrency.
                table_name = context.schema.tableName(ExtraTables.ApdbVisitDetector)
                query = f'INSERT INTO "{self._keyspace}"."{table_name}" (visit, detector) VALUES (%s, %s)'
                for item in visit_detector:
                    context.session.execute(query, item, execution_profile="write")

        objects = self._fix_input_timestamps(objects)
        if sources is not None:
            sources = self._fix_input_timestamps(sources)
        if forced_sources is not None:
            forced_sources = self._fix_input_timestamps(forced_sources)

        replica_chunk: ReplicaChunk | None = None
        if context.schema.replication_enabled:
            replica_chunk = ReplicaChunk.make_replica_chunk(visit_time, config.replica_chunk_seconds)
            self._storeReplicaChunk(replica_chunk)

        # fill region partition column for DiaObjects
        objects = self._add_apdb_part(objects)
        self._storeDiaObjects(objects, visit_time, replica_chunk)

        if sources is not None and len(sources) > 0:
            # copy apdb_part column from DiaObjects to DiaSources
            sources = self._add_apdb_part(sources)
            subchunk = self._storeDiaSources(ApdbTables.DiaSource, sources, replica_chunk)
            self._storeDiaSourcesPartitions(sources, visit_time, replica_chunk, subchunk)

        if forced_sources is not None and len(forced_sources) > 0:
            forced_sources = self._add_apdb_part(forced_sources)
            self._storeDiaSources(ApdbTables.DiaForcedSource, forced_sources, replica_chunk)

    def reassignDiaSources(self, idMap: Mapping[int, int]) -> None:
        # docstring is inherited from a base class
        context = self._context
        config = context.config

        if self.schema.has_mjd_timestamps:
            reassign_time_column = "ssObjectReassocTimeMjdTai"
            reassignTime = float(astropy.time.Time.now().tai.mjd)
        else:
            reassign_time_column = "ssObjectReassocTime"
            # Current time as milliseconds since epoch.
            reassignTime = int(datetime.datetime.now(tz=datetime.UTC).timestamp() * 1000)

        # To update a record we need to know its exact primary key (including
        # partition key) so we start by querying for diaSourceId to find the
        # primary keys.

        table_name = context.schema.tableName(ExtraTables.DiaSourceToPartition)
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
            select_concurrent(
                context.session, selects, "read_tuples", config.connection_config.read_concurrency
            ),
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
        queries: list[tuple[cassandra.query.PreparedStatement, tuple]] = []
        for diaSourceId, ssObjectId in idMap.items():
            apdb_part, apdb_time_part = id2partitions[diaSourceId]
            values: tuple
            if config.partitioning.time_partition_tables:
                table_name = context.schema.tableName(ApdbTables.DiaSource, apdb_time_part)
                query = (
                    f'UPDATE "{self._keyspace}"."{table_name}"'
                    f' SET "ssObjectId" = ?, "diaObjectId" = NULL, "{reassign_time_column}" = ?'
                    ' WHERE "apdb_part" = ? AND "diaSourceId" = ?'
                )
                values = (ssObjectId, reassignTime, apdb_part, diaSourceId)
            else:
                table_name = context.schema.tableName(ApdbTables.DiaSource)
                query = (
                    f'UPDATE "{self._keyspace}"."{table_name}"'
                    f' SET "ssObjectId" = ?, "diaObjectId" = NULL, "{reassign_time_column}" = ?'
                    ' WHERE "apdb_part" = ? AND "apdb_time_part" = ? AND "diaSourceId" = ?'
                )
                values = (ssObjectId, reassignTime, apdb_part, apdb_time_part, diaSourceId)
            queries.append((context.preparer.prepare(query), values))

        # TODO: (DM-50190) Replication for updated records is not implemented.
        if id2chunk_id:
            warnings.warn("Replication of reassigned DiaSource records is not implemented.", stacklevel=2)

        _LOG.debug("%s: will update %d records", table_name, len(idMap))
        with self._timer("source_reassign_time") as timer:
            execute_concurrent(context.session, queries, execution_profile="write")
            timer.add_values(source_count=len(idMap))

    def countUnassociatedObjects(self) -> int:
        # docstring is inherited from a base class

        # It's too inefficient to implement it for Cassandra in current schema.
        raise NotImplementedError()

    @property
    def schema(self) -> ApdbSchema:
        # docstring is inherited from a base class
        return self._schema

    @property
    def metadata(self) -> ApdbMetadata:
        # docstring is inherited from a base class
        context = self._context
        return context.metadata

    @property
    def admin(self) -> ApdbCassandraAdmin:
        # docstring is inherited from a base class
        return ApdbCassandraAdmin(self)

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
        context = self._context
        config = context.config

        object_id_set: Set[int] = set()
        if object_ids is not None:
            object_id_set = set(object_ids)
            if len(object_id_set) == 0:
                return self._make_empty_catalog(table_name)

        sp_where, num_sp_part = context.partitioner.spatial_where(region, for_prepare=True)
        tables, temporal_where = context.partitioner.temporal_where(
            table_name, mjd_start, mjd_end, for_prepare=True, partitons_range=context.time_partitions_range
        )
        if not tables:
            start = astropy.time.Time(mjd_start, format="mjd", scale="tai")
            end = astropy.time.Time(mjd_end, format="mjd", scale="tai")
            warnings.warn(
                f"Query time range ({start.isot} - {end.isot}) does not overlap database time partitions."
            )

        # We need to exclude extra partitioning columns from result.
        column_names = context.schema.apdbColumnNames(table_name)
        what = ",".join(quote_id(column) for column in column_names)

        # Build all queries
        statements: list[tuple] = []
        for table in tables:
            prefix = f'SELECT {what} from "{self._keyspace}"."{table}"'
            statements += list(self._combine_where(prefix, sp_where, temporal_where))
        _LOG.debug("_getSources %s: #queries: %s", table_name, len(statements))

        with _MON.context_tags({"table": table_name.name}):
            _MON.add_record(
                "select_query_stats", values={"num_sp_part": num_sp_part, "num_queries": len(statements)}
            )
            with self._timer("select_time") as timer:
                catalog = cast(
                    pandas.DataFrame,
                    select_concurrent(
                        context.session,
                        statements,
                        "read_pandas_multi",
                        config.connection_config.read_concurrency,
                    ),
                )
                timer.add_values(row_count_from_db=len(catalog))

                # filter by given object IDs
                if len(object_id_set) > 0:
                    catalog = cast(pandas.DataFrame, catalog[catalog["diaObjectId"].isin(object_id_set)])

                # precise filtering on midpointMjdTai
                catalog = cast(pandas.DataFrame, catalog[catalog["midpointMjdTai"] > mjd_start])

                timer.add_values(row_count=len(catalog))

        _LOG.debug("found %d %ss", catalog.shape[0], table_name.name)
        return catalog

    def _storeReplicaChunk(self, replica_chunk: ReplicaChunk) -> None:
        context = self._context
        config = context.config

        # Cassandra timestamp uses milliseconds since epoch
        timestamp = int(replica_chunk.last_update_time.unix_tai * 1000)

        # everything goes into a single partition
        partition = 0

        table_name = context.schema.tableName(ExtraTables.ApdbReplicaChunks)

        columns = ["partition", "apdb_replica_chunk", "last_update_time", "unique_id"]
        values = [partition, replica_chunk.id, timestamp, replica_chunk.unique_id]
        if context.has_chunk_sub_partitions:
            columns.append("has_subchunks")
            values.append(True)

        column_list = ", ".join(columns)
        placeholders = ",".join(["%s"] * len(columns))
        query = f'INSERT INTO "{self._keyspace}"."{table_name}" ({column_list}) VALUES ({placeholders})'

        context.session.execute(
            query,
            values,
            timeout=config.connection_config.write_timeout,
            execution_profile="write",
        )

    def _queryDiaObjectLastPartitions(self, ids: Iterable[int]) -> Mapping[int, int]:
        """Return existing mapping of diaObjectId to its last partition."""
        context = self._context
        config = context.config

        table_name = context.schema.tableName(ExtraTables.DiaObjectLastToPartition)
        queries = []
        object_count = 0
        for id_chunk in chunk_iterable(ids, 10_000):
            id_chunk_list = list(id_chunk)
            query = (
                f'SELECT "diaObjectId", apdb_part FROM "{self._keyspace}"."{table_name}" '
                f'WHERE "diaObjectId" in ({",".join(str(oid) for oid in id_chunk_list)})'
            )
            queries.append((query, ()))
            object_count += len(id_chunk_list)

        with self._timer("query_object_last_partitions") as timer:
            data = cast(
                ApdbTableData,
                select_concurrent(
                    context.session,
                    queries,
                    "read_raw_multi",
                    config.connection_config.read_concurrency,
                ),
            )
            timer.add_values(object_count=object_count, row_count=len(data.rows()))

        if data.column_names() != ["diaObjectId", "apdb_part"]:
            raise RuntimeError(f"Unexpected column names in query result: {data.column_names()}")

        return {row[0]: row[1] for row in data.rows()}

    def _deleteMovingObjects(self, objs: pandas.DataFrame) -> None:
        """Objects in DiaObjectsLast can move from one spatial partition to
        another. For those objects inserting new version does not replace old
        one, so we need to explicitly remove old versions before inserting new
        ones.
        """
        context = self._context

        # Extract all object IDs.
        new_partitions = dict(zip(objs["diaObjectId"], objs["apdb_part"]))
        old_partitions = self._queryDiaObjectLastPartitions(objs["diaObjectId"])

        moved_oids: dict[int, tuple[int, int]] = {}
        for oid, old_part in old_partitions.items():
            new_part = new_partitions.get(oid, old_part)
            if new_part != old_part:
                moved_oids[oid] = (old_part, new_part)
        _LOG.debug("DiaObject IDs that moved to new partition: %s", moved_oids)

        if moved_oids:
            # Delete old records from DiaObjectLast.
            table_name = context.schema.tableName(ApdbTables.DiaObjectLast)
            query = f'DELETE FROM "{self._keyspace}"."{table_name}" WHERE apdb_part = ? AND "diaObjectId" = ?'
            statement = context.preparer.prepare(query)
            queries = []
            for oid, (old_part, _) in moved_oids.items():
                queries.append((statement, (old_part, oid)))
            with self._timer("delete_object_last") as timer:
                execute_concurrent(context.session, queries, execution_profile="write")
                timer.add_values(row_count=len(moved_oids))

        # Add all new records to the map.
        table_name = context.schema.tableName(ExtraTables.DiaObjectLastToPartition)
        query = f'INSERT INTO "{self._keyspace}"."{table_name}" ("diaObjectId", apdb_part) VALUES (?,?)'
        statement = context.preparer.prepare(query)

        queries = []
        for oid, new_part in new_partitions.items():
            queries.append((statement, (oid, new_part)))

        with self._timer("update_object_last_partition") as timer:
            execute_concurrent(context.session, queries, execution_profile="write")
            timer.add_values(row_count=len(queries))

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

        context = self._context
        config = context.config

        if context.has_dia_object_last_to_partition:
            self._deleteMovingObjects(objs)

        timestamp: float | datetime.datetime
        if self.schema.has_mjd_timestamps:
            validity_start_column = "validityStartMjdTai"
            timestamp = float(visit_time.tai.mjd)
        else:
            validity_start_column = "validityStart"
            timestamp = visit_time.datetime

        # DiaObjectLast did not have this column in the past.
        extra_columns: dict[str, Any] = {}
        if context.schema.check_column(ApdbTables.DiaObjectLast, validity_start_column):
            extra_columns[validity_start_column] = timestamp

        self._storeObjectsPandas(objs, ApdbTables.DiaObjectLast, extra_columns=extra_columns)

        extra_columns[validity_start_column] = timestamp
        visit_time_part = context.partitioner.time_partition(visit_time)
        time_part: int | None = visit_time_part
        if (time_partitions_range := context.time_partitions_range) is not None:
            self._check_time_partitions([visit_time_part], time_partitions_range)
        if not config.partitioning.time_partition_tables:
            extra_columns["apdb_time_part"] = time_part
            time_part = None

        # Only store DiaObects if not doing replication or explicitly
        # configured to always store them.
        if replica_chunk is None or not config.replica_skips_diaobjects:
            self._storeObjectsPandas(
                objs, ApdbTables.DiaObject, extra_columns=extra_columns, time_part=time_part
            )

        if replica_chunk is not None:
            extra_columns = {"apdb_replica_chunk": replica_chunk.id, validity_start_column: timestamp}
            table = ExtraTables.DiaObjectChunks
            if context.has_chunk_sub_partitions:
                table = ExtraTables.DiaObjectChunks2
                # Use a random number for a second part of partitioning key so
                # that different clients could wrtite to different partitions.
                # This makes it not exactly reproducible.
                extra_columns["apdb_replica_subchunk"] = random.randrange(config.replica_sub_chunk_count)
            self._storeObjectsPandas(objs, table, extra_columns=extra_columns)

        # Store copy of the records in dedup table.
        if context.has_dedup_table:
            table = ExtraTables.DiaObjectDedup
            extra_columns = {
                "dedup_part": random.randrange(config.partitioning.num_part_dedup),
                validity_start_column: timestamp,
            }
            self._storeObjectsPandas(objs, table, extra_columns=extra_columns)

    def _storeDiaSources(
        self,
        table_name: ApdbTables,
        sources: pandas.DataFrame,
        replica_chunk: ReplicaChunk | None,
    ) -> int | None:
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

        Returns
        -------
        subchunk : `int` or `None`
            Subchunk number for resulting replica data, `None` if relication is
            not enabled ot subchunking is not enabled.
        """
        context = self._context
        config = context.config

        # Time partitioning has to be based on midpointMjdTai, not visit_time
        # as visit_time is not really a visit time.
        tp_sources = sources.copy(deep=False)
        tp_sources["apdb_time_part"] = tp_sources["midpointMjdTai"].apply(context.partitioner.time_partition)
        if (time_partitions_range := context.time_partitions_range) is not None:
            self._check_time_partitions(tp_sources["apdb_time_part"], time_partitions_range)
        extra_columns: dict[str, Any] = {}
        if not config.partitioning.time_partition_tables:
            self._storeObjectsPandas(tp_sources, table_name)
        else:
            # Group by time partition
            partitions = set(tp_sources["apdb_time_part"])
            if len(partitions) == 1:
                # Single partition - just save the whole thing.
                time_part = partitions.pop()
                self._storeObjectsPandas(sources, table_name, time_part=time_part)
            else:
                # group by time partition.
                for time_part, sub_frame in tp_sources.groupby(by="apdb_time_part"):
                    sub_frame.drop(columns="apdb_time_part", inplace=True)
                    self._storeObjectsPandas(sub_frame, table_name, time_part=time_part)

        subchunk: int | None = None
        if replica_chunk is not None:
            extra_columns = {"apdb_replica_chunk": replica_chunk.id}
            if context.has_chunk_sub_partitions:
                subchunk = random.randrange(config.replica_sub_chunk_count)
                extra_columns["apdb_replica_subchunk"] = subchunk
                if table_name is ApdbTables.DiaSource:
                    extra_table = ExtraTables.DiaSourceChunks2
                else:
                    extra_table = ExtraTables.DiaForcedSourceChunks2
            else:
                if table_name is ApdbTables.DiaSource:
                    extra_table = ExtraTables.DiaSourceChunks
                else:
                    extra_table = ExtraTables.DiaForcedSourceChunks
            self._storeObjectsPandas(sources, extra_table, extra_columns=extra_columns)

        return subchunk

    def _check_time_partitions(
        self, partitions: Iterable[int], time_partitions_range: ApdbCassandraTimePartitionRange
    ) -> None:
        """Check that time partitons for new data actually exist.

        Parameters
        ----------
        partitions : `~collections.abc.Iterable` [`int`]
            Time partitions for new data.
        time_partitions_range : `ApdbCassandraTimePartitionRange`
            Currrent time partition range.
        """
        partitions = set(partitions)
        min_part = min(partitions)
        max_part = max(partitions)
        if min_part < time_partitions_range.start or max_part > time_partitions_range.end:
            raise ValueError(
                "Attempt to store data for time partitions that do not yet exist. "
                f"Partitons for new records: {min_part}-{max_part}. "
                f"Database partitons: {time_partitions_range.start}-{time_partitions_range.end}."
            )
        # Make a noise when writing to the last partition.
        if max_part == time_partitions_range.end:
            warnings.warn(
                "Writing into the last temporal partition. Partition range needs to be extended soon.",
                stacklevel=3,
            )

    def _storeDiaSourcesPartitions(
        self,
        sources: pandas.DataFrame,
        visit_time: astropy.time.Time,
        replica_chunk: ReplicaChunk | None,
        subchunk: int | None,
    ) -> None:
        """Store mapping of diaSourceId to its partitioning values.

        Parameters
        ----------
        sources : `pandas.DataFrame`
            Catalog containing DiaSource records
        visit_time : `astropy.time.Time`
            Time of the current visit.
        replica_chunk : `ReplicaChunk` or `None`
            Replication chunk, or `None` when replication is disabled.
        subchunk : `int` or `None`
            Replication sub-chunk, or `None` when replication is disabled or
            sub-chunking is not used.
        """
        context = self._context

        id_map = cast(pandas.DataFrame, sources[["diaSourceId", "apdb_part"]])
        extra_columns = {
            "apdb_time_part": context.partitioner.time_partition(visit_time),
            "apdb_replica_chunk": replica_chunk.id if replica_chunk is not None else None,
        }
        if context.has_chunk_sub_partitions:
            extra_columns["apdb_replica_subchunk"] = subchunk

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
        context = self._context

        # use extra columns if specified
        if extra_columns is None:
            extra_columns = {}
        extra_fields = list(extra_columns.keys())

        # Fields that will come from dataframe.
        df_fields = [column for column in records.columns if column not in extra_fields]

        column_map = context.schema.getColumnMap(table_name)
        # list of columns (as in felis schema)
        fields = [column_map[field].name for field in df_fields if field in column_map]
        fields += extra_fields

        # check that all partitioning and clustering columns are defined
        partition_columns = context.schema.partitionColumns(table_name)
        required_columns = partition_columns + context.schema.clusteringColumns(table_name)
        missing_columns = [column for column in required_columns if column not in fields]
        if missing_columns:
            raise ValueError(f"Primary key columns are missing from catalog: {missing_columns}")

        qfields = [quote_id(field) for field in fields]
        qfields_str = ",".join(qfields)

        batch_size = self._batch_size(table_name)

        with self._timer("insert_build_time", tags={"table": table_name.name}):
            # Multi-partition batches are problematic in general, so we want to
            # group records in a batch by their partition key.
            values_by_key: dict[tuple, list[list]] = defaultdict(list)
            for rec in records.itertuples(index=False):
                values = []
                partitioning_values: dict[str, Any] = {}
                for field in df_fields:
                    if field not in column_map:
                        continue
                    value = getattr(rec, field)
                    if column_map[field].datatype is felis.datamodel.DataType.timestamp:
                        if isinstance(value, pandas.Timestamp):
                            value = value.to_pydatetime()
                        elif value is pandas.NaT:
                            value = None
                        else:
                            # Assume it's seconds since epoch, Cassandra
                            # datetime is in milliseconds
                            value = int(value * 1000)
                    value = literal(value)
                    values.append(UNSET_VALUE if value is None else value)
                    if field in partition_columns:
                        partitioning_values[field] = value
                for field in extra_fields:
                    value = literal(extra_columns[field])
                    values.append(UNSET_VALUE if value is None else value)
                    if field in partition_columns:
                        partitioning_values[field] = value

                key = tuple(partitioning_values[field] for field in partition_columns)
                values_by_key[key].append(values)

            table = context.schema.tableName(table_name, time_part)

            holders = ",".join(["?"] * len(qfields))
            query = f'INSERT INTO "{self._keyspace}"."{table}" ({qfields_str}) VALUES ({holders})'
            statement = context.preparer.prepare(query)
            # Cassandra has 64k limit on batch size, normally that should be
            # enough but some tests generate too many forced sources.
            queries = []
            for key_values in values_by_key.values():
                for values_chunk in chunk_iterable(key_values, batch_size):
                    batch = cassandra.query.BatchStatement()
                    for row_values in values_chunk:
                        batch.add(statement, row_values)
                    queries.append((batch, None))
                    assert batch.routing_key is not None and batch.keyspace is not None

        _LOG.debug("%s: will store %d records", context.schema.tableName(table_name), records.shape[0])
        with self._timer("insert_time", tags={"table": table_name.name}) as timer:
            execute_concurrent(context.session, queries, execution_profile="write")
            timer.add_values(row_count=len(records), num_batches=len(queries))

    def _storeUpdateRecords(
        self, records: Iterable[ApdbUpdateRecord], chunk: ReplicaChunk, *, store_chunk: bool = False
    ) -> None:
        """Store ApdbUpdateRecords in the replica table for those records.

        Parameters
        ----------
        records : `list` [`ApdbUpdateRecord`]
            Records to store.
        chunk : `ReplicaChunk`
            Replica chunk for these records.
        store_chunk : `bool`
            If True then also store replica chunk.

        Raises
        ------
        TypeError
            Raised if replication is not enabled for this instance.
        """
        context = self._context
        config = context.config

        if not context.schema.replication_enabled:
            raise TypeError("Replication is not enabled for this APDB instance.")

        if store_chunk:
            self._storeReplicaChunk(chunk)

        apdb_replica_chunk = chunk.id
        # Do not use unique_if from ReplicaChunk as it could be reused in
        # multiple calls to this method.
        update_unique_id = uuid.uuid4()

        rows = []
        for record in records:
            rows.append(
                [
                    apdb_replica_chunk,
                    record.update_time_ns,
                    record.update_order,
                    update_unique_id,
                    record.to_json(),
                ]
            )
        columns = [
            "apdb_replica_chunk",
            "update_time_ns",
            "update_order",
            "update_unique_id",
            "update_payload",
        ]
        if context.has_chunk_sub_partitions:
            subchunk = random.randrange(config.replica_sub_chunk_count)
            for row in rows:
                row.append(subchunk)
            columns.append("apdb_replica_subchunk")

        table_name = context.schema.tableName(ExtraTables.ApdbUpdateRecordChunks)
        placeholders = ", ".join(["%s"] * len(columns))
        columns_str = ", ".join(columns)
        query = f'INSERT INTO "{self._keyspace}"."{table_name}" ({columns_str}) VALUES ({placeholders})'
        queries = [(query, row) for row in rows]

        with self._timer("store_update_record") as timer:
            execute_concurrent(context.session, queries, execution_profile="write")
            timer.add_values(row_count=len(queries))

    def _add_apdb_part(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """Calculate spatial partition for each record and add it to a
        DataFrame.

        Parameters
        ----------
        df : `pandas.DataFrame`
            DataFrame which has to contain ra/dec columns, names of these
            columns are defined by configuration ``ra_dec_columns`` field.

        Returns
        -------
        df : `pandas.DataFrame`
            DataFrame with ``apdb_part`` column which contains pixel index
            for ra/dec coordinates.

        Notes
        -----
        This overrides any existing column in a DataFrame with the same name
        (``apdb_part``). Original DataFrame is not changed, copy of a DataFrame
        is returned.
        """
        context = self._context
        config = context.config

        # Calculate pixelization index for every record.
        apdb_part = np.zeros(df.shape[0], dtype=np.int64)
        ra_col, dec_col = config.ra_dec_columns
        for i, (ra, dec) in enumerate(zip(df[ra_col], df[dec_col])):
            uv3d = sphgeom.UnitVector3d(sphgeom.LonLat.fromDegrees(ra, dec))
            idx = context.partitioner.pixel(uv3d)
            apdb_part[i] = idx
        df = df.copy()
        df["apdb_part"] = apdb_part
        return df

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
        table = self.schema.tableSchemas[table_name]

        data = {
            columnDef.name: pandas.Series(dtype=self.schema.column_dtype(columnDef.datatype))
            for columnDef in table.columns
        }
        return pandas.DataFrame(data)

    def _combine_where(
        self,
        prefix: str,
        where1: list[tuple[str, tuple]],
        where2: list[tuple[str, tuple]],
        where3: tuple[str, tuple] | None = None,
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
        context = self._context

        # If lists are empty use special sentinels.
        if not where1:
            where1 = [("", ())]
        if not where2:
            where2 = [("", ())]

        for expr1, params1 in where1:
            for expr2, params2 in where2:
                full_query = prefix
                wheres = []
                params = params1 + params2
                if expr1:
                    wheres.append(expr1)
                if expr2:
                    wheres.append(expr2)
                if where3:
                    wheres.append(where3[0])
                    params += where3[1]
                if wheres:
                    full_query += " WHERE " + " AND ".join(wheres)
                if suffix:
                    full_query += " " + suffix
                if params:
                    statement = context.preparer.prepare(full_query)
                else:
                    # If there are no params then it is likely that query
                    # has a bunch of literals rendered already, no point
                    # trying to prepare it.
                    statement = cassandra.query.SimpleStatement(full_query)
                yield (statement, params)

    def _fix_input_timestamps(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """Update timestamp columns in input DataFrame to be naive datetime
        type.

        Clients may or may not generate aware timestamps, code in this class
        assumes that timestamps are naive, so we convert them to UTC and
        drop timezone.
        """
        # Find all columns with aware timestamps.
        columns = [column for column, dtype in df.dtypes.items() if isinstance(dtype, pandas.DatetimeTZDtype)]
        for column in columns:
            # tz_convert(None) will convert to UTC and drop timezone.
            df[column] = df[column].dt.tz_convert(None)
        return df

    def _batch_size(self, table: ApdbTables | ExtraTables) -> int:
        """Calculate batch size based on config parameters."""
        context = self._context
        config = context.config

        # Cassandra limit on number of statements in a batch is 64k.
        batch_size = 65_535
        if 0 < config.batch_statement_limit < batch_size:
            batch_size = config.batch_statement_limit
        if config.batch_size_limit > 0:
            # The purpose of this limit is to try not to exceed batch size
            # threshold which is set on server side. Cassandra wire protocol
            # for prepared queries (and batches) only sends column values with
            # with an additional 4 bytes per value specifying size. Value is
            # not included for NULL or NOT_SET values, but the size is always
            # there. There is additional small per-query overhead, which we
            # ignore.
            row_size = context.schema.table_row_size(table)
            row_size += 4 * len(context.schema.getColumnMap(table))
            batch_size = min(batch_size, (config.batch_size_limit // row_size) + 1)
        return batch_size
