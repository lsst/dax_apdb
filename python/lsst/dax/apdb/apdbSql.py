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

"""Module defining Apdb class and related methods.
"""

from __future__ import annotations

__all__ = ["ApdbSqlConfig", "ApdbSql"]

import logging
from collections.abc import Iterable, Mapping, MutableMapping
from contextlib import closing, suppress
from typing import TYPE_CHECKING, Any, cast

import astropy.time
import numpy as np
import pandas
import sqlalchemy
from felis.simple import Table
from lsst.pex.config import ChoiceField, Field, ListField
from lsst.sphgeom import HtmPixelization, LonLat, Region, UnitVector3d
from lsst.utils.iteration import chunk_iterable
from sqlalchemy import func, sql
from sqlalchemy.pool import NullPool

from .apdb import Apdb, ApdbConfig
from .apdbConfigFreezer import ApdbConfigFreezer
from .apdbMetadataSql import ApdbMetadataSql
from .apdbReplica import ApdbInsertId
from .apdbSchema import ApdbTables
from .apdbSqlReplica import ApdbSqlReplica
from .apdbSqlSchema import ApdbSqlSchema, ExtraTables
from .timer import Timer
from .versionTuple import IncompatibleVersionError, VersionTuple

if TYPE_CHECKING:
    import sqlite3

    from .apdbMetadata import ApdbMetadata

_LOG = logging.getLogger(__name__)

VERSION = VersionTuple(0, 1, 0)
"""Version for the code controlling non-replication tables. This needs to be
updated following compatibility rules when schema produced by this code
changes.
"""

REPLICA_VERSION = VersionTuple(0, 1, 0)
"""Version for the code controlling replication tables. This needs to be
updated following compatibility rules when schema produced by this code
changes.
"""


def _coerce_uint64(df: pandas.DataFrame) -> pandas.DataFrame:
    """Change the type of uint64 columns to int64, and return copy of data
    frame.
    """
    names = [c[0] for c in df.dtypes.items() if c[1] == np.uint64]
    return df.astype({name: np.int64 for name in names})


def _make_midpointMjdTai_start(visit_time: astropy.time.Time, months: int) -> float:
    """Calculate starting point for time-based source search.

    Parameters
    ----------
    visit_time : `astropy.time.Time`
        Time of current visit.
    months : `int`
        Number of months in the sources history.

    Returns
    -------
    time : `float`
        A ``midpointMjdTai`` starting point, MJD time.
    """
    # TODO: Use of MJD must be consistent with the code in ap_association
    # (see DM-31996)
    return visit_time.mjd - months * 30


def _onSqlite3Connect(
    dbapiConnection: sqlite3.Connection, connectionRecord: sqlalchemy.pool._ConnectionRecord
) -> None:
    # Enable foreign keys
    with closing(dbapiConnection.cursor()) as cursor:
        cursor.execute("PRAGMA foreign_keys=ON;")


class ApdbSqlConfig(ApdbConfig):
    """APDB configuration class for SQL implementation (ApdbSql)."""

    db_url = Field[str](doc="SQLAlchemy database connection URI")
    isolation_level = ChoiceField[str](
        doc=(
            "Transaction isolation level, if unset then backend-default value "
            "is used, except for SQLite backend where we use READ_UNCOMMITTED. "
            "Some backends may not support every allowed value."
        ),
        allowed={
            "READ_COMMITTED": "Read committed",
            "READ_UNCOMMITTED": "Read uncommitted",
            "REPEATABLE_READ": "Repeatable read",
            "SERIALIZABLE": "Serializable",
        },
        default=None,
        optional=True,
    )
    connection_pool = Field[bool](
        doc="If False then disable SQLAlchemy connection pool. Do not use connection pool when forking.",
        default=True,
    )
    connection_timeout = Field[float](
        doc=(
            "Maximum time to wait time for database lock to be released before exiting. "
            "Defaults to sqlalchemy defaults if not set."
        ),
        default=None,
        optional=True,
    )
    sql_echo = Field[bool](doc="If True then pass SQLAlchemy echo option.", default=False)
    dia_object_index = ChoiceField[str](
        doc="Indexing mode for DiaObject table",
        allowed={
            "baseline": "Index defined in baseline schema",
            "pix_id_iov": "(pixelId, objectId, iovStart) PK",
            "last_object_table": "Separate DiaObjectLast table",
        },
        default="baseline",
    )
    htm_level = Field[int](doc="HTM indexing level", default=20)
    htm_max_ranges = Field[int](doc="Max number of ranges in HTM envelope", default=64)
    htm_index_column = Field[str](
        default="pixelId", doc="Name of a HTM index column for DiaObject and DiaSource tables"
    )
    ra_dec_columns = ListField[str](default=["ra", "dec"], doc="Names of ra/dec columns in DiaObject table")
    dia_object_columns = ListField[str](
        doc="List of columns to read from DiaObject, by default read all columns", default=[]
    )
    prefix = Field[str](doc="Prefix to add to table names and index names", default="")
    namespace = Field[str](
        doc=(
            "Namespace or schema name for all tables in APDB database. "
            "Presently only works for PostgreSQL backend. "
            "If schema with this name does not exist it will be created when "
            "APDB tables are created."
        ),
        default=None,
        optional=True,
    )
    timer = Field[bool](doc="If True then print/log timing information", default=False)

    def validate(self) -> None:
        super().validate()
        if len(self.ra_dec_columns) != 2:
            raise ValueError("ra_dec_columns must have exactly two column names")


class ApdbSql(Apdb):
    """Implementation of APDB interface based on SQL database.

    The implementation is configured via standard ``pex_config`` mechanism
    using `ApdbSqlConfig` configuration class. For an example of different
    configurations check ``config/`` folder.

    Parameters
    ----------
    config : `ApdbSqlConfig`
        Configuration object.
    """

    ConfigClass = ApdbSqlConfig

    metadataSchemaVersionKey = "version:schema"
    """Name of the metadata key to store schema version number."""

    metadataCodeVersionKey = "version:ApdbSql"
    """Name of the metadata key to store code version number."""

    metadataReplicaVersionKey = "version:ApdbSqlReplica"
    """Name of the metadata key to store replica code version number."""

    metadataConfigKey = "config:apdb-sql.json"
    """Name of the metadata key to store code version number."""

    _frozen_parameters = (
        "use_insert_id",
        "dia_object_index",
        "htm_level",
        "htm_index_column",
        "ra_dec_columns",
    )
    """Names of the config parameters to be frozen in metadata table."""

    def __init__(self, config: ApdbSqlConfig):
        self._engine = self._makeEngine(config)

        sa_metadata = sqlalchemy.MetaData(schema=config.namespace)
        meta_table_name = ApdbTables.metadata.table_name(prefix=config.prefix)
        meta_table: sqlalchemy.schema.Table | None = None
        with suppress(sqlalchemy.exc.NoSuchTableError):
            meta_table = sqlalchemy.schema.Table(meta_table_name, sa_metadata, autoload_with=self._engine)

        self._metadata = ApdbMetadataSql(self._engine, meta_table)

        # Read frozen config from metadata.
        config_json = self._metadata.get(self.metadataConfigKey)
        if config_json is not None:
            # Update config from metadata.
            freezer = ApdbConfigFreezer[ApdbSqlConfig](self._frozen_parameters)
            self.config = freezer.update(config, config_json)
        else:
            self.config = config
        self.config.validate()

        self._schema = ApdbSqlSchema(
            engine=self._engine,
            dia_object_index=self.config.dia_object_index,
            schema_file=self.config.schema_file,
            schema_name=self.config.schema_name,
            prefix=self.config.prefix,
            namespace=self.config.namespace,
            htm_index_column=self.config.htm_index_column,
            use_insert_id=self.config.use_insert_id,
        )

        if self._metadata.table_exists():
            self._versionCheck(self._metadata)

        self.pixelator = HtmPixelization(self.config.htm_level)
        self.use_insert_id = self._schema.has_insert_id

        _LOG.debug("APDB Configuration:")
        _LOG.debug("    dia_object_index: %s", self.config.dia_object_index)
        _LOG.debug("    read_sources_months: %s", self.config.read_sources_months)
        _LOG.debug("    read_forced_sources_months: %s", self.config.read_forced_sources_months)
        _LOG.debug("    dia_object_columns: %s", self.config.dia_object_columns)
        _LOG.debug("    schema_file: %s", self.config.schema_file)
        _LOG.debug("    extra_schema_file: %s", self.config.extra_schema_file)
        _LOG.debug("    schema prefix: %s", self.config.prefix)

    @classmethod
    def _makeEngine(cls, config: ApdbSqlConfig) -> sqlalchemy.engine.Engine:
        """Make SQLALchemy engine based on configured parameters.

        Parameters
        ----------
        config : `ApdbSqlConfig`
            Configuration object.
        """
        # engine is reused between multiple processes, make sure that we don't
        # share connections by disabling pool (by using NullPool class)
        kw: MutableMapping[str, Any] = dict(echo=config.sql_echo)
        conn_args: dict[str, Any] = dict()
        if not config.connection_pool:
            kw.update(poolclass=NullPool)
        if config.isolation_level is not None:
            kw.update(isolation_level=config.isolation_level)
        elif config.db_url.startswith("sqlite"):  # type: ignore
            # Use READ_UNCOMMITTED as default value for sqlite.
            kw.update(isolation_level="READ_UNCOMMITTED")
        if config.connection_timeout is not None:
            if config.db_url.startswith("sqlite"):
                conn_args.update(timeout=config.connection_timeout)
            elif config.db_url.startswith(("postgresql", "mysql")):
                conn_args.update(connect_timeout=config.connection_timeout)
        kw.update(connect_args=conn_args)
        engine = sqlalchemy.create_engine(config.db_url, **kw)

        if engine.dialect.name == "sqlite":
            # Need to enable foreign keys on every new connection.
            sqlalchemy.event.listen(engine, "connect", _onSqlite3Connect)

        return engine

    def _versionCheck(self, metadata: ApdbMetadataSql) -> None:
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
        if not self._schema.schemaVersion().checkCompatibility(db_schema_version, True):
            raise IncompatibleVersionError(
                f"Configured schema version {self._schema.schemaVersion()} "
                f"is not compatible with database version {db_schema_version}"
            )
        if not self.apdbImplementationVersion().checkCompatibility(db_code_version, True):
            raise IncompatibleVersionError(
                f"Current code version {self.apdbImplementationVersion()} "
                f"is not compatible with database version {db_code_version}"
            )

        # Check replica code version only if replica is enabled.
        if self._schema.has_insert_id:
            db_replica_version = _get_version(self.metadataReplicaVersionKey, initial_version)
            code_replica_version = ApdbSqlReplica.apdbReplicaImplementationVersion()
            if not code_replica_version.checkCompatibility(db_replica_version, True):
                raise IncompatibleVersionError(
                    f"Current replication code version {code_replica_version} "
                    f"is not compatible with database version {db_replica_version}"
                )

    @classmethod
    def apdbImplementationVersion(cls) -> VersionTuple:
        # Docstring inherited from base class.
        return VERSION

    @classmethod
    def init_database(
        cls,
        db_url: str,
        *,
        schema_file: str | None = None,
        schema_name: str | None = None,
        read_sources_months: int | None = None,
        read_forced_sources_months: int | None = None,
        use_insert_id: bool = False,
        connection_timeout: int | None = None,
        dia_object_index: str | None = None,
        htm_level: int | None = None,
        htm_index_column: str | None = None,
        ra_dec_columns: list[str] | None = None,
        prefix: str | None = None,
        namespace: str | None = None,
        drop: bool = False,
    ) -> ApdbSqlConfig:
        """Initialize new APDB instance and make configuration object for it.

        Parameters
        ----------
        db_url : `str`
            SQLAlchemy database URL.
        schema_file : `str`, optional
            Location of (YAML) configuration file with APDB schema. If not
            specified then default location will be used.
        schema_name : str | None
            Name of the schema in YAML configuration file. If not specified
            then default name will be used.
        read_sources_months : `int`, optional
            Number of months of history to read from DiaSource.
        read_forced_sources_months : `int`, optional
            Number of months of history to read from DiaForcedSource.
        use_insert_id : `bool`
            If True, make additional tables used for replication to PPDB.
        connection_timeout : `int`, optional
            Database connection timeout in seconds.
        dia_object_index : `str`, optional
            Indexing mode for DiaObject table.
        htm_level : `int`, optional
            HTM indexing level.
        htm_index_column : `str`, optional
            Name of a HTM index column for DiaObject and DiaSource tables.
        ra_dec_columns : `list` [`str`], optional
            Names of ra/dec columns in DiaObject table.
        prefix : `str`, optional
            Optional prefix for all table names.
        namespace : `str`, optional
            Name of the database schema for all APDB tables. If not specified
            then default schema is used.
        drop : `bool`, optional
            If `True` then drop existing tables before re-creating the schema.

        Returns
        -------
        config : `ApdbSqlConfig`
            Resulting configuration object for a created APDB instance.
        """
        config = ApdbSqlConfig(db_url=db_url, use_insert_id=use_insert_id)
        if schema_file is not None:
            config.schema_file = schema_file
        if schema_name is not None:
            config.schema_name = schema_name
        if read_sources_months is not None:
            config.read_sources_months = read_sources_months
        if read_forced_sources_months is not None:
            config.read_forced_sources_months = read_forced_sources_months
        if connection_timeout is not None:
            config.connection_timeout = connection_timeout
        if dia_object_index is not None:
            config.dia_object_index = dia_object_index
        if htm_level is not None:
            config.htm_level = htm_level
        if htm_index_column is not None:
            config.htm_index_column = htm_index_column
        if ra_dec_columns is not None:
            config.ra_dec_columns = ra_dec_columns
        if prefix is not None:
            config.prefix = prefix
        if namespace is not None:
            config.namespace = namespace

        cls._makeSchema(config, drop=drop)

        return config

    def apdbSchemaVersion(self) -> VersionTuple:
        # Docstring inherited from base class.
        return self._schema.schemaVersion()

    def get_replica(self) -> ApdbSqlReplica:
        """Return `ApdbReplica` instance for this database."""
        return ApdbSqlReplica(self._schema, self._engine)

    def tableRowCount(self) -> dict[str, int]:
        """Return dictionary with the table names and row counts.

        Used by ``ap_proto`` to keep track of the size of the database tables.
        Depending on database technology this could be expensive operation.

        Returns
        -------
        row_counts : `dict`
            Dict where key is a table name and value is a row count.
        """
        res = {}
        tables = [ApdbTables.DiaObject, ApdbTables.DiaSource, ApdbTables.DiaForcedSource]
        if self.config.dia_object_index == "last_object_table":
            tables.append(ApdbTables.DiaObjectLast)
        with self._engine.begin() as conn:
            for table in tables:
                sa_table = self._schema.get_table(table)
                stmt = sql.select(func.count()).select_from(sa_table)
                count: int = conn.execute(stmt).scalar_one()
                res[table.name] = count

        return res

    def tableDef(self, table: ApdbTables) -> Table | None:
        # docstring is inherited from a base class
        return self._schema.tableSchemas.get(table)

    @classmethod
    def _makeSchema(cls, config: ApdbConfig, drop: bool = False) -> None:
        # docstring is inherited from a base class

        if not isinstance(config, ApdbSqlConfig):
            raise TypeError(f"Unexpected type of configuration object: {type(config)}")

        engine = cls._makeEngine(config)

        # Ask schema class to create all tables.
        schema = ApdbSqlSchema(
            engine=engine,
            dia_object_index=config.dia_object_index,
            schema_file=config.schema_file,
            schema_name=config.schema_name,
            prefix=config.prefix,
            namespace=config.namespace,
            htm_index_column=config.htm_index_column,
            use_insert_id=config.use_insert_id,
        )
        schema.makeSchema(drop=drop)

        # Need metadata table to store few items in it, if table exists.
        meta_table: sqlalchemy.schema.Table | None = None
        with suppress(ValueError):
            meta_table = schema.get_table(ApdbTables.metadata)

        apdb_meta = ApdbMetadataSql(engine, meta_table)
        if apdb_meta.table_exists():
            # Fill version numbers, overwrite if they are already there.
            apdb_meta.set(cls.metadataSchemaVersionKey, str(schema.schemaVersion()), force=True)
            apdb_meta.set(cls.metadataCodeVersionKey, str(cls.apdbImplementationVersion()), force=True)
            if config.use_insert_id:
                # Only store replica code version if replcia is enabled.
                apdb_meta.set(
                    cls.metadataReplicaVersionKey,
                    str(ApdbSqlReplica.apdbReplicaImplementationVersion()),
                    force=True,
                )

            # Store frozen part of a configuration in metadata.
            freezer = ApdbConfigFreezer[ApdbSqlConfig](cls._frozen_parameters)
            apdb_meta.set(cls.metadataConfigKey, freezer.to_json(config), force=True)

    def getDiaObjects(self, region: Region) -> pandas.DataFrame:
        # docstring is inherited from a base class

        # decide what columns we need
        if self.config.dia_object_index == "last_object_table":
            table_enum = ApdbTables.DiaObjectLast
        else:
            table_enum = ApdbTables.DiaObject
        table = self._schema.get_table(table_enum)
        if not self.config.dia_object_columns:
            columns = self._schema.get_apdb_columns(table_enum)
        else:
            columns = [table.c[col] for col in self.config.dia_object_columns]
        query = sql.select(*columns)

        # build selection
        query = query.where(self._filterRegion(table, region))

        # select latest version of objects
        if self.config.dia_object_index != "last_object_table":
            query = query.where(table.c.validityEnd == None)  # noqa: E711

        # _LOG.debug("query: %s", query)

        # execute select
        with Timer("DiaObject select", self.config.timer):
            with self._engine.begin() as conn:
                objects = pandas.read_sql_query(query, conn)
        _LOG.debug("found %s DiaObjects", len(objects))
        return objects

    def getDiaSources(
        self, region: Region, object_ids: Iterable[int] | None, visit_time: astropy.time.Time
    ) -> pandas.DataFrame | None:
        # docstring is inherited from a base class
        if self.config.read_sources_months == 0:
            _LOG.debug("Skip DiaSources fetching")
            return None

        if object_ids is None:
            # region-based select
            return self._getDiaSourcesInRegion(region, visit_time)
        else:
            return self._getDiaSourcesByIDs(list(object_ids), visit_time)

    def getDiaForcedSources(
        self, region: Region, object_ids: Iterable[int] | None, visit_time: astropy.time.Time
    ) -> pandas.DataFrame | None:
        # docstring is inherited from a base class
        if self.config.read_forced_sources_months == 0:
            _LOG.debug("Skip DiaForceSources fetching")
            return None

        if object_ids is None:
            # This implementation does not support region-based selection.
            raise NotImplementedError("Region-based selection is not supported")

        # TODO: DateTime.MJD must be consistent with code in ap_association,
        # alternatively we can fill midpointMjdTai ourselves in store()
        midpointMjdTai_start = _make_midpointMjdTai_start(visit_time, self.config.read_forced_sources_months)
        _LOG.debug("midpointMjdTai_start = %.6f", midpointMjdTai_start)

        with Timer("DiaForcedSource select", self.config.timer):
            sources = self._getSourcesByIDs(
                ApdbTables.DiaForcedSource, list(object_ids), midpointMjdTai_start
            )

        _LOG.debug("found %s DiaForcedSources", len(sources))
        return sources

    def containsVisitDetector(self, visit: int, detector: int) -> bool:
        # docstring is inherited from a base class
        raise NotImplementedError()

    def containsCcdVisit(self, ccdVisitId: int) -> bool:
        """Test whether data for a given visit-detector is present in the APDB.

        This method is a placeholder until `Apdb.containsVisitDetector` can
        be implemented.

        Parameters
        ----------
        ccdVisitId : `int`
            The packed ID of the visit-detector to search for.

        Returns
        -------
        present : `bool`
            `True` if some DiaSource records exist for the specified
            observation, `False` otherwise.
        """
        # TODO: remove this method in favor of containsVisitDetector on either
        # DM-41671 or a ticket that removes ccdVisitId from these tables
        src_table: sqlalchemy.schema.Table = self._schema.get_table(ApdbTables.DiaSource)
        frcsrc_table: sqlalchemy.schema.Table = self._schema.get_table(ApdbTables.DiaForcedSource)
        # Query should load only one leaf page of the index
        query1 = sql.select(src_table.c.ccdVisitId).filter_by(ccdVisitId=ccdVisitId).limit(1)
        # Backup query in case an image was processed but had no diaSources
        query2 = sql.select(frcsrc_table.c.ccdVisitId).filter_by(ccdVisitId=ccdVisitId).limit(1)

        with self._engine.begin() as conn:
            result = conn.execute(query1).scalar_one_or_none()
            if result is not None:
                return True
            else:
                result = conn.execute(query2).scalar_one_or_none()
                return result is not None

    def getSSObjects(self) -> pandas.DataFrame:
        # docstring is inherited from a base class

        columns = self._schema.get_apdb_columns(ApdbTables.SSObject)
        query = sql.select(*columns)

        # execute select
        with Timer("DiaObject select", self.config.timer):
            with self._engine.begin() as conn:
                objects = pandas.read_sql_query(query, conn)
        _LOG.debug("found %s SSObjects", len(objects))
        return objects

    def store(
        self,
        visit_time: astropy.time.Time,
        objects: pandas.DataFrame,
        sources: pandas.DataFrame | None = None,
        forced_sources: pandas.DataFrame | None = None,
    ) -> None:
        # docstring is inherited from a base class

        # We want to run all inserts in one transaction.
        with self._engine.begin() as connection:
            insert_id: ApdbInsertId | None = None
            if self._schema.has_insert_id:
                insert_id = ApdbInsertId.new_insert_id(visit_time, self.config.replica_chunk_seconds)
                self._storeInsertId(insert_id, visit_time, connection)

            # fill pixelId column for DiaObjects
            objects = self._add_obj_htm_index(objects)
            self._storeDiaObjects(objects, visit_time, insert_id, connection)

            if sources is not None:
                # copy pixelId column from DiaObjects to DiaSources
                sources = self._add_src_htm_index(sources, objects)
                self._storeDiaSources(sources, insert_id, connection)

            if forced_sources is not None:
                self._storeDiaForcedSources(forced_sources, insert_id, connection)

    def storeSSObjects(self, objects: pandas.DataFrame) -> None:
        # docstring is inherited from a base class

        idColumn = "ssObjectId"
        table = self._schema.get_table(ApdbTables.SSObject)

        # everything to be done in single transaction
        with self._engine.begin() as conn:
            # Find record IDs that already exist. Some types like np.int64 can
            # cause issues with sqlalchemy, convert them to int.
            ids = sorted(int(oid) for oid in objects[idColumn])

            query = sql.select(table.columns[idColumn], table.columns[idColumn].in_(ids))
            result = conn.execute(query)
            knownIds = set(row.ssObjectId for row in result)

            filter = objects[idColumn].isin(knownIds)
            toUpdate = cast(pandas.DataFrame, objects[filter])
            toInsert = cast(pandas.DataFrame, objects[~filter])

            # insert new records
            if len(toInsert) > 0:
                toInsert.to_sql(table.name, conn, if_exists="append", index=False, schema=table.schema)

            # update existing records
            if len(toUpdate) > 0:
                whereKey = f"{idColumn}_param"
                update = table.update().where(table.columns[idColumn] == sql.bindparam(whereKey))
                toUpdate = toUpdate.rename({idColumn: whereKey}, axis="columns")
                values = toUpdate.to_dict("records")
                result = conn.execute(update, values)

    def reassignDiaSources(self, idMap: Mapping[int, int]) -> None:
        # docstring is inherited from a base class

        table = self._schema.get_table(ApdbTables.DiaSource)
        query = table.update().where(table.columns["diaSourceId"] == sql.bindparam("srcId"))

        with self._engine.begin() as conn:
            # Need to make sure that every ID exists in the database, but
            # executemany may not support rowcount, so iterate and check what
            # is missing.
            missing_ids: list[int] = []
            for key, value in idMap.items():
                params = dict(srcId=key, diaObjectId=0, ssObjectId=value)
                result = conn.execute(query, params)
                if result.rowcount == 0:
                    missing_ids.append(key)
            if missing_ids:
                missing = ",".join(str(item) for item in missing_ids)
                raise ValueError(f"Following DiaSource IDs do not exist in the database: {missing}")

    def dailyJob(self) -> None:
        # docstring is inherited from a base class
        pass

    def countUnassociatedObjects(self) -> int:
        # docstring is inherited from a base class

        # Retrieve the DiaObject table.
        table: sqlalchemy.schema.Table = self._schema.get_table(ApdbTables.DiaObject)

        # Construct the sql statement.
        stmt = sql.select(func.count()).select_from(table).where(table.c.nDiaSources == 1)
        stmt = stmt.where(table.c.validityEnd == None)  # noqa: E711

        # Return the count.
        with self._engine.begin() as conn:
            count = conn.execute(stmt).scalar_one()

        return count

    @property
    def metadata(self) -> ApdbMetadata:
        # docstring is inherited from a base class
        if self._metadata is None:
            raise RuntimeError("Database schema was not initialized.")
        return self._metadata

    def _getDiaSourcesInRegion(self, region: Region, visit_time: astropy.time.Time) -> pandas.DataFrame:
        """Return catalog of DiaSource instances from given region.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region to search for DIASources.
        visit_time : `astropy.time.Time`
            Time of the current visit.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog containing DiaSource records.
        """
        # TODO: DateTime.MJD must be consistent with code in ap_association,
        # alternatively we can fill midpointMjdTai ourselves in store()
        midpointMjdTai_start = _make_midpointMjdTai_start(visit_time, self.config.read_sources_months)
        _LOG.debug("midpointMjdTai_start = %.6f", midpointMjdTai_start)

        table = self._schema.get_table(ApdbTables.DiaSource)
        columns = self._schema.get_apdb_columns(ApdbTables.DiaSource)
        query = sql.select(*columns)

        # build selection
        time_filter = table.columns["midpointMjdTai"] > midpointMjdTai_start
        where = sql.expression.and_(self._filterRegion(table, region), time_filter)
        query = query.where(where)

        # execute select
        with Timer("DiaSource select", self.config.timer):
            with self._engine.begin() as conn:
                sources = pandas.read_sql_query(query, conn)
        _LOG.debug("found %s DiaSources", len(sources))
        return sources

    def _getDiaSourcesByIDs(self, object_ids: list[int], visit_time: astropy.time.Time) -> pandas.DataFrame:
        """Return catalog of DiaSource instances given set of DiaObject IDs.

        Parameters
        ----------
        object_ids :
            Collection of DiaObject IDs
        visit_time : `astropy.time.Time`
            Time of the current visit.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog contaning DiaSource records.
        """
        # TODO: DateTime.MJD must be consistent with code in ap_association,
        # alternatively we can fill midpointMjdTai ourselves in store()
        midpointMjdTai_start = _make_midpointMjdTai_start(visit_time, self.config.read_sources_months)
        _LOG.debug("midpointMjdTai_start = %.6f", midpointMjdTai_start)

        with Timer("DiaSource select", self.config.timer):
            sources = self._getSourcesByIDs(ApdbTables.DiaSource, object_ids, midpointMjdTai_start)

        _LOG.debug("found %s DiaSources", len(sources))
        return sources

    def _getSourcesByIDs(
        self, table_enum: ApdbTables, object_ids: list[int], midpointMjdTai_start: float
    ) -> pandas.DataFrame:
        """Return catalog of DiaSource or DiaForcedSource instances given set
        of DiaObject IDs.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Database table.
        object_ids :
            Collection of DiaObject IDs
        midpointMjdTai_start : `float`
            Earliest midpointMjdTai to retrieve.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog contaning DiaSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0 or
            when ``object_ids`` is empty.
        """
        table = self._schema.get_table(table_enum)
        columns = self._schema.get_apdb_columns(table_enum)

        sources: pandas.DataFrame | None = None
        if len(object_ids) <= 0:
            _LOG.debug("ID list is empty, just fetch empty result")
            query = sql.select(*columns).where(sql.literal(False))
            with self._engine.begin() as conn:
                sources = pandas.read_sql_query(query, conn)
        else:
            data_frames: list[pandas.DataFrame] = []
            for ids in chunk_iterable(sorted(object_ids), 1000):
                query = sql.select(*columns)

                # Some types like np.int64 can cause issues with
                # sqlalchemy, convert them to int.
                int_ids = [int(oid) for oid in ids]

                # select by object id
                query = query.where(
                    sql.expression.and_(
                        table.columns["diaObjectId"].in_(int_ids),
                        table.columns["midpointMjdTai"] > midpointMjdTai_start,
                    )
                )

                # execute select
                with self._engine.begin() as conn:
                    data_frames.append(pandas.read_sql_query(query, conn))

            if len(data_frames) == 1:
                sources = data_frames[0]
            else:
                sources = pandas.concat(data_frames)
        assert sources is not None, "Catalog cannot be None"
        return sources

    def _storeInsertId(
        self, insert_id: ApdbInsertId, visit_time: astropy.time.Time, connection: sqlalchemy.engine.Connection
    ) -> None:
        dt = visit_time.datetime

        table = self._schema.get_table(ExtraTables.DiaInsertId)

        # We need UPSERT which is dialect-specific construct
        values = {"insert_time": dt, "unique_id": insert_id.unique_id}
        row = {"insert_id": insert_id.id} | values
        if connection.dialect.name == "sqlite":
            insert_sqlite = sqlalchemy.dialects.sqlite.insert(table)
            insert_sqlite = insert_sqlite.on_conflict_do_update(index_elements=table.primary_key, set_=values)
            connection.execute(insert_sqlite, row)
        elif connection.dialect.name == "postgresql":
            insert_pg = sqlalchemy.dialects.postgresql.dml.insert(table)
            insert_pg = insert_pg.on_conflict_do_update(constraint=table.primary_key, set_=values)
            connection.execute(insert_pg, row)
        else:
            raise TypeError(f"Unsupported dialect {connection.dialect.name} for upsert.")

    def _storeDiaObjects(
        self,
        objs: pandas.DataFrame,
        visit_time: astropy.time.Time,
        insert_id: ApdbInsertId | None,
        connection: sqlalchemy.engine.Connection,
    ) -> None:
        """Store catalog of DiaObjects from current visit.

        Parameters
        ----------
        objs : `pandas.DataFrame`
            Catalog with DiaObject records.
        visit_time : `astropy.time.Time`
            Time of the visit.
        insert_id : `ApdbInsertId`
            Insert identifier.
        """
        if len(objs) == 0:
            _LOG.debug("No objects to write to database.")
            return

        # Some types like np.int64 can cause issues with sqlalchemy, convert
        # them to int.
        ids = sorted(int(oid) for oid in objs["diaObjectId"])
        _LOG.debug("first object ID: %d", ids[0])

        # TODO: Need to verify that we are using correct scale here for
        # DATETIME representation (see DM-31996).
        dt = visit_time.datetime

        # everything to be done in single transaction
        if self.config.dia_object_index == "last_object_table":
            # Insert and replace all records in LAST table.
            table = self._schema.get_table(ApdbTables.DiaObjectLast)

            # Drop the previous objects (pandas cannot upsert).
            query = table.delete().where(table.columns["diaObjectId"].in_(ids))

            with Timer(table.name + " delete", self.config.timer):
                res = connection.execute(query)
            _LOG.debug("deleted %s objects", res.rowcount)

            # DiaObjectLast is a subset of DiaObject, strip missing columns
            last_column_names = [column.name for column in table.columns]
            last_objs = objs[last_column_names]
            last_objs = _coerce_uint64(last_objs)

            if "lastNonForcedSource" in last_objs.columns:
                # lastNonForcedSource is defined NOT NULL, fill it with visit
                # time just in case.
                last_objs["lastNonForcedSource"].fillna(dt, inplace=True)
            else:
                extra_column = pandas.Series([dt] * len(objs), name="lastNonForcedSource")
                last_objs.set_index(extra_column.index, inplace=True)
                last_objs = pandas.concat([last_objs, extra_column], axis="columns")

            with Timer("DiaObjectLast insert", self.config.timer):
                last_objs.to_sql(
                    table.name,
                    connection,
                    if_exists="append",
                    index=False,
                    schema=table.schema,
                )
        else:
            # truncate existing validity intervals
            table = self._schema.get_table(ApdbTables.DiaObject)

            update = (
                table.update()
                .values(validityEnd=dt)
                .where(
                    sql.expression.and_(
                        table.columns["diaObjectId"].in_(ids),
                        table.columns["validityEnd"].is_(None),
                    )
                )
            )

            # _LOG.debug("query: %s", query)

            with Timer(table.name + " truncate", self.config.timer):
                res = connection.execute(update)
            _LOG.debug("truncated %s intervals", res.rowcount)

        objs = _coerce_uint64(objs)

        # Fill additional columns
        extra_columns: list[pandas.Series] = []
        if "validityStart" in objs.columns:
            objs["validityStart"] = dt
        else:
            extra_columns.append(pandas.Series([dt] * len(objs), name="validityStart"))
        if "validityEnd" in objs.columns:
            objs["validityEnd"] = None
        else:
            extra_columns.append(pandas.Series([None] * len(objs), name="validityEnd"))
        if "lastNonForcedSource" in objs.columns:
            # lastNonForcedSource is defined NOT NULL, fill it with visit time
            # just in case.
            objs["lastNonForcedSource"].fillna(dt, inplace=True)
        else:
            extra_columns.append(pandas.Series([dt] * len(objs), name="lastNonForcedSource"))
        if extra_columns:
            objs.set_index(extra_columns[0].index, inplace=True)
            objs = pandas.concat([objs] + extra_columns, axis="columns")

        # Insert history data
        table = self._schema.get_table(ApdbTables.DiaObject)
        history_data: list[dict] = []
        history_stmt: Any = None
        if insert_id is not None:
            pk_names = [column.name for column in table.primary_key]
            history_data = objs[pk_names].to_dict("records")
            for row in history_data:
                row["insert_id"] = insert_id.id
            history_table = self._schema.get_table(ExtraTables.DiaObjectInsertId)
            history_stmt = history_table.insert()

        # insert new versions
        with Timer("DiaObject insert", self.config.timer):
            objs.to_sql(table.name, connection, if_exists="append", index=False, schema=table.schema)
            if history_stmt is not None:
                connection.execute(history_stmt, history_data)

    def _storeDiaSources(
        self,
        sources: pandas.DataFrame,
        insert_id: ApdbInsertId | None,
        connection: sqlalchemy.engine.Connection,
    ) -> None:
        """Store catalog of DiaSources from current visit.

        Parameters
        ----------
        sources : `pandas.DataFrame`
            Catalog containing DiaSource records
        """
        table = self._schema.get_table(ApdbTables.DiaSource)

        # Insert history data
        history: list[dict] = []
        history_stmt: Any = None
        if insert_id is not None:
            pk_names = [column.name for column in table.primary_key]
            history = sources[pk_names].to_dict("records")
            for row in history:
                row["insert_id"] = insert_id.id
            history_table = self._schema.get_table(ExtraTables.DiaSourceInsertId)
            history_stmt = history_table.insert()

        # everything to be done in single transaction
        with Timer("DiaSource insert", self.config.timer):
            sources = _coerce_uint64(sources)
            sources.to_sql(table.name, connection, if_exists="append", index=False, schema=table.schema)
            if history_stmt is not None:
                connection.execute(history_stmt, history)

    def _storeDiaForcedSources(
        self,
        sources: pandas.DataFrame,
        insert_id: ApdbInsertId | None,
        connection: sqlalchemy.engine.Connection,
    ) -> None:
        """Store a set of DiaForcedSources from current visit.

        Parameters
        ----------
        sources : `pandas.DataFrame`
            Catalog containing DiaForcedSource records
        """
        table = self._schema.get_table(ApdbTables.DiaForcedSource)

        # Insert history data
        history: list[dict] = []
        history_stmt: Any = None
        if insert_id is not None:
            pk_names = [column.name for column in table.primary_key]
            history = sources[pk_names].to_dict("records")
            for row in history:
                row["insert_id"] = insert_id.id
            history_table = self._schema.get_table(ExtraTables.DiaForcedSourceInsertId)
            history_stmt = history_table.insert()

        # everything to be done in single transaction
        with Timer("DiaForcedSource insert", self.config.timer):
            sources = _coerce_uint64(sources)
            sources.to_sql(table.name, connection, if_exists="append", index=False, schema=table.schema)
            if history_stmt is not None:
                connection.execute(history_stmt, history)

    def _htm_indices(self, region: Region) -> list[tuple[int, int]]:
        """Generate a set of HTM indices covering specified region.

        Parameters
        ----------
        region: `sphgeom.Region`
            Region that needs to be indexed.

        Returns
        -------
        Sequence of ranges, range is a tuple (minHtmID, maxHtmID).
        """
        _LOG.debug("region: %s", region)
        indices = self.pixelator.envelope(region, self.config.htm_max_ranges)

        return indices.ranges()

    def _filterRegion(self, table: sqlalchemy.schema.Table, region: Region) -> sql.ColumnElement:
        """Make SQLAlchemy expression for selecting records in a region."""
        htm_index_column = table.columns[self.config.htm_index_column]
        exprlist = []
        pixel_ranges = self._htm_indices(region)
        for low, upper in pixel_ranges:
            upper -= 1
            if low == upper:
                exprlist.append(htm_index_column == low)
            else:
                exprlist.append(sql.expression.between(htm_index_column, low, upper))

        return sql.expression.or_(*exprlist)

    def _add_obj_htm_index(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """Calculate HTM index for each record and add it to a DataFrame.

        Notes
        -----
        This overrides any existing column in a DataFrame with the same name
        (pixelId). Original DataFrame is not changed, copy of a DataFrame is
        returned.
        """
        # calculate HTM index for every DiaObject
        htm_index = np.zeros(df.shape[0], dtype=np.int64)
        ra_col, dec_col = self.config.ra_dec_columns
        for i, (ra, dec) in enumerate(zip(df[ra_col], df[dec_col])):
            uv3d = UnitVector3d(LonLat.fromDegrees(ra, dec))
            idx = self.pixelator.index(uv3d)
            htm_index[i] = idx
        df = df.copy()
        df[self.config.htm_index_column] = htm_index
        return df

    def _add_src_htm_index(self, sources: pandas.DataFrame, objs: pandas.DataFrame) -> pandas.DataFrame:
        """Add pixelId column to DiaSource catalog.

        Notes
        -----
        This method copies pixelId value from a matching DiaObject record.
        DiaObject catalog needs to have a pixelId column filled by
        ``_add_obj_htm_index`` method and DiaSource records need to be
        associated to DiaObjects via ``diaObjectId`` column.

        This overrides any existing column in a DataFrame with the same name
        (pixelId). Original DataFrame is not changed, copy of a DataFrame is
        returned.
        """
        pixel_id_map: dict[int, int] = {
            diaObjectId: pixelId
            for diaObjectId, pixelId in zip(objs["diaObjectId"], objs[self.config.htm_index_column])
        }
        # DiaSources associated with SolarSystemObjects do not have an
        # associated DiaObject hence we skip them and set their htmIndex
        # value to 0.
        pixel_id_map[0] = 0
        htm_index = np.zeros(sources.shape[0], dtype=np.int64)
        for i, diaObjId in enumerate(sources["diaObjectId"]):
            htm_index[i] = pixel_id_map[diaObjId]
        sources = sources.copy()
        sources[self.config.htm_index_column] = htm_index
        return sources
