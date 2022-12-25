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
from typing import Any, Dict, List, Optional, Tuple, cast

import lsst.daf.base as dafBase
import numpy as np
import pandas
import sqlalchemy
from felis.simple import Table
from lsst.pex.config import ChoiceField, Field, ListField
from lsst.sphgeom import HtmPixelization, LonLat, Region, UnitVector3d
from lsst.utils.iteration import chunk_iterable
from sqlalchemy import func, sql
from sqlalchemy.pool import NullPool

from .apdb import Apdb, ApdbConfig, ApdbInsertId, ApdbTableData
from .apdbSchema import ApdbTables
from .apdbSqlSchema import ApdbSqlSchema, ExtraTables
from .timer import Timer

_LOG = logging.getLogger(__name__)


def _coerce_uint64(df: pandas.DataFrame) -> pandas.DataFrame:
    """Change type of the uint64 columns to int64, return copy of data frame.
    """
    names = [c[0] for c in df.dtypes.items() if c[1] == np.uint64]
    return df.astype({name: np.int64 for name in names})


def _make_midPointTai_start(visit_time: dafBase.DateTime, months: int) -> float:
    """Calculate starting point for time-based source search.

    Parameters
    ----------
    visit_time : `lsst.daf.base.DateTime`
        Time of current visit.
    months : `int`
        Number of months in the sources history.

    Returns
    -------
    time : `float`
        A ``midPointTai`` starting point, MJD time.
    """
    # TODO: `system` must be consistent with the code in ap_association
    # (see DM-31996)
    return visit_time.get(system=dafBase.DateTime.MJD) - months * 30


class ApdbSqlConfig(ApdbConfig):
    """APDB configuration class for SQL implementation (ApdbSql).
    """
    db_url = Field[str](
        doc="SQLAlchemy database connection URI"
    )
    isolation_level = ChoiceField[str](
        doc="Transaction isolation level, if unset then backend-default value "
            "is used, except for SQLite backend where we use READ_UNCOMMITTED. "
            "Some backends may not support every allowed value.",
        allowed={
            "READ_COMMITTED": "Read committed",
            "READ_UNCOMMITTED": "Read uncommitted",
            "REPEATABLE_READ": "Repeatable read",
            "SERIALIZABLE": "Serializable"
        },
        default=None,
        optional=True
    )
    connection_pool = Field[bool](
        doc="If False then disable SQLAlchemy connection pool. "
            "Do not use connection pool when forking.",
        default=True
    )
    connection_timeout = Field[float](
        doc="Maximum time to wait time for database lock to be released before "
            "exiting. Defaults to sqlalchemy defaults if not set.",
        default=None,
        optional=True
    )
    sql_echo = Field[bool](
        doc="If True then pass SQLAlchemy echo option.",
        default=False
    )
    dia_object_index = ChoiceField[str](
        doc="Indexing mode for DiaObject table",
        allowed={
            'baseline': "Index defined in baseline schema",
            'pix_id_iov': "(pixelId, objectId, iovStart) PK",
            'last_object_table': "Separate DiaObjectLast table"
        },
        default='baseline'
    )
    htm_level = Field[int](
        doc="HTM indexing level",
        default=20
    )
    htm_max_ranges = Field[int](
        doc="Max number of ranges in HTM envelope",
        default=64
    )
    htm_index_column = Field[str](
        default="pixelId",
        doc="Name of a HTM index column for DiaObject and DiaSource tables"
    )
    ra_dec_columns = ListField[str](
        default=["ra", "decl"],
        doc="Names ra/dec columns in DiaObject table"
    )
    dia_object_columns = ListField[str](
        doc="List of columns to read from DiaObject, by default read all columns",
        default=[]
    )
    object_last_replace = Field[bool](
        doc="If True (default) then use \"upsert\" for DiaObjectsLast table",
        default=True,
        deprecated="This field is not used and will be removed on 2022-12-31."
    )
    prefix = Field[str](
        doc="Prefix to add to table names and index names",
        default=""
    )
    namespace = Field[str](
        doc=(
            "Namespace or schema name for all tables in APDB database. "
            "Presently only makes sense for PostgresQL backend. "
            "If schema with this name does not exist it will be created when "
            "APDB tables are created."
        ),
        default=None,
        optional=True
    )
    explain = Field[bool](
        doc="If True then run EXPLAIN SQL command on each executed query",
        default=False
    )
    timer = Field[bool](
        doc="If True then print/log timing information",
        default=False
    )

    def validate(self) -> None:
        super().validate()
        if len(self.ra_dec_columns) != 2:
            raise ValueError("ra_dec_columns must have exactly two column names")


class ApdbSqlTableData(ApdbTableData):
    """Implementation of ApdbTableData that wraps sqlalchemy Result."""

    def __init__(self, result: sqlalchemy.engine.Result):
        self.result = result

    def column_names(self) -> list[str]:
        return self.result.keys()

    def rows(self) -> Iterable[tuple]:
        for row in self.result:
            yield tuple(row)


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

    def __init__(self, config: ApdbSqlConfig):

        config.validate()
        self.config = config

        _LOG.debug("APDB Configuration:")
        _LOG.debug("    dia_object_index: %s", self.config.dia_object_index)
        _LOG.debug("    read_sources_months: %s", self.config.read_sources_months)
        _LOG.debug("    read_forced_sources_months: %s", self.config.read_forced_sources_months)
        _LOG.debug("    dia_object_columns: %s", self.config.dia_object_columns)
        _LOG.debug("    schema_file: %s", self.config.schema_file)
        _LOG.debug("    extra_schema_file: %s", self.config.extra_schema_file)
        _LOG.debug("    schema prefix: %s", self.config.prefix)

        # engine is reused between multiple processes, make sure that we don't
        # share connections by disabling pool (by using NullPool class)
        kw: MutableMapping[str, Any] = dict(echo=self.config.sql_echo)
        conn_args: Dict[str, Any] = dict()
        if not self.config.connection_pool:
            kw.update(poolclass=NullPool)
        if self.config.isolation_level is not None:
            kw.update(isolation_level=self.config.isolation_level)
        elif self.config.db_url.startswith("sqlite"):  # type: ignore
            # Use READ_UNCOMMITTED as default value for sqlite.
            kw.update(isolation_level="READ_UNCOMMITTED")
        if self.config.connection_timeout is not None:
            if self.config.db_url.startswith("sqlite"):
                conn_args.update(timeout=self.config.connection_timeout)
            elif self.config.db_url.startswith(("postgresql", "mysql")):
                conn_args.update(connect_timeout=self.config.connection_timeout)
        kw.update(connect_args=conn_args)
        self._engine = sqlalchemy.create_engine(self.config.db_url, **kw)

        self._schema = ApdbSqlSchema(engine=self._engine,
                                     dia_object_index=self.config.dia_object_index,
                                     schema_file=self.config.schema_file,
                                     schema_name=self.config.schema_name,
                                     prefix=self.config.prefix,
                                     namespace=self.config.namespace,
                                     htm_index_column=self.config.htm_index_column,
                                     use_insert_id=config.use_insert_id)

        self.pixelator = HtmPixelization(self.config.htm_level)
        self.use_insert_id = self._schema.has_insert_id

    def tableRowCount(self) -> Dict[str, int]:
        """Returns dictionary with the table names and row counts.

        Used by ``ap_proto`` to keep track of the size of the database tables.
        Depending on database technology this could be expensive operation.

        Returns
        -------
        row_counts : `dict`
            Dict where key is a table name and value is a row count.
        """
        res = {}
        tables = [ApdbTables.DiaObject, ApdbTables.DiaSource, ApdbTables.DiaForcedSource]
        if self.config.dia_object_index == 'last_object_table':
            tables.append(ApdbTables.DiaObjectLast)
        for table in tables:
            sa_table = self._schema.get_table(table)
            stmt = sql.select([func.count()]).select_from(sa_table)
            count = self._engine.scalar(stmt)
            res[table.name] = count

        return res

    def tableDef(self, table: ApdbTables) -> Optional[Table]:
        # docstring is inherited from a base class
        return self._schema.tableSchemas.get(table)

    def makeSchema(self, drop: bool = False) -> None:
        # docstring is inherited from a base class
        self._schema.makeSchema(drop=drop)

    def getDiaObjects(self, region: Region) -> pandas.DataFrame:
        # docstring is inherited from a base class

        # decide what columns we need
        if self.config.dia_object_index == 'last_object_table':
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
        if self.config.dia_object_index != 'last_object_table':
            query = query.where(table.c.validityEnd == None)  # noqa: E711

        # _LOG.debug("query: %s", query)

        # execute select
        with Timer('DiaObject select', self.config.timer):
            with self._engine.begin() as conn:
                objects = pandas.read_sql_query(query, conn)
        _LOG.debug("found %s DiaObjects", len(objects))
        return objects

    def getDiaSources(self, region: Region,
                      object_ids: Optional[Iterable[int]],
                      visit_time: dafBase.DateTime) -> Optional[pandas.DataFrame]:
        # docstring is inherited from a base class
        if self.config.read_sources_months == 0:
            _LOG.debug("Skip DiaSources fetching")
            return None

        if object_ids is None:
            # region-based select
            return self._getDiaSourcesInRegion(region, visit_time)
        else:
            return self._getDiaSourcesByIDs(list(object_ids), visit_time)

    def getDiaForcedSources(self, region: Region,
                            object_ids: Optional[Iterable[int]],
                            visit_time: dafBase.DateTime) -> Optional[pandas.DataFrame]:
        """Return catalog of DiaForcedSource instances from a given region.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region to search for DIASources.
        object_ids : iterable [ `int` ], optional
            List of DiaObject IDs to further constrain the set of returned
            sources. If list is empty then empty catalog is returned with a
            correct schema.
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit.

        Returns
        -------
        catalog : `pandas.DataFrame`, or `None`
            Catalog containing DiaSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0.

        Raises
        ------
        NotImplementedError
            Raised if ``object_ids`` is `None`.

        Notes
        -----
        Even though base class allows `None` to be passed for ``object_ids``,
        this class requires ``object_ids`` to be not-`None`.
        `NotImplementedError` is raised if `None` is passed.

        This method returns DiaForcedSource catalog for a region with additional
        filtering based on DiaObject IDs. Only a subset of DiaSource history
        is returned limited by ``read_forced_sources_months`` config parameter,
        w.r.t. ``visit_time``. If ``object_ids`` is empty then an empty catalog
        is always returned with a correct schema (columns/types).
        """

        if self.config.read_forced_sources_months == 0:
            _LOG.debug("Skip DiaForceSources fetching")
            return None

        if object_ids is None:
            # This implementation does not support region-based selection.
            raise NotImplementedError("Region-based selection is not supported")

        # TODO: DateTime.MJD must be consistent with code in ap_association,
        # alternatively we can fill midPointTai ourselves in store()
        midPointTai_start = _make_midPointTai_start(visit_time, self.config.read_forced_sources_months)
        _LOG.debug("midPointTai_start = %.6f", midPointTai_start)

        with Timer('DiaForcedSource select', self.config.timer):
            sources = self._getSourcesByIDs(ApdbTables.DiaForcedSource, list(object_ids), midPointTai_start)

        _LOG.debug("found %s DiaForcedSources", len(sources))
        return sources

    def getInsertIds(self) -> list[ApdbInsertId] | None:
        # docstring is inherited from a base class
        if not self._schema.has_insert_id:
            return None

        table = self._schema.get_table(ExtraTables.DiaInsertId)
        assert table is not None, "has_insert_id=True means it must be defined"
        query = sql.select(table.columns["insert_id"]).order_by(table.columns["insert_time"])
        with Timer('DiaObject insert id select', self.config.timer):
            with self._engine.connect() as conn:
                result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
                return [ApdbInsertId(row) for row in result.scalars()]

    def deleteInsertIds(self, ids: Iterable[ApdbInsertId]) -> None:
        # docstring is inherited from a base class
        if not self._schema.has_insert_id:
            raise ValueError("APDB is not configured for history storage")

        table = self._schema.get_table(ExtraTables.DiaInsertId)

        insert_ids = [id.id for id in ids]
        where_clause = table.columns["insert_id"].in_(insert_ids)
        stmt = table.delete().where(where_clause)
        with self._engine.begin() as conn:
            conn.execute(stmt)

    def getDiaObjectsHistory(self, ids: Iterable[ApdbInsertId]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_history(ids, ApdbTables.DiaObject, ExtraTables.DiaObjectInsertId)

    def getDiaSourcesHistory(self, ids: Iterable[ApdbInsertId]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_history(ids, ApdbTables.DiaSource, ExtraTables.DiaSourceInsertId)

    def getDiaForcedSourcesHistory(self, ids: Iterable[ApdbInsertId]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_history(ids, ApdbTables.DiaForcedSource, ExtraTables.DiaForcedSourceInsertId)

    def _get_history(
        self, ids: Iterable[ApdbInsertId], table_enum: ApdbTables, history_table_enum: ExtraTables,
    ) -> ApdbTableData:
        """Common implementation of the history methods."""
        if not self._schema.has_insert_id:
            raise ValueError("APDB is not configured for history retrieval")

        table = self._schema.get_table(table_enum)
        history_table = self._schema.get_table(history_table_enum)

        join = table.join(history_table)
        insert_ids = [id.id for id in ids]
        history_id_column = history_table.columns["insert_id"]
        apdb_columns = self._schema.get_apdb_columns(table_enum)
        where_clause = history_id_column.in_(insert_ids)
        query = sql.select(history_id_column, *apdb_columns).select_from(join).where(where_clause)

        # execute select
        with Timer(f"{table.name} history select", self.config.timer):
            connection = self._engine.connect(close_with_result=True)
            result = connection.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
        return ApdbSqlTableData(result)

    def getSSObjects(self) -> pandas.DataFrame:
        # docstring is inherited from a base class

        columns = self._schema.get_apdb_columns(ApdbTables.SSObject)
        query = sql.select(*columns)

        # execute select
        with Timer('DiaObject select', self.config.timer):
            with self._engine.begin() as conn:
                objects = pandas.read_sql_query(query, conn)
        _LOG.debug("found %s SSObjects", len(objects))
        return objects

    def store(self,
              visit_time: dafBase.DateTime,
              objects: pandas.DataFrame,
              sources: Optional[pandas.DataFrame] = None,
              forced_sources: Optional[pandas.DataFrame] = None) -> None:
        # docstring is inherited from a base class

        insert_id: ApdbInsertId | None = None
        if self._schema.has_insert_id:
            insert_id = ApdbInsertId.new_insert_id()
            self._storeInsertId(insert_id, visit_time)

        # fill pixelId column for DiaObjects
        objects = self._add_obj_htm_index(objects)
        self._storeDiaObjects(objects, visit_time, insert_id)

        if sources is not None:
            # copy pixelId column from DiaObjects to DiaSources
            sources = self._add_src_htm_index(sources, objects)
            self._storeDiaSources(sources, insert_id)

        if forced_sources is not None:
            self._storeDiaForcedSources(forced_sources, insert_id)

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
            knownIds = set(row[idColumn] for row in result)

            filter = objects[idColumn].isin(knownIds)
            toUpdate = cast(pandas.DataFrame, objects[filter])
            toInsert = cast(pandas.DataFrame, objects[~filter])

            # insert new records
            if len(toInsert) > 0:
                toInsert.to_sql(table.name, conn, if_exists='append', index=False, schema=table.schema)

            # update existing records
            if len(toUpdate) > 0:
                whereKey = f"{idColumn}_param"
                query = table.update().where(table.columns[idColumn] == sql.bindparam(whereKey))
                toUpdate = toUpdate.rename({idColumn: whereKey}, axis="columns")
                values = toUpdate.to_dict("records")
                result = conn.execute(query, values)

    def reassignDiaSources(self, idMap: Mapping[int, int]) -> None:
        # docstring is inherited from a base class

        table = self._schema.get_table(ApdbTables.DiaSource)
        query = table.update().where(table.columns["diaSourceId"] == sql.bindparam("srcId"))

        with self._engine.begin() as conn:
            # Need to make sure that every ID exists in the database, but
            # executemany may not support rowcount, so iterate and check what is
            # missing.
            missing_ids: List[int] = []
            for key, value in idMap.items():
                params = dict(srcId=key, diaObjectId=0, ssObjectId=value)
                result = conn.execute(query, params)
                if result.rowcount == 0:
                    missing_ids.append(key)
            if missing_ids:
                missing = ",".join(str(item)for item in missing_ids)
                raise ValueError(f"Following DiaSource IDs do not exist in the database: {missing}")

    def dailyJob(self) -> None:
        # docstring is inherited from a base class

        if self._engine.name == 'postgresql':

            # do VACUUM on all tables
            _LOG.info("Running VACUUM on all tables")
            connection = self._engine.raw_connection()
            ISOLATION_LEVEL_AUTOCOMMIT = 0
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = connection.cursor()
            cursor.execute("VACUUM ANALYSE")

    def countUnassociatedObjects(self) -> int:
        # docstring is inherited from a base class

        # Retrieve the DiaObject table.
        table: sqlalchemy.schema.Table = self._schema.get_table(ApdbTables.DiaObject)

        # Construct the sql statement.
        stmt = sql.select([func.count()]).select_from(table).where(table.c.nDiaSources == 1)
        stmt = stmt.where(table.c.validityEnd == None)  # noqa: E711

        # Return the count.
        with self._engine.begin() as conn:
            count = conn.scalar(stmt)

        return count

    def _getDiaSourcesInRegion(self, region: Region, visit_time: dafBase.DateTime) -> pandas.DataFrame:
        """Returns catalog of DiaSource instances from given region.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region to search for DIASources.
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog containing DiaSource records.
        """
        # TODO: DateTime.MJD must be consistent with code in ap_association,
        # alternatively we can fill midPointTai ourselves in store()
        midPointTai_start = _make_midPointTai_start(visit_time, self.config.read_sources_months)
        _LOG.debug("midPointTai_start = %.6f", midPointTai_start)

        table = self._schema.get_table(ApdbTables.DiaSource)
        columns = self._schema.get_apdb_columns(ApdbTables.DiaSource)
        query = sql.select(*columns)

        # build selection
        time_filter = table.columns["midPointTai"] > midPointTai_start
        where = sql.expression.and_(self._filterRegion(table, region), time_filter)
        query = query.where(where)

        # execute select
        with Timer('DiaSource select', self.config.timer):
            with self._engine.begin() as conn:
                sources = pandas.read_sql_query(query, conn)
        _LOG.debug("found %s DiaSources", len(sources))
        return sources

    def _getDiaSourcesByIDs(self, object_ids: List[int], visit_time: dafBase.DateTime) -> pandas.DataFrame:
        """Returns catalog of DiaSource instances given set of DiaObject IDs.

        Parameters
        ----------
        object_ids :
            Collection of DiaObject IDs
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog contaning DiaSource records.
        """
        # TODO: DateTime.MJD must be consistent with code in ap_association,
        # alternatively we can fill midPointTai ourselves in store()
        midPointTai_start = _make_midPointTai_start(visit_time, self.config.read_sources_months)
        _LOG.debug("midPointTai_start = %.6f", midPointTai_start)

        with Timer('DiaSource select', self.config.timer):
            sources = self._getSourcesByIDs(ApdbTables.DiaSource, object_ids, midPointTai_start)

        _LOG.debug("found %s DiaSources", len(sources))
        return sources

    def _getSourcesByIDs(
        self, table_enum: ApdbTables, object_ids: List[int], midPointTai_start: float
    ) -> pandas.DataFrame:
        """Returns catalog of DiaSource or DiaForcedSource instances given set
        of DiaObject IDs.

        Parameters
        ----------
        table : `sqlalchemy.schema.Table`
            Database table.
        object_ids :
            Collection of DiaObject IDs
        midPointTai_start : `float`
            Earliest midPointTai to retrieve.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog contaning DiaSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0 or
            when ``object_ids`` is empty.
        """
        table = self._schema.get_table(table_enum)
        columns = self._schema.get_apdb_columns(table_enum)

        sources: Optional[pandas.DataFrame] = None
        if len(object_ids) <= 0:
            _LOG.debug("ID list is empty, just fetch empty result")
            query = sql.select(*columns).where(False)
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
                        table.columns["midPointTai"] > midPointTai_start,
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

    def _storeInsertId(self, insert_id: ApdbInsertId, visit_time: dafBase.DateTime) -> None:

        dt = visit_time.toPython()

        table = self._schema.get_table(ExtraTables.DiaInsertId)

        stmt = table.insert().values(insert_id=insert_id.id, insert_time=dt)
        with self._engine.begin() as conn:
            conn.execute(stmt)

    def _storeDiaObjects(
        self, objs: pandas.DataFrame, visit_time: dafBase.DateTime, insert_id: ApdbInsertId | None
    ) -> None:
        """Store catalog of DiaObjects from current visit.

        Parameters
        ----------
        objs : `pandas.DataFrame`
            Catalog with DiaObject records.
        visit_time : `lsst.daf.base.DateTime`
            Time of the visit.
        insert_id : `ApdbInsertId`
            Insert identifier.
        """

        # Some types like np.int64 can cause issues with sqlalchemy, convert
        # them to int.
        ids = sorted(int(oid) for oid in objs['diaObjectId'])
        _LOG.debug("first object ID: %d", ids[0])

        # TODO: Need to verify that we are using correct scale here for
        # DATETIME representation (see DM-31996).
        dt = visit_time.toPython()

        # everything to be done in single transaction
        if self.config.dia_object_index == 'last_object_table':

            # insert and replace all records in LAST table, mysql and postgres have
            # non-standard features
            table = self._schema.get_table(ApdbTables.DiaObjectLast)

            # Drop the previous objects (pandas cannot upsert).
            query = table.delete().where(
                table.columns["diaObjectId"].in_(ids)
            )

            with Timer(table.name + ' delete', self.config.timer):
                with self._engine.begin() as conn:
                    res = conn.execute(query)
            _LOG.debug("deleted %s objects", res.rowcount)

            # DiaObjectLast is a subset of DiaObject, strip missing columns
            last_column_names = [column.name for column in table.columns]
            last_objs = objs[last_column_names]
            last_objs = _coerce_uint64(last_objs)

            if "lastNonForcedSource" in last_objs.columns:
                # lastNonForcedSource is defined NOT NULL, fill it with visit time
                # just in case.
                last_objs["lastNonForcedSource"].fillna(dt, inplace=True)
            else:
                extra_column = pandas.Series([dt] * len(objs), name="lastNonForcedSource")
                last_objs.set_index(extra_column.index, inplace=True)
                last_objs = pandas.concat([last_objs, extra_column], axis="columns")

            with Timer("DiaObjectLast insert", self.config.timer):
                with self._engine.begin() as conn:
                    last_objs.to_sql(table.name, conn, if_exists='append', index=False, schema=table.schema)
        else:

            # truncate existing validity intervals
            table = self._schema.get_table(ApdbTables.DiaObject)

            query = table.update().values(validityEnd=dt).where(
                sql.expression.and_(
                    table.columns["diaObjectId"].in_(ids),
                    table.columns["validityEnd"].is_(None),
                )
            )

            # _LOG.debug("query: %s", query)

            with Timer(table.name + ' truncate', self.config.timer):
                with self._engine.begin() as conn:
                    res = conn.execute(query)
            _LOG.debug("truncated %s intervals", res.rowcount)

        objs = _coerce_uint64(objs)

        # Fill additional columns
        extra_columns: List[pandas.Series] = []
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
            with self._engine.begin() as conn:
                objs.to_sql(table.name, conn, if_exists='append', index=False, schema=table.schema)
                if history_stmt is not None:
                    conn.execute(history_stmt, *history_data)

    def _storeDiaSources(self, sources: pandas.DataFrame, insert_id: ApdbInsertId | None) -> None:
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
            with self._engine.begin() as conn:
                sources.to_sql(table.name, conn, if_exists='append', index=False, schema=table.schema)
                if history_stmt is not None:
                    conn.execute(history_stmt, *history)

    def _storeDiaForcedSources(self, sources: pandas.DataFrame, insert_id: ApdbInsertId | None) -> None:
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
            with self._engine.begin() as conn:
                sources.to_sql(table.name, conn, if_exists='append', index=False, schema=table.schema)
                if history_stmt is not None:
                    conn.execute(history_stmt, *history)

    def _htm_indices(self, region: Region) -> List[Tuple[int, int]]:
        """Generate a set of HTM indices covering specified region.

        Parameters
        ----------
        region: `sphgeom.Region`
            Region that needs to be indexed.

        Returns
        -------
        Sequence of ranges, range is a tuple (minHtmID, maxHtmID).
        """
        _LOG.debug('region: %s', region)
        indices = self.pixelator.envelope(region, self.config.htm_max_ranges)

        return indices.ranges()

    def _filterRegion(self, table: sqlalchemy.schema.Table, region: Region) -> sql.ClauseElement:
        """Make SQLAlchemy expression for selecting records in a region.
        """
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
        pixel_id_map: Dict[int, int] = {
            diaObjectId: pixelId for diaObjectId, pixelId
            in zip(objs["diaObjectId"], objs[self.config.htm_index_column])
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
