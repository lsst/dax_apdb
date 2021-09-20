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

__all__ = ["ApdbConfig", "Apdb"]

from contextlib import contextmanager
from datetime import datetime
import logging
import numpy as np
import os
import pandas
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple, Type

import lsst.pex.config as pexConfig
from lsst.pex.config import Field, ChoiceField, ListField
from lsst.sphgeom import HtmPixelization, LonLat, Region, UnitVector3d
import sqlalchemy
from sqlalchemy import (func, sql)
from sqlalchemy.pool import NullPool
from . import timer, apdbSchema


_LOG = logging.getLogger(__name__)


class Timer:
    """Timer class defining context manager which tracks execution timing.

    Typical use:

        with Timer("timer_name"):
            do_something

    On exit from block it will print elapsed time.

    See also :py:mod:`timer` module.
    """
    def __init__(self, name: str, do_logging: bool = True, log_before_cursor_execute: bool = False):
        self._log_before_cursor_execute = log_before_cursor_execute
        self._do_logging = do_logging
        self._timer1 = timer.Timer(name)
        self._timer2 = timer.Timer(name + " (before/after cursor)")

    def __enter__(self) -> Timer:
        """
        Enter context, start timer
        """
#         event.listen(engine.Engine, "before_cursor_execute", self._start_timer)
#         event.listen(engine.Engine, "after_cursor_execute", self._stop_timer)
        self._timer1.start()
        return self

    def __exit__(self, exc_type: Optional[Type], exc_val: Any, exc_tb: Any) -> Any:
        """
        Exit context, stop and dump timer
        """
        if exc_type is None:
            self._timer1.stop()
            if self._do_logging:
                self._timer1.dump()
#         event.remove(engine.Engine, "before_cursor_execute", self._start_timer)
#         event.remove(engine.Engine, "after_cursor_execute", self._stop_timer)
        return False

    def _start_timer(self, conn, cursor, statement, parameters, context, executemany):  # type: ignore
        """Start counting"""
        if self._log_before_cursor_execute:
            _LOG.info("before_cursor_execute")
        self._timer2.start()

    def _stop_timer(self, conn, cursor, statement, parameters, context, executemany):  # type: ignore
        """Stop counting"""
        self._timer2.stop()
        if self._do_logging:
            self._timer2.dump()


def _split(seq: Iterable, nItems: int) -> Iterator[List]:
    """Split a sequence into smaller sequences"""
    seq = list(seq)
    while seq:
        yield seq[:nItems]
        del seq[:nItems]


def _coerce_uint64(df: pandas.DataFrame) -> pandas.DataFrame:
    """Change type of the uint64 columns to int64, return copy of data frame.
    """
    names = [c[0] for c in df.dtypes.items() if c[1] == np.uint64]
    return df.astype({name: np.int64 for name in names})


@contextmanager
def _ansi_session(engine: sqlalchemy.engine.Engine) -> Iterator[sqlalchemy.engine.Connection]:
    """Returns a connection, makes sure that ANSI mode is set for MySQL
    """
    with engine.begin() as conn:
        if engine.name == 'mysql':
            conn.execute(sql.text("SET SESSION SQL_MODE = 'ANSI'"))
        yield conn
    return


def _data_file_name(basename: str) -> str:
    """Return path name of a data file.
    """
    return os.path.join("${DAX_APDB_DIR}", "data", basename)


class ApdbConfig(pexConfig.Config):

    db_url = Field(dtype=str, doc="SQLAlchemy database connection URI")
    isolation_level = ChoiceField(dtype=str,
                                  doc="Transaction isolation level",
                                  allowed={"READ_COMMITTED": "Read committed",
                                           "READ_UNCOMMITTED": "Read uncommitted",
                                           "REPEATABLE_READ": "Repeatable read",
                                           "SERIALIZABLE": "Serializable"},
                                  default="READ_COMMITTED",
                                  optional=True)
    connection_pool = Field(dtype=bool,
                            doc=("If False then disable SQLAlchemy connection pool. "
                                 "Do not use connection pool when forking."),
                            default=True)
    connection_timeout = Field(dtype=float,
                               doc="Maximum time to wait time for database lock to be released before "
                                   "exiting. Defaults to sqlachemy defaults if not set.",
                               default=None,
                               optional=True)
    sql_echo = Field(dtype=bool,
                     doc="If True then pass SQLAlchemy echo option.",
                     default=False)
    dia_object_index = ChoiceField(dtype=str,
                                   doc="Indexing mode for DiaObject table",
                                   allowed={'baseline': "Index defined in baseline schema",
                                            'pix_id_iov': "(pixelId, objectId, iovStart) PK",
                                            'last_object_table': "Separate DiaObjectLast table"},
                                   default='baseline')
    dia_object_nightly = Field(dtype=bool,
                               doc="Use separate nightly table for DiaObject",
                               default=False)
    htm_level = Field(dtype=int,
                      doc="HTM indexing level",
                      default=20)
    htm_max_ranges = Field(dtype=int,
                           doc="Max number of ranges in HTM envelope",
                           default=64)
    htm_index_column = Field(dtype=str, default="pixelId",
                             doc="Name of a HTM index column for DiaObject and DiaSource tables")
    ra_dec_columns = ListField(dtype=str, default=["ra", "decl"],
                               doc="Names ra/dec columns in DiaObject table")
    read_sources_months = Field(dtype=int,
                                doc="Number of months of history to read from DiaSource",
                                default=12)
    read_forced_sources_months = Field(dtype=int,
                                       doc="Number of months of history to read from DiaForcedSource",
                                       default=12)
    dia_object_columns = ListField(dtype=str,
                                   doc="List of columns to read from DiaObject, by default read all columns",
                                   default=[])
    object_last_replace = Field(dtype=bool,
                                doc="If True (default) then use \"upsert\" for DiaObjectsLast table",
                                default=True)
    schema_file = Field(dtype=str,
                        doc="Location of (YAML) configuration file with standard schema",
                        default=_data_file_name("apdb-schema.yaml"))
    extra_schema_file = Field(dtype=str,
                              doc="Location of (YAML) configuration file with extra schema",
                              default=_data_file_name("apdb-schema-extra.yaml"))
    prefix = Field(dtype=str,
                   doc="Prefix to add to table names and index names",
                   default="")
    explain = Field(dtype=bool,
                    doc="If True then run EXPLAIN SQL command on each executed query",
                    default=False)
    timer = Field(dtype=bool,
                  doc="If True then print/log timing information",
                  default=False)

    def validate(self) -> None:
        super().validate()
        if self.isolation_level == "READ_COMMITTED" and self.db_url.startswith("sqlite"):
            raise ValueError("Attempting to run Apdb with SQLITE and isolation level 'READ_COMMITTED.' "
                             "Use 'READ_UNCOMMITTED' instead.")
        if len(self.ra_dec_columns) != 2:
            raise ValueError("ra_dec_columns must have exactly two column names")


class Apdb(object):
    """Interface to L1 database, hides all database access details.

    The implementation is configured via standard ``pex_config`` mechanism
    using `ApdbConfig` configuration class. For an example of different
    configurations check config/ folder.

    Parameters
    ----------
    config : `ApdbConfig`
        Configuration object.
    """

    def __init__(self, config: ApdbConfig):

        self.config = config

        # logging.getLogger('sqlalchemy').setLevel(logging.INFO)
        _LOG.debug("APDB Configuration:")
        _LOG.debug("    dia_object_index: %s", self.config.dia_object_index)
        _LOG.debug("    dia_object_nightly: %s", self.config.dia_object_nightly)
        _LOG.debug("    read_sources_months: %s", self.config.read_sources_months)
        _LOG.debug("    read_forced_sources_months: %s", self.config.read_forced_sources_months)
        _LOG.debug("    dia_object_columns: %s", self.config.dia_object_columns)
        _LOG.debug("    object_last_replace: %s", self.config.object_last_replace)
        _LOG.debug("    schema_file: %s", self.config.schema_file)
        _LOG.debug("    extra_schema_file: %s", self.config.extra_schema_file)
        _LOG.debug("    schema prefix: %s", self.config.prefix)

        # engine is reused between multiple processes, make sure that we don't
        # share connections by disabling pool (by using NullPool class)
        kw = dict(echo=self.config.sql_echo)
        conn_args: Dict[str, Any] = dict()
        if not self.config.connection_pool:
            kw.update(poolclass=NullPool)
        if self.config.isolation_level is not None:
            kw.update(isolation_level=self.config.isolation_level)
        if self.config.connection_timeout is not None:
            if self.config.db_url.startswith("sqlite"):
                conn_args.update(timeout=self.config.connection_timeout)
            elif self.config.db_url.startswith(("postgresql", "mysql")):
                conn_args.update(connect_timeout=self.config.connection_timeout)
        kw.update(connect_args=conn_args)
        self._engine = sqlalchemy.create_engine(self.config.db_url, **kw)

        self._schema = apdbSchema.ApdbSchema(engine=self._engine,
                                             dia_object_index=self.config.dia_object_index,
                                             dia_object_nightly=self.config.dia_object_nightly,
                                             schema_file=self.config.schema_file,
                                             extra_schema_file=self.config.extra_schema_file,
                                             prefix=self.config.prefix)

        self.pixelator = HtmPixelization(self.config.htm_level)

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
        tables: List[sqlalchemy.schema.Table] = [
            self._schema.objects, self._schema.sources, self._schema.forcedSources]
        if self.config.dia_object_index == 'last_object_table':
            tables.append(self._schema.objects_last)
        for table in tables:
            stmt = sql.select([func.count()]).select_from(table)
            count = self._engine.scalar(stmt)
            res[table.name] = count

        return res

    def getDiaObjects(self, region: Region) -> pandas.DataFrame:
        """Returns catalog of DiaObject instances from given region.

        Objects are searched based on pixelization index and region is
        determined by the set of indices. There is no assumption on a
        particular type of index, client is responsible for consistency
        when calculating pixelization indices.

        This method returns DataFrame catalog with schema determined by
        the schema of APDB table. Re-mapping of the column names is done for
        some columns (based on column map passed to constructor) but types
        or units are not changed.

        Returns only the last version of each DiaObject.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region to search for DIAObjects.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog containing DiaObject records.
        """

        # decide what columns we need
        table: sqlalchemy.schema.Table
        if self.config.dia_object_index == 'last_object_table':
            table = self._schema.objects_last
        else:
            table = self._schema.objects
        if not self.config.dia_object_columns:
            query = table.select()
        else:
            columns = [table.c[col] for col in self.config.dia_object_columns]
            query = sql.select(columns)

        # build selection
        htm_index_column = table.columns[self.config.htm_index_column]
        exprlist = []
        pixel_ranges = self._htm_indices(region)
        for low, upper in pixel_ranges:
            upper -= 1
            if low == upper:
                exprlist.append(htm_index_column == low)
            else:
                exprlist.append(sql.expression.between(htm_index_column, low, upper))
        query = query.where(sql.expression.or_(*exprlist))

        # select latest version of objects
        if self.config.dia_object_index != 'last_object_table':
            query = query.where(table.c.validityEnd == None)  # noqa: E711

        _LOG.debug("query: %s", query)

        if self.config.explain:
            # run the same query with explain
            self._explain(query, self._engine)

        # execute select
        with Timer('DiaObject select', self.config.timer):
            with self._engine.begin() as conn:
                objects = pandas.read_sql_query(query, conn)
        _LOG.debug("found %s DiaObjects", len(objects))
        return objects

    def getDiaSourcesInRegion(self, region: Region, dt: datetime
                              ) -> Optional[pandas.DataFrame]:
        """Returns catalog of DiaSource instances from given region.

        Sources are searched based on pixelization index and region is
        determined by the set of indices. There is no assumption on a
        particular type of index, client is responsible for consistency
        when calculating pixelization indices.

        This method returns DataFrame catalog with schema determined by
        the schema of APDB table. Re-mapping of the column names is done for
        some columns (based on column map passed to constructor) but types or
        units are not changed.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region to search for DIASources.
        dt : `datetime.datetime`
            Time of the current visit

        Returns
        -------
        catalog : `pandas.DataFrame`, or `None`
            Catalog containing DiaSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0.
        """

        if self.config.read_sources_months == 0:
            _LOG.info("Skip DiaSources fetching")
            return None

        table: sqlalchemy.schema.Table = self._schema.sources
        query = table.select()

        # build selection
        htm_index_column = table.columns[self.config.htm_index_column]
        exprlist = []
        pixel_ranges = self._htm_indices(region)
        for low, upper in pixel_ranges:
            upper -= 1
            if low == upper:
                exprlist.append(htm_index_column == low)
            else:
                exprlist.append(sql.expression.between(htm_index_column, low, upper))
        query = query.where(sql.expression.or_(*exprlist))

        # execute select
        with Timer('DiaSource select', self.config.timer):
            with _ansi_session(self._engine) as conn:
                sources = pandas.read_sql_query(query, conn)
        _LOG.debug("found %s DiaSources", len(sources))
        return sources

    def getDiaSources(self, object_ids: List[int], dt: datetime) -> Optional[pandas.DataFrame]:
        """Returns catalog of DiaSource instances given set of DiaObject IDs.

        This method returns DataFrame catalog with schema determined by
        the schema of APDB table. Re-mapping of the column names is done for
        some columns (based on column map passed to constructor) but types or
        units are not changed.

        Parameters
        ----------
        object_ids :
            Collection of DiaObject IDs
        dt : `datetime.datetime`
            Time of the current visit

        Returns
        -------
        catalog : `pandas.DataFrame`, or `None`
            Catalog contaning DiaSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0 or
            when ``object_ids`` is empty.
        """

        if self.config.read_sources_months == 0:
            _LOG.info("Skip DiaSources fetching")
            return None

        if len(object_ids) <= 0:
            _LOG.info("Skip DiaSources fetching - no Objects")
            # this should create a catalog, but the list of columns may be empty
            return None

        table: sqlalchemy.schema.Table = self._schema.sources
        sources: Optional[pandas.DataFrame] = None
        with Timer('DiaSource select', self.config.timer):
            with _ansi_session(self._engine) as conn:
                for ids in _split(sorted(object_ids), 1000):
                    query = 'SELECT *  FROM "' + table.name + '" WHERE '

                    # select by object id
                    ids_str = ",".join(str(id) for id in ids)
                    query += '"diaObjectId" IN (' + ids_str + ') '

                    # execute select
                    df = pandas.read_sql_query(sql.text(query), conn)
                    if sources is None:
                        sources = df
                    else:
                        sources = sources.append(df)

        _LOG.debug("found %s DiaSources", len(sources) if sources is not None else 0)
        return sources

    def getDiaForcedSources(self, object_ids: List[int], dt: datetime) -> Optional[pandas.DataFrame]:
        """Returns catalog of DiaForcedSource instances matching given
        DiaObjects.

        This method returns DataFrame catalog with schema determined by
        the schema of L1 database table. Re-mapping of the column names may
        be done for some columns (based on column map passed to constructor)
        but types or units are not changed.

        Parameters
        ----------
        object_ids :
            Collection of DiaObject IDs
        dt : `datetime.datetime`
            Time of the current visit

        Returns
        -------
        catalog : `pandas.DataFrame` or `None`
            Catalog contaning DiaForcedSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0 or
            when ``object_ids`` is empty.
        """

        if self.config.read_forced_sources_months == 0:
            _LOG.info("Skip DiaForceSources fetching")
            return None

        if len(object_ids) <= 0:
            _LOG.info("Skip DiaForceSources fetching - no Objects")
            # this should create a catalog, but the list of columns may be empty
            return None

        table: sqlalchemy.schema.Table = self._schema.forcedSources
        sources: Optional[pandas.DataFrame] = None

        with Timer('DiaForcedSource select', self.config.timer):
            with _ansi_session(self._engine) as conn:
                for ids in _split(sorted(object_ids), 1000):

                    query = 'SELECT *  FROM "' + table.name + '" WHERE '

                    # select by object id
                    ids_str = ",".join(str(id) for id in ids)
                    query += '"diaObjectId" IN (' + ids_str + ') '

                    # execute select
                    df = pandas.read_sql_query(sql.text(query), conn)
                    if sources is None:
                        sources = df
                    else:
                        sources = sources.append(df)

        if sources is not None:
            _LOG.debug("found %s DiaForcedSources", len(sources))
        return sources

    def store(self,
              visit_time: datetime,
              objects: pandas.DataFrame,
              sources: Optional[pandas.DataFrame] = None,
              forced_sources: Optional[pandas.DataFrame] = None) -> None:
        """Store all three types of catalogs in the database.

        This methods takes DataFrame catalogs, their schema must be
        compatible with the schema of APDB table:

          - column names must correspond to database table columns
          - types and units of the columns must match database definitions,
            no unit conversion is performed presently
          - columns that have default values in database schema can be
            omitted from catalog
          - this method knows how to fill interval-related columns of DiaObject
            (validityStart, validityEnd) they do not need to appear in a
            catalog
          - source catalogs have ``diaObjectId`` column associating sources
            with objects

        Parameters
        ----------
        visit_time : `datetime.datetime`
            Time of the visit
        objects : `pandas.DataFrame`
            Catalog with DiaObject records
        sources : `pandas.DataFrame`, optional
            Catalog with DiaSource records
        forced_sources : `pandas.DataFrame`, optional
            Catalog with DiaForcedSource records
        """
        # fill pixelId column for DiaObjects
        objects = self._add_obj_htm_index(objects)
        self._storeDiaObjects(objects, visit_time)

        if sources is not None:
            # copy pixelId column from DiaObjects to DiaSources
            sources = self._add_src_htm_index(sources, objects)
            self._storeDiaSources(sources)

        if forced_sources is not None:
            self._storeDiaForcedSources(forced_sources)

    def _storeDiaObjects(self, objs: pandas.DataFrame, dt: datetime) -> None:
        """Store catalog of DiaObjects from current visit.

        This methods takes DataFrame catalog, its schema must be
        compatible with the schema of APDB table:

          - column names must correspond to database table columns
          - types and units of the columns must match database definitions,
            no unit conversion is performed presently
          - columns that have default values in database schema can be
            omitted from catalog
          - this method knows how to fill interval-related columns
            (validityStart, validityEnd) they do not need to appear in
            a catalog

        Parameters
        ----------
        objs : `pandas.DataFrame`
            Catalog with DiaObject records
        dt : `datetime.datetime`
            Time of the visit
        """

        ids = sorted(objs['diaObjectId'])
        _LOG.debug("first object ID: %d", ids[0])

        # NOTE: workaround for sqlite, need this here to avoid
        # "database is locked" error.
        table: sqlalchemy.schema.Table = self._schema.objects

        # everything to be done in single transaction
        with _ansi_session(self._engine) as conn:

            ids_str = ",".join(str(id) for id in ids)

            if self.config.dia_object_index == 'last_object_table':

                # insert and replace all records in LAST table, mysql and postgres have
                # non-standard features
                table = self._schema.objects_last
                do_replace = self.config.object_last_replace
                # If the input data is of type Pandas, we drop the previous
                # objects regardless of the do_replace setting due to how
                # Pandas inserts objects.
                if not do_replace or isinstance(objs, pandas.DataFrame):
                    query = 'DELETE FROM "' + table.name + '" '
                    query += 'WHERE "diaObjectId" IN (' + ids_str + ') '

                    if self.config.explain:
                        # run the same query with explain
                        self._explain(query, conn)

                    with Timer(table.name + ' delete', self.config.timer):
                        res = conn.execute(sql.text(query))
                    _LOG.debug("deleted %s objects", res.rowcount)

                extra_columns: Dict[str, Any] = dict(lastNonForcedSource=dt)
                with Timer("DiaObjectLast insert", self.config.timer):
                    objs = _coerce_uint64(objs)
                    for col, data in extra_columns.items():
                        objs[col] = data
                    objs.to_sql("DiaObjectLast", conn, if_exists='append',
                                index=False)
            else:

                # truncate existing validity intervals
                table = self._schema.objects
                query = 'UPDATE "' + table.name + '" '
                query += "SET \"validityEnd\" = '" + str(dt) + "' "
                query += 'WHERE "diaObjectId" IN (' + ids_str + ') '
                query += 'AND "validityEnd" IS NULL'

                # _LOG.debug("query: %s", query)

                if self.config.explain:
                    # run the same query with explain
                    self._explain(query, conn)

                with Timer(table.name + ' truncate', self.config.timer):
                    res = conn.execute(sql.text(query))
                _LOG.debug("truncated %s intervals", res.rowcount)

            # insert new versions
            if self.config.dia_object_nightly:
                table = self._schema.objects_nightly
            else:
                table = self._schema.objects
            extra_columns = dict(lastNonForcedSource=dt, validityStart=dt,
                                 validityEnd=None)
            with Timer("DiaObject insert", self.config.timer):
                objs = _coerce_uint64(objs)
                for col, data in extra_columns.items():
                    objs[col] = data
                objs.to_sql("DiaObject", conn, if_exists='append',
                            index=False)

    def _storeDiaSources(self, sources: pandas.DataFrame) -> None:
        """Store catalog of DIASources from current visit.

        This methods takes ``DataFrame`` catalog, its schema must be
        compatible with the schema of APDB table:

          - column names must correspond to database table columns
          - types and units of the columns must match database definitions,
            no unit conversion is performed presently
          - columns that have default values in database schema can be
            omitted from catalog

        Parameters
        ----------
        sources : `pandas.DataFrame`
            Catalog containing DiaSource records
        """
        # everything to be done in single transaction
        with _ansi_session(self._engine) as conn:

            with Timer("DiaSource insert", self.config.timer):
                sources = _coerce_uint64(sources)
                sources.to_sql("DiaSource", conn, if_exists='append', index=False)

    def _storeDiaForcedSources(self, sources: pandas.DataFrame) -> None:
        """Store a set of DIAForcedSources from current visit.

        This methods takes DataFrame catalog, its schema must be
        compatible with the schema of APDB table:

          - column names must correspond to database table columns
          - types and units of the columns must match database definitions,
            no unit conversion is performed presently
          - columns that have default values in database schema can be
            omitted from catalog

        Parameters
        ----------
        sources : `pandas.DataFrame`
            Catalog containing DiaForcedSource records
        """

        # everything to be done in single transaction
        with _ansi_session(self._engine) as conn:

            with Timer("DiaForcedSource insert", self.config.timer):
                sources = _coerce_uint64(sources)
                sources.to_sql("DiaForcedSource", conn, if_exists='append', index=False)

    def countUnassociatedObjects(self) -> int:
        """Return the number of DiaObjects that have only one DiaSource associated
        with them.

        Used as part of ap_verify metrics.

        Returns
        -------
        count : `int`
            Number of DiaObjects with exactly one associated DiaSource.
        """
        # Retrieve the DiaObject table.
        table: sqlalchemy.schema.Table = self._schema.objects

        # Construct the sql statement.
        stmt = sql.select([func.count()]).select_from(table).where(table.c.nDiaSources == 1)
        stmt = stmt.where(table.c.validityEnd == None)  # noqa: E711

        # Return the count.
        count = self._engine.scalar(stmt)

        return count

    def isVisitProcessed(self, visitInfo: Any) -> bool:
        """Test whether data from an image has been loaded into the database.

        Used as part of ap_verify metrics.

        Parameters
        ----------
        visitInfo : `lsst.afw.image.VisitInfo`
            The metadata for the image of interest.

        Returns
        -------
        isProcessed : `bool`
            `True` if the data are present, `False` otherwise.
        """
        id = visitInfo.getExposureId()
        table: sqlalchemy.schema.Table = self._schema.sources
        idField = table.c.ccdVisitId

        # Hopefully faster than SELECT DISTINCT
        query = sql.select([idField]).select_from(table) \
            .where(idField == id).limit(1)

        return self._engine.scalar(query) is not None

    def dailyJob(self) -> None:
        """Implement daily activities like cleanup/vacuum.

        What should be done during daily cleanup is determined by
        configuration/schema.
        """

        # move data from DiaObjectNightly into DiaObject
        if self.config.dia_object_nightly:
            with _ansi_session(self._engine) as conn:
                query = 'INSERT INTO "' + self._schema.objects.name + '" '  # type:ignore
                query += 'SELECT * FROM "' + self._schema.objects_nightly.name + '"'  # type:ignore
                with Timer('DiaObjectNightly copy', self.config.timer):
                    conn.execute(sql.text(query))

                query = 'DELETE FROM "' + self._schema.objects_nightly.name + '"'  # type:ignore
                with Timer('DiaObjectNightly delete', self.config.timer):
                    conn.execute(sql.text(query))

        if self._engine.name == 'postgresql':

            # do VACUUM on all tables
            _LOG.info("Running VACUUM on all tables")
            connection = self._engine.raw_connection()
            ISOLATION_LEVEL_AUTOCOMMIT = 0
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = connection.cursor()
            cursor.execute("VACUUM ANALYSE")

    def makeSchema(self, drop: bool = False, mysql_engine: str = 'InnoDB') -> None:
        """Create or re-create all tables.

        Parameters
        ----------
        drop : `bool`
            If True then drop tables before creating new ones.
        mysql_engine : `str`, optional
            Name of the MySQL engine to use for new tables.
        """
        self._schema.makeSchema(drop=drop, mysql_engine=mysql_engine)

    def _explain(self, query: str, conn: sqlalchemy.engine.Connection) -> None:
        """Run the query with explain
        """

        _LOG.info("explain for query: %s...", query[:64])

        if conn.engine.name == 'mysql':
            query = "EXPLAIN EXTENDED " + query
        else:
            query = "EXPLAIN " + query

        res = conn.execute(sql.text(query))
        if res.returns_rows:
            _LOG.info("explain: %s", res.keys())
            for row in res:
                _LOG.info("explain: %s", row)
        else:
            _LOG.info("EXPLAIN returned nothing")

    def _htm_indices(self, region: Region) -> List[Tuple[int, int]]:
        """Generate a set of HTM indices covering specified field of view.

        Parameters
        ----------
        region: `sphgeom.Region`
            Region that needs to be indexed

        Returns
        -------
        Sequence of ranges, range is a tuple (minHtmID, maxHtmID).
        """
        _LOG.debug('region: %s', region)
        indices = self.pixelator.envelope(region, self.config.htm_max_ranges)

        if _LOG.isEnabledFor(logging.DEBUG):
            for irange in indices.ranges():
                _LOG.debug('range: %s %s', self.pixelator.toString(irange[0]),
                           self.pixelator.toString(irange[1]))

        return indices.ranges()

    def _add_obj_htm_index(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """Calculate HTM index for each record and add it to a DataFrame.

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

        htm_index = np.zeros(sources.shape[0], dtype=np.int64)
        for i, diaObjId in enumerate(sources["diaObjectId"]):
            htm_index[i] = pixel_id_map[diaObjId]
        sources = sources.copy()
        sources[self.config.htm_index_column] = htm_index
        return sources
