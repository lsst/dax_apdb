# This file is part of dax_ppdb.
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

"""Module defining Ppdb class and related methods.
"""

__all__ = ["PpdbCassandraConfig", "PpdbCassandra", "Visit"]

from collections import namedtuple
from datetime import datetime, timedelta
import logging
import numpy as np

from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
import cassandra.query
import lsst.afw.table as afwTable
import lsst.geom as geom
from lsst.pex.config import Field, ListField
from . import timer
from .ppdbCassandraSchema import PpdbCassandraSchema, PpdbCassandraSchemaConfig


_LOG = logging.getLogger(__name__.partition(".")[2])  # strip leading "lsst."


def _pixelId2partition(pixelId):
    """Converts pixelID (HTM index) into partition number.

    This assumes that pixelId indexing is HTM level=20. Partitoning is done
    with HTM level=8.
    """
    htm20 = pixelId
    htm8 = htm20 >> 24
    part = htm8 & 0xf
    return part


class Timer(object):
    """Timer class defining context manager which tracks execution timing.

    Typical use:

        with Timer("timer_name"):
            do_something

    On exit from block it will print elapsed time.

    See also :py:mod:`timer` module.
    """
    def __init__(self, name, do_logging=True, log_before_cursor_execute=False):
        self._log_before_cursor_execute = log_before_cursor_execute
        self._do_logging = do_logging
        self._timer1 = timer.Timer(name)
        self._timer2 = timer.Timer(name + " (before/after cursor)")

    def __enter__(self):
        """
        Enter context, start timer
        """
#         event.listen(engine.Engine, "before_cursor_execute", self._start_timer)
#         event.listen(engine.Engine, "after_cursor_execute", self._stop_timer)
        self._timer1.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
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

    def _start_timer(self, conn, cursor, statement, parameters, context, executemany):
        """Start counting"""
        if self._log_before_cursor_execute:
            _LOG.info("before_cursor_execute")
        self._timer2.start()

    def _stop_timer(self, conn, cursor, statement, parameters, context, executemany):
        """Stop counting"""
        self._timer2.stop()
        if self._do_logging:
            self._timer2.dump()


def _split(seq, nItems):
    """Split a sequence into smaller sequences"""
    seq = list(seq)
    while seq:
        yield seq[:nItems]
        del seq[:nItems]


# Information about single visit
Visit = namedtuple('Visit', 'visitId visitTime lastObjectId lastSourceId')


class PpdbCassandraConfig(PpdbCassandraSchemaConfig):

    contact_points = ListField(dtype=str,
                               doc="The list of contact points to try connecting for cluster discovery.",
                               default=["127.0.0.1"])
    keyspace = Field(dtype=str,
                     doc="Default keyspace for operations.",
                     default="PPDB")

    read_sources_months = Field(dtype=int,
                                doc="Number of months of history to read from DiaSource",
                                default=12)
    read_forced_sources_months = Field(dtype=int,
                                       doc="Number of months of history to read from DiaForcedSource",
                                       default=12)
    dia_object_columns = ListField(dtype=str,
                                   doc="List of columns to read from DiaObject, by default read all columns",
                                   default=[])
    timer = Field(dtype=bool,
                  doc="If True then print/log timing information",
                  default=False)


class PpdbCassandra:
    """Implementation of PPDB database on to of Apache Cassandra.

    The implementation is configured via standard ``pex_config`` mechanism
    using `PpdbCassandra` configuration class. For an example of different
    configurations check config/ folder.

    Parameters
    ----------
    config : `PpdbCassandra`
    afw_schemas : `dict`, optional
        Dictionary with table name for a key and `afw.table.Schema`
        for a value. Columns in schema will be added to standard
        PPDB schema.
    """

    def __init__(self, config, afw_schemas=None):

        self.config = config

        # logging.getLogger('sqlalchemy').setLevel(logging.INFO)
        _LOG.debug("PpdbCassandra Configuration:")
        _LOG.debug("    read_sources_months: %s", self.config.read_sources_months)
        _LOG.debug("    read_forced_sources_months: %s", self.config.read_forced_sources_months)
        _LOG.debug("    dia_object_columns: %s", self.config.dia_object_columns)
        _LOG.debug("    schema_file: %s", self.config.schema_file)
        _LOG.debug("    extra_schema_file: %s", self.config.extra_schema_file)
        _LOG.debug("    column_map: %s", self.config.column_map)
        _LOG.debug("    schema prefix: %s", self.config.prefix)

        self._cluster = Cluster(contact_points=self.config.contact_points,
                                load_balancing_policy=RoundRobinPolicy())
        self._session = self._cluster.connect(keyspace=config.keyspace)
        self._session.row_factory = cassandra.query.named_tuple_factory

        self._schema = PpdbCassandraSchema(session=self._session,
                                           config=self.config,
                                           afw_schemas=afw_schemas)

    def makeSchema(self, drop=False, **kw):
        """Create or re-create all tables.

        Parameters
        ----------
        drop : `bool`
            If True then drop tables before creating new ones.
        """
        self._schema.makeSchema(drop=drop)

    def lastVisit(self):
        """Returns last visit information or `None` if visits table is empty.

        Visits table is used by ap_proto to track visit information, it is
        not a part of the regular PPDB schema.

        Returns
        -------
        visit : `Visit` or `None`
            Last stored visit info or `None` if there was nothing stored yet.
        """
        query = 'SELECT MAX("visitId"), MAX("visitTime") FROM "{}" WHERE "ppdb_part" = 0'.format(
                self._schema.visitTableName)
        rows = self._session.execute(query)
        for row in rows:
            _LOG.debug("lastVisit: row = %s", row)
            visitId, visitTime = row
            if visitId is None:
                return None
            break
        else:
            # no rows
            return None

        # TODO: This is super-inefficient
        lastObjectId = 0
        query = 'SELECT MAX("diaObjectId") FROM "{}"'.format(self._schema.objectTableName)
        rows = self._session.execute(query, timeout=300)
        for row in rows:
            if row[0] is not None:
                lastObjectId = row[0]

        lastSourceId = 0
        query = 'SELECT MAX("diaSourceId") FROM "{}"'.format(self._schema.sourceTableName)
        rows = self._session.execute(query, timeout=300)
        for row in rows:
            if row[0] is not None:
                lastSourceId = row[0]

        return Visit(visitId=visitId, visitTime=visitTime,
                     lastObjectId=lastObjectId, lastSourceId=lastSourceId)

    def saveVisit(self, visitId, visitTime):
        """Store visit information.

        This method is only used by ``ap_proto`` script from ``l1dbproto``
        and is not intended for production pipelines.

        Parameters
        ----------
        visitId : `int`
            Visit identifier
        visitTime : `datetime.datetime`
            Visit timestamp.
        """
        # Cassandra timestamps is in milliseconds since UTC
        timestamp = int((visitTime - datetime(1970, 1, 1)) / timedelta(seconds=1))*1000
        query = 'INSERT INTO "{}" ("ppdb_part", "visitId", "visitTime") VALUES (0, {}, {})'.format(
                self._schema.visitTableName, visitId, timestamp)
        _LOG.debug("saveVisit: query = %s", query)
        self._session.execute(query)

    def tableRowCount(self):
        """Returns dictionary with the table names and row counts.

        Used by ``ap_proto`` to keep track of the size of the database tables.
        Depending on database technology this could be expensive operation.

        Returns
        -------
        row_counts : `dict`
            Dict where key is a table name and value is a row count.
        """
        # We probably do not want it ever implemented for Cassandra
        return {}

    def getDiaObjects(self, pixel_ranges, return_pandas=False):
        """Returns catalog of DiaObject instances from given region.

        Objects are searched based on pixelization index and region is
        determined by the set of indices. There is no assumption on a
        particular type of index, client is responsible for consistency
        when calculating pixelization indices.

        This method returns :doc:`/modules/lsst.afw.table/index` catalog with schema determined by
        the schema of PPDB table. Re-mapping of the column names is done for
        some columns (based on column map passed to constructor) but types
        or units are not changed.

        Returns only the last version of each DiaObject.

        Parameters
        ----------
        pixel_ranges : `list` of `tuple`
            Sequence of ranges, range is a tuple (minPixelID, maxPixelID).
            This defines set of pixel indices to be included in result.
        return_pandas : `bool`
            Return a `pandas.DataFrame` instead of
            `lsst.afw.table.SourceCatalog`.

        Returns
        -------
        catalog : `lsst.afw.table.SourceCatalog` or `pandas.DataFrame`
            Catalog containing DiaObject records.
        """
        # Need a separate query for each partition and pixelId range
        queries = []
        for lower, upper in pixel_ranges:
            # need to be careful with inclusive/exclusive ranges
            part_low = _pixelId2partition(lower)
            part_high = _pixelId2partition(upper-1)
            for part in range(part_low, part_high+1):
                if lower + 1 == upper:
                    expr = '"ppdb_part" = {} AND "pixelId" = {}'.format(part, lower)
                else:
                    expr = '"ppdb_part" = {} AND "pixelId" >= {} AND "pixelId" < {}'
                    expr = expr.format(part, lower, upper)
                query = 'SELECT * from "DiaObjectLast" WHERE ' + expr
                queries.append(query)
        _LOG.debug("getDiaObjects: #queries: %s", len(queries))

        objects = None
        with Timer('DiaObject select', self.config.timer):
            futures = [self._session.execute_async(query) for query in queries]
            for future in futures:
                rows = future.result()
                objects = self._convertResult(rows, "DiaObject", catalog=objects)
        return objects

    def getDiaSourcesInRegion(self, pixel_ranges, dt, return_pandas=False):
        """Returns catalog of DiaSource instances from given region.

        Sources are searched based on pixelization index and region is
        determined by the set of indices. There is no assumption on a
        particular type of index, client is responsible for consistency
        when calculating pixelization indices.

        This method returns :doc:`/modules/lsst.afw.table/index` catalog with schema determined by
        the schema of PPDB table. Re-mapping of the column names is done for
        some columns (based on column map passed to constructor) but types or
        units are not changed.

        Parameters
        ----------
        pixel_ranges : `list` of `tuple`
            Sequence of ranges, range is a tuple (minPixelID, maxPixelID).
            This defines set of pixel indices to be included in result.
        dt : `datetime.datetime`
            Time of the current visit
        return_pandas : `bool`
            Return a `pandas.DataFrame` instead of
            `lsst.afw.table.SourceCatalog`.

        Returns
        -------
        catalog : `lsst.afw.table.SourceCatalog`, `pandas.DataFrame`, or `None`
            Catalog containing DiaSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0.
        """
        sources = self._convertResult([], "DiaSource")
        return sources

    def getDiaSources(self, object_ids, dt, return_pandas=False):
        """Returns catalog of DiaSource instances given set of DiaObject IDs.

        This method returns :doc:`/modules/lsst.afw.table/index` catalog with schema determined by
        the schema of PPDB table. Re-mapping of the column names is done for
        some columns (based on column map passed to constructor) but types or
        units are not changed.

        Parameters
        ----------
        object_ids :
            Collection of DiaObject IDs
        dt : `datetime.datetime`
            Time of the current visit
        return_pandas : `bool`
            Return a `pandas.DataFrame` instead of
            `lsst.afw.table.SourceCatalog`.


        Returns
        -------
        catalog : `lsst.afw.table.SourceCatalog`, `pandas.DataFrame`, or `None`
            Catalog contaning DiaSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0 or
            when ``object_ids`` is empty.
        """
        sources = self._convertResult([], "DiaSource")
        return sources

    def getDiaForcedSources(self, object_ids, dt, return_pandas=False):
        """Returns catalog of DiaForcedSource instances matching given
        DiaObjects.

        This method returns :doc:`/modules/lsst.afw.table/index` catalog with schema determined by
        the schema of L1 database table. Re-mapping of the column names may
        be done for some columns (based on column map passed to constructor)
        but types or units are not changed.

        Parameters
        ----------
        object_ids :
            Collection of DiaObject IDs
        dt : `datetime.datetime`
            Time of the current visit
        return_pandas : `bool`
            Return a `pandas.DataFrame` instead of
            `lsst.afw.table.SourceCatalog`.

        Returns
        -------
        catalog : `lsst.afw.table.SourceCatalog` or `None`
            Catalog contaning DiaForcedSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0 or
            when ``object_ids`` is empty.
        """
        sources = self._convertResult([], "DiaForcedSource")
        return sources

    def storeDiaObjects(self, objs, dt):
        """Store catalog of DiaObjects from current visit.

        This methods takes :doc:`/modules/lsst.afw.table/index` catalog, its schema must be
        compatible with the schema of PPDB table:

          - column names must correspond to database table columns
          - some columns names are re-mapped based on column map passed to
            constructor
          - types and units of the columns must match database definitions,
            no unit conversion is performed presently
          - columns that have default values in database schema can be
            omitted from afw schema
          - this method knows how to fill interval-related columns
            (validityStart, validityEnd) they do not need to appear in
            afw schema

        Parameters
        ----------
        objs : `lsst.afw.table.BaseCatalog` or `pandas.DataFrame`
            Catalog with DiaObject records
        dt : `datetime.datetime`
            Time of the visit
        """
        extra_columns = dict(lastNonForcedSource=dt)
        self._storeObjectsAfw(objs, "DiaObjectLast", extra_columns=extra_columns)

        extra_columns = dict(lastNonForcedSource=dt, validityStart=dt, validityEnd=None)
        self._storeObjectsAfw(objs, "DiaObject", extra_columns=extra_columns)

    def storeDiaSources(self, sources):
        """Store catalog of DIASources from current visit.

        This methods takes :doc:`/modules/lsst.afw.table/index` catalog, its schema must be
        compatible with the schema of L1 database table:

          - column names must correspond to database table columns
          - some columns names may be re-mapped based on column map passed to
            constructor
          - types and units of the columns must match database definitions,
            no unit conversion is performed presently
          - columns that have default values in database schema can be
            omitted from afw schema

        Parameters
        ----------
        sources : `lsst.afw.table.BaseCatalog` or `pandas.DataFrame`
            Catalog containing DiaSource records
        """
        self._storeObjectsAfw(sources, "DiaSource")

    def storeDiaForcedSources(self, sources):
        """Store a set of DIAForcedSources from current visit.

        This methods takes :doc:`/modules/lsst.afw.table/index` catalog, its schema must be
        compatible with the schema of L1 database table:

          - column names must correspond to database table columns
          - some columns names may be re-mapped based on column map passed to
            constructor
          - types and units of the columns must match database definitions,
            no unit conversion is performed presently
          - columns that have default values in database schema can be
            omitted from afw schema

        Parameters
        ----------
        sources : `lsst.afw.table.BaseCatalog` or `pandas.DataFrame`
            Catalog containing DiaForcedSource records
        """
        self._storeObjectsAfw(sources, "DiaForcedSource")

    def dailyJob(self):
        """Implement daily activities like cleanup/vacuum.

        What should be done during daily cleanup is determined by
        configuration/schema.
        """
        pass

    def _convertResult(self, res, table_name, catalog=None):
        """Convert result set into output catalog.

        Parameters
        ----------
        res : `cassandra.cluster.ResultSet`
            Cassandra result set returned by query.
        table_name : `str`
            Name of the table.
        catalog : `lsst.afw.table.BaseCatalog`
            If not None then extend existing catalog

        Returns
        -------
        catalog : `lsst.afw.table.SourceCatalog`
             If ``catalog`` is None then new instance is returned, otherwise
             ``catalog`` is updated and returned.
        """
        # make catalog schema
        schema, col_map = self._schema.getAfwSchema(table_name)
        if catalog is None:
            # _LOG.debug("_convertResult: schema: %s", schema)
            # _LOG.debug("_convertResult: col_map: %s", col_map)
            catalog = afwTable.SourceCatalog(schema)

        # fill catalog
        for row in res:
            record = catalog.addNew()
            for col, value in zip(row._fields, row):
                # some columns may exist in database but not included in afw schema
                col = col_map.get(col)
                if col is not None:
                    if isinstance(value, datetime):
                        # convert datetime to number of seconds
                        value = int((value - datetime.utcfromtimestamp(0)).total_seconds())
                    elif col.getTypeString() == 'Angle' and value is not None:
                        value = value * geom.degrees
                    if value is not None:
                        record.set(col, value)

        return catalog

    def _storeObjectsAfw(self, objects, table_name, extra_columns=None):
        """Generic store method.

        Takes catalog of records and stores a bunch of objects in a table.

        Parameters
        ----------
        objects : `lsst.afw.table.BaseCatalog`
            Catalog containing object records
        table_name : `str`
            Name of the table as defined in PPDB schema.
        extra_columns : `dict`, optional
            Mapping (column_name, column_value) which gives column values to add
            to every row, only if column is missing in catalog records.
        """

        def quoteValue(v):
            """Quote and escape values"""
            if v is None:
                v = "NULL"
            elif isinstance(v, datetime):
                v = "'" + str(v) + "'"
            elif isinstance(v, str):
                # we don't expect nasty stuff in strings
                v = "'" + v + "'"
            elif isinstance(v, geom.Angle):
                v = v.asDegrees()
                if np.isfinite(v):
                    v = str(v)
                else:
                    v = "NULL"
            else:
                if np.isfinite(v):
                    v = str(v)
                else:
                    v = "NULL"
            return v

        def quoteId(columnName):
            """Smart quoting for column names.
            Lower-case names are not quoted.
            """
            if not columnName.islower():
                columnName = '"' + columnName + '"'
            return columnName

        schema = objects.getSchema()
        # use extra columns if specified
        extra_fields = list((extra_columns or {}).keys())

        afw_fields = [field.getName() for key, field in schema
                      if field.getName() not in extra_fields]

        column_map = self._schema.getAfwColumns(table_name)
        # list of columns (as in cat schema)
        fields = [column_map[field].name for field in afw_fields if field in column_map]
        fields += extra_fields

        # some partioning columns may not be in afwtable, they need to be added explicitly
        part_columns = self._schema.partitionColumns(table_name)
        part_columns = [column for column in part_columns if column not in fields]
        fields += part_columns

        qfields = ','.join([quoteId(field) for field in fields])

        with Timer(table_name + ' query build', self.config.timer):
            queries = ["BEGIN BATCH"]
            for rec in objects:
                values = []
                for field in afw_fields:
                    if field not in column_map:
                        continue
                    value = rec[field]
                    if column_map[field].type == "DATETIME" and np.isfinite(value):
                        # CAssandra datetime is in millisconds
                        value = int(value * 1000)
                    values.append(quoteValue(value))
                for field in extra_fields:
                    values.append(quoteValue(extra_columns[field]))
                if part_columns:
                    part_values = self._partitionValues(rec, table_name, part_columns)
                    values += [quoteValue(val) for val in part_values]
                values = ','.join(values)
                query = 'INSERT INTO "{}" ({}) VALUES ({});'.format(self._schema.tableName(table_name),
                                                                    qfields, values)
                queries.append(query)

            queries.append("APPLY BATCH;")
            query = '\n'.join(queries)
            _LOG.info("%s: query size: %d", self._schema.tableName(table_name), len(query))

        # _LOG.debug("query: %s", query)
        _LOG.info("%s: will store %d records", self._schema.tableName(table_name), len(objects))
        with Timer(table_name + ' insert', self.config.timer):
            self._session.execute(query)

    def _partitionValues(self, rec, table_name, part_columns):
        """Return values of partition columns for a record.

        Parameters
        ----------
        rec : `afw.table.Record`
            Single record from a catalog.
        table_name : `str`
            Table name as defined in PPDB schema.
        part_columns : `list` of `str`
            Names of the columns for which to return values.

        Returns
        -------
        values : `list`
            List of column values.
        """

        if table_name in ("DiaObject", "DiaObjectLast", "DiaSource", "DiaForcedSource"):
            if part_columns != ["ppdb_part"]:
                raise ValueError("unexpected partitionig columns for {}: {}".format(
                    table_name, part_columns))
            # TODO: expecting level=20 for HTM, need to check
            part = _pixelId2partition(rec["pixelId"])
            return [part]
        else:
            raise ValueError("unexpected table {}".format(table_name))
