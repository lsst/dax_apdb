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

__all__ = ["ApdbCassandraConfig", "ApdbCassandra", "Visit"]

from collections import namedtuple
from datetime import datetime, timedelta
import logging
import numpy as np
import pandas
import random
import string

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy, WhiteListRoundRobinPolicy, AddressTranslator
import cassandra.query
import lsst.afw.table as afwTable
import lsst.geom as geom
from lsst.pex.config import ChoiceField, Field, ListField
from lsst.sphgeom import HtmPixelization, Mq3cPixelization, Q3cPixelization
from . import timer
from .apdbCassandraSchema import ApdbCassandraSchema, ApdbCassandraSchemaConfig


_LOG = logging.getLogger(__name__.partition(".")[2])  # strip leading "lsst."

SECONDS_IN_MONTH = 30*24*3600


def _pixelId2partition(pixelId):
    """Converts pixelID (HTM index) into partition number.

    This assumes that pixelId indexing is HTM level=20. Partitoning is done
    with HTM level=8.
    """
    htm20 = pixelId
    htm8 = htm20 >> 24
    part = htm8
    # part = part & 0xf
    return part


def _filterObjectIds(rows, object_ids):
    """Generator which filters rows returned from query based on diaObjectId.

    Parameters
    ----------
    rows : iterable of `namedtuple`
        Rows returned from database query.

    Yields
    ------
    row : `namedtuple`
        Filtered rows.
    """
    object_id_set = set(object_ids)
    for row in rows:
        if row.diaObjectId in object_id_set:
            yield row


def _nrows(table):
    if isinstance(table, pandas.DataFrame):
        return table.shape[0]
    else:
        return len(table)


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


class ApdbCassandraConfig(ApdbCassandraSchemaConfig):

    contact_points = ListField(dtype=str,
                               doc="The list of contact points to try connecting for cluster discovery.",
                               default=["127.0.0.1"])
    private_ips = ListField(dtype=str,
                            doc="List of internal IP addresses for contact_points.",
                            default=[])
    keyspace = Field(dtype=str,
                     doc="Default keyspace for operations.",
                     default="apdb")
    read_consistency = Field(dtype=str,
                             doc="Name for consistency level of read operations, defalut: QUORUM, can be ONE.",
                             default="QUORUM")
    write_consistency = Field(dtype=str,
                              doc="Name for consistency level of write operations, defalut: QUORUM, can be ONE.",
                              default="QUORUM")
    protocol_version = Field(dtype=int,
                             doc="Cassandra protocol version to use, default is V4",
                             default=cassandra.ProtocolVersion.V4)
    read_sources_months = Field(dtype=int,
                                doc="Number of months of history to read from DiaSource",
                                default=12)
    read_forced_sources_months = Field(dtype=int,
                                       doc="Number of months of history to read from DiaForcedSource",
                                       default=12)
    dia_object_columns = ListField(dtype=str,
                                   doc="List of columns to read from DiaObject, by default read all columns",
                                   default=[])
    part_pixelization = ChoiceField(dtype=str,
                                    allowed=dict(htm="HTM pixelization", q3c="Q3C pixelization",
                                                 mq3c="MQ3C pixelization"),
                                    doc="Pixelization used for patitioning index.",
                                    default="mq3c")
    part_pix_level = Field(dtype=int,
                           doc="Pixelization level used for patitioning index.",
                           default=10)
    timer = Field(dtype=bool,
                  doc="If True then print/log timing information",
                  default=False)
    fillEmptyFields = Field(dtype=bool,
                            doc=("If True then store random values for fields not explicitly filled, "
                                 "for testing only"),
                            default=False)


class Partitioner:
    """Class that caclulates indices of the objects for paritioning.

    Used internally by `ApdbCassandra`

    Parameters
    ----------
    config : `ApdbCassandraConfig`
    """
    def __init__(self, config):
        pix = config.part_pixelization
        if pix == "htm":
            self.pixelator = HtmPixelization(config.part_pix_level)
        elif pix == "q3c":
            self.pixelator = Q3cPixelization(config.part_pix_level)
        elif pix == "mq3c":
            self.pixelator = Mq3cPixelization(config.part_pix_level)
        else:
            raise ValueError(f"unknown pixelization: {pix}")

    def pixels(self, region):
        """Compute set of the pixel indices for given region.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
        """
        # we want finest set of pixels, so ask as many pixel as possible
        ranges = self.pixelator.envelope(region, 1_000_000)
        indices = []
        for lower, upper in ranges:
            indices += list(range(lower, upper))
        return indices

    def pixel(self, direction):
        """Compute the index of the pixel for given direction.

        Parameters
        ----------
        direction : `lsst.sphgeom.UnitVector3d`
        """
        index = self.pixelator.index(direction)
        return index


class _AddressTranslator(AddressTranslator):
    """Translate internal IP address to external.

    Only used for docker-based setup, not viable long-term solution.
    """
    def __init__(self, public_ips, private_ips):
        self._map = dict((k, v) for k, v in zip(private_ips, public_ips))

    def translate(self, private_ip):
        return self._map.get(private_ip, private_ip)


class _PandasRowFactory:
    """Create pandas DataFrame from Cassandra result set.
    """
    def __call__(self, colnames, rows):
        """Convert result set into output catalog.

        Parameters
        ----------
        colname : `list` for `str`
            Names of the columns.
        rows : `list` of `tuple`
            Result rows

        Returns
        -------
        catalog : `pandas.DataFrame`
            DataFrame with the result set.
        """
        return pandas.DataFrame.from_records(rows, columns=colnames)


class ApdbCassandra:
    """Implementation of APDB database on to of Apache Cassandra.

    The implementation is configured via standard ``pex_config`` mechanism
    using `ApdbCassandra` configuration class. For an example of different
    configurations check config/ folder.

    Parameters
    ----------
    config : `ApdbCassandra`
    afw_schemas : `dict`, optional
        Dictionary with table name for a key and `afw.table.Schema`
        for a value. Columns in schema will be added to standard
        APDB schema.
    """

    def __init__(self, config, afw_schemas=None):

        self.config = config

        # logging.getLogger('sqlalchemy').setLevel(logging.INFO)
        _LOG.debug("ApdbCassandra Configuration:")
        _LOG.debug("    read_sources_months: %s", self.config.read_sources_months)
        _LOG.debug("    read_forced_sources_months: %s", self.config.read_forced_sources_months)
        _LOG.debug("    dia_object_columns: %s", self.config.dia_object_columns)
        _LOG.debug("    schema_file: %s", self.config.schema_file)
        _LOG.debug("    extra_schema_file: %s", self.config.extra_schema_file)
        _LOG.debug("    column_map: %s", self.config.column_map)
        _LOG.debug("    schema prefix: %s", self.config.prefix)
        _LOG.debug("    part_pixelization: %s", self.config.part_pixelization)
        _LOG.debug("    part_pix_level: %s", self.config.part_pix_level)

        self._partitioner = Partitioner(config)

        if config.private_ips:
            loadBalancePolicy = WhiteListRoundRobinPolicy(hosts=config.contact_points)
            addressTranslator = _AddressTranslator(config.contact_points, config.private_ips)
        else:
            loadBalancePolicy = RoundRobinPolicy()
            addressTranslator = None

        self._read_consistency = getattr(ConsistencyLevel, config.read_consistency)
        self._write_consistency = getattr(ConsistencyLevel, config.write_consistency)

        self._cluster = Cluster(contact_points=self.config.contact_points,
                                load_balancing_policy=loadBalancePolicy,
                                address_translator=addressTranslator,
                                protocol_version=self.config.protocol_version)
        self._session = self._cluster.connect(keyspace=config.keyspace)
        self._session.row_factory = cassandra.query.named_tuple_factory

        self._schema = ApdbCassandraSchema(session=self._session,
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
        not a part of the regular APDB schema.

        Returns
        -------
        visit : `Visit` or `None`
            Last stored visit info or `None` if there was nothing stored yet.
        """
        self._session.row_factory = cassandra.query.tuple_factory

        query = 'SELECT MAX("visitId") FROM "{}" WHERE "apdb_part" = 0'.format(
                self._schema.visitTableName)
        rows = self._session.execute(query)
        for row in rows:
            _LOG.debug("lastVisit: row = %s", row)
            visitId = row[0]
            if visitId is None:
                return None
            break
        else:
            # no rows
            return None

        query = """SELECT "visitTime", "lastObjectId", "lastSourceId" FROM "{}"
                WHERE "apdb_part" = %s AND "visitId" = %s""".format(self._schema.visitTableName)
        rows = self._session.execute(query, [0, visitId])
        for row in rows:
            _LOG.debug("lastVisit: row = %s", row)
            visitTime, lastObjectId, lastSourceId = row
            if visitTime is None:
                return None
            return Visit(visitId=visitId, visitTime=visitTime,
                         lastObjectId=lastObjectId, lastSourceId=lastSourceId)
        else:
            # no rows
            return None

    def saveVisit(self, visitId, visitTime, lastObjectId, lastSourceId):
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
        query = """INSERT INTO "{}" ("apdb_part", "visitId", "visitTime", "lastObjectId", "lastSourceId")
                VALUES (%s, %s, %s, %s, %s)""".format(self._schema.visitTableName)
        params = (0, visitId, timestamp, lastObjectId, lastSourceId)
        _LOG.debug("saveVisit: params = %s", params)
        self._session.execute(query, params)

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

    def getDiaObjects(self, region, return_pandas=True):
        """Returns catalog of DiaObject instances from given region.

        Returned catalog can contain DiaObjects that are outside specified
        region, it is client responsibility to filter objects if necessary.

        This method returns :doc:`/modules/lsst.afw.table/index` catalog with schema determined by
        the schema of APDB table. Re-mapping of the column names is done for
        some columns (based on column map passed to constructor) but types
        or units are not changed.

        Returns only the last version of each DiaObject.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Spherical region.
        return_pandas : `bool`
            Return a `pandas.DataFrame` instead of
            `lsst.afw.table.SourceCatalog`.

        Returns
        -------
        catalog : `lsst.afw.table.SourceCatalog` or `pandas.DataFrame`
            Catalog containing DiaObject records.
        """
        if return_pandas:
            self._session.row_factory = _PandasRowFactory()
        else:
            self._session.row_factory = cassandra.query.named_tuple_factory
        self._session.default_fetch_size = None

        pixels = self._partitioner.pixels(region)
        _LOG.info("getDiaObjects: #partitions: %s", len(pixels))
        pixels = ",".join([str(pix) for pix in pixels])

        queries = []
        query = f'SELECT * from "DiaObjectLast" WHERE "apdb_part" IN ({pixels})'
        queries += [(cassandra.query.SimpleStatement(query, consistency_level=self._read_consistency), {})]
        _LOG.info("getDiaObjects: #queries: %s", len(queries))
        # _LOG.debug("getDiaObjects: queries: %s", queries)

        objects = None
        with Timer('DiaObject select', self.config.timer):
            # submit all queries
            futures = [self._session.execute_async(query, values, timeout=120.) for query, values in queries]
            if return_pandas:
                # TODO: This orders result processing which is not very efficient
                dataframes = [future.result()._current_rows for future in futures]
                # concatenate all frames
                if len(dataframes) == 1:
                    objects = dataframes[0]
                else:
                    objects = pandas.concat(dataframes)
            else:
                for future in futures:
                    rows = future.result()
                    objects = self._convertResult(rows, "DiaObject", catalog=objects)

        _LOG.debug("found %s DiaObjects", _nrows(objects))
        return objects

    def getDiaSources(self, region, object_ids, dt, return_pandas=True):
        """Returns catalog of DiaSource instances given a region and a set of
        DiaObject IDs.

        This method returns :doc:`/modules/lsst.afw.table/index` catalog with schema determined by
        the schema of APDB table. Re-mapping of the column names is done for
        some columns (based on column map passed to constructor) but types or
        units are not changed.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Spherical region.
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

        Note
        ----
        Implementation can chose to query database using either region or
        Object IDs (or both). For performance reasons returned set of sources
        can contain instances that are either ouside of the region or do not
        match DiaObject IDs in the input set. It is client responsibility to
        filter the returned catalog if necessary.
        """
        return self._getSources(region, object_ids, dt, "DiaSource",
                                self.config.read_sources_months, return_pandas)

    def getDiaForcedSources(self, region, object_ids, dt, return_pandas=False):
        """Returns catalog of DiaForcedSource instances given a region and a
        set of DiaObject IDs.

        This method returns :doc:`/modules/lsst.afw.table/index` catalog with schema determined by
        the schema of L1 database table. Re-mapping of the column names may
        be done for some columns (based on column map passed to constructor)
        but types or units are not changed.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Spherical region.
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

        Note
        ----
        Implementation can chose to query database using either region or
        Object IDs (or both). For performance reasons returned set of sources
        can contain instances that are either ouside of the region or do not
        match DiaObject IDs in the input set. It is client responsibility to
        filter the returned catalog if necessary.
        """
        return self._getSources(region, object_ids, dt, "DiaForcedSource",
                                self.config.read_forced_sources_months, return_pandas)

    def _getSources(self, region, object_ids, dt, table_name, months, return_pandas=False):
        """Returns catalog of DiaSource instances given set of DiaObject IDs.

        This method returns :doc:`/modules/lsst.afw.table/index` catalog with schema determined by
        the schema of APDB table. Re-mapping of the column names is done for
        some columns (based on column map passed to constructor) but types or
        units are not changed.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Spherical region.
        object_ids :
            Collection of DiaObject IDs
        dt : `datetime.datetime`
            Time of the current visit
        table_name : `str`
            Name of the table, either "DiaSource" or "DiaForcedSource"
        months : `int`
            Number of months of history to return, if negative returns whole
            history.
        return_pandas : `bool`
            Return a `pandas.DataFrame` instead of
            `lsst.afw.table.SourceCatalog`.

        Returns
        -------
        catalog : `lsst.afw.table.SourceCatalog`, `pandas.DataFrame`, or `None`
            Catalog contaning DiaSource records. `None` is returned if
            ``months`` 0 or when ``object_ids`` is empty.
        """
        if months == 0 or len(object_ids) == 0:
            return None

        if return_pandas:
            self._session.row_factory = _PandasRowFactory()
        else:
            self._session.row_factory = cassandra.query.named_tuple_factory
        self._session.default_fetch_size = None

        months_list = ''
        tables = [table_name]
        if months > 0:
            seconds_now = int((dt - datetime(1970, 1, 1)) / timedelta(seconds=1))
            month_now = seconds_now // SECONDS_IN_MONTH
            months = list(range(month_now-months, month_now+1))
            if self.config.per_month_tables:
                tables = [f"{table_name}_{month}" for month in months]
            else:
                months_list = ','.join([str(i) for i in range(month_now-months, month_now+1)])
                _LOG.debug("_getSources: months_list: %s", months_list)

        pixels = self._partitioner.pixels(region)
        _LOG.info("_getSources: %s #partitions: %s", table_name, len(pixels))
        pixels = ",".join([str(pix) for pix in pixels])

        queries = []
        for table in tables:
            query = f'SELECT * from "{table}" WHERE "apdb_part" IN ({pixels})'
            if months_list:
                query += f' AND "apdb_month" IN ({months_list})'
            queries += [(
                cassandra.query.SimpleStatement(query, consistency_level=self._read_consistency),
                {}
            )]
        _LOG.info("_getSources %s: #queries: %s", table_name, len(queries))
        # _LOG.debug("_getSources: queries: %s", queries)

        catalog = None
        with Timer(table_name + ' select', self.config.timer):
            # submit all queries
            futures = [self._session.execute_async(query, values, timeout=120.) for query, values in queries]
            if return_pandas:
                # TODO: This orders result processing which is not very efficient
                dataframes = [future.result()._current_rows for future in futures]
                # concatenate all frames
                if len(dataframes) == 1:
                    catalog = dataframes[0]
                else:
                    catalog = pandas.concat(dataframes)
                # filter by given object IDs
                catalog = catalog[catalog.diaObjectId.isin(set(object_ids))]
            else:
                for future in futures:
                    rows = future.result()
                    rows = _filterObjectIds(rows, object_ids)
                    catalog = self._convertResult(rows, table_name, catalog=catalog)

        _LOG.debug("found %d %ss", _nrows(catalog), table_name)
        return catalog

    def storeDiaObjects(self, objs, dt, pos_func):
        """Store catalog of DiaObjects from current visit.

        This methods takes :doc:`/modules/lsst.afw.table/index` catalog, its schema must be
        compatible with the schema of APDB table:

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
        pos_func : callable
            Function of single argument which takes one catalog record as
            input and returns its position (`lsst.sphgeom.UnitVector3d`).
        """
        if isinstance(objs, pandas.DataFrame):

            extra_columns = dict(lastNonForcedSource=dt)
            self._storeObjectsPandas(objs, "DiaObjectLast", dt, pos_func, extra_columns=extra_columns)

            extra_columns = dict(lastNonForcedSource=dt, validityStart=dt)
            self._storeObjectsPandas(objs, "DiaObject", dt, pos_func, extra_columns=extra_columns)

        else:

            extra_columns = dict(lastNonForcedSource=dt)
            self._storeObjectsAfw(objs, "DiaObjectLast", dt, pos_func, extra_columns=extra_columns)

            extra_columns = dict(lastNonForcedSource=dt, validityStart=dt)
            self._storeObjectsAfw(objs, "DiaObject", dt, pos_func, extra_columns=extra_columns)

    def storeDiaSources(self, sources, dt, pos_func):
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
        dt : `datetime.datetime`
            Time of the visit
        pos_func : callable
            Function of single argument which takes one catalog record as
            input and returns its position (`lsst.sphgeom.UnitVector3d`).
        """

        month = None
        if self.config.per_month_tables:
            seconds_now = int((dt - datetime(1970, 1, 1)) / timedelta(seconds=1))
            month = seconds_now // SECONDS_IN_MONTH

        # this is a lie of course
        extra_columns = dict(midPointTai=dt)
        if isinstance(sources, pandas.DataFrame):
            self._storeObjectsPandas(sources, "DiaSource", dt, pos_func,
                                     extra_columns=extra_columns, month=month)
        else:
            self._storeObjectsAfw(sources, "DiaSource", dt, pos_func,
                                  extra_columns=extra_columns, month=month)

    def storeDiaForcedSources(self, sources, dt, pos_func):
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
        dt : `datetime.datetime`
            Time of the visit
        pos_func : callable
            Function of single argument which takes one catalog record as
            input and returns its position (`lsst.sphgeom.UnitVector3d`).
        """
        month = None
        if self.config.per_month_tables:
            seconds_now = int((dt - datetime(1970, 1, 1)) / timedelta(seconds=1))
            month = seconds_now // SECONDS_IN_MONTH

        extra_columns = dict(midPointTai=dt)
        if isinstance(sources, pandas.DataFrame):
            self._storeObjectsPandas(sources, "DiaForcedSource", dt, pos_func,
                                     extra_columns=extra_columns, month=month)
        else:
            self._storeObjectsAfw(sources, "DiaForcedSource", dt, pos_func,
                                  extra_columns=extra_columns, month=month)

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
        res : iterator for `namedtuple`
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

    def _storeObjectsAfw(self, objects, table_name, dt, pos_func, extra_columns=None, month=None):
        """Generic store method.

        Takes catalog of records and stores a bunch of objects in a table.

        Parameters
        ----------
        objects : `lsst.afw.table.BaseCatalog`
            Catalog containing object records
        table_name : `str`
            Name of the table as defined in APDB schema.
        dt : `datetime.datetime`
            Time of the visit
        pos_func : callable
            Function of single argument which takes one catalog record as
            input and returns its position (`lsst.sphgeom.UnitVector3d`).
        extra_columns : `dict`, optional
            Mapping (column_name, column_value) which gives column values to add
            to every row, only if column is missing in catalog records.
        month : `int`, optional
            of not `None` then insert into a per-month table.
        """

        def qValue(v):
            """Transform object into a value for query"""
            if v is None:
                pass
            elif isinstance(v, datetime):
                v = int((v - datetime(1970, 1, 1)) / timedelta(seconds=1))*1000
            elif isinstance(v, (bytes, str)):
                pass
            elif isinstance(v, geom.Angle):
                v = v.asDegrees()
                if not np.isfinite(v):
                    v = None
            else:
                try:
                    if not np.isfinite(v):
                        v = None
                except TypeError:
                    pass
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

        # set of columns to fill with random values
        random_columns = []
        random_column_names = []
        if self.config.fillEmptyFields:
            fieldsSet = frozenset(fields)
            random_columns = [col for col in column_map.values() if col.name not in fieldsSet]
            random_column_names = [col.name for col in random_columns]

        qfields = ','.join([quoteId(field) for field in fields + random_column_names])

        with Timer(table_name + ' query build', self.config.timer):
            queries = cassandra.query.BatchStatement(consistency_level=self._write_consistency)
            for rec in objects:
                values = []
                for field in afw_fields:
                    if field not in column_map:
                        continue
                    value = rec[field]
                    if column_map[field].type == "DATETIME" and np.isfinite(value):
                        # CAssandra datetime is in millisconds
                        value = int(value * 1000)
                    values.append(qValue(value))
                for field in extra_fields:
                    value = extra_columns[field]
                    values.append(qValue(value))
                if part_columns:
                    part_values = self._partitionValues(rec, table_name, part_columns, dt, pos_func)
                    values += [qValue(value) for value in part_values]
                for col in random_columns:
                    if col.type in ("FLOAT", "DOUBLE"):
                        value = random.random()
                    elif "INT" in col.type:
                        value = random.randint(0, 1000)
                    elif col.type == "DATETIME":
                        value = random.randint(0, 1000000000)
                    elif col.type == "BLOB":
                        # random byte sequence
                        value = ''.join(random.sample(string.ascii_letters, random.randint(10, 30))).encode()
                    else:
                        value = ""
                    values.append(qValue(value))
                holders = ','.join(['%s'] * len(values))
                table = self._schema.tableName(table_name)
                if month is not None:
                    table = f"{table}_{month}"
                query = 'INSERT INTO "{}" ({}) VALUES ({});'.format(table, qfields, holders)
                # _LOG.debug("query: %r", query)
                # _LOG.debug("values: %s", values)
                query = cassandra.query.SimpleStatement(query, consistency_level=self._write_consistency)
                queries.add(query, values)

        # _LOG.debug("query: %s", query)
        _LOG.info("%s: will store %d records", self._schema.tableName(table_name), len(objects))
        with Timer(table_name + ' insert', self.config.timer):
            self._session.execute(queries)

    def _storeObjectsPandas(self, objects, table_name, dt, pos_func, extra_columns=None, month=None):
        """Generic store method.

        Takes catalog of records and stores a bunch of objects in a table.

        Parameters
        ----------
        objects : `pandas.DataFrame`
            Catalog containing object records
        table_name : `str`
            Name of the table as defined in APDB schema.
        dt : `datetime.datetime`
            Time of the visit
        pos_func : callable
            Function of single argument which takes one catalog record as
            input and returns its position (`lsst.sphgeom.UnitVector3d`).
        extra_columns : `dict`, optional
            Mapping (column_name, column_value) which gives column values to add
            to every row, only if column is missing in catalog records.
        month : `int`, optional
            of not `None` then insert into a per-month table.
        """

        def qValue(v):
            """Transform object into a value for query"""
            if v is None:
                pass
            elif isinstance(v, datetime):
                v = int((v - datetime(1970, 1, 1)) / timedelta(seconds=1))*1000
            elif isinstance(v, (bytes, str)):
                pass
            elif isinstance(v, geom.Angle):
                v = v.asDegrees()
                if not np.isfinite(v):
                    v = None
            else:
                try:
                    if not np.isfinite(v):
                        v = None
                except TypeError:
                    pass
            return v

        def quoteId(columnName):
            """Smart quoting for column names.
            Lower-case names are not quoted.
            """
            if not columnName.islower():
                columnName = '"' + columnName + '"'
            return columnName

        # use extra columns if specified
        extra_fields = list((extra_columns or {}).keys())

        df_fields = [column for column in objects.columns
                     if column not in extra_fields]

        column_map = self._schema.getAfwColumns(table_name)
        # list of columns (as in cat schema)
        fields = [column_map[field].name for field in df_fields if field in column_map]
        fields += extra_fields

        # some partioning columns may not be in dataframe, they need to be added explicitly
        part_columns = self._schema.partitionColumns(table_name)
        part_columns = [column for column in part_columns if column not in fields]
        fields += part_columns

        # set of columns to fill with random values
        random_columns = []
        random_column_names = []
        if self.config.fillEmptyFields:
            fieldsSet = frozenset(fields)
            random_columns = [col for col in column_map.values() if col.name not in fieldsSet]
            random_column_names = [col.name for col in random_columns]

        qfields = ','.join([quoteId(field) for field in fields + random_column_names])

        with Timer(table_name + ' query build', self.config.timer):
            queries = cassandra.query.BatchStatement(consistency_level=self._write_consistency)
            for rec in objects.itertuples(index=False):
                values = []
                for field in df_fields:
                    if field not in column_map:
                        continue
                    value = getattr(rec, field)
                    if column_map[field].type == "DATETIME" and np.isfinite(value):
                        # Cassandra datetime is in milliseconds
                        value = int(value * 1000)
                    values.append(qValue(value))
                for field in extra_fields:
                    value = extra_columns[field]
                    values.append(qValue(value))
                if part_columns:
                    part_values = self._partitionValues(rec, table_name, part_columns, dt, pos_func)
                    values += [qValue(value) for value in part_values]
                for col in random_columns:
                    if col.type in ("FLOAT", "DOUBLE"):
                        value = random.random()
                    elif "INT" in col.type:
                        value = random.randint(0, 1000)
                    elif col.type == "DATETIME":
                        value = random.randint(0, 1000000000)
                    elif col.type == "BLOB":
                        # random byte sequence
                        value = ''.join(random.sample(string.ascii_letters, random.randint(10, 30))).encode()
                    else:
                        value = ""
                    values.append(qValue(value))
                holders = ','.join(['%s'] * len(values))
                table = self._schema.tableName(table_name)
                if month is not None:
                    table = f"{table}_{month}"
                query = 'INSERT INTO "{}" ({}) VALUES ({});'.format(table, qfields, holders)
                # _LOG.debug("query: %r", query)
                # _LOG.debug("values: %s", values)
                query = cassandra.query.SimpleStatement(query, consistency_level=self._write_consistency)
                queries.add(query, values)

        # _LOG.debug("query: %s", query)
        _LOG.info("%s: will store %d records", self._schema.tableName(table_name), objects.shape[0])
        with Timer(table_name + ' insert', self.config.timer):
            self._session.execute(queries)

    def _partitionValues(self, rec, table_name, part_columns, dt, pos_func):
        """Return values of partition columns for a record.

        Parameters
        ----------
        rec : `afw.table.Record`
            Single record from a catalog.
        table_name : `str`
            Table name as defined in APDB schema.
        part_columns : `list` of `str`
            Names of the columns for which to return values.
        dt : `datetime.datetime`
            Time of the visit
        pos_func : callable
            Function of single argument which takes one catalog record as
            input and returns its position (`lsst.sphgeom.UnitVector3d`).

        Returns
        -------
        values : `list`
            List of column values.
        """

        part_by_month = False
        if not self.config.per_month_tables and \
                table_name in ("DiaSource", "DiaForcedSource"):
            part_by_month = True
        if table_name == "DiaObject":
            part_by_month = True

        if part_by_month:
            if part_columns != ["apdb_part", "apdb_month"]:
                raise ValueError("unexpected partitionig columns for {}: {}".format(
                    table_name, part_columns))
            pos = pos_func(rec)
            part = self._partitioner.pixel(pos)
            value = int((dt - datetime.utcfromtimestamp(0)).total_seconds())
            month = value // SECONDS_IN_MONTH
            return [part, month]
        else:
            if part_columns != ["apdb_part"]:
                raise ValueError("unexpected partitionig columns for {}: {}".format(
                    table_name, part_columns))
            pos = pos_func(rec)
            part = self._partitioner.pixel(pos)
            return [part]
