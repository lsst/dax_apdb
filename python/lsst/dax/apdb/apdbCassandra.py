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

from datetime import datetime, timedelta
import logging
import numpy as np
import pandas
import random
import string
from typing import cast, Any, Dict, Iterable, Iterator, List, Mapping, Optional, Set, Tuple

try:
    import cbor
except ImportError:
    cbor = None

# If cassandra-driver is not there the module can still be imported
# but ApdbCassandra cannot be instantiated.
try:
    import cassandra
    from cassandra.cluster import Cluster
    from cassandra.concurrent import execute_concurrent
    from cassandra.policies import RoundRobinPolicy, WhiteListRoundRobinPolicy, AddressTranslator
    import cassandra.query
    CASSANDRA_IMPORTED = True
except ImportError:
    CASSANDRA_IMPORTED = False

import lsst.daf.base as dafBase
from lsst.pex.config import ChoiceField, Field, ListField
from lsst import sphgeom
from . import timer
from .apdb import Apdb, ApdbConfig
from .apdbSchema import ColumnDef
from .apdbCassandraSchema import ApdbCassandraSchema


_LOG = logging.getLogger(__name__.partition(".")[2])  # strip leading "lsst."


class CassandraMissingError(Exception):
    def __init__(self) -> None:
        super().__init__("cassandra-driver module cannot be imported")


class Timer(object):
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

    def __enter__(self) -> Any:
        """
        Enter context, start timer
        """
#         event.listen(engine.Engine, "before_cursor_execute", self._start_timer)
#         event.listen(engine.Engine, "after_cursor_execute", self._stop_timer)
        self._timer1.start()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> Any:
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
            _LOG.debug("before_cursor_execute")
        self._timer2.start()

    def _stop_timer(self, conn, cursor, statement, parameters, context, executemany):  # type: ignore
        """Stop counting"""
        self._timer2.stop()
        if self._do_logging:
            self._timer2.dump()


def _split(seq: Iterable[Any], nItems: int) -> Iterator[List[Any]]:
    """Split a sequence into smaller sequences"""
    seq = list(seq)
    while seq:
        yield seq[:nItems]
        del seq[:nItems]


class ApdbCassandraConfig(ApdbConfig):

    contact_points = ListField(dtype=str,
                               doc="The list of contact points to try connecting for cluster discovery.",
                               default=["127.0.0.1"])
    private_ips = ListField(dtype=str,
                            doc="List of internal IP addresses for contact_points.",
                            default=[])
    keyspace = Field(dtype=str,
                     doc="Default keyspace for operations.",
                     default="apdb")
    read_consistency = Field(
        dtype=str,
        doc="Name for consistency level of read operations, default: QUORUM, can be ONE.",
        default="QUORUM")
    write_consistency = Field(
        dtype=str,
        doc="Name for consistency level of write operations, default: QUORUM, can be ONE.",
        default="QUORUM")
    protocol_version = Field(dtype=int,
                             doc="Cassandra protocol version to use, default is V4",
                             default=cassandra.ProtocolVersion.V4 if CASSANDRA_IMPORTED else 0)
    read_sources_months = Field(dtype=int,
                                doc="Number of months of history to read from DiaSource",
                                default=12)
    read_forced_sources_months = Field(dtype=int,
                                       doc="Number of months of history to read from DiaForcedSource",
                                       default=12)
    dia_object_columns = ListField(dtype=str,
                                   doc="List of columns to read from DiaObject, by default read all columns",
                                   default=[])
    prefix = Field(dtype=str,
                   doc="Prefix to add to table names",
                   default="")
    part_pixelization = ChoiceField(dtype=str,
                                    allowed=dict(htm="HTM pixelization", q3c="Q3C pixelization",
                                                 mq3c="MQ3C pixelization"),
                                    doc="Pixelization used for patitioning index.",
                                    default="mq3c")
    part_pix_level = Field(dtype=int,
                           doc="Pixelization level used for patitioning index.",
                           default=10)
    ra_dec_columns = ListField(dtype=str, default=["ra", "decl"],
                               doc="Names ra/dec columns in DiaObject table")
    timer = Field(dtype=bool,
                  doc="If True then print/log timing information",
                  default=False)
    fillEmptyFields = Field(dtype=bool,
                            doc=("If True then store random values for fields not explicitly filled, "
                                 "for testing only"),
                            default=False)
    time_partition_tables = Field(
        dtype=bool,
        doc="Use per-partition tables for sources instead of paritioning by time",
        default=True
    )
    time_partition_days = Field(
        dtype=int,
        doc="Time partitoning granularity in days",
        default=30
    )
    query_per_time_part = Field(
        dtype=bool,
        default=False,
        doc=(
            "If True then build separate query for each time partition, otherwise build one single query. "
            "This is only used when time_partition_tables is False in schema config."
        )
    )
    query_per_spatial_part = Field(
        dtype=bool,
        default=False,
        doc=(
            "If True then build one query per spacial partition, otherwise build single query. "
        )
    )
    pandas_delay_conv = Field(
        dtype=bool,
        default=True,
        doc=(
            "If True then combine result rows before converting to pandas. "
        )
    )
    packing = ChoiceField(
        dtype=str,
        allowed=dict(none="No field packing", cbor="Pack using CBOR"),
        doc="Packing method for table records.",
        default="none"
    )


class Partitioner:
    """Class that caclulates indices of the objects for paritioning.

    Used internally by `ApdbCassandra`

    Parameters
    ----------
    config : `ApdbCassandraConfig`
    """
    def __init__(self, config: ApdbCassandraConfig):
        pix = config.part_pixelization
        if pix == "htm":
            self.pixelator = sphgeom.HtmPixelization(config.part_pix_level)
        elif pix == "q3c":
            self.pixelator = sphgeom.Q3cPixelization(config.part_pix_level)
        elif pix == "mq3c":
            self.pixelator = sphgeom.Mq3cPixelization(config.part_pix_level)
        else:
            raise ValueError(f"unknown pixelization: {pix}")

    def pixels(self, region: sphgeom.Region) -> List[int]:
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

    def pixel(self, direction: sphgeom.UnitVector3d) -> int:
        """Compute the index of the pixel for given direction.

        Parameters
        ----------
        direction : `lsst.sphgeom.UnitVector3d`
        """
        index = self.pixelator.index(direction)
        return index


if CASSANDRA_IMPORTED:

    class _AddressTranslator(AddressTranslator):
        """Translate internal IP address to external.

        Only used for docker-based setup, not viable long-term solution.
        """
        def __init__(self, public_ips: List[str], private_ips: List[str]):
            self._map = dict((k, v) for k, v in zip(private_ips, public_ips))

        def translate(self, private_ip: str) -> str:
            return self._map.get(private_ip, private_ip)


def _rows_to_pandas(colnames: List[str], rows: List[Tuple],
                    packedColumns: List[ColumnDef]) -> pandas.DataFrame:
    """Convert result rows to pandas.

    Unpacks BLOBs that were packed on insert.

    Parameters
    ----------
    colname : `list` [ `str` ]
        Names of the columns.
    rows : `list` of `tuple`
        Result rows.
    packedColumns : `list` [ `ColumnDef` ]
        Column definitions for packed columns.

    Returns
    -------
    catalog : `pandas.DataFrame`
        DataFrame with the result set.
    """
    try:
        idx = colnames.index("apdb_packed")
    except ValueError:
        # no packed columns
        return pandas.DataFrame.from_records(rows, columns=colnames)

    # make data frame for non-packed columns
    df = pandas.DataFrame.from_records(rows, columns=colnames, exclude=["apdb_packed"])

    # make records with packed data only as dicts
    packed_rows = []
    for row in rows:
        blob = row[idx]
        if blob[:5] == b"cbor:":
            blob = cbor.loads(blob[5:])
        else:
            raise ValueError("Unexpected BLOB format: %r", blob)
        packed_rows.append(blob)

    # make data frome from packed data
    packed = pandas.DataFrame.from_records(packed_rows, columns=[col.name for col in packedColumns])

    # convert timestamps which are integer milliseconds into datetime
    for col in packedColumns:
        if col.type == "DATETIME":
            packed[col.name] = pandas.to_datetime(packed[col.name], unit="ms", origin="unix")

    return pandas.concat([df, packed], axis=1)


class _PandasRowFactory:
    """Create pandas DataFrame from Cassandra result set.

    Parameters
    ----------
    packedColumns : `list` [ `ColumnDef` ]
        Column definitions for packed columns.
    """
    def __init__(self, packedColumns: Iterable[ColumnDef]):
        self.packedColumns = list(packedColumns)

    def __call__(self, colnames: List[str], rows: List[Tuple]) -> pandas.DataFrame:
        """Convert result set into output catalog.

        Parameters
        ----------
        colname : `list` [ `str` ]
            Names of the columns.
        rows : `list` of `tuple`
            Result rows

        Returns
        -------
        catalog : `pandas.DataFrame`
            DataFrame with the result set.
        """
        return _rows_to_pandas(colnames, rows, self.packedColumns)


class _RawRowFactory:
    """Row factory that makes no conversions.

    Parameters
    ----------
    packedColumns : `list` [ `ColumnDef` ]
        Column definitions for packed columns.
    """
    def __init__(self, packedColumns: Iterable[ColumnDef]):
        self.packedColumns = list(packedColumns)

    def __call__(self, colnames: List[str], rows: List[Tuple]) -> Tuple[List[str], List[Tuple]]:
        """Return parameters without change.

        Parameters
        ----------
        colname : `list` of `str`
            Names of the columns.
        rows : `list` of `tuple`
            Result rows

        Returns
        -------
        colname : `list` of `str`
            Names of the columns.
        rows : `list` of `tuple`
            Result rows
        """
        return (colnames, rows)


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

    partition_zero_epoch = dafBase.DateTime(1970, 1, 1, 0, 0, 0, dafBase.DateTime.TAI)
    """Start time for partition 0"""

    def __init__(self, config: ApdbCassandraConfig):

        if not CASSANDRA_IMPORTED:
            raise CassandraMissingError()

        self.config = config

        # logging.getLogger('sqlalchemy').setLevel(logging.INFO)
        _LOG.debug("ApdbCassandra Configuration:")
        _LOG.debug("    read_consistency: %s", self.config.read_consistency)
        _LOG.debug("    write_consistency: %s", self.config.write_consistency)
        _LOG.debug("    read_sources_months: %s", self.config.read_sources_months)
        _LOG.debug("    read_forced_sources_months: %s", self.config.read_forced_sources_months)
        _LOG.debug("    dia_object_columns: %s", self.config.dia_object_columns)
        _LOG.debug("    schema_file: %s", self.config.schema_file)
        _LOG.debug("    extra_schema_file: %s", self.config.extra_schema_file)
        _LOG.debug("    schema prefix: %s", self.config.prefix)
        _LOG.debug("    part_pixelization: %s", self.config.part_pixelization)
        _LOG.debug("    part_pix_level: %s", self.config.part_pix_level)
        _LOG.debug("    query_per_time_part: %s", self.config.query_per_time_part)
        _LOG.debug("    query_per_spatial_part: %s", self.config.query_per_spatial_part)

        self._partitioner = Partitioner(config)

        addressTranslator: Optional[AddressTranslator] = None
        if config.private_ips:
            loadBalancePolicy = WhiteListRoundRobinPolicy(hosts=config.contact_points)
            addressTranslator = _AddressTranslator(config.contact_points, config.private_ips)
        else:
            loadBalancePolicy = RoundRobinPolicy()

        self._read_consistency = getattr(cassandra.ConsistencyLevel, config.read_consistency)
        self._write_consistency = getattr(cassandra.ConsistencyLevel, config.write_consistency)

        self._cluster = Cluster(contact_points=self.config.contact_points,
                                load_balancing_policy=loadBalancePolicy,
                                address_translator=addressTranslator,
                                protocol_version=self.config.protocol_version)
        self._session = self._cluster.connect(keyspace=config.keyspace)
        self._session.row_factory = cassandra.query.named_tuple_factory

        self._schema = ApdbCassandraSchema(session=self._session,
                                           schema_file=self.config.schema_file,
                                           extra_schema_file=self.config.extra_schema_file,
                                           prefix=self.config.prefix,
                                           time_partition_tables=config.time_partition_tables,
                                           time_partition_days=config.time_partition_days,
                                           packing=config.packing)

    def makeSchema(self, drop: bool = False) -> None:
        # docstring is inherited from a base class
        self._schema.makeSchema(drop=drop)

    def getDiaObjects(self, region: sphgeom.Region) -> pandas.DataFrame:
        # docstring is inherited from a base class
        packedColumns = self._schema.packedColumns("DiaObjectLast")
        self._session.row_factory = _PandasRowFactory(packedColumns)
        self._session.default_fetch_size = None

        pixels = self._partitioner.pixels(region)
        _LOG.debug("getDiaObjects: #partitions: %s", len(pixels))
        pixels_str = ",".join([str(pix) for pix in pixels])

        queries: List[Tuple] = []
        query = f'SELECT * from "DiaObjectLast" WHERE "apdb_part" IN ({pixels_str})'
        queries += [(cassandra.query.SimpleStatement(query, consistency_level=self._read_consistency), {})]
        _LOG.debug("getDiaObjects: #queries: %s", len(queries))
        # _LOG.debug("getDiaObjects: queries: %s", queries)

        objects = None
        with Timer('DiaObject select', self.config.timer):
            # submit all queries
            futures = [self._session.execute_async(query, values, timeout=120.) for query, values in queries]
            # TODO: This orders result processing which is not very efficient
            dataframes = [future.result()._current_rows for future in futures]
            # concatenate all frames
            if len(dataframes) == 1:
                objects = dataframes[0]
            else:
                objects = pandas.concat(dataframes)

        _LOG.debug("found %s DiaObjects", objects.shape[0])
        return objects

    def getDiaSources(self, region: sphgeom.Region,
                      object_ids: Optional[Iterable[int]],
                      visit_time: dafBase.DateTime) -> Optional[pandas.DataFrame]:
        # docstring is inherited from a base class
        return self._getSources(region, object_ids, visit_time, "DiaSource",
                                self.config.read_sources_months)

    def getDiaForcedSources(self, region: sphgeom.Region,
                            object_ids: Optional[Iterable[int]],
                            visit_time: dafBase.DateTime) -> Optional[pandas.DataFrame]:
        return self._getSources(region, object_ids, visit_time, "DiaForcedSource",
                                self.config.read_forced_sources_months)

    def _getSources(self, region: sphgeom.Region,
                    object_ids: Optional[Iterable[int]],
                    visit_time: dafBase.DateTime,
                    table_name: str,
                    months: int) -> Optional[pandas.DataFrame]:
        """Returns catalog of DiaSource instances given set of DiaObject IDs.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Spherical region.
        object_ids :
            Collection of DiaObject IDs
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit
        table_name : `str`
            Name of the table, either "DiaSource" or "DiaForcedSource"
        months : `int`
            Number of months of history to return, if negative returns whole
            history (Note: negative does not work with table-per-partition
            case)

        Returns
        -------
        catalog : `pandas.DataFrame`, or `None`
            Catalog contaning DiaSource records. `None` is returned if
            ``months`` is 0 or when ``object_ids`` is empty.
        """
        if months == 0:
            return None
        object_id_set: Set[int] = set()
        if object_ids is not None:
            object_id_set = set(object_ids)
            if len(object_id_set) == 0:
                # TODO: need correct column schema for this
                return pandas.DataFrame()

        packedColumns = self._schema.packedColumns(table_name)
        if self.config.pandas_delay_conv:
            self._session.row_factory = _RawRowFactory(packedColumns)
        else:
            self._session.row_factory = _PandasRowFactory(packedColumns)
        self._session.default_fetch_size = None

        # spatial pixels included into query
        pixels = self._partitioner.pixels(region)
        _LOG.debug("_getSources: %s #partitions: %s", table_name, len(pixels))

        # spatial part of WHERE
        spatial_where = []
        if self.config.query_per_spatial_part:
            spatial_where = [f'"apdb_part" = {pixel}' for pixel in pixels]
        else:
            pixels_str = ",".join([str(pix) for pix in pixels])
            spatial_where = [f'"apdb_part" IN ({pixels_str})']

        # temporal part of WHERE, can be empty
        temporal_where = []
        # time partitions and table names to query, there may be multiple
        # tables depending on configuration
        tables = [table_name]
        mjd_now = visit_time.get(system=dafBase.DateTime.MJD)
        mjd_begin = mjd_now - months*30
        epoch = self.partition_zero_epoch.get(system=dafBase.DateTime.MJD)
        time_part_now = int(mjd_now - epoch) // self.config.time_partition_days
        time_part_begin = int(mjd_begin - epoch) // self.config.time_partition_days
        time_parts = list(range(time_part_begin, time_part_now + 1))
        if self.config.time_partition_tables:
            tables = [f"{table_name}_{part}" for part in time_parts]
        else:
            if self.config.query_per_time_part:
                temporal_where = [f'"apdb_time_part" = {time_part}' for time_part in time_parts]
            else:
                time_part_list = ",".join([str(part) for part in time_parts])
                temporal_where = [f'"apdb_time_part" IN ({time_part_list})']

        # Build all queries
        queries: List[str] = []
        for table in tables:
            query = f'SELECT * from "{table}" WHERE '
            for spacial in spatial_where:
                if temporal_where:
                    for temporal in temporal_where:
                        queries.append(query + spacial + " AND " + temporal)
                else:
                    queries.append(query + spacial)
        # _LOG.debug("_getSources: queries: %s", queries)

        statements: List[Tuple] = [
            (cassandra.query.SimpleStatement(query, consistency_level=self._read_consistency), {})
            for query in queries
        ]
        _LOG.debug("_getSources %s: #queries: %s", table_name, len(statements))

        with Timer(table_name + ' select', self.config.timer):
            # submit all queries
            results = execute_concurrent(self._session, statements, concurrency=500)
            if self.config.pandas_delay_conv:
                _LOG.debug("making pandas data frame out of rows/columns")
                columns: Any = None
                rows = []
                for success, result in results:
                    result = result._current_rows
                    if success:
                        if columns is None:
                            columns = result[0]
                        elif columns != result[0]:
                            _LOG.error("different columns returned by queries: %s and %s",
                                       columns, result[0])
                            raise ValueError(
                                f"diferent columns returned by queries: {columns} and {result[0]}"
                            )
                        rows += result[1]
                    else:
                        _LOG.error("error returned by query: %s", result)
                        raise result
                catalog = _rows_to_pandas(columns, rows, self._schema.packedColumns(table_name))
                _LOG.debug("pandas catalog shape: %s", catalog.shape)
                # filter by given object IDs
                if len(object_id_set) > 0:
                    catalog = cast(pandas.DataFrame, catalog[catalog["diaObjectId"].isin(object_id_set)])
            else:
                _LOG.debug("making pandas data frame out of set of data frames")
                dataframes = []
                for success, result in results:
                    if success:
                        dataframes.append(result._current_rows)
                    else:
                        _LOG.error("error returned by query: %s", result)
                        raise result
                # concatenate all frames
                if len(dataframes) == 1:
                    catalog = dataframes[0]
                else:
                    catalog = pandas.concat(dataframes)
                _LOG.debug("pandas catalog shape: %s", catalog.shape)
                # filter by given object IDs
                if len(object_id_set) > 0:
                    catalog = cast(pandas.DataFrame, catalog[catalog["diaObjectId"].isin(object_id_set)])

        # precise filtering on midPointTai
        catalog = cast(pandas.DataFrame, catalog[catalog["midPointTai"] > mjd_begin])

        _LOG.debug("found %d %ss", catalog.shape[0], table_name)
        return catalog

    def store(self,
              visit_time: dafBase.DateTime,
              objects: pandas.DataFrame,
              sources: Optional[pandas.DataFrame] = None,
              forced_sources: Optional[pandas.DataFrame] = None) -> None:
        # docstring is inherited from a base class

        # fill region partition column for DiaObjects
        objects = self._add_obj_part(objects)
        self._storeDiaObjects(objects, visit_time)

        if sources is not None:
            # copy apdb_part column from DiaObjects to DiaSources
            sources = self._add_src_part(sources, objects)
            self._storeDiaSources(sources, visit_time)

        if forced_sources is not None:
            forced_sources = self._add_fsrc_part(forced_sources, objects)
            self._storeDiaForcedSources(forced_sources, visit_time)

    def _storeDiaObjects(self, objs: pandas.DataFrame, visit_time: dafBase.DateTime) -> None:
        """Store catalog of DiaObjects from current visit.

        Parameters
        ----------
        objs : `pandas.DataFrame`
            Catalog with DiaObject records
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit.
        """
        visit_time_dt = visit_time.toPython()
        extra_columns = dict(lastNonForcedSource=visit_time_dt)
        self._storeObjectsPandas(objs, "DiaObjectLast", visit_time, extra_columns=extra_columns)

        extra_columns = dict(lastNonForcedSource=visit_time_dt, validityStart=visit_time_dt)
        if not self.config.time_partition_tables:
            mjd_now = visit_time.get(system=dafBase.DateTime.MJD)
            epoch = self.partition_zero_epoch.get(system=dafBase.DateTime.MJD)
            time_part = int(mjd_now - epoch) // self.config.time_partition_days
            extra_columns["apdb_time_part"] = time_part
        self._storeObjectsPandas(objs, "DiaObject", visit_time, extra_columns=extra_columns)

    def _storeDiaSources(self, sources: pandas.DataFrame, visit_time: dafBase.DateTime) -> None:
        """Store catalog of DIASources from current visit.

        Parameters
        ----------
        sources : `pandas.DataFrame`
            Catalog containing DiaSource records
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit.
        """
        mjd_now = visit_time.get(system=dafBase.DateTime.MJD)
        epoch = self.partition_zero_epoch.get(system=dafBase.DateTime.MJD)
        time_part = int(mjd_now - epoch) // self.config.time_partition_days
        extra_columns = {}
        if not self.config.time_partition_tables:
            extra_columns["apdb_time_part"] = time_part
            time_part = None

        self._storeObjectsPandas(sources, "DiaSource", visit_time,
                                 extra_columns=extra_columns, time_part=time_part)

    def _storeDiaForcedSources(self, sources: pandas.DataFrame, visit_time: dafBase.DateTime) -> None:
        """Store a set of DIAForcedSources from current visit.

        Parameters
        ----------
        sources : `pandas.DataFrame`
            Catalog containing DiaForcedSource records
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit.
        """
        mjd_now = visit_time.get(system=dafBase.DateTime.MJD)
        epoch = self.partition_zero_epoch.get(system=dafBase.DateTime.MJD)
        time_part = int(mjd_now - epoch) // self.config.time_partition_days
        extra_columns = {}
        if not self.config.time_partition_tables:
            extra_columns["apdb_time_part"] = time_part
            time_part = None

        self._storeObjectsPandas(sources, "DiaForcedSource", visit_time,
                                 extra_columns=extra_columns, time_part=time_part)

    def dailyJob(self) -> None:
        # docstring is inherited from a base class
        pass

    def countUnassociatedObjects(self) -> int:
        # docstring is inherited from a base class
        raise NotImplementedError()

    def _storeObjectsPandas(self, objects: pandas.DataFrame, table_name: str,
                            visit_time: dafBase.DateTime, extra_columns: Optional[Mapping] = None,
                            time_part: Optional[int] = None) -> None:
        """Generic store method.

        Takes catalog of records and stores a bunch of objects in a table.

        Parameters
        ----------
        objects : `pandas.DataFrame`
            Catalog containing object records
        table_name : `str`
            Name of the table as defined in APDB schema.
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit.
        extra_columns : `dict`, optional
            Mapping (column_name, column_value) which gives column values to add
            to every row, only if column is missing in catalog records.
        time_part : `int`, optional
            If not `None` then insert into a per-partition table.
        """

        def qValue(v: Any) -> Any:
            """Transform object into a value for query"""
            if v is None:
                pass
            elif isinstance(v, datetime):
                v = int((v - datetime(1970, 1, 1)) / timedelta(seconds=1))*1000
            elif isinstance(v, (bytes, str)):
                pass
            else:
                try:
                    if not np.isfinite(v):
                        v = None
                except TypeError:
                    pass
            return v

        def quoteId(columnName: str) -> str:
            """Smart quoting for column names.
            Lower-case names are not quoted.
            """
            if not columnName.islower():
                columnName = '"' + columnName + '"'
            return columnName

        # use extra columns if specified
        if extra_columns is None:
            extra_columns = {}
        extra_fields = list(extra_columns.keys())

        df_fields = [column for column in objects.columns
                     if column not in extra_fields]

        column_map = self._schema.getColumnMap(table_name)
        # list of columns (as in cat schema)
        fields = [column_map[field].name for field in df_fields if field in column_map]
        fields += extra_fields

        # check that all partitioning and clustering columns are defined
        required_columns = self._schema.partitionColumns(table_name) \
            + self._schema.clusteringColumns(table_name)
        missing_columns = [column for column in required_columns if column not in fields]
        if missing_columns:
            raise ValueError(f"Primary key columns are missing from catalog: {missing_columns}")

        # set of columns to fill with random values
        random_columns = []
        random_column_names = []
        if self.config.fillEmptyFields:
            fieldsSet = frozenset(fields)
            random_columns = [col for col in column_map.values() if col.name not in fieldsSet]
            random_column_names = [col.name for col in random_columns]

        blob_columns = set(col.name for col in self._schema.packedColumns(table_name))
        # _LOG.debug("blob_columns: %s", blob_columns)

        qfields = [quoteId(field) for field in fields + random_column_names if field not in blob_columns]
        if blob_columns:
            qfields += [quoteId("apdb_packed")]
        qfields_str = ','.join(qfields)

        with Timer(table_name + ' query build', self.config.timer):
            queries = cassandra.query.BatchStatement(consistency_level=self._write_consistency)
            for rec in objects.itertuples(index=False):
                values = []
                blob = {}
                for field in df_fields:
                    if field not in column_map:
                        continue
                    value = getattr(rec, field)
                    if column_map[field].type == "DATETIME" and np.isfinite(value):
                        # Cassandra datetime is in milliseconds
                        value = int(value * 1000)
                    if field in blob_columns:
                        blob[field] = qValue(value)
                    else:
                        values.append(qValue(value))
                for field in extra_fields:
                    value = extra_columns[field]
                    if field in blob_columns:
                        blob[field] = qValue(value)
                    else:
                        values.append(qValue(value))
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
                    if col.name in blob_columns:
                        blob[col.name] = qValue(value)
                    else:
                        values.append(qValue(value))
                if blob_columns:
                    if self.config.packing == "cbor":
                        blob = b"cbor:" + cbor.dumps(blob)
                    values.append(blob)
                holders = ','.join(['%s'] * len(values))
                table = self._schema.tableName(table_name)
                if time_part is not None:
                    table = f"{table}_{time_part}"
                query = 'INSERT INTO "{}" ({}) VALUES ({});'.format(table, qfields_str, holders)
                # _LOG.debug("query: %r", query)
                # _LOG.debug("values: %s", values)
                query = cassandra.query.SimpleStatement(query, consistency_level=self._write_consistency)
                queries.add(query, values)

        # _LOG.debug("query: %s", query)
        _LOG.debug("%s: will store %d records", self._schema.tableName(table_name), objects.shape[0])
        with Timer(table_name + ' insert', self.config.timer):
            self._session.execute(queries)

    def _add_obj_part(self, df: pandas.DataFrame) -> pandas.DataFrame:
        """Calculate spacial partition for each record and add it to a
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
            idx = self._partitioner.pixel(uv3d)
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
        pixel_id_map: Dict[int, int] = {
            diaObjectId: apdb_part for diaObjectId, apdb_part
            in zip(objs["diaObjectId"], objs["apdb_part"])
        }
        apdb_part = np.zeros(sources.shape[0], dtype=np.int64)
        ra_col, dec_col = self.config.ra_dec_columns
        for i, (diaObjId, ra, dec) in enumerate(zip(sources["diaObjectId"],
                                                    sources[ra_col], sources[dec_col])):
            if diaObjId == 0:
                # DiaSources associated with SolarSystemObjects do not have an
                # associated DiaObject hence we skip them and set partition
                # based on its own ra/dec
                uv3d = sphgeom.UnitVector3d(sphgeom.LonLat.fromDegrees(ra, dec))
                idx = self._partitioner.pixel(uv3d)
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
        pixel_id_map: Dict[int, int] = {
            diaObjectId: apdb_part for diaObjectId, apdb_part
            in zip(objs["diaObjectId"], objs["apdb_part"])
        }
        apdb_part = np.zeros(sources.shape[0], dtype=np.int64)
        for i, diaObjId in enumerate(sources["diaObjectId"]):
            apdb_part[i] = pixel_id_map[diaObjId]
        sources = sources.copy()
        sources["apdb_part"] = apdb_part
        return sources
