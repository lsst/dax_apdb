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

import logging
import numpy as np
import pandas
from typing import Any, cast, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Set, Tuple, Union

# If cassandra-driver is not there the module can still be imported
# but ApdbCassandra cannot be instantiated.
try:
    import cassandra
    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.policies import RoundRobinPolicy, WhiteListRoundRobinPolicy, AddressTranslator
    import cassandra.query
    CASSANDRA_IMPORTED = True
except ImportError:
    CASSANDRA_IMPORTED = False

import lsst.daf.base as dafBase
from lsst import sphgeom
from lsst.pex.config import ChoiceField, Field, ListField
from lsst.utils.iteration import chunk_iterable
from .timer import Timer
from .apdb import Apdb, ApdbConfig
from .apdbSchema import ApdbTables, TableDef
from .apdbCassandraSchema import ApdbCassandraSchema, ExtraTables
from .cassandra_utils import (
    literal,
    pandas_dataframe_factory,
    quote_id,
    raw_data_factory,
    select_concurrent,
)
from .pixelization import Pixelization

_LOG = logging.getLogger(__name__)


class CassandraMissingError(Exception):
    def __init__(self) -> None:
        super().__init__("cassandra-driver module cannot be imported")


class ApdbCassandraConfig(ApdbConfig):

    contact_points = ListField(
        dtype=str,
        doc="The list of contact points to try connecting for cluster discovery.",
        default=["127.0.0.1"]
    )
    private_ips = ListField(
        dtype=str,
        doc="List of internal IP addresses for contact_points.",
        default=[]
    )
    keyspace = Field(
        dtype=str,
        doc="Default keyspace for operations.",
        default="apdb"
    )
    read_consistency = Field(
        dtype=str,
        doc="Name for consistency level of read operations, default: QUORUM, can be ONE.",
        default="QUORUM"
    )
    write_consistency = Field(
        dtype=str,
        doc="Name for consistency level of write operations, default: QUORUM, can be ONE.",
        default="QUORUM"
    )
    read_timeout = Field(
        dtype=float,
        doc="Timeout in seconds for read operations.",
        default=120.
    )
    write_timeout = Field(
        dtype=float,
        doc="Timeout in seconds for write operations.",
        default=10.
    )
    read_concurrency = Field(
        dtype=int,
        doc="Concurrency level for read operations.",
        default=500
    )
    protocol_version = Field(
        dtype=int,
        doc="Cassandra protocol version to use, default is V4",
        default=cassandra.ProtocolVersion.V4 if CASSANDRA_IMPORTED else 0
    )
    dia_object_columns = ListField(
        dtype=str,
        doc="List of columns to read from DiaObject, by default read all columns",
        default=[]
    )
    prefix = Field(
        dtype=str,
        doc="Prefix to add to table names",
        default=""
    )
    part_pixelization = ChoiceField(
        dtype=str,
        allowed=dict(htm="HTM pixelization", q3c="Q3C pixelization", mq3c="MQ3C pixelization"),
        doc="Pixelization used for partitioning index.",
        default="mq3c"
    )
    part_pix_level = Field(
        dtype=int,
        doc="Pixelization level used for partitioning index.",
        default=10
    )
    part_pix_max_ranges = Field(
        dtype=int,
        doc="Max number of ranges in pixelization envelope",
        default=64
    )
    ra_dec_columns = ListField(
        dtype=str,
        default=["ra", "decl"],
        doc="Names ra/dec columns in DiaObject table"
    )
    timer = Field(
        dtype=bool,
        doc="If True then print/log timing information",
        default=False
    )
    time_partition_tables = Field(
        dtype=bool,
        doc="Use per-partition tables for sources instead of partitioning by time",
        default=True
    )
    time_partition_days = Field(
        dtype=int,
        doc="Time partitoning granularity in days, this value must not be changed"
            " after database is initialized",
        default=30
    )
    time_partition_start = Field(
        dtype=str,
        doc="Starting time for per-partion tables, in yyyy-mm-ddThh:mm:ss format, in TAI."
            " This is used only when time_partition_tables is True.",
        default="2018-12-01T00:00:00"
    )
    time_partition_end = Field(
        dtype=str,
        doc="Ending time for per-partion tables, in yyyy-mm-ddThh:mm:ss format, in TAI"
            " This is used only when time_partition_tables is True.",
        default="2030-01-01T00:00:00"
    )
    query_per_time_part = Field(
        dtype=bool,
        default=False,
        doc="If True then build separate query for each time partition, otherwise build one single query. "
            "This is only used when time_partition_tables is False in schema config."
    )
    query_per_spatial_part = Field(
        dtype=bool,
        default=False,
        doc="If True then build one query per spacial partition, otherwise build single query. "
    )
    pandas_delay_conv = Field(
        dtype=bool,
        default=True,
        doc="If True then combine result rows before converting to pandas. "
    )


if CASSANDRA_IMPORTED:

    class _AddressTranslator(AddressTranslator):
        """Translate internal IP address to external.

        Only used for docker-based setup, not viable long-term solution.
        """
        def __init__(self, public_ips: List[str], private_ips: List[str]):
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

    partition_zero_epoch = dafBase.DateTime(1970, 1, 1, 0, 0, 0, dafBase.DateTime.TAI)
    """Start time for partition 0, this should never be changed."""

    def __init__(self, config: ApdbCassandraConfig):

        if not CASSANDRA_IMPORTED:
            raise CassandraMissingError()

        self.config = config

        _LOG.debug("ApdbCassandra Configuration:")
        for key, value in self.config.items():
            _LOG.debug("    %s: %s", key, value)

        self._pixelization = Pixelization(
            config.part_pixelization, config.part_pix_level, config.part_pix_max_ranges
        )

        addressTranslator: Optional[AddressTranslator] = None
        if config.private_ips:
            addressTranslator = _AddressTranslator(config.contact_points, config.private_ips)

        self._keyspace = config.keyspace

        self._cluster = Cluster(execution_profiles=self._makeProfiles(config),
                                contact_points=self.config.contact_points,
                                address_translator=addressTranslator,
                                protocol_version=self.config.protocol_version)
        self._session = self._cluster.connect()
        # Disable result paging
        self._session.default_fetch_size = None

        self._schema = ApdbCassandraSchema(session=self._session,
                                           keyspace=self._keyspace,
                                           schema_file=self.config.schema_file,
                                           extra_schema_file=self.config.extra_schema_file,
                                           prefix=self.config.prefix,
                                           time_partition_tables=self.config.time_partition_tables)
        self._partition_zero_epoch_mjd = self.partition_zero_epoch.get(system=dafBase.DateTime.MJD)

        # Cache for prepared statements
        self._prepared_statements: Dict[str, cassandra.query.PreparedStatement] = {}

    def tableDef(self, table: ApdbTables) -> Optional[TableDef]:
        # docstring is inherited from a base class
        return self._schema.tableSchemas.get(table)

    def makeSchema(self, drop: bool = False) -> None:
        # docstring is inherited from a base class

        if self.config.time_partition_tables:
            time_partition_start = dafBase.DateTime(self.config.time_partition_start, dafBase.DateTime.TAI)
            time_partition_end = dafBase.DateTime(self.config.time_partition_end, dafBase.DateTime.TAI)
            part_range = (
                self._time_partition(time_partition_start),
                self._time_partition(time_partition_end) + 1
            )
            self._schema.makeSchema(drop=drop, part_range=part_range)
        else:
            self._schema.makeSchema(drop=drop)

    def getDiaObjects(self, region: sphgeom.Region) -> pandas.DataFrame:
        # docstring is inherited from a base class

        sp_where = self._spatial_where(region)
        _LOG.debug("getDiaObjects: #partitions: %s", len(sp_where))

        table_name = self._schema.tableName(ApdbTables.DiaObjectLast)
        query = f'SELECT * from "{self._keyspace}"."{table_name}"'
        statements: List[Tuple] = []
        for where, params in sp_where:
            full_query = f"{query} WHERE {where}"
            if params:
                statement = self._prep_statement(full_query)
            else:
                # If there are no params then it is likely that query has a
                # bunch of literals rendered already, no point trying to
                # prepare it because it's not reusable.
                statement = cassandra.query.SimpleStatement(full_query)
            statements.append((statement, params))
        _LOG.debug("getDiaObjects: #queries: %s", len(statements))

        with Timer('DiaObject select', self.config.timer):
            objects = cast(
                pandas.DataFrame,
                select_concurrent(
                    self._session, statements, "read_pandas_multi", self.config.read_concurrency
                )
            )

        _LOG.debug("found %s DiaObjects", objects.shape[0])
        return objects

    def getDiaSources(self, region: sphgeom.Region,
                      object_ids: Optional[Iterable[int]],
                      visit_time: dafBase.DateTime) -> Optional[pandas.DataFrame]:
        # docstring is inherited from a base class
        months = self.config.read_sources_months
        if months == 0:
            return None
        mjd_end = visit_time.get(system=dafBase.DateTime.MJD)
        mjd_start = mjd_end - months*30

        return self._getSources(region, object_ids, mjd_start, mjd_end, ApdbTables.DiaSource)

    def getDiaForcedSources(self, region: sphgeom.Region,
                            object_ids: Optional[Iterable[int]],
                            visit_time: dafBase.DateTime) -> Optional[pandas.DataFrame]:
        # docstring is inherited from a base class
        months = self.config.read_forced_sources_months
        if months == 0:
            return None
        mjd_end = visit_time.get(system=dafBase.DateTime.MJD)
        mjd_start = mjd_end - months*30

        return self._getSources(region, object_ids, mjd_start, mjd_end, ApdbTables.DiaForcedSource)

    def getDiaObjectsHistory(self,
                             start_time: dafBase.DateTime,
                             end_time: dafBase.DateTime,
                             region: Optional[sphgeom.Region] = None) -> pandas.DataFrame:
        # docstring is inherited from a base class

        sp_where = self._spatial_where(region, use_ranges=True)
        tables, temporal_where = self._temporal_where(ApdbTables.DiaObject, start_time, end_time, True)

        # Build all queries
        statements: List[Tuple] = []
        for table in tables:
            prefix = f'SELECT * from "{self._keyspace}"."{table}"'
            statements += list(self._combine_where(prefix, sp_where, temporal_where, "ALLOW FILTERING"))
        _LOG.debug("getDiaObjectsHistory: #queries: %s", len(statements))

        # Run all selects in parallel
        with Timer("DiaObject history", self.config.timer):
            catalog = cast(
                pandas.DataFrame,
                select_concurrent(
                    self._session, statements, "read_pandas_multi", self.config.read_concurrency
                )
            )

        # precise filtering on validityStart
        validity_start = start_time.toPython()
        validity_end = end_time.toPython()
        catalog = cast(
            pandas.DataFrame,
            catalog[(catalog["validityStart"] >= validity_start) & (catalog["validityStart"] < validity_end)]
        )

        _LOG.debug("found %d DiaObjects", catalog.shape[0])
        return catalog

    def getDiaSourcesHistory(self,
                             start_time: dafBase.DateTime,
                             end_time: dafBase.DateTime,
                             region: Optional[sphgeom.Region] = None) -> pandas.DataFrame:
        # docstring is inherited from a base class
        return self._getSourcesHistory(ApdbTables.DiaSource, start_time, end_time, region)

    def getDiaForcedSourcesHistory(self,
                                   start_time: dafBase.DateTime,
                                   end_time: dafBase.DateTime,
                                   region: Optional[sphgeom.Region] = None) -> pandas.DataFrame:
        # docstring is inherited from a base class
        return self._getSourcesHistory(ApdbTables.DiaForcedSource, start_time, end_time, region)

    def getSSObjects(self) -> pandas.DataFrame:
        # docstring is inherited from a base class
        tableName = self._schema.tableName(ApdbTables.SSObject)
        query = f'SELECT * from "{self._keyspace}"."{tableName}"'

        objects = None
        with Timer('SSObject select', self.config.timer):
            result = self._session.execute(query, execution_profile="read_pandas")
            objects = result._current_rows

        _LOG.debug("found %s DiaObjects", objects.shape[0])
        return objects

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
            self._storeDiaSources(ApdbTables.DiaSource, sources, visit_time)
            self._storeDiaSourcesPartitions(sources, visit_time)

        if forced_sources is not None:
            forced_sources = self._add_fsrc_part(forced_sources, objects)
            self._storeDiaSources(ApdbTables.DiaForcedSource, forced_sources, visit_time)

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
        selects: List[Tuple] = []
        for ids in chunk_iterable(idMap.keys(), 1_000):
            ids_str = ",".join(str(item) for item in ids)
            selects.append((
                (f'SELECT "diaSourceId", "apdb_part", "apdb_time_part" FROM "{self._keyspace}"."{table_name}"'
                 f' WHERE "diaSourceId" IN ({ids_str})'),
                {}
            ))

        # No need for DataFrame here, read data as tuples.
        result = cast(
            List[Tuple[int, int, int]],
            select_concurrent(self._session, selects, "read_tuples", self.config.read_concurrency)
        )

        # Make mapping from source ID to its partition.
        id2partitions: Dict[int, Tuple[int, int]] = {}
        for row in result:
            id2partitions[row[0]] = row[1:]

        # make sure we know partitions for each ID
        if set(id2partitions) != set(idMap):
            missing = ",".join(str(item) for item in set(idMap) - set(id2partitions))
            raise ValueError(f"Following DiaSource IDs do not exist in the database: {missing}")

        queries = cassandra.query.BatchStatement()
        table_name = self._schema.tableName(ApdbTables.DiaSource)
        for diaSourceId, ssObjectId in idMap.items():
            apdb_part, apdb_time_part = id2partitions[diaSourceId]
            values: Tuple
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
            queries.add(self._prep_statement(query), values)

        _LOG.debug("%s: will update %d records", table_name, len(idMap))
        with Timer(table_name + ' update', self.config.timer):
            self._session.execute(queries, execution_profile="write")

    def dailyJob(self) -> None:
        # docstring is inherited from a base class
        pass

    def countUnassociatedObjects(self) -> int:
        # docstring is inherited from a base class

        # It's too inefficient to implement it for Cassandra in current schema.
        raise NotImplementedError()

    def _makeProfiles(self, config: ApdbCassandraConfig) -> Mapping[Any, ExecutionProfile]:
        """Make all execution profiles used in the code."""

        if config.private_ips:
            loadBalancePolicy = WhiteListRoundRobinPolicy(hosts=config.contact_points)
        else:
            loadBalancePolicy = RoundRobinPolicy()

        pandas_row_factory: Callable
        if not config.pandas_delay_conv:
            pandas_row_factory = pandas_dataframe_factory
        else:
            pandas_row_factory = raw_data_factory

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
        read_pandas_multi_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.read_consistency),
            request_timeout=config.read_timeout,
            row_factory=pandas_row_factory,
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
            "read_pandas_multi": read_pandas_multi_profile,
            "write": write_profile,
            EXEC_PROFILE_DEFAULT: default_profile,
        }

    def _getSources(self, region: sphgeom.Region,
                    object_ids: Optional[Iterable[int]],
                    mjd_start: float,
                    mjd_end: float,
                    table_name: ApdbTables) -> pandas.DataFrame:
        """Returns catalog of DiaSource instances given set of DiaObject IDs.

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
            Catalog contaning DiaSource records. Empty catalog is returned if
            ``object_ids`` is empty.
        """
        object_id_set: Set[int] = set()
        if object_ids is not None:
            object_id_set = set(object_ids)
            if len(object_id_set) == 0:
                return self._make_empty_catalog(table_name)

        sp_where = self._spatial_where(region)
        tables, temporal_where = self._temporal_where(table_name, mjd_start, mjd_end)

        # Build all queries
        statements: List[Tuple] = []
        for table in tables:
            prefix = f'SELECT * from "{self._keyspace}"."{table}"'
            statements += list(self._combine_where(prefix, sp_where, temporal_where))
        _LOG.debug("_getSources %s: #queries: %s", table_name, len(statements))

        with Timer(table_name.name + ' select', self.config.timer):
            catalog = cast(
                pandas.DataFrame,
                select_concurrent(
                    self._session, statements, "read_pandas_multi", self.config.read_concurrency
                )
            )

        # filter by given object IDs
        if len(object_id_set) > 0:
            catalog = cast(pandas.DataFrame, catalog[catalog["diaObjectId"].isin(object_id_set)])

        # precise filtering on midPointTai
        catalog = cast(pandas.DataFrame, catalog[catalog["midPointTai"] > mjd_start])

        _LOG.debug("found %d %ss", catalog.shape[0], table_name.name)
        return catalog

    def _getSourcesHistory(
        self,
        table: ApdbTables,
        start_time: dafBase.DateTime,
        end_time: dafBase.DateTime,
        region: Optional[sphgeom.Region] = None,
    ) -> pandas.DataFrame:
        """Returns catalog of DiaSource instances given set of DiaObject IDs.

        Parameters
        ----------
        table : `ApdbTables`
            Name of the table.
        start_time : `dafBase.DateTime`
            Starting time for DiaSource history search. DiaSource record is
            selected when its ``midPointTai`` falls into an interval between
            ``start_time`` (inclusive) and ``end_time`` (exclusive).
        end_time : `dafBase.DateTime`
            Upper limit on time for DiaSource history search.
        region : `lsst.sphgeom.Region`
            Spherical region.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog contaning DiaSource records.
        """
        sp_where = self._spatial_where(region, use_ranges=False)
        tables, temporal_where = self._temporal_where(table, start_time, end_time, True)

        # Build all queries
        statements: List[Tuple] = []
        for table_name in tables:
            prefix = f'SELECT * from "{self._keyspace}"."{table_name}"'
            statements += list(self._combine_where(prefix, sp_where, temporal_where, "ALLOW FILTERING"))
        _LOG.debug("getDiaObjectsHistory: #queries: %s", len(statements))

        # Run all selects in parallel
        with Timer(f"{table.name} history", self.config.timer):
            catalog = cast(
                pandas.DataFrame,
                select_concurrent(
                    self._session, statements, "read_pandas_multi", self.config.read_concurrency
                )
            )

        # precise filtering on validityStart
        period_start = start_time.get(system=dafBase.DateTime.MJD)
        period_end = end_time.get(system=dafBase.DateTime.MJD)
        catalog = cast(
            pandas.DataFrame,
            catalog[(catalog["midPointTai"] >= period_start) & (catalog["midPointTai"] < period_end)]
        )

        _LOG.debug("found %d %ss", catalog.shape[0], table.name)
        return catalog

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
        self._storeObjectsPandas(objs, ApdbTables.DiaObjectLast, extra_columns=extra_columns)

        extra_columns["validityStart"] = visit_time_dt
        time_part: Optional[int] = self._time_partition(visit_time)
        if not self.config.time_partition_tables:
            extra_columns["apdb_time_part"] = time_part
            time_part = None

        self._storeObjectsPandas(objs, ApdbTables.DiaObject, extra_columns=extra_columns, time_part=time_part)

    def _storeDiaSources(self, table_name: ApdbTables, sources: pandas.DataFrame,
                         visit_time: dafBase.DateTime) -> None:
        """Store catalog of DIASources or DIAForcedSources from current visit.

        Parameters
        ----------
        sources : `pandas.DataFrame`
            Catalog containing DiaSource records
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit.
        """
        time_part: Optional[int] = self._time_partition(visit_time)
        extra_columns = {}
        if not self.config.time_partition_tables:
            extra_columns["apdb_time_part"] = time_part
            time_part = None

        self._storeObjectsPandas(sources, table_name, extra_columns=extra_columns, time_part=time_part)

    def _storeDiaSourcesPartitions(self, sources: pandas.DataFrame, visit_time: dafBase.DateTime) -> None:
        """Store mapping of diaSourceId to its partitioning values.

        Parameters
        ----------
        sources : `pandas.DataFrame`
            Catalog containing DiaSource records
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit.
        """
        id_map = cast(pandas.DataFrame, sources[["diaSourceId", "apdb_part"]])
        extra_columns = {
            "apdb_time_part": self._time_partition(visit_time),
        }

        self._storeObjectsPandas(
            id_map, ExtraTables.DiaSourceToPartition, extra_columns=extra_columns, time_part=None
        )

    def _storeObjectsPandas(self, objects: pandas.DataFrame, table_name: Union[ApdbTables, ExtraTables],
                            extra_columns: Optional[Mapping] = None,
                            time_part: Optional[int] = None) -> None:
        """Generic store method.

        Takes catalog of records and stores a bunch of objects in a table.

        Parameters
        ----------
        objects : `pandas.DataFrame`
            Catalog containing object records
        table_name : `ApdbTables`
            Name of the table as defined in APDB schema.
        extra_columns : `dict`, optional
            Mapping (column_name, column_value) which gives column values to add
            to every row, only if column is missing in catalog records.
        time_part : `int`, optional
            If not `None` then insert into a per-partition table.
        """
        # use extra columns if specified
        if extra_columns is None:
            extra_columns = {}
        extra_fields = list(extra_columns.keys())

        df_fields = [
            column for column in objects.columns if column not in extra_fields
        ]

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

        qfields = [quote_id(field) for field in fields]
        qfields_str = ','.join(qfields)

        with Timer(table_name.name + ' query build', self.config.timer):

            table = self._schema.tableName(table_name)
            if time_part is not None:
                table = f"{table}_{time_part}"

            holders = ','.join(['?']*len(qfields))
            query = f'INSERT INTO "{self._keyspace}"."{table}" ({qfields_str}) VALUES ({holders})'
            statement = self._prep_statement(query)
            queries = cassandra.query.BatchStatement()
            for rec in objects.itertuples(index=False):
                values = []
                for field in df_fields:
                    if field not in column_map:
                        continue
                    value = getattr(rec, field)
                    if column_map[field].type == "DATETIME":
                        if isinstance(value, pandas.Timestamp):
                            value = literal(value.to_pydatetime())
                        else:
                            # Assume it's seconds since epoch, Cassandra
                            # datetime is in milliseconds
                            value = int(value*1000)
                    values.append(literal(value))
                for field in extra_fields:
                    value = extra_columns[field]
                    values.append(literal(value))
                queries.add(statement, values)

        _LOG.debug("%s: will store %d records", self._schema.tableName(table_name), objects.shape[0])
        with Timer(table_name.name + ' insert', self.config.timer):
            self._session.execute(queries, timeout=self.config.write_timeout, execution_profile="write")

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

    def _time_partition(self, time: Union[float, dafBase.DateTime]) -> int:
        """Calculate time partiton number for a given time.

        Parameters
        ----------
        time : `float` or `lsst.daf.base.DateTime`
            Time for which to calculate partition number. Can be float to mean
            MJD or `lsst.daf.base.DateTime`

        Returns
        -------
        partition : `int`
            Partition number for a given time.
        """
        if isinstance(time, dafBase.DateTime):
            mjd = time.get(system=dafBase.DateTime.MJD)
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

        data = {columnDef.name: pandas.Series(dtype=columnDef.dtype) for columnDef in table.columns}
        return pandas.DataFrame(data)

    def _prep_statement(self, query: str) -> cassandra.query.PreparedStatement:
        """Convert query string into prepared statement."""
        stmt = self._prepared_statements.get(query)
        if stmt is None:
            stmt = self._session.prepare(query)
            self._prepared_statements[query] = stmt
        return stmt

    def _combine_where(
        self,
        prefix: str,
        where1: List[Tuple[str, Tuple]],
        where2: List[Tuple[str, Tuple]],
        suffix: Optional[str] = None,
    ) -> Iterator[Tuple[cassandra.query.Statement, Tuple]]:
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
                    statement = self._prep_statement(full_query)
                else:
                    # If there are no params then it is likely that query
                    # has a bunch of literals rendered already, no point
                    # trying to prepare it.
                    statement = cassandra.query.SimpleStatement(full_query)
                yield (statement, params)

    def _spatial_where(
        self, region: Optional[sphgeom.Region], use_ranges: bool = False
    ) -> List[Tuple[str, Tuple]]:
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
            expressions: List[Tuple[str, Tuple]] = []
            for lower, upper in pixel_ranges:
                upper -= 1
                if lower == upper:
                    expressions.append(('"apdb_part" = ?', (lower, )))
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
        start_time: Union[float, dafBase.DateTime],
        end_time: Union[float, dafBase.DateTime],
        query_per_time_part: Optional[bool] = None,
    ) -> Tuple[List[str], List[Tuple[str, Tuple]]]:
        """Generate table names and expressions for temporal part of WHERE
        clauses.

        Parameters
        ----------
        table : `ApdbTables`
            Table to select from.
        start_time : `dafBase.DateTime` or `float`
            Starting Datetime of MJD value of the time range.
        start_time : `dafBase.DateTime` or `float`
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
        tables: List[str]
        temporal_where: List[Tuple[str, Tuple]] = []
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
                temporal_where = [
                    ('"apdb_time_part" = ?', (time_part,)) for time_part in time_parts
                ]
            else:
                time_part_list = ",".join([str(part) for part in time_parts])
                temporal_where = [(f'"apdb_time_part" IN ({time_part_list})', ())]

        return tables, temporal_where
