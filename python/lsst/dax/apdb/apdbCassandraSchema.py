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

__all__ = ["ApdbCassandraSchema"]

import enum
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, List, Optional, Tuple

import felis.types
from felis import simple

from .apdbSchema import ApdbSchema, ApdbTables

if TYPE_CHECKING:
    import cassandra.cluster


_LOG = logging.getLogger(__name__)


class _FelisUUID(felis.types.FelisType, felis_name="uuid", votable_name="uuid"):
    """Special internal type for UUID columns. Felis does not support UUID,
    but we need it here, to simplify logic it's easier to add a special class.
    """


@enum.unique
class ExtraTables(enum.Enum):
    """Names of the extra tables used by Cassandra implementation."""

    DiaInsertId = "DiaInsertId"
    """Name of the table for insert ID records."""

    DiaObjectInsertId = "DiaObjectInsertId"
    """Name of the table for DIAObject insert ID records."""

    DiaSourceInsertId = "DiaSourceInsertId"
    """Name of the table for DIASource insert ID records."""

    DiaForcedSourceInsertId = "DiaFSourceInsertId"
    """Name of the table for DIAForcedSource insert ID records."""

    DiaSourceToPartition = "DiaSourceToPartition"
    "Maps diaSourceId to its partition values (pixel and time)."

    def table_name(self, prefix: str = "") -> str:
        """Return full table name."""
        return prefix + self.value

    @classmethod
    def insert_id_tables(cls) -> Mapping[ExtraTables, ApdbTables]:
        """Return mapping of tables used for insert ID tracking to their
        corresponding regular tables.
        """
        return {
            cls.DiaObjectInsertId: ApdbTables.DiaObject,
            cls.DiaSourceInsertId: ApdbTables.DiaSource,
            cls.DiaForcedSourceInsertId: ApdbTables.DiaForcedSource,
        }


class ApdbCassandraSchema(ApdbSchema):
    """Class for management of APDB schema.

    Parameters
    ----------
    session : `cassandra.cluster.Session`
        Cassandra session object
    schema_file : `str`
        Name of the YAML schema file.
    schema_name : `str`, optional
        Name of the schema in YAML files.
    prefix : `str`, optional
        Prefix to add to all schema elements.
    time_partition_tables : `bool`
        If True then schema will have a separate table for each time partition.
    """

    _type_map = {
        felis.types.Double: "DOUBLE",
        felis.types.Float: "FLOAT",
        felis.types.Timestamp: "TIMESTAMP",
        felis.types.Long: "BIGINT",
        felis.types.Int: "INT",
        felis.types.Short: "INT",
        felis.types.Byte: "TINYINT",
        felis.types.Binary: "BLOB",
        felis.types.Char: "TEXT",
        felis.types.String: "TEXT",
        felis.types.Unicode: "TEXT",
        felis.types.Text: "TEXT",
        felis.types.Boolean: "BOOLEAN",
        _FelisUUID: "UUID",
    }
    """Map YAML column types to Cassandra"""

    _time_partitioned_tables = [
        ApdbTables.DiaObject,
        ApdbTables.DiaSource,
        ApdbTables.DiaForcedSource,
    ]
    _spatially_partitioned_tables = [ApdbTables.DiaObjectLast]

    def __init__(
        self,
        session: cassandra.cluster.Session,
        keyspace: str,
        schema_file: str,
        schema_name: str = "ApdbSchema",
        prefix: str = "",
        time_partition_tables: bool = False,
        use_insert_id: bool = False,
    ):
        super().__init__(schema_file, schema_name)

        self._session = session
        self._keyspace = keyspace
        self._prefix = prefix
        self._time_partition_tables = time_partition_tables
        self._use_insert_id = use_insert_id
        self._has_insert_id: bool | None = None

        self._apdb_tables = self._apdb_tables_schema(time_partition_tables)
        self._extra_tables = self._extra_tables_schema()

    def _apdb_tables_schema(self, time_partition_tables: bool) -> Mapping[ApdbTables, simple.Table]:
        """Generate schema for regular APDB tables."""
        apdb_tables: dict[ApdbTables, simple.Table] = {}

        # add columns and index for partitioning.
        for table, apdb_table_def in self.tableSchemas.items():
            part_columns = []
            add_columns = []
            primary_key = apdb_table_def.primary_key[:]
            if table in self._spatially_partitioned_tables:
                # DiaObjectLast does not need temporal partitioning
                part_columns = ["apdb_part"]
                add_columns = part_columns
            elif table in self._time_partitioned_tables:
                if time_partition_tables:
                    part_columns = ["apdb_part"]
                else:
                    part_columns = ["apdb_part", "apdb_time_part"]
                add_columns = part_columns
            elif table is ApdbTables.SSObject:
                # For SSObject there is no natural partition key but we have
                # to partition it because there are too many of them. I'm
                # going to partition on its primary key (and drop separate
                # primary key index).
                part_columns = ["ssObjectId"]
                primary_key = []
            else:
                # TODO: Do not know what to do with the other tables
                continue

            column_defs = []
            if add_columns:
                column_defs = [
                    simple.Column(id=f"#{name}", name=name, datatype=felis.types.Long, nullable=False)
                    for name in add_columns
                ]

            annotations = dict(apdb_table_def.annotations)
            annotations["cassandra:apdb_column_names"] = [column.name for column in apdb_table_def.columns]
            if part_columns:
                annotations["cassandra:partitioning_columns"] = part_columns

            apdb_tables[table] = simple.Table(
                id=apdb_table_def.id,
                name=apdb_table_def.name,
                columns=column_defs + apdb_table_def.columns,
                primary_key=primary_key,
                indexes=[],
                constraints=[],
                annotations=annotations,
            )

        return apdb_tables

    def _extra_tables_schema(self) -> Mapping[ExtraTables, simple.Table]:
        """Generate schema for extra tables."""
        extra_tables: dict[ExtraTables, simple.Table] = {}

        # This table maps DiaSource ID to its partitions in DiaSource table and
        # DiaSourceInsertId tables.
        extra_tables[ExtraTables.DiaSourceToPartition] = simple.Table(
            id="#" + ExtraTables.DiaSourceToPartition.value,
            name=ExtraTables.DiaSourceToPartition.table_name(self._prefix),
            columns=[
                simple.Column(
                    id="#diaSourceId", name="diaSourceId", datatype=felis.types.Long, nullable=False
                ),
                simple.Column(id="#apdb_part", name="apdb_part", datatype=felis.types.Long, nullable=False),
                simple.Column(
                    id="#apdb_time_part", name="apdb_time_part", datatype=felis.types.Int, nullable=False
                ),
                simple.Column(id="#insert_id", name="insert_id", datatype=_FelisUUID, nullable=True),
            ],
            primary_key=[],
            indexes=[],
            constraints=[],
            annotations={"cassandra:partitioning_columns": ["diaSourceId"]},
        )

        insert_id_column = simple.Column(
            id="#insert_id", name="insert_id", datatype=_FelisUUID, nullable=False
        )

        if not self._use_insert_id:
            return extra_tables

        # Table containing insert IDs, this one is not partitioned, but
        # partition key must be defined.
        extra_tables[ExtraTables.DiaInsertId] = simple.Table(
            id="#" + ExtraTables.DiaInsertId.value,
            name=ExtraTables.DiaInsertId.table_name(self._prefix),
            columns=[
                simple.Column(id="#partition", name="partition", datatype=felis.types.Int, nullable=False),
                insert_id_column,
                simple.Column(
                    id="#insert_time", name="insert_time", datatype=felis.types.Timestamp, nullable=False
                ),
            ],
            primary_key=[insert_id_column],
            indexes=[],
            constraints=[],
            annotations={"cassandra:partitioning_columns": ["partition"]},
        )

        for insert_id_table_enum, apdb_table_enum in ExtraTables.insert_id_tables().items():
            apdb_table_def = self.tableSchemas[apdb_table_enum]

            extra_tables[insert_id_table_enum] = simple.Table(
                id="#" + insert_id_table_enum.value,
                name=insert_id_table_enum.table_name(self._prefix),
                columns=[insert_id_column] + apdb_table_def.columns,
                primary_key=apdb_table_def.primary_key[:],
                indexes=[],
                constraints=[],
                annotations={
                    "cassandra:partitioning_columns": ["insert_id"],
                    "cassandra:apdb_column_names": [column.name for column in apdb_table_def.columns],
                },
            )

        return extra_tables

    @property
    def has_insert_id(self) -> bool:
        """Whether insert ID tables are to be used (`bool`)."""
        if self._has_insert_id is None:
            self._has_insert_id = self._use_insert_id and self._check_insert_id()
        return self._has_insert_id

    def _check_insert_id(self) -> bool:
        """Check whether database has tables for tracking insert IDs."""
        table_name = ExtraTables.DiaInsertId.table_name(self._prefix)
        query = "SELECT count(*) FROM system_schema.tables WHERE keyspace_name = %s and table_name = %s"
        result = self._session.execute(query, (self._keyspace, table_name))
        row = result.one()
        return bool(row[0])

    def tableName(self, table_name: ApdbTables | ExtraTables) -> str:
        """Return Cassandra table name for APDB table."""
        return table_name.table_name(self._prefix)

    def getColumnMap(self, table_name: ApdbTables | ExtraTables) -> Mapping[str, simple.Column]:
        """Return mapping of column names to Column definitions.

        Parameters
        ----------
        table_name : `ApdbTables`
            One of known APDB table names.

        Returns
        -------
        column_map : `dict`
            Mapping of column names to `ColumnDef` instances.
        """
        table_schema = self._table_schema(table_name)
        cmap = {column.name: column for column in table_schema.columns}
        return cmap

    def apdbColumnNames(self, table_name: ApdbTables | ExtraTables) -> List[str]:
        """Return a list of columns names for a table as defined in APDB
        schema.

        Parameters
        ----------
        table_name : `ApdbTables` or `ExtraTables`
            Enum for a table in APDB schema.

        Returns
        -------
        columns : `list` of `str`
            Names of regular columns in the table.
        """
        table_schema = self._table_schema(table_name)
        return table_schema.annotations["cassandra:apdb_column_names"]

    def partitionColumns(self, table_name: ApdbTables | ExtraTables) -> List[str]:
        """Return a list of columns used for table partitioning.

        Parameters
        ----------
        table_name : `ApdbTables`
            Table name in APDB schema

        Returns
        -------
        columns : `list` of `str`
            Names of columns used for partitioning.
        """
        table_schema = self._table_schema(table_name)
        return table_schema.annotations.get("cassandra:partitioning_columns", [])

    def clusteringColumns(self, table_name: ApdbTables | ExtraTables) -> List[str]:
        """Return a list of columns used for clustering.

        Parameters
        ----------
        table_name : `ApdbTables`
            Table name in APDB schema

        Returns
        -------
        columns : `list` of `str`
            Names of columns for used for clustering.
        """
        table_schema = self._table_schema(table_name)
        return [column.name for column in table_schema.primary_key]

    def makeSchema(self, drop: bool = False, part_range: Optional[Tuple[int, int]] = None) -> None:
        """Create or re-create all tables.

        Parameters
        ----------
        drop : `bool`
            If True then drop tables before creating new ones.
        part_range : `tuple` [ `int` ] or `None`
            Start and end partition number for time partitions, end is not
            inclusive. Used to create per-partition DiaObject, DiaSource, and
            DiaForcedSource tables. If `None` then per-partition tables are
            not created.
        """
        # Try to create keyspace if it does not exist
        query = (
            f'CREATE KEYSPACE IF NOT EXISTS "{self._keyspace}"'
            " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}"
        )
        self._session.execute(query)

        for table in self._apdb_tables:
            self._makeTableSchema(table, drop, part_range)
        for extra_table in self._extra_tables:
            self._makeTableSchema(extra_table, drop, part_range)
        # Reset cached information.
        self._has_insert_id = None

    def _makeTableSchema(
        self,
        table: ApdbTables | ExtraTables,
        drop: bool = False,
        part_range: Optional[Tuple[int, int]] = None,
    ) -> None:
        _LOG.debug("Making table %s", table)

        fullTable = table.table_name(self._prefix)

        table_list = [fullTable]
        if part_range is not None:
            if table in self._time_partitioned_tables:
                partitions = range(*part_range)
                table_list = [f"{fullTable}_{part}" for part in partitions]

        if drop:
            queries = [f'DROP TABLE IF EXISTS "{self._keyspace}"."{table_name}"' for table_name in table_list]
            futures = [self._session.execute_async(query, timeout=None) for query in queries]
            for future in futures:
                _LOG.debug("wait for query: %s", future.query)
                future.result()
                _LOG.debug("query finished: %s", future.query)

        queries = []
        for table_name in table_list:
            if_not_exists = "" if drop else "IF NOT EXISTS"
            columns = ", ".join(self._tableColumns(table))
            query = f'CREATE TABLE {if_not_exists} "{self._keyspace}"."{table_name}" ({columns})'
            _LOG.debug("query: %s", query)
            queries.append(query)
        futures = [self._session.execute_async(query, timeout=None) for query in queries]
        for future in futures:
            _LOG.debug("wait for query: %s", future.query)
            future.result()
            _LOG.debug("query finished: %s", future.query)

    def _tableColumns(self, table_name: ApdbTables | ExtraTables) -> List[str]:
        """Return set of columns in a table

        Parameters
        ----------
        table_name : `ApdbTables`
            Name of the table.

        Returns
        -------
        column_defs : `list`
            List of strings in the format "column_name type".
        """
        table_schema = self._table_schema(table_name)

        # must have partition columns and clustering columns
        part_columns = table_schema.annotations.get("cassandra:partitioning_columns", [])
        clust_columns = [column.name for column in table_schema.primary_key]
        _LOG.debug("part_columns: %s", part_columns)
        _LOG.debug("clust_columns: %s", clust_columns)
        if not part_columns:
            raise ValueError(f"Table {table_name} configuration is missing partition index")

        # all columns
        column_defs = []
        for column in table_schema.columns:
            ctype = self._type_map[column.datatype]
            column_defs.append(f'"{column.name}" {ctype}')

        # primary key definition
        part_columns = [f'"{col}"' for col in part_columns]
        clust_columns = [f'"{col}"' for col in clust_columns]
        if len(part_columns) > 1:
            columns = ", ".join(part_columns)
            part_columns = [f"({columns})"]
        pkey = ", ".join(part_columns + clust_columns)
        _LOG.debug("pkey: %s", pkey)
        column_defs.append(f"PRIMARY KEY ({pkey})")

        return column_defs

    def _table_schema(self, table: ApdbTables | ExtraTables) -> simple.Table:
        """Return schema definition for a table."""
        if isinstance(table, ApdbTables):
            table_schema = self._apdb_tables[table]
        else:
            table_schema = self._extra_tables[table]
        return table_schema
