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
from typing import TYPE_CHECKING, List, Optional, Tuple, Union

import felis.types
from felis import simple

from .apdbSchema import ApdbSchema, ApdbTables

if TYPE_CHECKING:
    import cassandra.cluster


_LOG = logging.getLogger(__name__)


@enum.unique
class ExtraTables(enum.Enum):
    """Names of the extra tables used by Cassandra implementation."""

    DiaSourceToPartition = "DiaSourceToPartition"
    "Maps diaSourceId ro its partition values (pixel and time)."

    def table_name(self, prefix: str = "") -> str:
        """Return full table name."""
        return prefix + self.value


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
        time_partition_tables: bool = False
    ):

        super().__init__(schema_file, schema_name)

        self._session = session
        self._keyspace = keyspace
        self._prefix = prefix
        self._time_partition_tables = time_partition_tables

        # add columns and index for partitioning.
        self._ignore_tables = []
        for table, tableDef in self.tableSchemas.items():
            columns = []
            add_columns = True
            if table in self._spatially_partitioned_tables:
                # DiaObjectLast does not need temporal partitioning
                columns = ["apdb_part"]
            elif table in self._time_partitioned_tables:
                if time_partition_tables:
                    columns = ["apdb_part"]
                else:
                    columns = ["apdb_part", "apdb_time_part"]
            elif table is ApdbTables.SSObject:
                # For SSObject there is no natural partition key but we have
                # to partition it because there are too many of them. I'm
                # going to partition on its primary key (and drop separate
                # primary key index).
                columns = ["ssObjectId"]
                tableDef.primary_key = []
                add_columns = False
            else:
                # TODO: Do not know yet how other tables can be partitioned
                self._ignore_tables.append(table)
                add_columns = False

            if add_columns:
                # add columns to the column list
                columnDefs = [
                    simple.Column(
                        id=f"#{name}",
                        name=name,
                        datatype=felis.types.Long,
                        nullable=False,
                    ) for name in columns
                ]
                tableDef.columns = columnDefs + tableDef.columns

            # make a partitioning index
            if columns:
                annotations = dict(tableDef.annotations)
                annotations["cassandra:paritioning_columns"] = columns
                tableDef.annotations = annotations

        self._extra_tables = self._extraTableSchema()

    def _extraTableSchema(self) -> Mapping[ExtraTables, simple.Table]:
        """Generate schema for extra tables."""
        return {
            ExtraTables.DiaSourceToPartition: simple.Table(
                id="#" + ExtraTables.DiaSourceToPartition.value,
                name=ExtraTables.DiaSourceToPartition.value,
                columns=[
                    simple.Column(
                        id="#diaSourceId", name="diaSourceId", datatype=felis.types.Long, nullable=False
                    ),
                    simple.Column(
                        id="#apdb_part", name="apdb_part", datatype=felis.types.Long, nullable=False
                    ),
                    simple.Column(
                        id="#apdb_time_part", name="apdb_time_part", datatype=felis.types.Int, nullable=False
                    ),
                ],
                primary_key=[],
                indexes=[],
                constraints=[],
                annotations={"cassandra:paritioning_columns": ["diaSourceId"]},
            ),
        }

    def tableName(self, table_name: Union[ApdbTables, ExtraTables]) -> str:
        """Return Cassandra table name for APDB table.
        """
        return table_name.table_name(self._prefix)

    def getColumnMap(self, table_name: Union[ApdbTables, ExtraTables]) -> Mapping[str, simple.Column]:
        """Returns mapping of column names to Column definitions.

        Parameters
        ----------
        table_name : `ApdbTables`
            One of known APDB table names.

        Returns
        -------
        column_map : `dict`
            Mapping of column names to `ColumnDef` instances.
        """
        if isinstance(table_name, ApdbTables):
            table_schema = self.tableSchemas[table_name]
        else:
            table_schema = self._extra_tables[table_name]
        cmap = {column.name: column for column in table_schema.columns}
        return cmap

    def partitionColumns(self, table_name: Union[ApdbTables, ExtraTables]) -> List[str]:
        """Return a list of columns used for table partitioning.

        Parameters
        ----------
        table_name : `ApdbTables`
            Table name in APDB schema

        Returns
        -------
        columns : `list` of `str`
            Names of columns for used for partitioning.
        """
        if isinstance(table_name, ApdbTables):
            table_schema = self.tableSchemas[table_name]
        else:
            table_schema = self._extra_tables[table_name]
        return table_schema.annotations.get("cassandra:paritioning_columns", [])

    def clusteringColumns(self, table_name: Union[ApdbTables, ExtraTables]) -> List[str]:
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
        if isinstance(table_name, ApdbTables):
            table_schema = self.tableSchemas[table_name]
        else:
            table_schema = self._extra_tables[table_name]
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
        for table in self.tableSchemas:
            self._makeTableSchema(table, drop, part_range)
        for extra_table in self._extra_tables:
            self._makeTableSchema(extra_table, drop, part_range)

    def _makeTableSchema(
        self,
        table: Union[ApdbTables, ExtraTables],
        drop: bool = False,
        part_range: Optional[Tuple[int, int]] = None
    ) -> None:
        if table in self._ignore_tables:
            _LOG.debug("Skipping schema for table %s", table)
            return
        _LOG.debug("Making table %s", table)

        fullTable = table.table_name(self._prefix)

        table_list = [fullTable]
        if part_range is not None:
            if table in self._time_partitioned_tables:
                partitions = range(*part_range)
                table_list = [f"{fullTable}_{part}" for part in partitions]

        if drop:
            queries = [
                f'DROP TABLE IF EXISTS "{self._keyspace}"."{table_name}"' for table_name in table_list
            ]
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

    def _tableColumns(self, table_name: Union[ApdbTables, ExtraTables]) -> List[str]:
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
        if isinstance(table_name, ApdbTables):
            table_schema = self.tableSchemas[table_name]
        else:
            table_schema = self._extra_tables[table_name]

        # must have partition columns and clustering columns
        part_columns = table_schema.annotations.get("cassandra:paritioning_columns", [])
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
