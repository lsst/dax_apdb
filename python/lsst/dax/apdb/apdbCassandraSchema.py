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

import logging
from typing import List, Mapping, Optional, TYPE_CHECKING, Tuple

from .apdbSchema import ApdbSchema, ApdbTables, ColumnDef, IndexDef, IndexType

if TYPE_CHECKING:
    import cassandra.cluster


_LOG = logging.getLogger(__name__)


class ApdbCassandraSchema(ApdbSchema):
    """Class for management of APDB schema.

    Parameters
    ----------
    session : `cassandra.cluster.Session`
        Cassandra session object
    schema_file : `str`
        Name of the YAML schema file.
    extra_schema_file : `str`, optional
        Name of the YAML schema file with extra column definitions.
    prefix : `str`, optional
        Prefix to add to all schema elements.
    packing : `str`
        Type of packing to apply to columns, string "none" disable packing,
        any other value enables it.
    time_partition_tables : `bool`
        If True then schema will have a separate table for each time partition.
    """

    _type_map = dict(DOUBLE="DOUBLE",
                     FLOAT="FLOAT",
                     DATETIME="TIMESTAMP",
                     BIGINT="BIGINT",
                     INTEGER="INT",
                     INT="INT",
                     TINYINT="TINYINT",
                     BLOB="BLOB",
                     CHAR="TEXT",
                     BOOL="BOOLEAN")
    """Map YAML column types to Cassandra"""

    def __init__(self, session: cassandra.cluster.Session, keyspace: str, schema_file: str,
                 extra_schema_file: Optional[str] = None, prefix: str = "",
                 time_partition_tables: bool = False):

        super().__init__(schema_file, extra_schema_file)

        self._session = session
        self._keyspace = keyspace
        self._prefix = prefix

        # add columns and index for partitioning.
        self._ignore_tables = []
        for table, tableDef in self.tableSchemas.items():
            columns = []
            add_columns = True
            if table is ApdbTables.DiaObjectLast:
                # DiaObjectLast does not need temporal partitioning
                columns = ["apdb_part"]
            elif table in (ApdbTables.DiaObject, ApdbTables.DiaSource, ApdbTables.DiaForcedSource):
                # these three tables can use either pure spatial or combined
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
                tableDef.indices = [
                    index for index in tableDef.indices if index.type is not IndexType.PRIMARY
                ]
                add_columns = False
            else:
                # TODO: Do not know yet how other tables can be partitioned
                self._ignore_tables.append(table)
                add_columns = False

            if add_columns:
                # add columns to the column list
                columnDefs = [ColumnDef(name=name,
                                        type="BIGINT",
                                        nullable=False,
                                        default=None,
                                        description="",
                                        unit=None,
                                        ucd=None) for name in columns]
                tableDef.columns = columnDefs + tableDef.columns

            # make an index
            if columns:
                index = IndexDef(name=f"Part_{tableDef.name}", type=IndexType.PARTITION, columns=columns)
                tableDef.indices.append(index)

    def tableName(self, table_name: ApdbTables) -> str:
        """Return Cassandra table name for APDB table.
        """
        return table_name.table_name(self._prefix)

    def getColumnMap(self, table_name: ApdbTables) -> Mapping[str, ColumnDef]:
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
        table = self.tableSchemas[table_name]
        cmap = {column.name: column for column in table.columns}
        return cmap

    def partitionColumns(self, table_name: ApdbTables) -> List[str]:
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
        table_schema = self.tableSchemas[table_name]
        for index in table_schema.indices:
            if index.type is IndexType.PARTITION:
                # there could be just one partitoning index (possibly with few columns)
                return index.columns
        return []

    def clusteringColumns(self, table_name: ApdbTables) -> List[str]:
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
        table_schema = self.tableSchemas[table_name]
        for index in table_schema.indices:
            if index.type is IndexType.PRIMARY:
                return index.columns
        return []

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
            if table in self._ignore_tables:
                _LOG.debug("Skipping schema for table %s", table)
                continue
            _LOG.debug("Making table %s", table)

            fullTable = table.table_name(self._prefix)

            table_list = [fullTable]
            if part_range is not None:
                if table in (ApdbTables.DiaSource, ApdbTables.DiaForcedSource, ApdbTables.DiaObject):
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

    def _tableColumns(self, table_name: ApdbTables) -> List[str]:
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
        table_schema = self.tableSchemas[table_name]

        # must have partition columns and clustering columns
        part_columns = []
        clust_columns = []
        index_columns = set()
        for index in table_schema.indices:
            if index.type is IndexType.PARTITION:
                part_columns = index.columns
            elif index.type is IndexType.PRIMARY:
                clust_columns = index.columns
            index_columns.update(index.columns)
        _LOG.debug("part_columns: %s", part_columns)
        _LOG.debug("clust_columns: %s", clust_columns)
        if not part_columns:
            raise ValueError(f"Table {table_name} configuration is missing partition index")

        # all columns
        column_defs = []
        for column in table_schema.columns:
            ctype = self._type_map[column.type]
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
