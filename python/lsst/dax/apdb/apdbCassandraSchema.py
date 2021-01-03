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

"""Module responsible for APDB schema operations.
"""

__all__ = ["ApdbCassandraSchema", "ApdbCassandraSchemaConfig"]

from datetime import datetime, timedelta
import logging

from lsst.pex.config import Field
from .apdbBaseSchema import ApdbBaseSchema, ApdbBaseSchemaConfig

_LOG = logging.getLogger(__name__.partition(".")[2])  # strip leading "lsst."

SECONDS_IN_MONTH = 30*24*3600


class ApdbCassandraSchemaConfig(ApdbBaseSchemaConfig):
    prefix = Field(dtype=str,
                   doc="Prefix to add to table names",
                   default="")
    per_month_tables = Field(dtype=bool,
                             doc="Use per-month tables for sources instead of paritioning by month",
                             default=True)


class ApdbCassandraSchema(ApdbBaseSchema):
    """Class for management of APDB schema.

    Parameters
    ----------
    session : `cassandra.cluster.Session`
        Cassandra session object
    config : `ApdbCassandraSchemaConfig`
        Configuration for this class.
    afw_schemas : `dict`, optional
        Dictionary with table name for a key and `afw.table.Schema`
        for a value. Columns in schema will be added to standard APDB
        schema (only if standard schema does not have matching column).
    """

    def __init__(self, session, config, afw_schemas=None):

        super().__init__(config, afw_schemas)

        self._session = session
        self._prefix = config.prefix
        self._per_month_tables = config.per_month_tables

        self.visitTableName = self._prefix + "ApdbProtoVisits"
        self.objectTableName = self._prefix + "DiaObject"
        self.lastObjectTableName = self._prefix + "DiaLastObject"
        self.sourceTableName = self._prefix + "DiaSource"
        self.forcedSourceTableName = self._prefix + "DiaForcedSource"

        # map cat column types to alchemy
        self._type_map = dict(DOUBLE="DOUBLE",
                              FLOAT="FLOAT",
                              DATETIME="TIMESTAMP",
                              BIGINT="BIGINT",
                              INTEGER="INT",
                              INT="INT",
                              TINYINT="TINYINT",
                              BLOB="BLOB",
                              CHAR="TEXT",
                              BOOL="BOOLEAN")

    def tableName(self, table_name):
        """Return Cassandra table name for APDB table.
        """
        return self._prefix + table_name

    def partitionColumns(self, table_name):
        """Return a list of columns used for table partitioning.

        Parameters
        ----------
        table_name : `str`
            Table name in APDB schema

        Returns
        -------
        columns : `list` of `str`
            Names of columns for used for partitioning.
        """
        table_schema = self.tableSchemas[table_name]
        for index in table_schema.indices:
            if index.type == 'PARTITION':
                # there could be just one partitoning index (possibly with few columns)
                return index.columns
        return []

    def makeSchema(self, drop=False):
        """Create or re-create all tables.

        Parameters
        ----------
        drop : `bool`, optional
            If True then drop tables before creating new ones.
        """

        # add internal visits table to the list of tables
        tables = list(self.tableSchemas) + [self.visitTableName]

        for table in tables:
            _LOG.debug("Making table %s", table)

            fullTable = self.tableName(table)

            table_list = [fullTable]
            if self._per_month_tables and \
                    table in ("DiaSource", "DiaForcedSource"):
                # TODO: this should not be hardcoded
                start_time = datetime(2020, 1, 1)
                seconds0 = int((start_time - datetime(1970, 1, 1)) / timedelta(seconds=1))
                month0 = seconds0 // SECONDS_IN_MONTH
                months = range(month0-13, month0+24)
                table_list = [f"{fullTable}_{month}" for month in months]

            if drop:
                for table_name in table_list:
                    query = 'DROP TABLE IF EXISTS "{}"'.format(table_name)
                    self._session.execute(query)

            for table_name in table_list:
                query = "CREATE TABLE "
                if not drop:
                    query += "IF NOT EXISTS "
                query += '"{}" ('.format(table_name)
                query += ", ".join(self._tableColumns(table))
                query += ")"
                _LOG.debug("query: %s", query)
                self._session.execute(query)

    def _tableColumns(self, table_name):
        """Return set of columns in a table

        Parameters
        ----------
        table_name : `str`
            Name of the table.

        Returns
        -------
        column_defs : `list`
            List of strings in the format "column_name type".
        """

        if table_name == "ApdbProtoVisits":
            column_defs = ['"apdb_part" INT',
                           '"visitId" INT',
                           '"visitTime" TIMESTAMP',
                           '"lastObjectId" BIGINT',
                           '"lastSourceId" BIGINT',
                           'PRIMARY KEY ("apdb_part", "visitId")']
            return column_defs

        table_schema = self.tableSchemas[table_name]

        # must have partition columns and clustering columns
        part_columns = []
        clust_columns = []
        for index in table_schema.indices:
            if index.type == 'PARTITION':
                part_columns = index.columns
            elif index.type == 'PRIMARY':
                clust_columns = index.columns
        _LOG.debug("part_columns: %s", part_columns)
        _LOG.debug("clust_columns: %s", clust_columns)
        if not part_columns:
            raise ValueError("Table {} configuration is missing partition index".format(table_name))
        if not clust_columns:
            raise ValueError("Table {} configuration is missing primary index".format(table_name))

        # all columns
        column_defs = []
        for column in table_schema.columns:
            ctype = self._type_map[column.type]
            column_defs.append('"{}" {}'.format(column.name, ctype))

        # primary key definition
        part_columns = ['"{}"'.format(col) for col in part_columns]
        clust_columns = ['"{}"'.format(col) for col in clust_columns]
        if len(part_columns) > 1:
            part_columns = ["(" + ", ".join(part_columns) + ")"]
        pkey = part_columns + clust_columns
        _LOG.debug("pkey: %s", pkey)
        column_defs.append('PRIMARY KEY ({})'.format(", ".join(pkey)))

        return column_defs
