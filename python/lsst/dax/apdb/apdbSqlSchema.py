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

from __future__ import annotations

__all__ = ["ApdbSqlSchema"]

import logging
from typing import Any, Dict, List, Mapping, Optional, Type

import felis.types
import sqlalchemy
from felis import simple
from sqlalchemy import DDL, Column, Index, MetaData, PrimaryKeyConstraint, Table, UniqueConstraint, event

from .apdbSchema import ApdbSchema, ApdbTables

_LOG = logging.getLogger(__name__)


class ApdbSqlSchema(ApdbSchema):
    """Class for management of APDB schema.

    Attributes
    ----------
    objects : `sqlalchemy.Table`
        DiaObject table instance
    objects_last : `sqlalchemy.Table`
        DiaObjectLast table instance, may be None
    sources : `sqlalchemy.Table`
        DiaSource table instance
    forcedSources : `sqlalchemy.Table`
        DiaForcedSource table instance

    Parameters
    ----------
    engine : `sqlalchemy.engine.Engine`
        SQLAlchemy engine instance
    dia_object_index : `str`
        Indexing mode for DiaObject table, see `ApdbSqlConfig.dia_object_index`
        for details.
    htm_index_column : `str`
        Name of a HTM index column for DiaObject and DiaSource tables.
    schema_file : `str`
        Name of the YAML schema file.
    schema_name : `str`, optional
        Name of the schema in YAML files.
    prefix : `str`, optional
        Prefix to add to all scheam elements.
    namespace : `str`, optional
        Namespace (or schema name) to use for all APDB tables.
    """
    def __init__(
        self,
        engine: sqlalchemy.engine.Engine,
        dia_object_index: str,
        htm_index_column: str,
        schema_file: str,
        schema_name: str = "ApdbSchema",
        prefix: str = "",
        namespace: Optional[str] = None,
    ):

        super().__init__(schema_file, schema_name)

        self._engine = engine
        self._dia_object_index = dia_object_index
        self._prefix = prefix

        self._metadata = MetaData(self._engine, schema=namespace)

        # map YAML column types to SQLAlchemy
        self._type_map = {
            felis.types.Double: self._getDoubleType(engine),
            felis.types.Float: sqlalchemy.types.Float,
            felis.types.Timestamp: sqlalchemy.types.TIMESTAMP,
            felis.types.Long: sqlalchemy.types.BigInteger,
            felis.types.Int: sqlalchemy.types.Integer,
            felis.types.Short: sqlalchemy.types.Integer,
            felis.types.Byte: sqlalchemy.types.Integer,
            felis.types.Binary: sqlalchemy.types.LargeBinary,
            felis.types.Text: sqlalchemy.types.CHAR,
            felis.types.String: sqlalchemy.types.CHAR,
            felis.types.Char: sqlalchemy.types.CHAR,
            felis.types.Unicode: sqlalchemy.types.CHAR,
            felis.types.Boolean: sqlalchemy.types.Boolean
        }

        # Add pixelId column and index to tables that need it
        for table in (ApdbTables.DiaObject, ApdbTables.DiaObjectLast, ApdbTables.DiaSource):
            tableDef = self.tableSchemas.get(table)
            if not tableDef:
                continue
            column = simple.Column(
                id=f"#{htm_index_column}",
                name=htm_index_column,
                datatype=felis.types.Long,
                nullable=False,
                value=None,
                description="Pixelization index column.",
                table=tableDef
            )
            tableDef.columns.append(column)

            # Adjust index if needed
            if table == ApdbTables.DiaObject and self._dia_object_index == 'pix_id_iov':
                tableDef.primary_key.insert(0, column)

            if table is ApdbTables.DiaObjectLast:
                # use it as a leading PK column
                tableDef.primary_key.insert(0, column)
            else:
                # make a regular index
                name = f"IDX_{tableDef.name}_{htm_index_column}"
                index = simple.Index(id=f"#{name}", name=name, columns=[column])
                tableDef.indexes.append(index)

        # generate schema for all tables, must be called last
        self._tables = self._makeTables()

        self.objects = self._tables[ApdbTables.DiaObject]
        self.objects_last = self._tables.get(ApdbTables.DiaObjectLast)
        self.sources = self._tables[ApdbTables.DiaSource]
        self.forcedSources = self._tables[ApdbTables.DiaForcedSource]
        self.ssObjects = self._tables[ApdbTables.SSObject]

    def _makeTables(self, mysql_engine: str = 'InnoDB') -> Mapping[ApdbTables, Table]:
        """Generate schema for all tables.

        Parameters
        ----------
        mysql_engine : `str`, optional
            MySQL engine type to use for new tables.
        """

        info: Dict[str, Any] = {}

        tables = {}
        for table_enum in ApdbTables:

            if table_enum is ApdbTables.DiaObjectLast and self._dia_object_index != "last_object_table":
                continue

            columns = self._tableColumns(table_enum)
            constraints = self._tableIndices(table_enum, info)
            table = Table(table_enum.table_name(self._prefix),
                          self._metadata,
                          *columns,
                          *constraints,
                          mysql_engine=mysql_engine,
                          info=info)
            tables[table_enum] = table

        return tables

    def makeSchema(self, drop: bool = False, mysql_engine: str = 'InnoDB') -> None:
        """Create or re-create all tables.

        Parameters
        ----------
        drop : `bool`, optional
            If True then drop tables before creating new ones.
        mysql_engine : `str`, optional
            MySQL engine type to use for new tables.
        """

        # re-make table schema for all needed tables with possibly different options
        _LOG.debug("clear metadata")
        self._metadata.clear()
        _LOG.debug("re-do schema mysql_engine=%r", mysql_engine)
        self._makeTables(mysql_engine=mysql_engine)

        # Create namespace if it does not exist yet, for now this only makes
        # sense for postgres.
        if self._metadata.schema:
            dialect = self._engine.dialect
            quoted_schema = dialect.preparer(dialect).quote_schema(self._metadata.schema)
            create_schema = DDL(
                "CREATE SCHEMA IF NOT EXISTS %(schema)s", context={"schema": quoted_schema}
            ).execute_if(dialect='postgresql')
            event.listen(self._metadata, "before_create", create_schema)

        # create all tables (optionally drop first)
        if drop:
            _LOG.info('dropping all tables')
            self._metadata.drop_all()
        _LOG.info('creating all tables')
        self._metadata.create_all()

    def _tableColumns(self, table_name: ApdbTables) -> List[Column]:
        """Return set of columns in a table

        Parameters
        ----------
        table_name : `ApdbTables`
            Name of the table.

        Returns
        -------
        column_defs : `list`
            List of `Column` objects.
        """

        # get the list of columns in primary key, they are treated somewhat
        # specially below
        table_schema = self.tableSchemas[table_name]

        # convert all column dicts into alchemy Columns
        column_defs = []
        for column in table_schema.columns:
            kwargs: Dict[str, Any] = dict(nullable=column.nullable)
            if column.value is not None:
                kwargs.update(server_default=str(column.value))
            if column in table_schema.primary_key:
                kwargs.update(autoincrement=False)
            ctype = self._type_map[column.datatype]
            column_defs.append(Column(column.name, ctype, **kwargs))

        return column_defs

    def _tableIndices(self, table_name: ApdbTables, info: Dict) -> List[sqlalchemy.schema.Constraint]:
        """Return set of constraints/indices in a table

        Parameters
        ----------
        table_name : `ApdbTables`
            Name of the table.
        info : `dict`
            Additional options passed to SQLAlchemy index constructor.

        Returns
        -------
        index_defs : `list`
            List of SQLAlchemy index/constraint objects.
        """

        table_schema = self.tableSchemas[table_name]

        # convert all index dicts into alchemy Columns
        index_defs: List[sqlalchemy.schema.Constraint] = []
        if table_schema.primary_key:
            index_defs.append(PrimaryKeyConstraint(*[column.name for column in table_schema.primary_key]))
        for index in table_schema.indexes:
            name = self._prefix + index.name if index.name else ""
            index_defs.append(Index(name, *[column.name for column in index.columns], info=info))
        for constraint in table_schema.constraints:
            kwargs = {}
            if constraint.name:
                kwargs['name'] = self._prefix + constraint.name
            if isinstance(constraint, simple.UniqueConstraint):
                index_defs.append(UniqueConstraint(*[column.name for column in constraint.columns], **kwargs))

        return index_defs

    @classmethod
    def _getDoubleType(cls, engine: sqlalchemy.engine.Engine) -> Type:
        """DOUBLE type is database-specific, select one based on dialect.

        Parameters
        ----------
        engine : `sqlalchemy.engine.Engine`
            Database engine.

        Returns
        -------
        type_object : `object`
            Database-specific type definition.
        """
        if engine.name == 'mysql':
            from sqlalchemy.dialects.mysql import DOUBLE
            return DOUBLE(asdecimal=False)
        elif engine.name == 'postgresql':
            from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION
            return DOUBLE_PRECISION
        elif engine.name == 'oracle':
            from sqlalchemy.dialects.oracle import DOUBLE_PRECISION
            return DOUBLE_PRECISION
        elif engine.name == 'sqlite':
            # all floats in sqlite are 8-byte
            from sqlalchemy.dialects.sqlite import REAL
            return REAL
        else:
            raise TypeError('cannot determine DOUBLE type, unexpected dialect: ' + engine.name)
