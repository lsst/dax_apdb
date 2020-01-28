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
from typing import Any, Dict, List, Optional, Type

import sqlalchemy
from sqlalchemy import (Column, Index, MetaData, PrimaryKeyConstraint,
                        UniqueConstraint, Table)

from .apdbBaseSchema import ApdbBaseSchema


_LOG = logging.getLogger(__name__)


class ApdbSqlSchema(ApdbBaseSchema):
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
        Indexing mode for DiaObject table, see `ApdbConfig.dia_object_index`
        for details.
    schema_file : `str`
        Name of the YAML schema file.
    extra_schema_file : `str`, optional
        Name of the YAML schema file with extra column definitions.
    prefix : `str`, optional
        Prefix to add to all scheam elements.
    """
    def __init__(self, engine: sqlalchemy.engine.Engine, dia_object_index: str,
                 schema_file: str, extra_schema_file: Optional[str] = None, prefix: str = ""):

        super().__init__(schema_file, extra_schema_file)

        self._engine = engine
        self._dia_object_index = dia_object_index
        self._prefix = prefix

        self._metadata = MetaData(self._engine)

        self.objects = None
        self.objects_last = None
        self.sources = None
        self.forcedSources = None

        # map cat column types to alchemy
        self._type_map = dict(DOUBLE=self._getDoubleType(engine),
                              FLOAT=sqlalchemy.types.Float,
                              DATETIME=sqlalchemy.types.TIMESTAMP,
                              BIGINT=sqlalchemy.types.BigInteger,
                              INTEGER=sqlalchemy.types.Integer,
                              INT=sqlalchemy.types.Integer,
                              TINYINT=sqlalchemy.types.Integer,
                              BLOB=sqlalchemy.types.LargeBinary,
                              CHAR=sqlalchemy.types.CHAR,
                              BOOL=sqlalchemy.types.Boolean)

        # generate schema for all tables, must be called last
        self._makeTables()

    def _makeTables(self, mysql_engine: str = 'InnoDB') -> None:
        """Generate schema for all tables.

        Parameters
        ----------
        mysql_engine : `str`, optional
            MySQL engine type to use for new tables.
        """

        info: Dict[str, Any] = {}

        if self._dia_object_index == 'pix_id_iov':
            # Special PK with HTM column in first position
            constraints = self._tableIndices('DiaObjectIndexHtmFirst', info)
        else:
            constraints = self._tableIndices('DiaObject', info)
        table = Table(self._prefix+'DiaObject', self._metadata,
                      *(self._tableColumns('DiaObject') + constraints),
                      mysql_engine=mysql_engine,
                      info=info)
        self.objects = table

        if self._dia_object_index == 'last_object_table':
            # Same as DiaObject but with special index
            table = Table(self._prefix+'DiaObjectLast', self._metadata,
                          *(self._tableColumns('DiaObjectLast')
                            + self._tableIndices('DiaObjectLast', info)),
                          mysql_engine=mysql_engine,
                          info=info)
            self.objects_last = table

        # for all other tables use index definitions in schema
        for table_name in ('DiaSource', 'SSObject', 'DiaForcedSource', 'DiaObject_To_Object_Match'):
            table = Table(self._prefix+table_name, self._metadata,
                          *(self._tableColumns(table_name)
                            + self._tableIndices(table_name, info)),
                          mysql_engine=mysql_engine,
                          info=info)
            if table_name == 'DiaSource':
                self.sources = table
            elif table_name == 'DiaForcedSource':
                self.forcedSources = table

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

        # create all tables (optionally drop first)
        if drop:
            _LOG.info('dropping all tables')
            self._metadata.drop_all()
        _LOG.info('creating all tables')
        self._metadata.create_all()

    def _tableColumns(self, table_name: str) -> List[Column]:
        """Return set of columns in a table

        Parameters
        ----------
        table_name : `str`
            Name of the table.

        Returns
        -------
        column_defs : `list`
            List of `Column` objects.
        """

        # get the list of columns in primary key, they are treated somewhat
        # specially below
        table_schema = self.tableSchemas[table_name]
        pkey_columns = set()
        for index in table_schema.indices:
            if index.type == 'PRIMARY':
                pkey_columns = set(index.columns)
                break

        # convert all column dicts into alchemy Columns
        column_defs = []
        for column in table_schema.columns:
            kwargs: Dict[str, Any] = dict(nullable=column.nullable)
            if column.default is not None:
                kwargs.update(server_default=str(column.default))
            if column.name in pkey_columns:
                kwargs.update(autoincrement=False)
            ctype = self._type_map[column.type]
            column_defs.append(Column(column.name, ctype, **kwargs))

        return column_defs

    def _tableIndices(self, table_name: str, info: Dict) -> List[sqlalchemy.schema.Constraint]:
        """Return set of constraints/indices in a table

        Parameters
        ----------
        table_name : `str`
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
        for index in table_schema.indices:
            if index.type == "INDEX":
                index_defs.append(Index(self._prefix + index.name, *index.columns, info=info))
            else:
                kwargs = {}
                if index.name:
                    kwargs['name'] = self._prefix+index.name
                if index.type == "PRIMARY":
                    index_defs.append(PrimaryKeyConstraint(*index.columns, **kwargs))
                elif index.type == "UNIQUE":
                    index_defs.append(UniqueConstraint(*index.columns, **kwargs))

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
