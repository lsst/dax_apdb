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

__all__ = ["ModelToSql", "GUID"]

import uuid
from collections.abc import Iterable, Mapping
from typing import Any

import felis.datamodel
import sqlalchemy
from sqlalchemy.dialects.postgresql import UUID

from .. import schema_model


#
# Copied from daf_butler.
#
class GUID(sqlalchemy.TypeDecorator):
    """Platform-independent GUID type.

    Uses PostgreSQL's UUID type, otherwise uses CHAR(32), storing as
    stringified hex values.
    """

    impl = sqlalchemy.CHAR

    cache_ok = True

    def load_dialect_impl(self, dialect: sqlalchemy.engine.Dialect) -> sqlalchemy.types.TypeEngine:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(sqlalchemy.CHAR(32))

    def process_bind_param(self, value: Any, dialect: sqlalchemy.engine.Dialect) -> str | None:
        if value is None:
            return value

        # Coerce input to UUID type, in general having UUID on input is the
        # only thing that we want but there is code right now that uses ints.
        if isinstance(value, int):
            value = uuid.UUID(int=value)
        elif isinstance(value, bytes):
            value = uuid.UUID(bytes=value)
        elif isinstance(value, str):
            # hexstring
            value = uuid.UUID(hex=value)
        elif not isinstance(value, uuid.UUID):
            raise TypeError(f"Unexpected type of a bind value: {type(value)}")

        if dialect.name == "postgresql":
            return str(value)
        else:
            return "%.32x" % value.int

    def process_result_value(
        self, value: str | uuid.UUID | None, dialect: sqlalchemy.engine.Dialect
    ) -> uuid.UUID | None:
        if value is None:
            return value
        elif isinstance(value, uuid.UUID):
            # sqlalchemy 2 converts to UUID internally
            return value
        else:
            return uuid.UUID(hex=value)


class ModelToSql:
    """Class which implements schema model conversion to SQLAlchemy format.

    Parameters
    ----------
    metadata : `sqlalchemy.schema.MetaData`
        Metadata object for created tables.
    prefix : `str`, optional
        Prefix to add to all schema elements.
    """

    def __init__(
        self,
        metadata: sqlalchemy.schema.MetaData,
        prefix: str = "",
    ):
        self._metadata = metadata
        self._prefix = prefix

        # Map model column types to SQLAlchemy.
        self._type_map: dict[felis.datamodel.DataType | schema_model.ExtraDataTypes, type] = {
            felis.datamodel.DataType.double: sqlalchemy.types.Double,
            felis.datamodel.DataType.float: sqlalchemy.types.Float,
            felis.datamodel.DataType.timestamp: sqlalchemy.types.TIMESTAMP,
            felis.datamodel.DataType.long: sqlalchemy.types.BigInteger,
            felis.datamodel.DataType.int: sqlalchemy.types.Integer,
            felis.datamodel.DataType.short: sqlalchemy.types.SmallInteger,
            felis.datamodel.DataType.byte: sqlalchemy.types.SmallInteger,  # Byte types are not very portable
            felis.datamodel.DataType.binary: sqlalchemy.types.LargeBinary,
            felis.datamodel.DataType.text: sqlalchemy.types.Text,
            felis.datamodel.DataType.string: sqlalchemy.types.CHAR,
            felis.datamodel.DataType.char: sqlalchemy.types.CHAR,
            felis.datamodel.DataType.unicode: sqlalchemy.types.CHAR,
            felis.datamodel.DataType.boolean: sqlalchemy.types.Boolean,
            schema_model.ExtraDataTypes.UUID: GUID,
        }

    def make_tables(self, tables: Iterable[schema_model.Table]) -> Mapping[str, sqlalchemy.schema.Table]:
        """Generate sqlalchemy table schema from the list of modedls.

        Parameters
        ----------
        tables : `~collections.abc.Iterable` [`schema_model.Table`]
            List of table models.

        Returns
        -------
        tables : `~collections.abc.Mapping` [`str`, `sqlalchemy.schema.Table`]
            SQLAlchemy table definitions indexed by identifier of the table
            model.
        """
        # Order tables based on their FK dependencies.
        tables = self._topo_sort(tables)

        table_map: dict[str, sqlalchemy.schema.Table] = {}
        for table in tables:
            columns = self._table_columns(table)
            constraints = self._table_constraints(table, table_map)
            sa_table = sqlalchemy.schema.Table(
                self._prefix + table.name,
                self._metadata,
                *columns,
                *constraints,
                schema=self._metadata.schema,
            )
            table_map[table.id] = sa_table

        return table_map

    def _table_columns(self, table: schema_model.Table) -> list[sqlalchemy.schema.Column]:
        """Return set of columns in a table

        Parameters
        ----------
        table : `schema_model.Table`
            Table model.

        Returns
        -------
        column_defs : `list` [`sqlalchemy.schema.Column`]
            List of columns.
        """
        column_defs: list[sqlalchemy.schema.Column] = []
        for column in table.columns:
            kwargs: dict[str, Any] = dict(nullable=column.nullable)
            if column.value is not None:
                kwargs.update(server_default=str(column.value))
            if column in table.primary_key and column.autoincrement is None:
                kwargs.update(autoincrement=False)
            else:
                kwargs.update(autoincrement=column.autoincrement)
            ctype = self._type_map[column.datatype]
            if column.length is not None:
                if ctype not in (sqlalchemy.types.Text, sqlalchemy.types.TIMESTAMP):
                    ctype = ctype(length=column.length)
            if ctype is sqlalchemy.types.TIMESTAMP:
                # Use TIMESTAMP WITH TIMEZONE.
                ctype = ctype(timezone=True)
            column_defs.append(sqlalchemy.schema.Column(column.name, ctype, **kwargs))

        return column_defs

    def _table_constraints(
        self,
        table: schema_model.Table,
        table_map: Mapping[str, sqlalchemy.schema.Table],
    ) -> list[sqlalchemy.schema.SchemaItem]:
        """Return set of constraints/indices in a table.

        Parameters
        ----------
        table : `schema_model.Table`
            Table model.
        table_map : `~collections.abc.Mapping`
            MApping of table ID to sqlalchemy table definition for tables
            that already exist, this must include all tables referenced by
            foreign keys in ``table``.

        Returns
        -------
        constraints : `list` [`sqlalchemy.schema.SchemaItem`]
            List of SQLAlchemy index/constraint objects.
        """
        constraints: list[sqlalchemy.schema.SchemaItem] = []
        if table.primary_key:
            # It is very useful to have named PK.
            name = self._prefix + table.name + "_pk"
            constraints.append(
                sqlalchemy.schema.PrimaryKeyConstraint(*[column.name for column in table.primary_key])
            )
        for index in table.indexes:
            if index.expressions:
                raise TypeError(f"Expression indices are not supported: {table}")
            name = self._prefix + index.name if index.name else ""
            constraints.append(sqlalchemy.schema.Index(name, *[column.name for column in index.columns]))
        for constraint in table.constraints:
            constr_name: str | None = None
            if constraint.name:
                constr_name = self._prefix + constraint.name
            if isinstance(constraint, schema_model.UniqueConstraint):
                constraints.append(
                    sqlalchemy.schema.UniqueConstraint(
                        *[column.name for column in constraint.columns], name=constr_name
                    )
                )
            elif isinstance(constraint, schema_model.ForeignKeyConstraint):
                column_names = [col.name for col in constraint.columns]
                foreign_table = table_map[constraint.referenced_table.id]
                refcolumns = [foreign_table.columns[col.name] for col in constraint.referenced_columns]
                constraints.append(
                    sqlalchemy.schema.ForeignKeyConstraint(
                        columns=column_names,
                        refcolumns=refcolumns,
                        name=constr_name,
                        deferrable=constraint.deferrable,
                        initially=constraint.initially,
                        onupdate=constraint.onupdate,
                        ondelete=constraint.ondelete,
                    )
                )
            elif isinstance(constraint, schema_model.CheckConstraint):
                constraints.append(
                    sqlalchemy.schema.CheckConstraint(
                        constraint.expression,
                        name=constr_name,
                        deferrable=constraint.deferrable,
                        initially=constraint.initially,
                    )
                )
            else:
                raise TypeError(f"Unknown constraint type: {constraint}")

        return constraints

    @staticmethod
    def _topo_sort(table_iter: Iterable[schema_model.Table]) -> list[schema_model.Table]:
        """Toplogical sorting of tables."""
        result: list[schema_model.Table] = []
        result_ids: set[str] = set()
        tables = list(table_iter)

        # Map of table ID to foreign table IDs.
        referenced_tables: dict[str, set[str]] = {}
        for table in tables:
            referenced_tables[table.id] = set()
            for constraint in table.constraints:
                if isinstance(constraint, schema_model.ForeignKeyConstraint):
                    referenced_tables[table.id].add(constraint.referenced_table.id)

        while True:
            keep = []
            changed = False
            for table in tables:
                if referenced_tables[table.id].issubset(result_ids):
                    changed = True
                    result.append(table)
                    result_ids.add(table.id)
                else:
                    keep.append(table)
            tables = keep
            if not changed:
                break

        # If nothing can be removed it means cycle.
        if tables:
            raise ValueError(f"Dependency cycle in foreign keys: {tables}")

        return result
