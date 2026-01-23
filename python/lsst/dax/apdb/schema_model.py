# This file is part of dax_apdb.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = [
    "CheckConstraint",
    "Column",
    "Constraint",
    "ExtraDataTypes",
    "ForeignKeyConstraint",
    "Index",
    "Schema",
    "Table",
    "UniqueConstraint",
]

import dataclasses
from collections.abc import Iterable, Mapping, MutableMapping
from enum import Enum
from typing import Any

import felis.datamodel

_Mapping = Mapping[str, Any]


class ExtraDataTypes(Enum):
    """Additional column data types that we need in dax_apdb."""

    UUID = "uuid"


DataTypes = felis.datamodel.DataType | ExtraDataTypes


def _strip_keys(map: _Mapping, keys: Iterable[str]) -> _Mapping:
    """Return a copy of a dictionary with some keys removed."""
    keys = set(keys)
    return {key: value for key, value in map.items() if key not in keys}


def _make_iterable(obj: str | Iterable[str]) -> Iterable[str]:
    """Make an iterable out of string or list of strings."""
    if isinstance(obj, str):
        yield obj
    else:
        yield from obj


_data_type_size: Mapping[DataTypes, int] = {
    felis.datamodel.DataType.boolean: 1,
    felis.datamodel.DataType.byte: 1,
    felis.datamodel.DataType.short: 2,
    felis.datamodel.DataType.int: 4,
    felis.datamodel.DataType.long: 8,
    felis.datamodel.DataType.float: 4,
    felis.datamodel.DataType.double: 8,
    felis.datamodel.DataType.char: 1,
    felis.datamodel.DataType.string: 2,  # approximation, depends on character set
    felis.datamodel.DataType.unicode: 2,  # approximation, depends on character set
    felis.datamodel.DataType.text: 2,  # approximation, depends on character set
    felis.datamodel.DataType.binary: 1,
    felis.datamodel.DataType.timestamp: 8,  # May be different depending on backend
    ExtraDataTypes.UUID: 16,
}


# The first entry in the returned mapping is for nullable columns,
# the second entry is for non-nullable columns.
_dtype_map: Mapping[felis.datamodel.DataType, tuple[str, str]] = {
    felis.datamodel.DataType.double: ("float64", "float64"),
    felis.datamodel.DataType.float: ("float32", "float32"),
    felis.datamodel.DataType.timestamp: ("datetime64[ms]", "datetime64[ms]"),
    felis.datamodel.DataType.long: ("Int64", "int64"),
    felis.datamodel.DataType.int: ("Int32", "int32"),
    felis.datamodel.DataType.short: ("Int16", "int16"),
    felis.datamodel.DataType.byte: ("Int8", "int8"),
    felis.datamodel.DataType.binary: ("object", "object"),
    felis.datamodel.DataType.char: ("object", "object"),
    felis.datamodel.DataType.text: ("object", "object"),
    felis.datamodel.DataType.string: ("object", "object"),
    felis.datamodel.DataType.unicode: ("object", "object"),
    felis.datamodel.DataType.boolean: ("boolean", "bool"),
}


@dataclasses.dataclass
class Column:
    """Column representation in schema."""

    name: str
    """Column name."""

    id: str
    """Felis ID for this column."""

    datatype: DataTypes
    """Column type, one of the enums defined in DataType."""

    length: int | None = None
    """Optional length for string/binary columns"""

    nullable: bool = True
    """True for nullable columns."""

    value: Any = None
    """Default value for column, can be `None`."""

    autoincrement: bool | None = None
    """Unspecified value results in `None`."""

    description: str | None = None
    """Column description."""

    annotations: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    """Additional annotations for this column."""

    table: Table | None = None
    """Table which defines this column, usually not `None`."""

    @classmethod
    def from_felis(cls, dm_column: felis.datamodel.Column) -> Column:
        """Convert Felis column definition into instance of this class.

        Parameters
        ----------
        dm_column : `felis.datamodel.Column`
            Felis column definition.

        Returns
        -------
        column : `Column`
            Converted column definition.
        """
        column = cls(
            name=dm_column.name,
            id=dm_column.id,
            datatype=dm_column.datatype,
            length=dm_column.length,
            value=dm_column.value,
            description=dm_column.description,
            nullable=dm_column.nullable if dm_column.nullable is not None else True,
            autoincrement=dm_column.autoincrement,
            annotations=_strip_keys(
                dict(dm_column),
                ["name", "id", "datatype", "length", "nullable", "value", "autoincrement", "description"],
            ),
        )
        return column

    def clone(self) -> Column:
        """Make a clone of self."""
        return dataclasses.replace(self, table=None)

    def size(self) -> int:
        """Return size in bytes of this column.

        Returns
        -------
        size : `int`
            Size in bytes for this column, typically represents in-memory size
            of the corresponding data type. May or may not be the same as
            storage size or wire-level protocol size.
        """
        size = _data_type_size[self.datatype]
        if self.length is not None:
            size *= self.length
        return size

    @property
    def pandas_type(self) -> str:
        """Type of this column in pandas.DataFrame (`str`)."""
        # We do not convert UUID columns to pandas.
        assert isinstance(self.datatype, felis.datamodel.DataType)
        # TODO: We have cases of NULLs in existing data for non-nullable
        # columns (in Cassandra). To avoid errors for such cases we allow all
        # types to be nullable. We should revisit this at some later time.
        return _dtype_map[self.datatype][0]


@dataclasses.dataclass
class Index:
    """Index representation."""

    name: str
    """index name, can be empty."""

    id: str
    """Felis ID for this index."""

    columns: list[Column] = dataclasses.field(default_factory=list)
    """List of columns in index, one of the ``columns`` or ``expressions``
    must be non-empty.
    """

    expressions: list[str] = dataclasses.field(default_factory=list)
    """List of expressions in index, one of the ``columns`` or ``expressions``
    must be non-empty.
    """

    description: str | None = None
    """Index description."""

    annotations: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    """Additional annotations for this index."""

    @classmethod
    def from_felis(cls, dm_index: felis.datamodel.Index, columns: Mapping[str, Column]) -> Index:
        """Convert Felis index definition into instance of this class.

        Parameters
        ----------
        dm_index : `felis.datamodel.Index`
            Felis index definition.
        columns : `~collections.abc.Mapping` [`str`, `Column`]
            Mapping of column ID to `Column` instance.

        Returns
        -------
        index : `Index`
            Converted index definition.
        """
        return cls(
            name=dm_index.name,
            id=dm_index.id,
            columns=[columns[c] for c in (dm_index.columns or [])],
            expressions=dm_index.expressions or [],
            description=dm_index.description,
            annotations=_strip_keys(dict(dm_index), ["name", "id", "columns", "expressions", "description"]),
        )


@dataclasses.dataclass
class Constraint:
    """Constraint description, this is a base class, actual constraints will be
    instances of one of the subclasses.
    """

    name: str | None
    """Constraint name."""

    id: str
    """Felis ID for this constraint."""

    deferrable: bool = False
    """If `True` then this constraint will be declared as deferrable."""

    initially: str | None = None
    """Value for ``INITIALLY`` clause, only used of ``deferrable`` is True."""

    description: str | None = None
    """Constraint description."""

    annotations: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    """Additional annotations for this constraint."""

    @classmethod
    def from_felis(cls, dm_constr: felis.datamodel.Constraint, columns: Mapping[str, Column]) -> Constraint:
        """Convert Felis constraint definition into instance of this class.

        Parameters
        ----------
        dm_const : `felis.datamodel.Constraint`
            Felis constraint definition.
        columns : `~collections.abc.Mapping` [`str`, `Column`]
            Mapping of column ID to `Column` instance.

        Returns
        -------
        constraint : `Constraint`
            Converted constraint definition.
        """
        if isinstance(dm_constr, felis.datamodel.UniqueConstraint):
            return UniqueConstraint(
                name=dm_constr.name,
                id=dm_constr.id,
                columns=[columns[c] for c in dm_constr.columns],
                deferrable=dm_constr.deferrable,
                initially=dm_constr.initially,
                description=dm_constr.description,
                annotations=_strip_keys(
                    dict(dm_constr),
                    ["name", "type", "id", "columns", "deferrable", "initially", "description"],
                ),
            )
        elif isinstance(dm_constr, felis.datamodel.ForeignKeyConstraint):
            return ForeignKeyConstraint(
                name=dm_constr.name,
                id=dm_constr.id,
                columns=[columns[c] for c in dm_constr.columns],
                referenced_columns=[columns[c] for c in dm_constr.referenced_columns],
                deferrable=dm_constr.deferrable,
                initially=dm_constr.initially,
                description=dm_constr.description,
                annotations=_strip_keys(
                    dict(dm_constr),
                    [
                        "name",
                        "id",
                        "type",
                        "columns",
                        "deferrable",
                        "initially",
                        "referenced_columns",
                        "description",
                    ],
                ),
            )
        elif isinstance(dm_constr, felis.datamodel.CheckConstraint):
            return CheckConstraint(
                name=dm_constr.name,
                id=dm_constr.id,
                expression=dm_constr.expression,
                deferrable=dm_constr.deferrable,
                initially=dm_constr.initially,
                description=dm_constr.description,
                annotations=_strip_keys(
                    dict(dm_constr),
                    ["name", "id", "type", "expression", "deferrable", "initially", "description"],
                ),
            )
        else:
            raise TypeError(f"Unexpected constraint type: {dm_constr}")


@dataclasses.dataclass
class UniqueConstraint(Constraint):
    """Description of unique constraint."""

    columns: list[Column] = dataclasses.field(default_factory=list)
    """List of columns in this constraint, all columns belong to the same table
    as the constraint itself.
    """


@dataclasses.dataclass
class ForeignKeyConstraint(Constraint):
    """Description of foreign key constraint."""

    columns: list[Column] = dataclasses.field(default_factory=list)
    """List of columns in this constraint, all columns belong to the same table
    as the constraint itself.
    """

    referenced_columns: list[Column] = dataclasses.field(default_factory=list)
    """List of referenced columns, the number of columns must be the same as in
    ``Constraint.columns`` list. All columns must belong to the same table,
    which is different from the table of this constraint.
    """

    onupdate: str | None = None
    """What to do when parent table columns are updated. Typical values are
    CASCADE, DELETE and RESTRICT.
    """

    ondelete: str | None = None
    """What to do when parent table columns are deleted. Typical values are
    CASCADE, DELETE and RESTRICT.
    """

    @property
    def referenced_table(self) -> Table:
        """Table referenced by this constraint."""
        assert len(self.referenced_columns) > 0, "column list cannot be empty"
        ref_table = self.referenced_columns[0].table
        assert ref_table is not None, "foreign key column must have table defined"
        return ref_table


@dataclasses.dataclass
class CheckConstraint(Constraint):
    """Description of check constraint."""

    expression: str = ""
    """Expression on one or more columns on the table, must be non-empty."""


@dataclasses.dataclass
class Table:
    """Description of a single table schema."""

    name: str
    """Table name."""

    id: str
    """Felis ID for this table."""

    columns: list[Column]
    """List of Column instances."""

    primary_key: list[Column]
    """List of Column that constitute a primary key, may be empty."""

    constraints: list[Constraint]
    """List of Constraint instances, can be empty."""

    indexes: list[Index]
    """List of Index instances, can be empty."""

    description: str | None = None
    """Table description."""

    annotations: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    """Additional annotations for this table."""

    def __post_init__(self) -> None:
        """Update all columns to point to this table."""
        for column in self.columns:
            column.table = self

    @classmethod
    def from_felis(cls, dm_table: felis.datamodel.Table, columns: Mapping[str, Column]) -> Table:
        """Convert Felis table definition into instance of this class.

        Parameters
        ----------
        dm_table : `felis.datamodel.Table`
            Felis table definition.
        columns : `~collections.abc.Mapping` [`str`, `Column`]
            Mapping of column ID to `Column` instance.

        Returns
        -------
        table : `Table`
            Converted table definition.
        """
        table_columns = [columns[c.id] for c in dm_table.columns]
        if dm_table.primary_key:
            pk_columns = [columns[c] for c in _make_iterable(dm_table.primary_key)]
        else:
            pk_columns = []
        constraints = [Constraint.from_felis(constr, columns) for constr in dm_table.constraints]
        indices = [Index.from_felis(dm_idx, columns) for dm_idx in dm_table.indexes]
        table = cls(
            name=dm_table.name,
            id=dm_table.id,
            columns=table_columns,
            primary_key=pk_columns,
            constraints=constraints,
            indexes=indices,
            description=dm_table.description,
            annotations=_strip_keys(
                dict(dm_table),
                ["name", "id", "columns", "primaryKey", "constraints", "indexes", "description"],
            ),
        )
        return table


@dataclasses.dataclass
class Schema:
    """Complete schema description, collection of tables."""

    name: str
    """Schema name."""

    id: str
    """Felis ID for this schema."""

    tables: list[Table]
    """Collection of table definitions."""

    version: felis.datamodel.SchemaVersion | None = None
    """Schema version description."""

    description: str | None = None
    """Schema description."""

    annotations: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    """Additional annotations for this table."""

    @classmethod
    def from_felis(cls, dm_schema: felis.datamodel.Schema) -> Schema:
        """Convert felis schema definition to instance of this class.

        Parameters
        ----------
        dm_schema : `felis.datamodel.Schema`
            Felis schema definition.

        Returns
        -------
        schema : `Schema`
            Converted schema definition.
        """
        # Convert all columns first.
        columns: MutableMapping[str, Column] = {}
        for dm_table in dm_schema.tables:
            for dm_column in dm_table.columns:
                column = Column.from_felis(dm_column)
                columns[column.id] = column

        tables = [Table.from_felis(dm_table, columns) for dm_table in dm_schema.tables]

        version: felis.datamodel.SchemaVersion | None
        if isinstance(dm_schema.version, str):
            version = felis.datamodel.SchemaVersion(current=dm_schema.version)
        else:
            version = dm_schema.version

        schema = cls(
            name=dm_schema.name,
            id=dm_schema.id,
            tables=tables,
            version=version,
            description=dm_schema.description,
            annotations=_strip_keys(dict(dm_schema), ["name", "id", "tables", "description"]),
        )
        return schema
