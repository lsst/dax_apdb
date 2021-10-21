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

"""This module contains methods and classes for generic APDB schema operations.

The code in this module is independent of the specific technology used to
implement APDB.
"""

from __future__ import annotations

__all__ = ["ColumnDef", "IndexType", "IndexDef", "TableDef", "ApdbTables", "ApdbSchema"]

import enum
from dataclasses import dataclass
import logging
import numpy
import os
from typing import Any, List, Mapping, Optional, Type, Union
import yaml


_LOG = logging.getLogger(__name__)

# In most cases column types are determined by Cassandra driver, but in some
# cases we need to create Pandas Dataframe ourselves and we use this map to
# infer types of columns from their YAML schema.
_dtype_map: Mapping[str, Union[Type, str]] = dict(
    DOUBLE=numpy.float64,
    FLOAT=numpy.float32,
    DATETIME="datetime64[ms]",
    BIGINT=numpy.int64,
    INTEGER=numpy.int32,
    INT=numpy.int32,
    TINYINT=numpy.int8,
    BLOB=object,
    CHAR=object,
    BOOL=bool,
)


@dataclass
class ColumnDef:
    """Column representation in schema.
    """
    name: str
    """column name"""
    type: str
    """name of cat type (INT, FLOAT, etc.)"""
    nullable: bool
    """True for nullable columns"""
    default: Any
    """default value for column, can be None"""
    description: Optional[str]
    """documentation, can be None or empty"""
    unit: Optional[str]
    """string with unit name, can be None"""
    ucd: Optional[str]
    """string with ucd, can be None"""

    @property
    def dtype(self) -> Union[Type, str]:
        """Pandas dtype for this column"""
        return _dtype_map.get(self.type, object)


@enum.unique
class IndexType(enum.Enum):
    """Types of indices.
    """
    PRIMARY = "PRIMARY"
    UNIQUE = "UNIQUE"
    INDEX = "INDEX"
    PARTITION = "PARTITION"


@dataclass
class IndexDef:
    """Index description.
    """
    name: str
    """index name, can be empty"""
    type: IndexType
    """Type of the index"""
    columns: List[str]
    """list of column names in index"""


@dataclass
class TableDef:
    """Table description
    """
    name: str
    """table name"""
    description: Optional[str]
    """documentation, can be None or empty"""
    columns: List[ColumnDef]
    """list of ColumnDef instances"""
    indices: List[IndexDef]
    """list of IndexDef instances, can be empty"""

    @property
    def primary_key(self) -> IndexDef:
        """Primary key index"""
        for index in self.indices:
            if index.type is IndexType.PRIMARY:
                return index
        raise ValueError(f"Table {self.name} has no primary key.")


@enum.unique
class ApdbTables(enum.Enum):
    """Names of the tables in APDB schema.
    """

    DiaObject = "DiaObject"
    """Name of the table for DIAObject records."""

    DiaSource = "DiaSource"
    """Name of the table for DIASource records."""

    DiaForcedSource = "DiaForcedSource"
    """Name of the table for DIAForcedSource records."""

    DiaObjectLast = "DiaObjectLast"
    """Name of the table for the last version of DIAObject records.

    This table may be optional for some implementations.
    """

    SSObject = "SSObject"
    """Name of the table for SSObject records."""

    DiaObject_To_Object_Match = "DiaObject_To_Object_Match"
    """Name of the table for DiaObject_To_Object_Match records."""

    def table_name(self, prefix: str = "") -> str:
        """Return full table name.
        """
        return prefix + self.value


class ApdbSchema:
    """Class for management of APDB schema.

    Attributes
    ----------
    tableSchemas : `dict`
        Maps table name to `TableDef` instance.

    Parameters
    ----------
    schema_file : `str`
        Name of the YAML schema file.
    extra_schema_file : `str`, optional
        Name of the YAML schema file with extra column definitions.
    """
    def __init__(self, schema_file: str, extra_schema_file: Optional[str] = None):
        # build complete table schema
        self.tableSchemas = self._buildSchemas(schema_file, extra_schema_file)

    def _buildSchemas(self, schema_file: str, extra_schema_file: Optional[str] = None,
                      ) -> Mapping[ApdbTables, TableDef]:
        """Create schema definitions for all tables.

        Reads YAML schemas and builds dictionary containing `TableDef`
        instances for each table.

        Parameters
        ----------
        schema_file : `str`
            Name of YAML file with standard cat schema.
        extra_schema_file : `str`, optional
            Name of YAML file with extra table information or `None`.

        Returns
        -------
        schemas : `dict`
            Mapping of table names to `TableDef` instances.
        """

        schema_file = os.path.expandvars(schema_file)
        _LOG.debug("Reading schema file %s", schema_file)
        with open(schema_file) as yaml_stream:
            tables = list(yaml.load_all(yaml_stream, Loader=yaml.SafeLoader))
            # index it by table name
        _LOG.debug("Read %d tables from schema", len(tables))

        if extra_schema_file:
            extra_schema_file = os.path.expandvars(extra_schema_file)
            _LOG.debug("Reading extra schema file %s", extra_schema_file)
            with open(extra_schema_file) as yaml_stream:
                extras = list(yaml.load_all(yaml_stream, Loader=yaml.SafeLoader))
                # index it by table name
                schemas_extra = {table['table']: table for table in extras}
        else:
            schemas_extra = {}

        # merge extra schema into a regular schema, for now only columns are merged
        for table in tables:
            table_name = table['table']
            if table_name in schemas_extra:
                columns = table['columns']
                extra_columns = schemas_extra[table_name].get('columns', [])
                extra_columns = {col['name']: col for col in extra_columns}
                _LOG.debug("Extra columns for table %s: %s", table_name, extra_columns.keys())
                columns = []
                for col in table['columns']:
                    if col['name'] in extra_columns:
                        columns.append(extra_columns.pop(col['name']))
                    else:
                        columns.append(col)
                # add all remaining extra columns
                table['columns'] = columns + list(extra_columns.values())

                if 'indices' in schemas_extra[table_name]:
                    raise RuntimeError("Extra table definition contains indices, "
                                       "merging is not implemented")

                del schemas_extra[table_name]

        # Pure "extra" table definitions may contain indices
        tables += schemas_extra.values()

        # convert all dicts into named tuples
        schemas = {}
        for table in tables:

            columns = table.get('columns', [])

            try:
                table_enum = ApdbTables(table['table'])
            except ValueError as exc:
                raise ValueError(f"{table['table']} is not a valid APDB table name") from exc

            table_columns = []
            for col in columns:
                # For prototype set default to 0 even if columns don't specify it
                if "default" not in col:
                    default = None
                    if col['type'] not in ("BLOB", "DATETIME"):
                        default = 0
                else:
                    default = col["default"]

                column = ColumnDef(name=col['name'],
                                   type=col['type'],
                                   nullable=col.get("nullable"),
                                   default=default,
                                   description=col.get("description"),
                                   unit=col.get("unit"),
                                   ucd=col.get("ucd"))
                table_columns.append(column)

            table_indices = []
            for idx in table.get('indices', []):
                try:
                    index_type = IndexType(idx.get('type'))
                except ValueError as exc:
                    raise ValueError(f"{idx.get('type')} is not a valid index type") from exc
                index = IndexDef(name=idx.get('name'),
                                 type=index_type,
                                 columns=idx.get('columns'))
                table_indices.append(index)

            schemas[table_enum] = TableDef(name=table_enum.value,
                                           description=table.get('description'),
                                           columns=table_columns,
                                           indices=table_indices)

        return schemas
