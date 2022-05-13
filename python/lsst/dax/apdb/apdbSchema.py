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
from typing import Any, Dict, List, Mapping, Optional, Type, Union
import yaml


_LOG = logging.getLogger(__name__)

# In most cases column types are determined by Cassandra driver, but in some
# cases we need to create Pandas Dataframe ourselves and we use this map to
# infer types of columns from their YAML schema.
_dtype_map: Mapping[str, Union[Type, str]] = dict(
    double=numpy.float64,
    float=numpy.float32,
    timestamp="datetime64[ms]",
    long=numpy.int64,
    int=numpy.int32,
    short=numpy.int16,
    byte=numpy.int8,
    binary=object,
    char=object,
    text=object,
    string=object,
    unicode=object,
    boolean=bool,
)


@dataclass
class ColumnDef:
    """Column representation in schema."""

    name: str
    """column name"""
    type: str
    """name of cat type (INT, FLOAT, etc.)"""
    nullable: bool
    """True for nullable columns"""
    length: Optional[int] = None
    """Optiona length for string/binary columns"""
    default: Any = None
    """default value for column, can be None"""
    description: Optional[str] = None
    """documentation, can be None or empty"""
    unit: Optional[str] = None
    """string with unit name, can be None"""
    ucd: Optional[str] = None
    """string with ucd, can be None"""

    @property
    def dtype(self) -> Union[Type, str]:
        """Pandas dtype for this column"""
        return _dtype_map.get(self.type, object)


@enum.unique
class IndexType(enum.Enum):
    """Types of indices."""

    PRIMARY = "PRIMARY"
    UNIQUE = "UNIQUE"
    INDEX = "INDEX"
    PARTITION = "PARTITION"


@dataclass
class IndexDef:
    """Index description."""

    name: str
    """index name, can be empty"""
    type: IndexType
    """Type of the index"""
    columns: List[str]
    """list of column names in index"""


@dataclass
class TableDef:
    """Table description"""

    name: str
    """table name"""
    columns: List[ColumnDef]
    """list of ColumnDef instances"""
    indices: List[IndexDef]
    """list of IndexDef instances, can be empty"""
    description: Optional[str] = None
    """documentation, can be None or empty"""

    @property
    def primary_key(self) -> IndexDef:
        """Primary key index"""
        for index in self.indices:
            if index.type is IndexType.PRIMARY:
                return index
        raise ValueError(f"Table {self.name} has no primary key.")


@enum.unique
class ApdbTables(enum.Enum):
    """Names of the tables in APDB schema."""

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
        """Return full table name."""
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
    schema_name : `str`, optional
        Name of the schema in YAML files.
    """

    def __init__(
        self,
        schema_file: str,
        schema_name: str = "ApdbSchema",
    ):
        # build complete table schema
        self.tableSchemas = self._buildSchemas(schema_file, schema_name)

    def _readTables(self, schema_file: str, schema_name: str) -> List[Dict[str, Any]]:
        """Read table schema from YAML file.

        Parameters
        ----------
        schema_file : `str`
            Name of YAML file with ``felis`` schema.
        schema_name : `str`, optional
            Name of the schema in YAML files.

        Returns
        -------
        tables : `list`
            List of table definition objects.
        """
        schema_file = os.path.expandvars(schema_file)
        _LOG.debug("Reading schema file %s", schema_file)
        with open(schema_file) as yaml_stream:
            schemas = list(yaml.load_all(yaml_stream, Loader=yaml.SafeLoader))
            schemas = [schema for schema in schemas if schema.get("name") == schema_name]
            if not schemas:
                raise ValueError(f"Schema file {schema_file!r} does not define schema {schema_name!r}")
            elif len(schemas) > 1:
                raise ValueError(f"Schema file {schema_file!r} defines multiple schemas {schema_name!r}")
            schema = schemas[0]
            try:
                tables = schema["tables"]
            except KeyError:
                raise ValueError(f"Schema definition file {schema_file!r} defines no tables")
        _LOG.debug("Read %d tables from schema", len(tables))
        return tables

    def _buildSchemas(
        self,
        schema_file: str,
        schema_name: str = "ApdbSchema",
    ) -> Mapping[ApdbTables, TableDef]:
        """Create schema definitions for all tables.

        Reads YAML schemas and builds dictionary containing `TableDef`
        instances for each table.

        Parameters
        ----------
        schema_file : `str`
            Name of YAML file with ``felis`` schema.
        schema_name : `str`, optional
            Name of the schema in YAML files.

        Returns
        -------
        schemas : `dict`
            Mapping of table names to `TableDef` instances.
        """

        schema_file = os.path.expandvars(schema_file)
        tables = self._readTables(schema_file, schema_name)

        # convert all dicts into classes
        schemas = {}
        for table in tables:
            try:
                table_enum = ApdbTables(table["name"])
            except ValueError:
                # There may be other tables in the schema that do not belong
                # to APDB.
                continue

            columns = table.get("columns", [])

            table_columns = []
            column_map = {}
            for col in columns:
                column = ColumnDef(
                    name=col["name"],
                    type=col["datatype"],
                    nullable=col.get("nullable", True),
                    length=col.get("length"),
                    default=col.get("value"),
                    description=col.get("description"),
                    unit=col.get("fits:tunit"),
                    ucd=col.get("ivoa:ucd"),
                )
                table_columns.append(column)
                column_map[col["@id"]] = column

            table_indices = []

            # PK
            if (idx := table.get("primaryKey")) is not None:
                if isinstance(idx, list):
                    columns = [column_map[col_id].name for col_id in idx]
                else:
                    columns = [column_map[idx].name]
                index = IndexDef(name="", type=IndexType.PRIMARY, columns=columns)
                table_indices.append(index)

            # usual indices
            for idx in table.get("indexes", []):
                columns = [column_map[col_id].name for col_id in idx.get("columns")]
                index = IndexDef(name=idx.get("name"), type=IndexType.INDEX, columns=columns)
                table_indices.append(index)

            # Other constraints, for now only Unique is going to work, foreign
            # keys support may be added later.
            for idx in table.get("constraints", []):
                try:
                    contraint_type = idx.get["@type"]
                    index_type = IndexType(contraint_type.upper())
                except ValueError:
                    raise ValueError(f"{contraint_type} is not a valid index type") from None
                index = IndexDef(name=idx.get("name"), type=index_type, columns=idx.get("columns"))
                table_indices.append(index)

            schemas[table_enum] = TableDef(
                name=table_enum.value,
                description=table.get("description"),
                columns=table_columns,
                indices=table_indices,
            )

        return schemas
