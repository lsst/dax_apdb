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

__all__ = ["ColumnDef", "IndexDef", "TableDef", "ApdbBaseSchema"]

import logging
import os
from typing import Any, List, Mapping, NamedTuple, Optional
import yaml


_LOG = logging.getLogger(__name__)


class ColumnDef(NamedTuple):
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


class IndexDef(NamedTuple):
    """Index description.
    """
    name: str
    """index name, can be empty"""
    type: str
    """one of "PRIMARY", "UNIQUE", "INDEX"
    """
    columns: List[str]
    """list of column names in index"""


class TableDef(NamedTuple):
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


class ApdbBaseSchema:
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
                      ) -> Mapping[str, TableDef]:
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

            table_name = table['table']

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
                index = IndexDef(name=idx.get('name'),
                                 type=idx.get('type'),
                                 columns=idx.get('columns'))
                table_indices.append(index)

            schemas[table_name] = TableDef(name=table_name,
                                           description=table.get('description'),
                                           columns=table_columns,
                                           indices=table_indices)

        return schemas
