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

"""Module containing methods and classes for generic APDB schema operations.

The code in this module is independent of the specific technology used to
implement APDB.
"""

from __future__ import annotations

__all__ = ["ApdbTables", "ApdbSchema"]

import enum
import logging
import os
from collections.abc import Mapping, MutableMapping

import felis.datamodel
import numpy
import yaml

from .schema_model import ExtraDataTypes, Schema, Table
from .versionTuple import VersionTuple

_LOG = logging.getLogger(__name__)

# In most cases column types are determined by Cassandra driver, but in some
# cases we need to create Pandas Dataframe ourselves and we use this map to
# infer types of columns from their YAML schema.
_dtype_map: Mapping[felis.datamodel.DataType | ExtraDataTypes, type | str] = {
    felis.datamodel.DataType.double: numpy.float64,
    felis.datamodel.DataType.float: numpy.float32,
    felis.datamodel.DataType.timestamp: "datetime64[ns]",
    felis.datamodel.DataType.long: numpy.int64,
    felis.datamodel.DataType.int: numpy.int32,
    felis.datamodel.DataType.short: numpy.int16,
    felis.datamodel.DataType.byte: numpy.int8,
    felis.datamodel.DataType.binary: object,
    felis.datamodel.DataType.char: object,
    felis.datamodel.DataType.text: object,
    felis.datamodel.DataType.string: object,
    felis.datamodel.DataType.unicode: object,
    felis.datamodel.DataType.boolean: bool,
}


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

    metadata = "metadata"
    """Name of the metadata table, this table may not always exist."""

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
        self.tableSchemas, self._schemaVersion = self._buildSchemas(schema_file, schema_name)

    def column_dtype(self, felis_type: felis.datamodel.DataType | ExtraDataTypes) -> type | str:
        """Return Pandas data type for a given Felis column type.

        Parameters
        ----------
        felis_type : `felis.datamodel.DataType`
            Felis type, on of the enums defined in `felis.datamodel` module.

        Returns
        -------
        column_dtype : `type` or `str`
            Type that can be used for columns in Pandas.

        Raises
        ------
        TypeError
            Raised if type is cannot be handled.
        """
        try:
            return _dtype_map[felis_type]
        except KeyError:
            raise TypeError(f"Unexpected Felis type: {felis_type}")

    def schemaVersion(self) -> VersionTuple:
        """Return schema version as defined in YAML schema file.

        Returns
        -------
        version : `VersionTuple`
            Version number read from YAML file, if YAML file does not define
            schema version then "0.1.0" is returned.
        """
        if self._schemaVersion is None:
            return VersionTuple(0, 1, 0)
        else:
            return self._schemaVersion

    @classmethod
    def _buildSchemas(
        cls, schema_file: str, schema_name: str = "ApdbSchema"
    ) -> tuple[Mapping[ApdbTables, Table], VersionTuple | None]:
        """Create schema definitions for all tables.

        Reads YAML schema and builds a dictionary containing
        `.schema_model.Table` instances for each table.

        Parameters
        ----------
        schema_file : `str`
            Name of YAML file with ``felis`` schema.
        schema_name : `str`, optional
            Name of the schema in YAML files.

        Returns
        -------
        tables : `dict`
            Mapping of table names to `.schema_model.Table` instances.
        version : `VersionTuple` or `None`
            Schema version defined in schema file, `None` if version is not
            defined.
        """
        schema_file = os.path.expandvars(schema_file)
        with open(schema_file) as yaml_stream:
            schemas_list = list(yaml.load_all(yaml_stream, Loader=yaml.SafeLoader))
            schemas_list = [schema for schema in schemas_list if schema.get("name") == schema_name]
            if not schemas_list:
                raise ValueError(f"Schema file {schema_file!r} does not define schema {schema_name!r}")
            elif len(schemas_list) > 1:
                raise ValueError(f"Schema file {schema_file!r} defines multiple schemas {schema_name!r}")
            felis_schema = felis.datamodel.Schema.model_validate(schemas_list[0])
            schema = Schema.from_felis(felis_schema)

        # convert all dicts into classes
        tables: MutableMapping[ApdbTables, Table] = {}
        for table in schema.tables:
            try:
                table_enum = ApdbTables(table.name)
            except ValueError:
                # There may be other tables in the schema that do not belong
                # to APDB.
                continue
            else:
                tables[table_enum] = table

        version: VersionTuple | None = None
        if schema.version is not None:
            version = VersionTuple.fromString(schema.version.current)

        return tables, version
