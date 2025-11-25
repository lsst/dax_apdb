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

__all__ = ["ApdbSchema", "ApdbTables"]

import enum
import logging
from collections.abc import Mapping, MutableMapping
from functools import cached_property

import felis.datamodel
import numpy

from .schema_model import ExtraDataTypes, Schema, Table
from .versionTuple import VersionTuple

_LOG = logging.getLogger(__name__)

# In most cases column types are determined by Cassandra driver, but in some
# cases we need to create Pandas Dataframe ourselves and we use this map to
# infer types of columns from their YAML schema. Note that Cassandra saves
# timestamps with millisecond precision, but pandas maps datetime type to
# "datetime64[ns]".
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

    SSSource = "SSSource"
    """Name of the table for SSSource records."""

    DiaObject_To_Object_Match = "DiaObject_To_Object_Match"
    """Name of the table for DiaObject_To_Object_Match records."""

    metadata = "metadata"
    """Name of the metadata table, this table may not always exist."""

    def table_name(self, prefix: str = "", time_partition: int | None = None) -> str:
        """Return full table name.

        Parameters
        ----------
        prefix : `str`, optional
            Optional prefix for table name.
        time_partition : `int`, optional
            Optional time partition, should only be used for tables that
            support time partitioning.
        """
        name = f"{prefix}{self.value}"
        if time_partition is not None:
            name = f"{name}_{time_partition}"
        return name


class ApdbSchema:
    """Class for management of APDB schema.

    Attributes
    ----------
    tableSchemas : `dict`
        Maps table name to `TableDef` instance.

    Parameters
    ----------
    schema_file : `str`
        Location of the YAML file with APDB schema.
    ss_schema_file : `str`
        Location of the YAML file with SSO schema. File will be loaded if APDB
        schema file does not contain SSObject/SSSource tables. Can be set to
        empty string to skip loading of SSObject/SSSource schema.
    """

    def __init__(
        self,
        schema_file: str,
        ss_schema_file: str,
    ):
        # build complete table schema
        self.tableSchemas, self._schemaVersion = self._buildSchemas(schema_file)
        if ss_schema_file:
            if ApdbTables.SSObject not in self.tableSchemas or ApdbTables.SSSource not in self.tableSchemas:
                # Read additional SSP schema.
                ssp_tables, _ = self._buildSchemas(ss_schema_file)
                if ApdbTables.SSObject not in ssp_tables or ApdbTables.SSSource not in ssp_tables:
                    raise LookupError(f"Cannot locate SSObject/SSSource table in {ss_schema_file}")
                self.tableSchemas = dict(self.tableSchemas) | {
                    ApdbTables.SSObject: ssp_tables[ApdbTables.SSObject],
                    ApdbTables.SSSource: ssp_tables[ApdbTables.SSSource],
                }

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
    def _buildSchemas(cls, schema_file: str) -> tuple[Mapping[ApdbTables, Table], VersionTuple | None]:
        """Create schema definitions for tables from felis schema.

        Reads YAML schema and builds a dictionary containing
        `.schema_model.Table` instances for each APDB table appearing in that
        schema.

        Parameters
        ----------
        schema_file : `str`
            Name of YAML file with ``felis`` schema.

        Returns
        -------
        tables : `dict` [`ApdbTables`, `schema_model.Table`]
            Mapping of table names to `.schema_model.Table` instances.
        version : `VersionTuple` or `None`
            Schema version defined in schema file, `None` if version is not
            defined.
        """
        _LOG.debug("Loading felis schema from %s", schema_file)
        felis_schema = felis.datamodel.Schema.from_uri(schema_file, context={"id_generation": True})
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

        _LOG.debug("Loaded schema for tables %s", list(tables))
        return tables, version

    @cached_property
    def has_mjd_timestamps(self) -> bool:
        """True if timestamps columns are in MJD (`bool`)."""
        table = self.tableSchemas[ApdbTables.DiaObject]
        # Look for validityStartMjdTai or validityStart
        for column in table.columns:
            if column.name == "validityStartMjdTai":
                return True
            elif column.name == "validityStart":
                return False
        raise LookupError(
            "Could not find validityStart or validityStartMjdTai column in DiaObject table schema."
        )
