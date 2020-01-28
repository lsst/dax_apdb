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

__all__ = ["ColumnDef", "IndexDef", "TableDef",
           "make_minimal_dia_object_schema", "make_minimal_dia_source_schema",
           "ApdbBaseSchema", "ApdbBaseSchemaConfig"]

from collections import namedtuple
import logging
import os
import yaml

import lsst.afw.table as afwTable
import lsst.pex.config as pexConfig
from lsst.pex.config import Field
from lsst.utils import getPackageDir


_LOG = logging.getLogger(__name__.partition(".")[2])  # strip leading "lsst."

# Classes for representing schema

# Column description:
#    name : column name
#    type : name of cat type (INT, FLOAT, etc.)
#    nullable : True or False
#    default : default value for column, can be None
#    description : documentation, can be None or empty
#    unit : string with unit name, can be None
#    ucd : string with ucd, can be None
ColumnDef = namedtuple('ColumnDef', 'name type nullable default description unit ucd')

# Index description:
#    name : index name, can be None or empty
#    type : one of "PRIMARY", "UNIQUE", "INDEX"
#    columns : list of column names in index
IndexDef = namedtuple('IndexDef', 'name type columns')

# Table description:
#    name : table name
#    description : documentation, can be None or empty
#    columns : list of ColumnDef instances
#    indices : list of IndexDef instances, can be empty or None
TableDef = namedtuple('TableDef', 'name description columns indices')


def make_minimal_dia_object_schema():
    """Define and create the minimal afw.table schema for a DIAObject.

    Returns
    -------
    schema : `lsst.afw.table.Schema`
        Minimal schema for DIAObjects.
    """
    schema = afwTable.SourceTable.makeMinimalSchema()
    schema.addField("pixelId", type='L',
                    doc='Unique spherical pixelization identifier.')
    schema.addField("nDiaSources", type='L')
    return schema


def make_minimal_dia_source_schema():
    """ Define and create the minimal afw.table schema for a DIASource.

    Returns
    -------
    schema : `lsst.afw.table.Schema`
        Minimal schema for DIASources.
    """
    schema = afwTable.SourceTable.makeMinimalSchema()
    schema.addField("diaObjectId", type='L',
                    doc='Unique identifier of the DIAObject this source is '
                        'associated to.')
    schema.addField("ccdVisitId", type='L',
                    doc='Id of the exposure and ccd this object was detected '
                        'in.')
    schema.addField("psFlux", type='D',
                    doc='Calibrated PSF flux of this source.')
    schema.addField("psFluxErr", type='D',
                    doc='Calibrated PSF flux err of this source.')
    schema.addField("flags", type='L',
                    doc='Quality flags for this DIASource.')
    schema.addField("pixelId", type='L',
                    doc='Unique spherical pixelization identifier.')
    return schema


def _data_file_name(basename):
    """Return path name of a data file.
    """
    return os.path.join(getPackageDir("dax_apdb"), "data", basename)


class ApdbBaseSchemaConfig(pexConfig.Config):

    schema_file = Field(dtype=str,
                        doc="Location of (YAML) configuration file with standard schema",
                        default=_data_file_name("apdb-schema.yaml"))
    extra_schema_file = Field(dtype=str,
                              doc="Location of (YAML) configuration file with extra schema",
                              default=_data_file_name("apdb-schema-extra.yaml"))
    column_map = Field(dtype=str,
                       doc="Location of (YAML) configuration file with column mapping",
                       default=_data_file_name("apdb-afw-map.yaml"))


class ApdbBaseSchema:
    """Class for management of APDB schema.

    Attributes
    ----------
    tableSchemas : `dict`
        Maps table name to `TableDef` instance.

    Parameters
    ----------
    config : `ApdbBaseSchemaConfig`
        Configuration for this class.
    afw_schemas : `dict`, optional
        Dictionary with table name for a key and `afw.table.Schema`
        for a value. Columns in schema will be added to standard APDB
        schema (only if standard schema does not have matching column).
    """

    # map afw type names into cat type names
    _afw_type_map = {"I": "INT",
                     "L": "BIGINT",
                     "F": "FLOAT",
                     "D": "DOUBLE",
                     "Angle": "DOUBLE",
                     "String": "CHAR",
                     "Flag": "BOOL"}
    _afw_type_map_reverse = {"INT": "I",
                             "BIGINT": "L",
                             "FLOAT": "F",
                             "DOUBLE": "D",
                             "DATETIME": "L",
                             "CHAR": "String",
                             "BOOL": "Flag"}

    def __init__(self, config, afw_schemas=None):

        if config.column_map:
            _LOG.debug("Reading column map file %s", config.column_map)
            with open(config.column_map) as yaml_stream:
                # maps cat column name to afw column name
                self._column_map = yaml.load(yaml_stream, Loader=yaml.SafeLoader)
                _LOG.debug("column map: %s", self._column_map)
        else:
            _LOG.debug("No column map file is given, initialize to empty")
            self._column_map = {}
        self._column_map_reverse = {}
        for table, cmap in self._column_map.items():
            # maps afw column name to cat column name
            self._column_map_reverse[table] = {v: k for k, v in cmap.items()}
        _LOG.debug("reverse column map: %s", self._column_map_reverse)

        # build complete table schema
        self.tableSchemas = self._buildSchemas(config.schema_file,
                                               config.extra_schema_file,
                                               afw_schemas)
        self.afwSchemas = {}

    def getAfwSchema(self, table_name, columns=None):
        """Return afw schema for given table.

        Parameters
        ----------
        table_name : `str`
            One of known APDB table names.
        columns : `list` of `str`, optional
            Include only given table columns in schema, by default all columns
            are included.

        Returns
        -------
        schema : `lsst.afw.table.Schema`
        column_map : `dict`
            Mapping of the table/result column names into schema key.
        """

        cacheKey = (table_name, frozenset(columns or []))
        res = self.afwSchemas.get(cacheKey)
        if res is not None:
            return res

        table = self.tableSchemas[table_name]
        col_map = self._column_map.get(table_name, {})

        # make a schema
        col2afw = {}
        schema = afwTable.SourceTable.makeMinimalSchema()
        for column in table.columns:
            if columns and column.name not in columns:
                continue
            afw_col = col_map.get(column.name, column.name)
            if afw_col in schema.getNames():
                # Continue if the column is already in the minimal schema.
                key = schema.find(afw_col).getKey()
            elif column.type in ("DOUBLE", "FLOAT") and column.unit == "deg":
                #
                # NOTE: degree to radian conversion is not supported (yet)
                #
                # angles in afw are radians and have special "Angle" type
                key = schema.addField(afw_col,
                                      type="Angle",
                                      doc=column.description or "",
                                      units="rad")
            elif column.type == "BLOB":
                # No BLOB support for now
                key = None
            else:
                units = column.unit or ""
                # some units in schema are not recognized by afw but we do not care
                if self._afw_type_map_reverse[column.type] == 'String':
                    key = schema.addField(afw_col,
                                          type=self._afw_type_map_reverse[column.type],
                                          doc=column.description or "",
                                          units=units,
                                          parse_strict="silent",
                                          size=10)
                elif units == "deg":
                    key = schema.addField(afw_col,
                                          type='Angle',
                                          doc=column.description or "",
                                          parse_strict="silent")
                else:
                    key = schema.addField(afw_col,
                                          type=self._afw_type_map_reverse[column.type],
                                          doc=column.description or "",
                                          units=units,
                                          parse_strict="silent")
            col2afw[column.name] = key

        self.afwSchemas[cacheKey] = (schema, col2afw)
        return schema, col2afw

    def getAfwColumns(self, table_name):
        """Returns mapping of afw column names to Column definitions.

        Parameters
        ----------
        table_name : `str`
            One of known APDB table names.

        Returns
        -------
        column_map : `dict`
            Mapping of afw column names to `ColumnDef` instances.
        """
        table = self.tableSchemas[table_name]
        col_map = self._column_map.get(table_name, {})

        cmap = {}
        for column in table.columns:
            afw_name = col_map.get(column.name, column.name)
            cmap[afw_name] = column
        return cmap

    def getColumnMap(self, table_name):
        """Returns mapping of column names to Column definitions.

        Parameters
        ----------
        table_name : `str`
            One of known APDB table names.

        Returns
        -------
        column_map : `dict`
            Mapping of column names to `ColumnDef` instances.
        """
        table = self.tableSchemas[table_name]
        cmap = {column.name: column for column in table.columns}
        return cmap

    def _buildSchemas(self, schema_file, extra_schema_file=None, afw_schemas=None):
        """Create schema definitions for all tables.

        Reads YAML schemas and builds dictionary containing `TableDef`
        instances for each table.

        Parameters
        ----------
        schema_file : `str`
            Name of YAML file with standard cat schema.
        extra_schema_file : `str`, optional
            Name of YAML file with extra table information or `None`.
        afw_schemas : `dict`, optional
            Dictionary with table name for a key and `afw.table.Schema`
            for a value. Columns in schema will be added to standard APDB
            schema (only if standard schema does not have matching column).

        Returns
        -------
        schemas : `dict`
            Mapping of table names to `TableDef` instances.
        """

        _LOG.debug("Reading schema file %s", schema_file)
        with open(schema_file) as yaml_stream:
            tables = list(yaml.load_all(yaml_stream, Loader=yaml.SafeLoader))
            # index it by table name
        _LOG.debug("Read %d tables from schema", len(tables))

        if extra_schema_file:
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
            afw_schema = afw_schemas and afw_schemas.get(table_name)
            if afw_schema:
                # use afw schema to create extra columns
                column_names = {col['name'] for col in columns}
                column_names_lower = {col.lower() for col in column_names}
                for _, field in afw_schema:
                    column = self._field2dict(field, table_name)
                    if column['name'] not in column_names:
                        # check that there is no column name that only differs in case
                        if column['name'].lower() in column_names_lower:
                            raise ValueError("afw.table column name case does not match schema column name")
                        columns.append(column)

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

    def _field2dict(self, field, table_name):
        """Convert afw schema field definition into a dict format.

        Parameters
        ----------
        field : `lsst.afw.table.Field`
            Field in afw table schema.
        table_name : `str`
            Name of the table.

        Returns
        -------
        field_dict : `dict`
            Field attributes for SQL schema:

            - ``name`` : field name (`str`)
            - ``type`` : type name in SQL, e.g. "INT", "FLOAT" (`str`)
            - ``nullable`` : `True` if column can be ``NULL`` (`bool`)
        """
        column = field.getName()
        column = self._column_map_reverse[table_name].get(column, column)
        ctype = self._afw_type_map[field.getTypeString()]
        return dict(name=column, type=ctype, nullable=True)
