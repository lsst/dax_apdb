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

"""Unit test for ApdbSqlSchema class.
"""

import os
import unittest

from lsst.dax.apdb.apdbSqlSchema import ApdbSqlSchema
from lsst.utils import getPackageDir
import lsst.utils.tests
from sqlalchemy import create_engine


def _data_file_name(basename):
    """Return path name of a data file.
    """
    return os.path.join(getPackageDir("dax_apdb"), "data", basename)


class ApdbSchemaTestCase(unittest.TestCase):
    """A test case for ApdbSqlSchema class
    """

    @classmethod
    def setUpClass(cls):
        pass

    def _assertTable(self, table, name, ncol):
        """validation for tables schema.

        Parameters
        ----------
        table : `sqlalchemy.Table`
        name : `str`
            Expected table name
        ncol : `int`
            Expected number of columns
        """
        self.assertIsNotNone(table)
        self.assertEqual(table.name, name)
        self.assertEqual(len(table.columns), ncol)

    def test_makeSchema(self):
        """Test for creating schemas.

        Schema is defined in YAML files, some checks here depend on that
        configuration and will need to be updated when configuration changes.
        """
        engine = create_engine('sqlite://')

        # create standard (baseline) schema
        schema = ApdbSqlSchema(engine=engine,
                               dia_object_index="baseline",
                               htm_index_column="pixelId",
                               schema_file=_data_file_name("apdb-schema.yaml"))
        schema.makeSchema()
        self._assertTable(schema.objects, "DiaObject", 92)
        self.assertEqual(len(schema.objects.primary_key), 2)
        self.assertIsNone(schema.objects_last)
        self._assertTable(schema.sources, "DiaSource", 108)
        self._assertTable(schema.forcedSources, "DiaForcedSource", 8)

        # create schema using prefix
        schema = ApdbSqlSchema(engine=engine,
                               dia_object_index="baseline",
                               htm_index_column="pixelId",
                               schema_file=_data_file_name("apdb-schema.yaml"),
                               prefix="Pfx")
        # Drop existing tables (but we don't check it here)
        schema.makeSchema(drop=True)
        self._assertTable(schema.objects, "PfxDiaObject", 92)
        self.assertIsNone(schema.objects_last)
        self._assertTable(schema.sources, "PfxDiaSource", 108)
        self._assertTable(schema.forcedSources, "PfxDiaForcedSource", 8)

        # use different indexing for DiaObject, need extra schema for that
        schema = ApdbSqlSchema(engine=engine,
                               dia_object_index="pix_id_iov",
                               htm_index_column="pixelId",
                               schema_file=_data_file_name("apdb-schema.yaml"),
                               extra_schema_file=_data_file_name("apdb-schema-extra.yaml"))
        schema.makeSchema(drop=True)
        self._assertTable(schema.objects, "DiaObject", 94)
        self.assertEqual(len(schema.objects.primary_key), 3)
        self.assertIsNone(schema.objects_last)
        self._assertTable(schema.sources, "DiaSource", 108)
        self._assertTable(schema.forcedSources, "DiaForcedSource", 8)

        # use DiaObjectLast table for DiaObject, need extra schema for that
        schema = ApdbSqlSchema(engine=engine,
                               dia_object_index="last_object_table",
                               htm_index_column="pixelId",
                               schema_file=_data_file_name("apdb-schema.yaml"),
                               extra_schema_file=_data_file_name("apdb-schema-extra.yaml"))
        schema.makeSchema(drop=True)
        self._assertTable(schema.objects, "DiaObject", 94)
        self.assertEqual(len(schema.objects.primary_key), 2)
        self._assertTable(schema.objects_last, "DiaObjectLast", 18)
        self.assertEqual(len(schema.objects_last.primary_key), 2)
        self._assertTable(schema.sources, "DiaSource", 108)
        self._assertTable(schema.forcedSources, "DiaForcedSource", 8)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
