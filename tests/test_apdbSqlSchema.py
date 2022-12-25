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
from typing import Any

import lsst.utils.tests
import sqlalchemy
from lsst.dax.apdb.apdbSqlSchema import ApdbSqlSchema, ExtraTables
from lsst.dax.apdb.apdbSchema import ApdbTables
from sqlalchemy import create_engine

TEST_SCHEMA = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema.yaml")


class ApdbSchemaTestCase(unittest.TestCase):
    """Test case for ApdbSqlSchema class."""

    # number of columns as defined in tests/config/schema.yaml
    table_column_count = {
        ApdbTables.DiaObject: 8,
        ApdbTables.DiaObjectLast: 5,
        ApdbTables.DiaSource: 10,
        ApdbTables.DiaForcedSource: 4,
        ApdbTables.SSObject: 3,
    }

    def _assertTable(self, table: sqlalchemy.schema.Table, name: str, ncol: int) -> None:
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

    def test_makeSchema(self) -> None:
        """Test for creating schemas.

        Schema is defined in YAML files, some checks here depend on that
        configuration and will need to be updated when configuration changes.
        """
        engine = create_engine("sqlite://")

        # create standard (baseline) schema
        schema = ApdbSqlSchema(
            engine=engine, dia_object_index="baseline", htm_index_column="pixelId", schema_file=TEST_SCHEMA
        )
        schema.makeSchema()
        table = schema.get_table(ApdbTables.DiaObject)
        # DiaObject table adds pixelId column.
        self._assertTable(table, "DiaObject", self.table_column_count[ApdbTables.DiaObject] + 1)
        self.assertEqual(len(table.primary_key), 2)
        self.assertEqual(
            len(schema.get_apdb_columns(ApdbTables.DiaObject)), self.table_column_count[ApdbTables.DiaObject]
        )
        with self.assertRaisesRegex(ValueError, ".*does not exist in the schema"):
            schema.get_table(ApdbTables.DiaObjectLast)
        # DiaSource table also adds pixelId column.
        self._assertTable(
            schema.get_table(ApdbTables.DiaSource),
            "DiaSource",
            self.table_column_count[ApdbTables.DiaSource] + 1,
        )
        self.assertEqual(
            len(schema.get_apdb_columns(ApdbTables.DiaSource)), self.table_column_count[ApdbTables.DiaSource]
        )
        self._assertTable(
            schema.get_table(ApdbTables.DiaForcedSource),
            "DiaForcedSource",
            self.table_column_count[ApdbTables.DiaForcedSource],
        )
        self.assertEqual(
            len(schema.get_apdb_columns(ApdbTables.DiaForcedSource)),
            self.table_column_count[ApdbTables.DiaForcedSource],
        )
        for table_enum in ExtraTables:
            with self.assertRaisesRegex(ValueError, ".*does not exist in the schema"):
                schema.get_table(table_enum)

        # create schema using prefix
        schema = ApdbSqlSchema(
            engine=engine,
            dia_object_index="baseline",
            htm_index_column="pixelId",
            schema_file=TEST_SCHEMA,
            prefix="Pfx",
        )
        # Drop existing tables (but we don't check it here)
        schema.makeSchema(drop=True)
        self._assertTable(
            schema.get_table(ApdbTables.DiaObject),
            "PfxDiaObject",
            self.table_column_count[ApdbTables.DiaObject] + 1,
        )
        with self.assertRaisesRegex(ValueError, ".*does not exist in the schema"):
            schema.get_table(ApdbTables.DiaObjectLast)
        self._assertTable(
            schema.get_table(ApdbTables.DiaSource),
            "PfxDiaSource",
            self.table_column_count[ApdbTables.DiaSource] + 1,
        )
        self._assertTable(
            schema.get_table(ApdbTables.DiaForcedSource),
            "PfxDiaForcedSource",
            self.table_column_count[ApdbTables.DiaForcedSource],
        )

        # use different indexing for DiaObject, changes number of PK columns
        schema = ApdbSqlSchema(
            engine=engine, dia_object_index="pix_id_iov", htm_index_column="pixelId", schema_file=TEST_SCHEMA
        )
        schema.makeSchema(drop=True)
        table = schema.get_table(ApdbTables.DiaObject)
        self._assertTable(table, "DiaObject", self.table_column_count[ApdbTables.DiaObject] + 1)
        self.assertEqual(len(table.primary_key), 3)
        with self.assertRaisesRegex(ValueError, ".*does not exist in the schema"):
            schema.get_table(ApdbTables.DiaObjectLast)
        self._assertTable(
            schema.get_table(ApdbTables.DiaSource),
            "DiaSource",
            self.table_column_count[ApdbTables.DiaSource] + 1,
        )
        self._assertTable(
            schema.get_table(ApdbTables.DiaForcedSource),
            "DiaForcedSource",
            self.table_column_count[ApdbTables.DiaForcedSource],
        )

        # use DiaObjectLast table for DiaObject
        schema = ApdbSqlSchema(
            engine=engine,
            dia_object_index="last_object_table",
            htm_index_column="pixelId",
            schema_file=TEST_SCHEMA,
        )
        schema.makeSchema(drop=True)
        table = schema.get_table(ApdbTables.DiaObject)
        self._assertTable(table, "DiaObject", self.table_column_count[ApdbTables.DiaObject] + 1)
        self.assertEqual(len(table.primary_key), 2)
        table = schema.get_table(ApdbTables.DiaObjectLast)
        self._assertTable(table, "DiaObjectLast", self.table_column_count[ApdbTables.DiaObjectLast] + 1)
        self.assertEqual(len(table.primary_key), 2)
        self._assertTable(
            schema.get_table(ApdbTables.DiaSource),
            "DiaSource",
            self.table_column_count[ApdbTables.DiaSource] + 1,
        )
        self._assertTable(
            schema.get_table(ApdbTables.DiaForcedSource),
            "DiaForcedSource",
            self.table_column_count[ApdbTables.DiaForcedSource],
        )

        # Add history_id tables
        schema = ApdbSqlSchema(
            engine=engine,
            dia_object_index="last_object_table",
            htm_index_column="pixelId",
            schema_file=TEST_SCHEMA,
            use_insert_id=True,
        )
        schema.makeSchema(drop=True)
        self._assertTable(schema.get_table(ExtraTables.DiaInsertId), "DiaInsertId", 2)
        self.assertEqual(len(schema.get_apdb_columns(ExtraTables.DiaInsertId)), 2)
        self._assertTable(schema.get_table(ExtraTables.DiaObjectInsertId), "DiaObjectInsertId", 3)
        self.assertEqual(len(schema.get_apdb_columns(ExtraTables.DiaObjectInsertId)), 3)
        self._assertTable(schema.get_table(ExtraTables.DiaSourceInsertId), "DiaSourceInsertId", 2)
        self.assertEqual(len(schema.get_apdb_columns(ExtraTables.DiaSourceInsertId)), 2)
        self._assertTable(schema.get_table(ExtraTables.DiaForcedSourceInsertId), "DiaFSourceInsertId", 3)
        self.assertEqual(len(schema.get_apdb_columns(ExtraTables.DiaForcedSourceInsertId)), 3)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module: Any) -> None:
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
