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
from lsst.dax.apdb.apdbSchema import ApdbTables
from lsst.dax.apdb.sql.apdbSqlSchema import ApdbSqlSchema, ExtraTables
from lsst.dax.apdb.tests import update_schema_yaml
from sqlalchemy import create_engine

TEST_SCHEMA = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema.yaml")


class ApdbSchemaTestCase(unittest.TestCase):
    """Test case for ApdbSqlSchema class.

    Schema is defined in YAML files, some checks here depend on that
    configuration and will need to be updated when configuration changes.
    """

    # number of columns as defined in tests/config/schema.yaml
    table_column_count = {
        ApdbTables.DiaObject: 8,
        ApdbTables.DiaObjectLast: 5,
        ApdbTables.DiaSource: 10,
        ApdbTables.DiaForcedSource: 4,
        ApdbTables.SSObject: 3,
        ApdbTables.metadata: 2,
    }

    def _assertTable(self, table: sqlalchemy.schema.Table, name: str, ncol: int) -> None:
        """Validate tables schema.

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

    def test_makeSchema_default(self) -> None:
        """Test for creating schema."""
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
        self._assertTable(
            schema.get_table(ApdbTables.metadata),
            "metadata",
            self.table_column_count[ApdbTables.metadata],
        )
        self.assertEqual(
            len(schema.get_apdb_columns(ApdbTables.metadata)),
            self.table_column_count[ApdbTables.metadata],
        )
        for table_enum in ExtraTables:
            with self.assertRaisesRegex(ValueError, ".*does not exist in the schema"):
                schema.get_table(table_enum)

    def test_makeSchema_prefix(self) -> None:
        """Create schema using prefix."""
        engine = create_engine("sqlite://")
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

    def test_makeSchema_other_index(self) -> None:
        """Use different indexing for DiaObject, this changes number of PK
        columns.
        """
        engine = create_engine("sqlite://")
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

    def test_makeSchema_diaobjectlast(self) -> None:
        """Use DiaObjectLast table for DiaObject."""
        engine = create_engine("sqlite://")
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

    def test_makeSchema_replica(self) -> None:
        """Add replica tables."""
        engine = create_engine("sqlite://")
        schema = ApdbSqlSchema(
            engine=engine,
            dia_object_index="last_object_table",
            htm_index_column="pixelId",
            schema_file=TEST_SCHEMA,
            enable_replica=True,
        )
        schema.makeSchema(drop=True)
        self._assertTable(schema.get_table(ExtraTables.ApdbReplicaChunks), "ApdbReplicaChunks", 3)
        self.assertEqual(len(schema.get_apdb_columns(ExtraTables.ApdbReplicaChunks)), 3)
        self._assertTable(schema.get_table(ExtraTables.DiaObjectChunks), "DiaObjectChunks", 3)
        self.assertEqual(len(schema.get_apdb_columns(ExtraTables.DiaObjectChunks)), 3)
        self._assertTable(schema.get_table(ExtraTables.DiaSourceChunks), "DiaSourceChunks", 2)
        self.assertEqual(len(schema.get_apdb_columns(ExtraTables.DiaSourceChunks)), 2)
        self._assertTable(schema.get_table(ExtraTables.DiaForcedSourceChunks), "DiaForcedSourceChunks", 3)
        self.assertEqual(len(schema.get_apdb_columns(ExtraTables.DiaForcedSourceChunks)), 3)

    def test_makeSchema_nometa(self) -> None:
        """Make schema using old yaml file without metadata table."""
        with update_schema_yaml(TEST_SCHEMA, drop_metadata=True) as schema_file:
            engine = create_engine("sqlite://")
            schema = ApdbSqlSchema(
                engine=engine,
                dia_object_index="baseline",
                htm_index_column="pixelId",
                schema_file=schema_file,
            )
            schema.makeSchema(drop=True)
            with self.assertRaisesRegex(ValueError, "Table type ApdbTables.metadata does not exist"):
                schema.get_table(ApdbTables.metadata)

            # Also check the case when database is missing metadata table but
            # YAML schema has it.
            schema = ApdbSqlSchema(
                engine=engine,
                dia_object_index="baseline",
                htm_index_column="pixelId",
                schema_file=TEST_SCHEMA,
            )
            with self.assertRaisesRegex(ValueError, "Table type ApdbTables.metadata does not exist"):
                schema.get_table(ApdbTables.metadata)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    """Run file leak tests."""


def setup_module(module: Any) -> None:
    """Configure pytest."""
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
