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

"""Unit tests for schema_model conversion from Felis schema objects."""

import unittest
from typing import Any

import felis.datamodel
from pydantic import ValidationError

from lsst.dax.apdb import schema_model


class SchemaModelFromFelisTestCase(unittest.TestCase):
    """Test conversion from Felis schema models to dax_apdb schema models."""

    def _make_schema(self, schema_data: dict[str, Any]) -> felis.datamodel.Schema:
        """Build a Felis schema with deterministic IDs for tests."""
        return felis.datamodel.Schema.model_validate(schema_data, context={"id_generation": True})

    def test_index_column_lookup_by_name(self) -> None:
        """Index column references by name should resolve during conversion."""
        dm_schema = self._make_schema(
            {
                "name": "TestSchema",
                "tables": [
                    {
                        "name": "DiaObject",
                        "columns": [
                            {"name": "diaObjectId", "datatype": "long", "nullable": False},
                            {"name": "validityStartMjdTai", "datatype": "double", "nullable": False},
                        ],
                        "primaryKey": ["diaObjectId", "validityStartMjdTai"],
                        "indexes": [
                            {
                                "name": "IDX_DiaObject_validityStartMjdTai",
                                "columns": ["validityStartMjdTai"],
                            }
                        ],
                    }
                ],
            }
        )

        apdb_schema = schema_model.Schema.from_felis(dm_schema)
        table = apdb_schema.tables[0]

        self.assertEqual(table.indexes[0].columns[0].name, "validityStartMjdTai")
        self.assertEqual([col.name for col in table.primary_key], ["diaObjectId", "validityStartMjdTai"])

    def test_schema_model_conversion(self) -> None:
        """Conversion produces correct instances of all schema_model target
        classes.
        """
        dm_schema = self._make_schema(
            {
                "name": "AllTargetsSchema",
                "tables": [
                    {
                        "name": "Parent",
                        "columns": [
                            {"name": "id", "datatype": "long", "nullable": False},
                            {"name": "code", "datatype": "string", "length": 32, "nullable": False},
                        ],
                        "primaryKey": ["id"],
                        "constraints": [
                            {
                                "name": "uq_parent_code",
                                "@type": "Unique",
                                "columns": ["code"],
                            },
                            {
                                "name": "chk_parent_id",
                                "@type": "Check",
                                "expression": "id > 0",
                            },
                        ],
                        "indexes": [{"name": "idx_parent_code", "columns": ["code"]}],
                    },
                    {
                        "name": "Child",
                        "columns": [
                            {"name": "id", "datatype": "long", "nullable": False},
                            {"name": "parentId", "datatype": "long", "nullable": False},
                        ],
                        "primaryKey": ["id"],
                        "constraints": [
                            {
                                "name": "fk_child_parent",
                                "@type": "ForeignKey",
                                "columns": ["parentId"],
                                "reference": {"table": "Parent", "columns": ["id"]},
                            }
                        ],
                    },
                ],
            }
        )

        apdb_schema = schema_model.Schema.from_felis(dm_schema)
        self.assertIsInstance(apdb_schema, schema_model.Schema)
        self.assertEqual(apdb_schema.name, "AllTargetsSchema")
        self.assertEqual(len(apdb_schema.tables), 2)
        self.assertEqual([table.name for table in apdb_schema.tables], ["Parent", "Child"])

        parent_table, child_table = apdb_schema.tables
        self.assertIsInstance(parent_table, schema_model.Table)
        self.assertIsInstance(child_table, schema_model.Table)

        self.assertEqual([col.name for col in parent_table.columns], ["id", "code"])
        self.assertEqual([col.name for col in child_table.columns], ["id", "parentId"])
        for column in [*parent_table.columns, *child_table.columns]:
            self.assertIsInstance(column, schema_model.Column)
            self.assertIsNotNone(column.table)

        self.assertEqual([col.name for col in parent_table.primary_key], ["id"])
        self.assertEqual([col.name for col in child_table.primary_key], ["id"])

        self.assertEqual(len(parent_table.indexes), 1)
        parent_index = parent_table.indexes[0]
        self.assertIsInstance(parent_index, schema_model.Index)
        self.assertEqual(parent_index.name, "idx_parent_code")
        self.assertEqual([col.name for col in parent_index.columns], ["code"])
        self.assertEqual(child_table.indexes, [])

        self.assertEqual(len(parent_table.constraints), 2)
        self.assertTrue(all(isinstance(c, schema_model.Constraint) for c in parent_table.constraints))
        parent_constraints = {constraint.name: constraint for constraint in parent_table.constraints}

        self.assertIn("uq_parent_code", parent_constraints)
        unique = parent_constraints["uq_parent_code"]
        assert isinstance(unique, schema_model.UniqueConstraint)
        self.assertEqual([col.name for col in unique.columns], ["code"])

        self.assertIn("chk_parent_id", parent_constraints)
        check = parent_constraints["chk_parent_id"]
        assert isinstance(check, schema_model.CheckConstraint)
        self.assertEqual(check.expression, "id > 0")

        self.assertEqual(len(child_table.constraints), 1)
        fk = child_table.constraints[0]
        assert isinstance(fk, schema_model.ForeignKeyConstraint)
        self.assertEqual(fk.name, "fk_child_parent")
        self.assertEqual([col.name for col in fk.columns], ["parentId"])
        self.assertEqual([col.name for col in fk.referenced_columns], ["id"])
        self.assertIs(fk.referenced_table, parent_table)

    def test_foreign_key_legacy_referenced_columns(self) -> None:
        """Legacy ForeignKey.referencedColumns should still resolve."""
        dm_schema = self._make_schema(
            {
                "name": "TestSchema",
                "tables": [
                    {
                        "name": "Parent",
                        "columns": [
                            {"name": "id", "datatype": "long", "nullable": False},
                        ],
                        "primaryKey": ["id"],
                    },
                    {
                        "name": "Child",
                        "columns": [
                            {"name": "childId", "datatype": "long", "nullable": False},
                            {"name": "parentId", "datatype": "long", "nullable": False},
                        ],
                        "primaryKey": ["childId"],
                        "constraints": [
                            {
                                "name": "fk_child_parent",
                                "@type": "ForeignKey",
                                "columns": ["parentId"],
                                "referencedColumns": ["#Parent.id"],
                            }
                        ],
                    },
                ],
            }
        )

        apdb_schema = schema_model.Schema.from_felis(dm_schema)
        child_table = next(table for table in apdb_schema.tables if table.name == "Child")
        fk = child_table.constraints[0]
        assert isinstance(fk, schema_model.ForeignKeyConstraint)

        self.assertEqual([col.name for col in fk.columns], ["parentId"])
        self.assertEqual([col.name for col in fk.referenced_columns], ["id"])

    def test_foreign_key_reference_style(self) -> None:
        """Name-based ForeignKey.reference should resolve during conversion."""
        dm_schema = self._make_schema(
            {
                "name": "TestSchema",
                "tables": [
                    {
                        "name": "Parent",
                        "columns": [
                            {"name": "id", "datatype": "long", "nullable": False},
                        ],
                        "primaryKey": ["id"],
                    },
                    {
                        "name": "Child",
                        "columns": [
                            {"name": "childId", "datatype": "long", "nullable": False},
                            {"name": "parentId", "datatype": "long", "nullable": False},
                        ],
                        "primaryKey": ["childId"],
                        "constraints": [
                            {
                                "name": "fk_child_parent",
                                "@type": "ForeignKey",
                                "columns": ["parentId"],
                                "reference": {"table": "Parent", "columns": ["id"]},
                            }
                        ],
                    },
                ],
            }
        )

        apdb_schema = schema_model.Schema.from_felis(dm_schema)
        child_table = next(table for table in apdb_schema.tables if table.name == "Child")
        fk = child_table.constraints[0]
        assert isinstance(fk, schema_model.ForeignKeyConstraint)

        self.assertEqual([col.name for col in fk.columns], ["parentId"])
        self.assertEqual([col.name for col in fk.referenced_columns], ["id"])

    def test_column_table_backrefs(self) -> None:
        """Converted columns should point back to their owning table."""
        dm_schema = self._make_schema(
            {
                "name": "BackrefSchema",
                "tables": [
                    {
                        "name": "Parent",
                        "columns": [{"name": "id", "datatype": "long", "nullable": False}],
                        "primaryKey": ["id"],
                    },
                    {
                        "name": "Child",
                        "columns": [
                            {"name": "id", "datatype": "long", "nullable": False},
                            {"name": "parentId", "datatype": "long", "nullable": False},
                        ],
                        "primaryKey": ["id"],
                        "constraints": [
                            {
                                "name": "fk_child_parent",
                                "@type": "ForeignKey",
                                "columns": ["parentId"],
                                "reference": {"table": "Parent", "columns": ["id"]},
                            }
                        ],
                    },
                ],
            }
        )

        apdb_schema = schema_model.Schema.from_felis(dm_schema)
        for table in apdb_schema.tables:
            for column in table.columns:
                self.assertIs(column.table, table)

    def test_foreign_key_referenced_column_identity(self) -> None:
        """FK referenced columns should be exact objects from parent table
        columns.
        """
        dm_schema = self._make_schema(
            {
                "name": "IdentitySchema",
                "tables": [
                    {
                        "name": "Parent",
                        "columns": [{"name": "id", "datatype": "long", "nullable": False}],
                        "primaryKey": ["id"],
                    },
                    {
                        "name": "Child",
                        "columns": [
                            {"name": "id", "datatype": "long", "nullable": False},
                            {"name": "parentId", "datatype": "long", "nullable": False},
                        ],
                        "primaryKey": ["id"],
                        "constraints": [
                            {
                                "name": "fk_child_parent",
                                "@type": "ForeignKey",
                                "columns": ["parentId"],
                                "reference": {"table": "Parent", "columns": ["id"]},
                            }
                        ],
                    },
                ],
            }
        )

        apdb_schema = schema_model.Schema.from_felis(dm_schema)
        parent_table = next(table for table in apdb_schema.tables if table.name == "Parent")
        child_table = next(table for table in apdb_schema.tables if table.name == "Child")
        fk = child_table.constraints[0]
        assert isinstance(fk, schema_model.ForeignKeyConstraint)

        self.assertIs(fk.referenced_columns[0], parent_table.columns[0])

    def test_foreign_key_multicolumn_order_preserved(self) -> None:
        """Source and referenced FK column order should match Felis
        definition.
        """
        dm_schema = self._make_schema(
            {
                "name": "OrderSchema",
                "tables": [
                    {
                        "name": "Parent",
                        "columns": [
                            {"name": "id1", "datatype": "long", "nullable": False},
                            {"name": "id2", "datatype": "long", "nullable": False},
                        ],
                        "primaryKey": ["id1", "id2"],
                    },
                    {
                        "name": "Child",
                        "columns": [
                            {"name": "id", "datatype": "long", "nullable": False},
                            {"name": "p1", "datatype": "long", "nullable": False},
                            {"name": "p2", "datatype": "long", "nullable": False},
                        ],
                        "primaryKey": ["id"],
                        "constraints": [
                            {
                                "name": "fk_child_parent",
                                "@type": "ForeignKey",
                                "columns": ["p2", "p1"],
                                "reference": {"table": "Parent", "columns": ["id2", "id1"]},
                            }
                        ],
                    },
                ],
            }
        )

        apdb_schema = schema_model.Schema.from_felis(dm_schema)
        child_table = next(table for table in apdb_schema.tables if table.name == "Child")
        fk = child_table.constraints[0]
        assert isinstance(fk, schema_model.ForeignKeyConstraint)

        self.assertEqual([col.name for col in fk.columns], ["p2", "p1"])
        self.assertEqual([col.name for col in fk.referenced_columns], ["id2", "id1"])

    def test_foreign_key_styles_equivalent(self) -> None:
        """Legacy and reference FK encodings should resolve to same column
        names.
        """
        legacy_schema = self._make_schema(
            {
                "name": "LegacySchema",
                "tables": [
                    {
                        "name": "Parent",
                        "columns": [
                            {"name": "id1", "datatype": "long", "nullable": False},
                            {"name": "id2", "datatype": "long", "nullable": False},
                        ],
                        "primaryKey": ["id1", "id2"],
                    },
                    {
                        "name": "Child",
                        "columns": [
                            {"name": "id", "datatype": "long", "nullable": False},
                            {"name": "p1", "datatype": "long", "nullable": False},
                            {"name": "p2", "datatype": "long", "nullable": False},
                        ],
                        "primaryKey": ["id"],
                        "constraints": [
                            {
                                "name": "fk_child_parent",
                                "@type": "ForeignKey",
                                "columns": ["p2", "p1"],
                                "referencedColumns": ["#Parent.id2", "#Parent.id1"],
                            }
                        ],
                    },
                ],
            }
        )

        reference_schema = self._make_schema(
            {
                "name": "ReferenceSchema",
                "tables": [
                    {
                        "name": "Parent",
                        "columns": [
                            {"name": "id1", "datatype": "long", "nullable": False},
                            {"name": "id2", "datatype": "long", "nullable": False},
                        ],
                        "primaryKey": ["id1", "id2"],
                    },
                    {
                        "name": "Child",
                        "columns": [
                            {"name": "id", "datatype": "long", "nullable": False},
                            {"name": "p1", "datatype": "long", "nullable": False},
                            {"name": "p2", "datatype": "long", "nullable": False},
                        ],
                        "primaryKey": ["id"],
                        "constraints": [
                            {
                                "name": "fk_child_parent",
                                "@type": "ForeignKey",
                                "columns": ["p2", "p1"],
                                "reference": {"table": "Parent", "columns": ["id2", "id1"]},
                            }
                        ],
                    },
                ],
            }
        )

        legacy_apdb_schema = schema_model.Schema.from_felis(legacy_schema)
        reference_apdb_schema = schema_model.Schema.from_felis(reference_schema)
        legacy_child = next(table for table in legacy_apdb_schema.tables if table.name == "Child")
        reference_child = next(table for table in reference_apdb_schema.tables if table.name == "Child")
        legacy_fk = legacy_child.constraints[0]
        reference_fk = reference_child.constraints[0]
        assert isinstance(legacy_fk, schema_model.ForeignKeyConstraint)
        assert isinstance(reference_fk, schema_model.ForeignKeyConstraint)

        self.assertEqual([col.name for col in legacy_fk.columns], [col.name for col in reference_fk.columns])
        self.assertEqual(
            [col.name for col in legacy_fk.referenced_columns],
            [col.name for col in reference_fk.referenced_columns],
        )

    def test_foreign_key_missing_referenced_table_raises(self) -> None:
        """Missing referenced table should fail Felis schema validation."""
        with self.assertRaises(ValidationError):
            self._make_schema(
                {
                    "name": "MissingTableSchema",
                    "tables": [
                        {
                            "name": "Child",
                            "columns": [
                                {"name": "id", "datatype": "long", "nullable": False},
                                {"name": "parentId", "datatype": "long", "nullable": False},
                            ],
                            "primaryKey": ["id"],
                            "constraints": [
                                {
                                    "name": "fk_child_parent",
                                    "@type": "ForeignKey",
                                    "columns": ["parentId"],
                                    "reference": {"table": "Parent", "columns": ["id"]},
                                }
                            ],
                        }
                    ],
                }
            )

    def test_foreign_key_missing_referenced_column_raises(self) -> None:
        """Missing referenced column should fail Felis schema validation."""
        with self.assertRaises(ValidationError):
            self._make_schema(
                {
                    "name": "MissingColumnSchema",
                    "tables": [
                        {
                            "name": "Parent",
                            "columns": [{"name": "id", "datatype": "long", "nullable": False}],
                            "primaryKey": ["id"],
                        },
                        {
                            "name": "Child",
                            "columns": [
                                {"name": "id", "datatype": "long", "nullable": False},
                                {"name": "parentId", "datatype": "long", "nullable": False},
                            ],
                            "primaryKey": ["id"],
                            "constraints": [
                                {
                                    "name": "fk_child_parent",
                                    "@type": "ForeignKey",
                                    "columns": ["parentId"],
                                    "reference": {"table": "Parent", "columns": ["missing"]},
                                }
                            ],
                        },
                    ],
                }
            )


if __name__ == "__main__":
    unittest.main()
