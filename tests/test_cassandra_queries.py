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

import unittest

from lsst.dax.apdb.cassandra.queries import ColumnExpr, Delete, Insert, Select, WhereClause


class CassandraWhereClauseTestCase(unittest.TestCase):
    """A test case for WhereClause class."""

    def test_basic(self) -> None:
        """Test construction of the class."""
        clause = WhereClause("x = 100")
        self.assertEqual(clause.expression, "x = 100")
        self.assertEqual(clause.parameters, ())
        self.assertFalse(clause.can_prepare)

        clause = WhereClause("x = {}", (100,))
        self.assertEqual(clause.expression, "x = {}")
        self.assertEqual(clause.parameters, (100,))
        self.assertTrue(clause.can_prepare)

        clause = WhereClause("x = 100", can_prepare=False)
        self.assertEqual(clause.expression, "x = 100")
        self.assertEqual(clause.parameters, ())
        self.assertFalse(clause.can_prepare)

    def test_error(self) -> None:
        """Test for exceptions."""
        with self.assertRaisesRegex(ValueError, "Number of placeholders .* does not match"):
            WhereClause("x = 100", (1,))
        with self.assertRaisesRegex(ValueError, "Number of placeholders .* does not match"):
            WhereClause("x = {} and y = {}", (1,))

    def test_combine(self) -> None:
        """Test combination of clauses."""
        clause1 = WhereClause("x = {}", (10,))
        clause2 = WhereClause("y = {}", (100,))
        clause3 = WhereClause("z = {}", (1000,), can_prepare=False)

        clause = clause1 & clause2
        self.assertEqual(clause.expression, "x = {} AND y = {}")
        self.assertEqual(clause.parameters, (10, 100))
        self.assertTrue(clause.can_prepare)

        clause &= clause3
        self.assertEqual(clause.expression, "x = {} AND y = {} AND z = {}")
        self.assertEqual(clause.parameters, (10, 100, 1000))
        self.assertFalse(clause.can_prepare)

    def test_combine_products(self) -> None:
        """Test combination of clauses."""
        clauses1 = [WhereClause("x = {}", (10,)), WhereClause("x = {}", (20,))]
        clauses2 = [WhereClause("y = {}", (100,)), WhereClause("y = {}", (200,))]
        extra = WhereClause("z = 1000")  # can_prepare will be False

        clauses = list(WhereClause.combine(clauses1, clauses2))
        self.assertEqual(len(clauses), 4)
        self.assertEqual(clauses[0].expression, "x = {} AND y = {}")
        self.assertEqual(clauses[0].parameters, (10, 100))
        self.assertEqual(clauses[1].expression, "x = {} AND y = {}")
        self.assertEqual(clauses[1].parameters, (10, 200))
        self.assertEqual(clauses[2].expression, "x = {} AND y = {}")
        self.assertEqual(clauses[2].parameters, (20, 100))
        self.assertEqual(clauses[3].expression, "x = {} AND y = {}")
        self.assertEqual(clauses[3].parameters, (20, 200))
        self.assertTrue(clauses[3].can_prepare)

        clauses = list(WhereClause.combine(clauses1, [], extra=extra))
        self.assertEqual(len(clauses), 2)
        self.assertEqual(clauses[0].expression, "x = {} AND z = 1000")
        self.assertEqual(clauses[0].parameters, (10,))
        self.assertFalse(clauses[1].can_prepare)
        self.assertEqual(clauses[1].expression, "x = {} AND z = 1000")
        self.assertEqual(clauses[1].parameters, (20,))
        self.assertFalse(clauses[1].can_prepare)


class SelectQueryTestCase(unittest.TestCase):
    """A test case for Select class."""

    def test_basic(self) -> None:
        """Test simple construction."""
        query = Select("keyspace", "table", ["*"])
        self.assertEqual(str(query), "SELECT * FROM keyspace.table")

        query = Select("Keyspace", "Table", ["A", "b", "c"])
        self.assertEqual(str(query), 'SELECT "A",b,c FROM "Keyspace"."Table"')

        query = Select("keyspace", "table", [ColumnExpr("COUNT(*)")], extra_clause="ALLOW FILTERING")
        self.assertEqual(str(query), "SELECT COUNT(*) FROM keyspace.table ALLOW FILTERING")

    def test_where(self) -> None:
        """Test WHERE clause."""
        query = Select("keyspace", "table", ["*"], extra_clause="LIMIT 1")
        query = query.where(WhereClause("x = {}", (10,)))
        self.assertEqual(str(query), "SELECT * FROM keyspace.table WHERE x = {} LIMIT 1")
        self.assertEqual(query.parameters, (10,))
        query = query.where(WhereClause("y IN ({*})", [100, 101]))
        self.assertEqual(str(query), "SELECT * FROM keyspace.table WHERE x = {} AND y IN ({},{}) LIMIT 1")
        self.assertEqual(query.parameters, (10, 100, 101))
        query = query.where("z = {}", [1000], can_prepare=False)
        self.assertEqual(
            str(query), "SELECT * FROM keyspace.table WHERE x = {} AND y IN ({},{}) AND z = {} LIMIT 1"
        )
        self.assertEqual(query.parameters, (10, 100, 101, 1000))

    def test_can_prepare(self) -> None:
        """Test can_prepare handling."""
        query = Select("keyspace", "table", ["*"])
        query = query.where("x = {}", (10,))
        self.assertTrue(query.can_prepare)

        query = query.where("y = {}", (10,), can_prepare=False)
        self.assertFalse(query.can_prepare)

        query = Select("keyspace", "table", ["*"], can_prepare=False)
        query = query.where(WhereClause("x = {}", (10,)))
        self.assertFalse(query.can_prepare)

    def test_render(self) -> None:
        """Test render() method."""
        query = Select("keyspace", "table", ["*"], where_clause=WhereClause("x = {} AND y = {}", (10, 100)))
        self.assertEqual(query.render(), "SELECT * FROM keyspace.table WHERE x = {} AND y = {}")
        self.assertEqual(query.render("?"), "SELECT * FROM keyspace.table WHERE x = ? AND y = ?")
        self.assertEqual(query.render("%s"), "SELECT * FROM keyspace.table WHERE x = %s AND y = %s")

    def test_errors(self) -> None:
        """Test exceptions."""
        query = Select("keyspace", "table", ["*"])
        with self.assertRaisesRegex(TypeError, "Unexpected arguments"):
            query.where()  # type: ignore[call-overload]
        with self.assertRaisesRegex(TypeError, "No keyword arguments expected"):
            query.where(WhereClause("x = {}", (10,)), can_prepare=False)  # type: ignore[call-overload]
        with self.assertRaisesRegex(TypeError, "Unexpected keyword arguments"):
            query.where("x = {}", (10,), cannot_prepare=False)  # type: ignore[call-overload]
        with self.assertRaisesRegex(TypeError, "Unexpected arguments"):
            query.where("x = {}", 10, can_prepare=False)  # type: ignore[call-overload]
        with self.assertRaisesRegex(TypeError, "Unexpected arguments"):
            query.where("x = {}", 10, 20)  # type: ignore[call-overload]


class InsertQueryTestCase(unittest.TestCase):
    """A test case for Insert class."""

    def test_basic(self) -> None:
        """Test simple construction."""
        query = Insert("keyspace", "table", ["x", "y"])
        self.assertEqual(str(query), "INSERT INTO keyspace.table (x,y) VALUES ({},{})")
        query = Insert("Keyspace", "Table", ["X", "Y"])
        self.assertEqual(str(query), 'INSERT INTO "Keyspace"."Table" ("X","Y") VALUES ({},{})')

    def test_can_prepare(self) -> None:
        """Test can_prepare handling."""
        query = Insert("keyspace", "table", ["x", "y"])
        self.assertTrue(query.can_prepare)

        query = Insert("keyspace", "table", ["x", "y"], can_prepare=False)
        self.assertFalse(query.can_prepare)

    def test_render(self) -> None:
        """Test render() method."""
        query = Insert("keyspace", "table", ["x", "y"])
        self.assertEqual(query.render(), "INSERT INTO keyspace.table (x,y) VALUES ({},{})")
        self.assertEqual(query.render("?"), "INSERT INTO keyspace.table (x,y) VALUES (?,?)")
        self.assertEqual(query.render("%s"), "INSERT INTO keyspace.table (x,y) VALUES (%s,%s)")


class DeleteQueryTestCase(unittest.TestCase):
    """A test case for Delete class."""

    def test_basic(self) -> None:
        """Test simple construction."""
        query = Delete("keyspace", "table")
        query = query.where("x = {}", (10,))
        self.assertEqual(str(query), "DELETE FROM keyspace.table WHERE x = {}")

        query = Delete("Keyspace", "Table")
        query = query.where('"Y" = {}', (10,))
        self.assertEqual(str(query), 'DELETE FROM "Keyspace"."Table" WHERE "Y" = {}')

    def test_can_prepare(self) -> None:
        """Test can_prepare handling."""
        query = Delete("keyspace", "table")
        query = query.where("x = {}", (10,))
        self.assertTrue(query.can_prepare)

        query = query.where("y = {}", (10,), can_prepare=False)
        self.assertFalse(query.can_prepare)

        query = Delete("keyspace", "table", can_prepare=False)
        query = query.where("x = {}", (10,))
        self.assertFalse(query.can_prepare)

    def test_render(self) -> None:
        """Test render() method."""
        query = Delete("keyspace", "table", where_clause=WhereClause("x = {} AND y = {}", (10, 100)))
        self.assertEqual(query.render(), "DELETE FROM keyspace.table WHERE x = {} AND y = {}")
        self.assertEqual(query.render("?"), "DELETE FROM keyspace.table WHERE x = ? AND y = ?")
        self.assertEqual(query.render("%s"), "DELETE FROM keyspace.table WHERE x = %s AND y = %s")

    def test_errors(self) -> None:
        """Test exceptions."""
        query = Delete("keyspace", "table")
        with self.assertRaisesRegex(RuntimeError, "DELETE statement without WHERE clause"):
            query.render()
        with self.assertRaisesRegex(TypeError, "Unexpected arguments"):
            query.where()  # type: ignore[call-overload]


if __name__ == "__main__":
    unittest.main()
