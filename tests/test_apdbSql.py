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

"""Unit test for Apdb class.
"""

import gc
import os
import unittest
from typing import Any

from lsst.dax.apdb import ApdbConfig, ApdbSqlConfig, ApdbTables
from lsst.dax.apdb.tests import ApdbTest
import lsst.utils.tests

try:
    import testing.postgresql
except ImportError:
    testing = None


TEST_SCHEMA = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema.yaml")


class ApdbSqlTest(ApdbTest):
    """Base class for unit tests for SQL backends."""

    def n_columns(self, table: ApdbTables) -> int:
        """Return number of columns for a specified table."""

        # Some tables add pixelId column to standard schema
        if table is ApdbTables.DiaObject:
            return self.n_obj_columns + 1
        elif table is ApdbTables.DiaObjectLast:
            return self.n_obj_last_columns + 1
        elif table is ApdbTables.DiaSource:
            return self.n_src_columns + 1
        elif table is ApdbTables.DiaForcedSource:
            return self.n_fsrc_columns
        elif table is ApdbTables.SSObject:
            return self.n_ssobj_columns


class ApdbSQLiteTestCase(unittest.TestCase, ApdbSqlTest):
    """A test case for ApdbSql class using SQLite backend."""

    fsrc_requires_id_list = True
    dia_object_index = "baseline"

    def make_config(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests."""
        kw = {
            "db_url": "sqlite://",
            "schema_file": TEST_SCHEMA,
            "dia_object_index": self.dia_object_index
        }
        kw.update(kwargs)
        return ApdbSqlConfig(**kw)

    def getDiaObjects_table(self) -> ApdbTables:
        """Return type of table returned from getDiaObjects method."""
        return ApdbTables.DiaObject


class ApdbSQLiteTestCaseLastObject(ApdbSQLiteTestCase):
    """A test case for ApdbSql class using SQLite backend and DiaObjectLast
    table.
    """

    dia_object_index = "last_object_table"

    def getDiaObjects_table(self) -> ApdbTables:
        """Return type of table returned from getDiaObjects method."""
        return ApdbTables.DiaObjectLast


class ApdbSQLiteTestCasePixIdIovIndex(ApdbSQLiteTestCase):
    """A test case for ApdbSql class using SQLite backend with pix_id_iov
    indexing.
    """

    dia_object_index = "pix_id_iov"


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class ApdbPostgresTestCase(unittest.TestCase, ApdbSqlTest):
    """A test case for ApdbSql class using Postgres backend."""

    fsrc_requires_id_list = True
    dia_object_index = "last_object_table"

    @classmethod
    def setUpClass(cls):
        # Create the postgres test server.
        cls.postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        # Clean up any lingering SQLAlchemy engines/connections
        # so they're closed before we shut down the server.
        gc.collect()
        cls.postgresql.clear_cache()
        super().tearDownClass()

    def setUp(self):
        self.server = self.postgresql()

    def tearDown(self):
        self.server = self.postgresql()

    def make_config(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests."""
        kw = {
            "db_url": self.server.url(),
            "schema_file": TEST_SCHEMA,
            "dia_object_index": self.dia_object_index
        }
        kw.update(kwargs)
        return ApdbSqlConfig(**kw)

    def getDiaObjects_table(self) -> ApdbTables:
        """Return type of table returned from getDiaObjects method."""
        return ApdbTables.DiaObjectLast


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class ApdbPostgresNamespaceTestCase(ApdbPostgresTestCase):
    """A test case for ApdbSql class using Postgres backend with schema name"""

    # use mixed case to trigger quoting
    namespace = "ApdbSchema"

    def make_config(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests."""
        return super().make_config(namespace=self.namespace, **kwargs)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
