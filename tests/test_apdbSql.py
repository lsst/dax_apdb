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
import shutil
import tempfile
import unittest
from typing import Any
from unittest.mock import patch

import lsst.utils.tests
from lsst.dax.apdb import Apdb, ApdbConfig, ApdbSql, ApdbTables
from lsst.dax.apdb.tests import ApdbSchemaUpdateTest, ApdbTest

try:
    import testing.postgresql
except ImportError:
    testing = None

TEST_SCHEMA = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema.yaml")


class ApdbSQLiteTestCase(ApdbTest, unittest.TestCase):
    """A test case for ApdbSql class using SQLite backend."""

    fsrc_requires_id_list = True
    dia_object_index = "baseline"
    allow_visit_query = False
    schema_path = TEST_SCHEMA

    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.db_url = f"sqlite:///{self.tempdir}/apdb.sqlite3"

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests."""
        kw = {
            "db_url": self.db_url,
            "schema_file": TEST_SCHEMA,
            "dia_object_index": self.dia_object_index,
            "use_insert_id": self.use_insert_id,
        }
        kw.update(kwargs)
        return ApdbSql.init_database(**kw)  # type: ignore[arg-type]

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


class ApdbSQLiteTestCaseInsertIds(ApdbSQLiteTestCase):
    """Test case for ApdbSql class using SQLite backend with use_insert_id."""

    use_insert_id = True


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class ApdbPostgresTestCase(ApdbTest, unittest.TestCase):
    """A test case for ApdbSql class using Postgres backend."""

    fsrc_requires_id_list = True
    dia_object_index = "last_object_table"
    postgresql: Any
    use_insert_id = True
    allow_visit_query = False
    schema_path = TEST_SCHEMA

    @classmethod
    def setUpClass(cls) -> None:
        # Create the postgres test server.
        cls.postgresql = testing.postgresql.PostgresqlFactory(cache_initialized_db=True)
        super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        # Clean up any lingering SQLAlchemy engines/connections
        # so they're closed before we shut down the server.
        gc.collect()
        cls.postgresql.clear_cache()
        super().tearDownClass()

    def setUp(self) -> None:
        self.server = self.postgresql()

    def tearDown(self) -> None:
        self.server = self.postgresql()

    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests."""
        kw = {
            "db_url": self.server.url(),
            "schema_file": TEST_SCHEMA,
            "dia_object_index": self.dia_object_index,
            "use_insert_id": self.use_insert_id,
        }
        kw.update(kwargs)
        return ApdbSql.init_database(**kw)

    def getDiaObjects_table(self) -> ApdbTables:
        """Return type of table returned from getDiaObjects method."""
        return ApdbTables.DiaObjectLast


@unittest.skipUnless(testing is not None, "testing.postgresql module not found")
class ApdbPostgresNamespaceTestCase(ApdbPostgresTestCase):
    """A test case for ApdbSql class using Postgres backend with schema name"""

    # use mixed case to trigger quoting
    namespace = "ApdbSchema"

    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests."""
        return super().make_instance(namespace=self.namespace, **kwargs)


class ApdbSchemaUpdateSQLiteTestCase(ApdbSchemaUpdateTest, unittest.TestCase):
    """A test case for schema updates using SQLite backend."""

    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.db_url = f"sqlite:///{self.tempdir}/apdb.sqlite3"

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests."""
        kw = {
            "db_url": self.db_url,
            "schema_file": TEST_SCHEMA,
        }
        kw.update(kwargs)
        return ApdbSql.init_database(**kw)  # type: ignore[arg-type]


class ApdbSQLiteFromUriTestCase(unittest.TestCase):
    """A test case for for instantiating ApdbSql via URI."""

    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.db_url = f"sqlite:///{self.tempdir}/apdb.sqlite3"
        config = ApdbSql.init_database(db_url=self.db_url, schema_file=TEST_SCHEMA)
        # This will need update when we switch to pydantic configs.
        self.config_path = os.path.join(self.tempdir, "apdb-config.py")
        config.save(self.config_path)
        self.index_path = os.path.join(self.tempdir, "apdb-index.yaml")
        with open(self.index_path, "w") as index_file:
            print(f'label1: "{self.config_path}"', file=index_file)
            print(f'label2/pex_config: "{self.config_path}"', file=index_file)
        self.bad_config_path = os.path.join(self.tempdir, "not-config.py")

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_make_apdb_from_path(self) -> None:
        """Check that we can make APDB instance from config URI."""
        Apdb.from_uri(self.config_path)
        with self.assertRaises(FileNotFoundError):
            Apdb.from_uri(self.bad_config_path)

    def test_make_apdb_from_labels(self) -> None:
        """Check that we can make APDB instance from config URI."""
        # Replace DAX_APDB_INDEX_URI value
        new_env = {"DAX_APDB_INDEX_URI": self.index_path}
        with patch.dict(os.environ, new_env, clear=True):
            Apdb.from_uri("label:label1")
            Apdb.from_uri("label:label2")
            with self.assertRaises(ValueError):
                Apdb.from_uri("label:bad-label")


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    """Run file leak tests."""


def setup_module(module: Any) -> None:
    """Configure pytest."""
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
