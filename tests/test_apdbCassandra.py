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

"""Unit test for `ApdbCassandra` class.

Notes
-----
For now this test can only run against actual Cassandra cluster, to specify
cluster location use ``DAX_APDB_TEST_CASSANDRA_CLUSTER`` environment variable,
e.g.:

    export DAX_APDB_TEST_CASSANDRA_CLUSTER=cassandra.example.com
    pytest tests/test_apdbCassandra.py

Individual tests create and destroy unique keyspaces in the cluster, there is
no need to pre-create a keyspace with predefined name.
"""

import logging
import os
import unittest
import uuid
from typing import TYPE_CHECKING, Any

import lsst.utils.tests
from lsst.dax.apdb import ApdbCassandra, ApdbCassandraConfig, ApdbTables
from lsst.dax.apdb.apdbCassandra import CASSANDRA_IMPORTED
from lsst.dax.apdb.tests import ApdbSchemaUpdateTest, ApdbTest

TEST_SCHEMA = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema.yaml")

logging.basicConfig(level=logging.INFO)


class ApdbCassandraMixin:
    """Mixin class which defines common methods for unit tests."""

    @classmethod
    def setUpClass(cls) -> None:
        """Prepare config for server connection."""
        cluster_host = os.environ.get("DAX_APDB_TEST_CASSANDRA_CLUSTER")
        if not cluster_host:
            raise unittest.SkipTest("DAX_APDB_TEST_CASSANDRA_CLUSTER is not set")
        if not CASSANDRA_IMPORTED:
            raise unittest.SkipTest("cassandra_driver cannot be imported")

    def setUp(self) -> None:
        """Prepare config for server connection."""
        self.cluster_host = os.environ.get("DAX_APDB_TEST_CASSANDRA_CLUSTER")
        self.keyspace = ""

        config = self.make_config()

        # create dedicated keyspace for each test
        key = uuid.uuid4()
        self.keyspace = f"apdb_{key.hex}"
        query = (
            f"CREATE KEYSPACE {self.keyspace}"
            " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        )

        apdb = ApdbCassandra(config)
        apdb._session.execute(query)
        del apdb

    def tearDown(self) -> None:
        config = self.make_config()
        apdb = ApdbCassandra(config)
        query = f"DROP KEYSPACE {self.keyspace}"
        apdb._session.execute(query)
        del apdb

    if TYPE_CHECKING:
        # For mypy.
        def make_config(self, **kwargs: Any) -> ApdbCassandraConfig:
            ...


class ApdbCassandraTestCase(ApdbCassandraMixin, ApdbTest, unittest.TestCase):
    """A test case for ApdbCassandra class"""

    allow_visit_query = False
    time_partition_tables = False
    time_partition_start: str | None = None
    time_partition_end: str | None = None

    def make_config(self, **kwargs: Any) -> ApdbCassandraConfig:
        """Make config class instance used in all tests."""
        kw = {
            "contact_points": [self.cluster_host],
            "keyspace": self.keyspace,
            "schema_file": TEST_SCHEMA,
            "time_partition_tables": self.time_partition_tables,
            "use_insert_id": self.use_insert_id,
        }
        if self.time_partition_start:
            kw["time_partition_start"] = self.time_partition_start
        if self.time_partition_end:
            kw["time_partition_end"] = self.time_partition_end
        kw.update(kwargs)
        return ApdbCassandraConfig(**kw)

    def getDiaObjects_table(self) -> ApdbTables:
        """Return type of table returned from getDiaObjects method."""
        return ApdbTables.DiaObjectLast


class ApdbCassandraPerMonthTestCase(ApdbCassandraTestCase):
    """A test case for ApdbCassandra class with per-month tables."""

    time_partition_tables = True
    time_partition_start = "2019-12-01T00:00:00"
    time_partition_end = "2022-01-01T00:00:00"


class ApdbCassandraTestCaseInsertIds(ApdbCassandraTestCase):
    """A test case  with use_insert_id."""

    use_insert_id = True


class ApdbSchemaUpdateCassandraTestCase(ApdbCassandraMixin, ApdbSchemaUpdateTest, unittest.TestCase):
    """A test case for schema updates using Cassandra backend."""

    def make_config(self, **kwargs: Any) -> ApdbCassandraConfig:
        """Make config class instance used in all tests."""
        kw = {
            "contact_points": [self.cluster_host],
            "keyspace": self.keyspace,
            "schema_file": TEST_SCHEMA,
            "time_partition_tables": False,
        }
        kw.update(kwargs)
        return ApdbCassandraConfig(**kw)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    """Run file leak tests."""


def setup_module(module: Any) -> None:
    """Configure pytest."""
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
