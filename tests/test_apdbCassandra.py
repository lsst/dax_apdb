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

import os
import unittest
from typing import Any

import astropy.time

import lsst.utils.tests
from lsst.dax.apdb import (
    Apdb,
    ApdbConfig,
    ApdbTables,
    ApdbUpdateRecord,
    IncompatibleVersionError,
    ReplicaChunk,
)
from lsst.dax.apdb.cassandra import ApdbCassandra, ApdbCassandraConfig
from lsst.dax.apdb.cassandra.connectionContext import ConnectionContext
from lsst.dax.apdb.cassandra.sessionFactory import SessionFactory
from lsst.dax.apdb.pixelization import Pixelization
from lsst.dax.apdb.tests import ApdbSchemaUpdateTest, ApdbTest, cassandra_mixin
from lsst.dax.apdb.tests.data_factory import makeObjectCatalog
from lsst.dax.apdb.tests.utils import modified_environment

TEST_SCHEMA = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema-apdb.yaml")
TEST_SCHEMA_SSO = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema-sso.yaml")
# Schema that uses `datetime` for timestamps and combines APDB and SSP.
TEST_SCHEMA_DT = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema-datetime.yaml")


class ApdbCassandraMixin(cassandra_mixin.ApdbCassandraMixin):
    """Mixin class which defines common methods for unit tests."""

    def pixelization(self, config: ApdbConfig) -> Pixelization:
        """Return pixelization used by implementation."""
        assert isinstance(config, ApdbCassandraConfig), "Only expect ApdbCassandraConfig here"
        return Pixelization(
            config.partitioning.part_pixelization,
            config.partitioning.part_pix_level,
            config.partitioning.part_pix_max_ranges,
        )


class ApdbCassandraTestCase(ApdbCassandraMixin, ApdbTest, unittest.TestCase):
    """A test case for ApdbCassandra class"""

    time_partition_tables = False
    time_partition_start: str | None = None
    time_partition_end: str | None = None
    extra_chunk_columns = 2

    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests."""
        kw: dict[str, Any] = {
            "hosts": (self.cluster_host,),
            "keyspace": self.keyspace,
            "schema_file": TEST_SCHEMA,
            "ss_schema_file": TEST_SCHEMA_SSO,
            "time_partition_tables": self.time_partition_tables,
            "enable_replica": self.enable_replica,
        }
        if self.time_partition_start:
            kw["time_partition_start"] = self.time_partition_start
        if self.time_partition_end:
            kw["time_partition_end"] = self.time_partition_end
        kw.update(kwargs)
        return ApdbCassandra.init_database(**kw)

    def getDiaObjects_table(self) -> ApdbTables:
        """Return type of table returned from getDiaObjects method."""
        return ApdbTables.DiaObjectLast

    def store_update_records(self, apdb: Apdb, records: list[ApdbUpdateRecord], chunk: ReplicaChunk) -> None:
        # Docstring inherited.
        assert isinstance(apdb, ApdbCassandra), "Expecting ApdbCassandra instance"
        apdb._storeUpdateRecords(records, chunk, store_chunk=True)

    def _count_after_reset_dedup(self, count_before: int) -> int:
        return 0


class ApdbCassandraPerMonthTestCase(ApdbCassandraTestCase):
    """A test case for ApdbCassandra class with per-month tables."""

    time_partition_tables = True
    time_partition_start = "2020-06-01T00:00:00"
    time_partition_end = "2021-06-01T00:00:00"
    meta_row_count = 4

    def test_store_partition_range(self) -> None:
        """Test that writing to non-existing partition raises an error."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = self.make_region()

        # Visit time is beyond time_partition_end.
        visit_time = astropy.time.Time("2022-01-01", format="isot", scale="tai")
        catalog = makeObjectCatalog(region, 100)
        with self.assertRaisesRegex(ValueError, "time partitions that do not yet exist"):
            apdb.store(visit_time, catalog)

        # Writing to last partition makes a warning.
        visit_time = astropy.time.Time("2021-06-01", format="isot", scale="tai")
        catalog = makeObjectCatalog(region, 100)
        with self.assertWarnsRegex(UserWarning, "Writing into the last temporal partition"):
            apdb.store(visit_time, catalog)


class ApdbCassandraTestCaseReplica(ApdbCassandraTestCase):
    """A test case  with enabled replica tables."""

    enable_replica = True
    meta_row_count = 4


class ApdbCassandraTestCaseDatetimeReplica(ApdbCassandraTestCaseReplica):
    """A test case with datetime timestamps."""

    use_mjd = False

    def setUp(self) -> None:
        super().setUp()
        # Schema for datetime case is also missing a validityTime column in
        # DiaObjectLast table.
        self.table_column_count = dict(self.table_column_count)
        self.table_column_count[ApdbTables.DiaObjectLast] = 5

    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        if "schema_file" in kwargs:
            return super().make_instance(**kwargs)
        else:
            return super().make_instance(schema_file=TEST_SCHEMA_DT, **kwargs)


class ApdbSchemaUpdateCassandraTestCase(ApdbCassandraMixin, ApdbSchemaUpdateTest, unittest.TestCase):
    """A test case for schema updates using Cassandra backend."""

    time_partition_tables = False

    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests."""
        kw = {
            "hosts": (self.cluster_host,),
            "keyspace": self.keyspace,
            "schema_file": TEST_SCHEMA,
            "ss_schema_file": TEST_SCHEMA_SSO,
            "time_partition_tables": self.time_partition_tables,
        }
        kw.update(kwargs)
        return ApdbCassandra.init_database(**kw)  # type: ignore[arg-type]


class ApdbCassandraVersionCheck(cassandra_mixin.ApdbCassandraMixin, unittest.TestCase):
    """A test case to verify that version check happens before reading
    frozen configuration.
    """

    def setUp(self) -> None:
        super().setUp()

        self.config = ApdbCassandra.init_database(
            hosts=(self.cluster_host,),
            keyspace=self.keyspace,
            schema_file=TEST_SCHEMA,
            ss_schema_file=TEST_SCHEMA_SSO,
            time_partition_tables=False,
        )

    def test_version_check(self) -> None:
        """Test that version check happens before reading config."""
        apdb = Apdb.from_config(self.config)
        assert isinstance(apdb, ApdbCassandra)

        # Store incompatible version.
        apdb.metadata.set(ConnectionContext.metadataSchemaVersionKey, "99.0.0", force=True)

        # Overwrite frozen config with something that will break.
        apdb.metadata.set(ConnectionContext.metadataConfigKey, '{"not_a_config_key": 0}', force=True)

        # Try again.
        with self.assertRaises(IncompatibleVersionError):
            # Need to call some actual method to initiate connection.
            Apdb.from_config(self.config).metadata.items()


_DB_AUTH_JSON = """\
[{
  "url": "cassandra://user1000@node1.slac.stanford.edu:9042/",
  "username": "user01",
  "password": "pass01"
}, {
  "url": "cassandra://node2.slac.stanford.edu:9042/",
  "username": "user02",
  "password": "pass02"
}, {
  "url": "cassandra://node1.slac.stanford.edu:9042/apdb_dev",
  "username": "user03",
  "password": "pass03"
}, {
  "url": "cassandra://user2000@test_cluster/",
  "username": "user04",
  "password": "pass04"
}, {
  "url": "cassandra://test_cluster/",
  "username": "user05",
  "password": "pass05"
}]
"""


class ApdbCassandraDbAuthTest(unittest.TestCase):
    """A test case for extracting credentials from db-auth.yaml."""

    def _make_config(self) -> ApdbCassandraConfig:
        config = ApdbCassandraConfig(
            contact_points=("node1.slac.stanford.edu", "node2.slac.stanford.edu"),
            keyspace="apdb",
        )
        return config

    @unittest.skipIf(not cassandra_mixin.CASSANDRA_IMPORTED, "cassandra_driver cannot be imported")
    def test_dbauth(self) -> None:
        """Check credentials access."""
        with modified_environment(LSST_DB_AUTH_CREDENTIALS=_DB_AUTH_JSON):
            config = self._make_config()

            factory = SessionFactory(config)

            # Should match second entry.
            auth = factory._make_auth_provider()
            assert auth is not None
            self.assertEqual(auth.username, "user02")

            config.keyspace = "apdb_dev"
            # Should match third entry.
            auth = factory._make_auth_provider()
            assert auth is not None
            self.assertEqual(auth.username, "user03")

            config.connection_config.username = "user1000"
            # Should match first entry, returns original user name.
            auth = factory._make_auth_provider()
            assert auth is not None
            self.assertEqual(auth.username, "user1000")
            self.assertEqual(auth.password, "pass01")

            config.connection_config.username = ""
            config.connection_config.dbauth_alias = "test_cluster"
            # Should match fifth entry.
            auth = factory._make_auth_provider()
            assert auth is not None
            self.assertEqual(auth.username, "user05")

            config.connection_config.username = "user2000"
            config.connection_config.dbauth_alias = "test_cluster"
            # Should match fourth entry.
            auth = factory._make_auth_provider()
            assert auth is not None
            self.assertEqual(auth.username, "user2000")
            self.assertEqual(auth.password, "pass04")


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    """Run file leak tests."""


def setup_module(module: Any) -> None:
    """Configure pytest."""
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
