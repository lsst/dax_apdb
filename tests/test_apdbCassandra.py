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
from typing import Any

import lsst.utils.tests
from lsst.dax.apdb import ApdbConfig, ApdbTables
from lsst.dax.apdb.cassandra import ApdbCassandra, ApdbCassandraConfig
from lsst.dax.apdb.pixelization import Pixelization
from lsst.dax.apdb.tests import ApdbSchemaUpdateTest, ApdbTest, cassandra_mixin

TEST_SCHEMA = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema.yaml")

logging.basicConfig(level=logging.INFO)


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
    # Cassandra stores timestamps with millisecond precision internally,
    # but pandas seem to convert them to nanosecond type.
    timestamp_type_name = "datetime64[ns]"
    extra_chunk_columns = 2

    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests."""
        kw: dict[str, Any] = {
            "hosts": (self.cluster_host,),
            "keyspace": self.keyspace,
            "schema_file": TEST_SCHEMA,
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


class ApdbCassandraPerMonthTestCase(ApdbCassandraTestCase):
    """A test case for ApdbCassandra class with per-month tables."""

    time_partition_tables = True
    time_partition_start = "2020-06-01T00:00:00"
    time_partition_end = "2021-06-01T00:00:00"
    meta_row_count = 4


class ApdbCassandraTestCaseReplica(ApdbCassandraTestCase):
    """A test case  with enabled replica tables."""

    enable_replica = True
    meta_row_count = 4


class ApdbSchemaUpdateCassandraTestCase(ApdbCassandraMixin, ApdbSchemaUpdateTest, unittest.TestCase):
    """A test case for schema updates using Cassandra backend."""

    time_partition_tables = False

    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests."""
        kw = {
            "hosts": (self.cluster_host,),
            "keyspace": self.keyspace,
            "schema_file": TEST_SCHEMA,
            "time_partition_tables": self.time_partition_tables,
        }
        kw.update(kwargs)
        return ApdbCassandra.init_database(**kw)  # type: ignore[arg-type]


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    """Run file leak tests."""


def setup_module(module: Any) -> None:
    """Configure pytest."""
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
