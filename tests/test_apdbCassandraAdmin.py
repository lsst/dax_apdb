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

"""Unit test for `ApdbCassandraAdmin` class.

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
from typing import Any, cast

import astropy.time

from lsst.dax.apdb import Apdb, ApdbConfig
from lsst.dax.apdb.cassandra import ApdbCassandra
from lsst.dax.apdb.cassandra.apdbCassandraAdmin import ApdbCassandraAdmin
from lsst.dax.apdb.cassandra.partitioner import Partitioner
from lsst.dax.apdb.tests import ApdbAdminTest, cassandra_mixin

TEST_SCHEMA = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema-apdb+sso.yaml")

logging.basicConfig(level=logging.INFO)


class ApdbCassandraAdminTestCase(cassandra_mixin.ApdbCassandraMixin, ApdbAdminTest, unittest.TestCase):
    """A test case for ApdbCassandraAdmin class."""

    schema_path = TEST_SCHEMA
    time_partition_tables = False
    time_partition_start: str | None = None
    time_partition_end: str | None = None

    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests."""
        kw: dict[str, Any] = {
            "hosts": (self.cluster_host,),
            "keyspace": self.keyspace,
            "schema_file": TEST_SCHEMA,
            "time_partition_tables": self.time_partition_tables,
        }
        if self.time_partition_start:
            kw["time_partition_start"] = self.time_partition_start
        if self.time_partition_end:
            kw["time_partition_end"] = self.time_partition_end
        kw.update(kwargs)
        return ApdbCassandra.init_database(**kw)

    def make_admin(self) -> ApdbCassandraAdmin:
        """Make ApdbCassandraAdmin instance."""
        config = self.make_instance()
        apdb = cast(ApdbCassandra, Apdb.from_config(config))
        return apdb.admin

    def test_extend_time_partitions(self) -> None:
        """Test extend_time_partitions() method."""
        admin = self.make_admin()

        self._test_extend_time_partitions(admin)

    def _test_extend_time_partitions(self, admin: ApdbCassandraAdmin) -> None:
        """Implement test_extend_time_partitions, to be specialized in
        subclasses.
        """
        with self.assertRaisesRegex(TypeError, "does not use time-partitioned tables"):
            admin.time_partitions()
        with self.assertRaisesRegex(TypeError, "does not use time-partitioned tables"):
            admin.extend_time_partitions(astropy.time.Time("2020-01-01T00:00:00Z", format="isot"))

    def test_delete_time_partitions(self) -> None:
        """Test delete_time_partitions() method."""
        admin = self.make_admin()

        self._test_delete_time_partitions(admin)

    def _test_delete_time_partitions(self, admin: ApdbCassandraAdmin) -> None:
        """Implement test_delete_time_partitions, to be specialized in
        subclasses.
        """
        with self.assertRaisesRegex(TypeError, "does not use time-partitioned tables"):
            admin.delete_time_partitions(astropy.time.Time("2020-01-01T00:00:00Z", format="isot"))


class ApdbCassandraAdminTimePartitionedTestCase(ApdbCassandraAdminTestCase):
    """A test case for ApdbCassandraAdmin class with per-month tables."""

    time_partition_tables = True
    time_partition_start = "2025-01-01T00:00:00"
    time_partition_end = "2026-01-01T00:00:00"

    def _test_extend_time_partitions(self, admin: ApdbCassandraAdmin) -> None:
        time_part = admin.time_partitions()
        self.assertEqual((time_part.start, time_part.end), (669, 681))

        # Test too long range first.
        with self.assertRaisesRegex(ValueError, "Extension exceeds limit"):
            admin.extend_time_partitions(astropy.time.Time("2027-02-02T00:00:00Z", format="isot"))
        with self.assertRaisesRegex(ValueError, "Extension exceeds limit"):
            admin.extend_time_partitions(
                astropy.time.Time("2023-11-25T00:00:00Z", format="isot"), forward=False
            )

        # Nothing to do.
        self.assertFalse(
            admin.extend_time_partitions(astropy.time.Time("2024-01-01T00:00:00Z", format="isot"))
        )
        self.assertFalse(
            admin.extend_time_partitions(
                astropy.time.Time("2027-01-01T00:00:00Z", format="isot"), forward=False
            )
        )
        time_part = admin.time_partitions()
        self.assertEqual((time_part.start, time_part.end), (669, 681))

        self.assertTrue(
            admin.extend_time_partitions(astropy.time.Time("2026-12-01T00:00:00Z", format="isot"))
        )
        time_part = admin.time_partitions()
        self.assertEqual((time_part.start, time_part.end), (669, 692))
        self.assertTrue(
            admin.extend_time_partitions(
                astropy.time.Time("2024-02-01T00:00:00Z", format="isot"), forward=False
            )
        )
        time_part = admin.time_partitions()
        self.assertEqual((time_part.start, time_part.end), (658, 692))

    def _no_confirm(self, *, partitions: list[int], tables: list[str], partitioner: Partitioner) -> bool:
        return False

    def _confirm(self, *, partitions: list[int], tables: list[str], partitioner: Partitioner) -> bool:
        return True

    def _test_delete_time_partitions(self, admin: ApdbCassandraAdmin) -> None:
        time_part = admin.time_partitions()
        self.assertEqual((time_part.start, time_part.end), (669, 681))

        # Should not delete anyhting.
        partitions = admin.delete_time_partitions(
            astropy.time.Time(self.time_partition_start, format="isot", scale="tai")
        )
        self.assertEqual(partitions, [])
        partitions = admin.delete_time_partitions(
            astropy.time.Time(self.time_partition_end, format="isot", scale="tai"), after=True
        )
        self.assertEqual(partitions, [])
        partitions = admin.delete_time_partitions(astropy.time.Time("2020-01-01", format="isot", scale="tai"))
        self.assertEqual(partitions, [])
        partitions = admin.delete_time_partitions(
            astropy.time.Time("2030-01-01", format="isot", scale="tai"), after=True
        )
        self.assertEqual(partitions, [])

        # Deletes one partiion
        partitions = admin.delete_time_partitions(astropy.time.Time("2025-01-31", format="isot", scale="tai"))
        self.assertEqual(partitions, [669])
        time_part = admin.time_partitions()
        self.assertEqual((time_part.start, time_part.end), (670, 681))

        # Not confirmed.
        partitions = admin.delete_time_partitions(
            astropy.time.Time("2025-03-31", format="isot", scale="tai"), confirm=self._no_confirm
        )
        self.assertEqual(partitions, [])

        # Confirmed.
        partitions = admin.delete_time_partitions(
            astropy.time.Time("2025-11-02", format="isot", scale="tai"), after=True, confirm=self._confirm
        )
        self.assertEqual(partitions, [680, 681])
        time_part = admin.time_partitions()
        self.assertEqual((time_part.start, time_part.end), (670, 679))

        with self.assertRaisesRegex(ValueError, "Cannot delete all partitions"):
            admin.delete_time_partitions(astropy.time.Time("2030-01-01", format="isot", scale="tai"))


class ApdbCassandraAdminNoDiaObjectTestCase(ApdbCassandraAdminTimePartitionedTestCase):
    """A test case for ApdbCassandraAdmin with replica table and no DiaObject
    table.
    """

    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        return super().make_instance(enable_replica=True, replica_skips_diaobjects=True)


if __name__ == "__main__":
    unittest.main()
