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

from __future__ import annotations

__all__ = ["ApdbCassandraMixin"]

import os
import unittest
import uuid

try:
    import cassandra  # noqa: F401

    CASSANDRA_IMPORTED = True
except ImportError:
    CASSANDRA_IMPORTED = False

from ..cassandra.apdbCassandraAdmin import ApdbCassandraAdmin
from .utils import TestCaseMixin


class ApdbCassandraMixin(TestCaseMixin):
    """Mixin class which defines common methods for unit tests."""

    cluster_host: str

    @classmethod
    def setUpClass(cls) -> None:
        """Prepare config for server connection."""
        if not CASSANDRA_IMPORTED:
            raise unittest.SkipTest("Failed to import Cassandra modules")
        cluster_host = os.environ.get("DAX_APDB_TEST_CASSANDRA_CLUSTER")
        if not cluster_host:
            raise unittest.SkipTest("DAX_APDB_TEST_CASSANDRA_CLUSTER is not set")
        if not CASSANDRA_IMPORTED:
            raise unittest.SkipTest("cassandra_driver cannot be imported")

    def setUp(self) -> None:
        """Prepare config for server connection."""
        cluster_host = os.environ.get("DAX_APDB_TEST_CASSANDRA_CLUSTER")
        assert cluster_host is not None
        self.cluster_host = cluster_host
        # Use dedicated keyspace for each test, keyspace is created by
        # init_database if it does not exist.
        key = uuid.uuid4()
        self.keyspace = f"apdb_{key.hex}"

    def tearDown(self) -> None:
        # Delete per-test keyspace.
        assert self.cluster_host is not None
        ApdbCassandraAdmin.delete_database(self.cluster_host, self.keyspace)
