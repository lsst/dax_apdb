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

"""Unit test for apdb-cli commands."""

import os
import shutil
import tempfile
import unittest

from lsst.dax.apdb import Apdb
from lsst.dax.apdb.cli import cli

TEST_SCHEMA = os.path.join(os.path.abspath(os.path.dirname(__file__)), "config/schema.yaml")


class CreateSqlTestCase(unittest.TestCase):
    """A test case for apdb-cli create-sql command."""

    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.db_url = f"sqlite:///{self.tempdir}/apdb.sqlite3"
        self.config_path = f"{self.tempdir}/apdb-sql.py"

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_create_sql(self) -> None:
        """Create SQLite APDB instance and config file for it."""
        args = ["create-sql", "--schema-file", TEST_SCHEMA, self.db_url, self.config_path]
        cli.cli(args)
        self.assertTrue(os.path.exists(f"{self.tempdir}/apdb.sqlite3"))
        self.assertTrue(os.path.exists(self.config_path))

        # Make Apdb instance from this new config.
        Apdb.from_uri(self.config_path)


if __name__ == "__main__":
    unittest.main()
