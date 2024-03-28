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

import os
import shutil
import tempfile
import unittest

from lsst.dax.apdb.apdbIndex import ApdbIndex
from lsst.resources import ResourcePath

VALID_INDEX = """\
dev: "/path/to/config-file.yaml"
"prod/pex_config": "s3://bucket/apdb-prod.py"
"prod/yaml": "s3://bucket/apdb-prod.yaml"
"""

BAD_INDEX = """\
dev: ["/path/to/config-file.yaml"]
prod:
    pex_config: "s3://bucket/apdb-prod.py"
    yaml: "s3://bucket/apdb-prod.yaml"
"""


class ApdbIndexTestCase(unittest.TestCase):
    """A test case for ApdbIndex class."""

    def setUp(self) -> None:
        self.tempdir = tempfile.mkdtemp()
        self.valid_index_path = os.path.join(self.tempdir, "apdb-index.yaml")
        with open(self.valid_index_path, "w") as index_file:
            index_file.write(VALID_INDEX)
        self.bad_index_path = os.path.join(self.tempdir, "bad-index.yaml")
        with open(self.bad_index_path, "w") as index_file:
            index_file.write(BAD_INDEX)
        self.missing_index_path = os.path.join(self.tempdir, "missing-index.yaml")

    def tearDown(self) -> None:
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_get_apdb_uri(self) -> None:
        """Check get_apdb_uri against existing valid file."""
        index = ApdbIndex(self.valid_index_path)
        self.assertEqual(index.get_apdb_uri("dev"), ResourcePath("/path/to/config-file.yaml"))
        self.assertEqual(index.get_apdb_uri("dev", "yaml"), ResourcePath("/path/to/config-file.yaml"))
        self.assertEqual(index.get_apdb_uri("dev", "anything"), ResourcePath("/path/to/config-file.yaml"))
        self.assertEqual(index.get_apdb_uri("prod/yaml"), ResourcePath("s3://bucket/apdb-prod.yaml"))
        self.assertEqual(index.get_apdb_uri("prod/pex_config"), ResourcePath("s3://bucket/apdb-prod.py"))
        with self.assertRaises(KeyError):
            index.get_apdb_uri("prod")
        with self.assertRaises(KeyError):
            index.get_apdb_uri("prod", "anything")
        with self.assertRaises(KeyError):
            index.get_apdb_uri("anything")

    def test_missing_config(self) -> None:
        """Check get_apdb_uri when index file is missing."""
        index = ApdbIndex(self.missing_index_path)
        with self.assertRaises(FileNotFoundError):
            index.get_apdb_uri("prod")

    def test_bad_config(self) -> None:
        """Check get_apdb_uri when badly formatted YAML file."""
        index = ApdbIndex(self.bad_index_path)
        with self.assertRaises(TypeError):
            index.get_apdb_uri("prod")

    def test_get_known_labels(self) -> None:
        """Check get_known_labels."""
        index = ApdbIndex(self.valid_index_path)
        self.assertEqual(index.get_known_labels(), {"dev", "prod/yaml", "prod/pex_config"})

        index = ApdbIndex(self.bad_index_path)
        self.assertEqual(index.get_known_labels(), set())

        index = ApdbIndex(self.missing_index_path)
        self.assertEqual(index.get_known_labels(), set())

    def test_get_entries(self) -> None:
        """Check get_entries."""
        index = ApdbIndex(self.valid_index_path)
        self.assertEqual(
            index.get_entries(),
            {
                "dev": "/path/to/config-file.yaml",
                "prod/pex_config": "s3://bucket/apdb-prod.py",
                "prod/yaml": "s3://bucket/apdb-prod.yaml",
            },
        )

        index = ApdbIndex(self.bad_index_path)
        self.assertEqual(index.get_entries(), {})

        index = ApdbIndex(self.missing_index_path)
        self.assertEqual(index.get_entries(), {})


if __name__ == "__main__":
    unittest.main()
