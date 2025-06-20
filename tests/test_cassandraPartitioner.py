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
from typing import Any

from astropy.time import Time

from lsst.dax.apdb import ApdbTables
from lsst.dax.apdb.cassandra import ApdbCassandraConfig, ApdbCassandraPartitioningConfig
from lsst.dax.apdb.cassandra.partitioner import Partitioner
from lsst.sphgeom import Box, UnitVector3d


class CassandraPartitionerTestCase(unittest.TestCase):
    """A test case for ApdbCassandra class"""

    def make_partitioner(self, **kwargs: Any) -> Partitioner:
        """Make Partitioner instance, keyword arguments are passed to
        ApdbCassandraPartitioningConfig.
        """
        params = {
            "part_pixelization": "mq3c",
            "part_pix_level": 10,
            "query_per_spatial_part": False,
        }
        params.update(kwargs)

        partitioning = ApdbCassandraPartitioningConfig(**params)  # type: ignore[arg-type]
        config = ApdbCassandraConfig(partitioning=partitioning)
        return Partitioner(config)

    def test_pixel(self) -> None:
        """Test pixel() method."""
        partitioner = self.make_partitioner()

        self.assertEqual(partitioner.pixel(UnitVector3d(1.0, 1.0, 1.0)), 0xD00000)
        self.assertEqual(partitioner.pixel(UnitVector3d(-1.0, 1.0, 1.0)), 0xE00000)
        self.assertEqual(partitioner.pixel(UnitVector3d(1.0, -1.0, 1.0)), 0xF55555)
        self.assertEqual(partitioner.pixel(UnitVector3d(1.0, 1.0, -1.0)), 0xAAAAAA)
        self.assertEqual(partitioner.pixel(UnitVector3d(-1.0, -1.0, 1.0)), 0xEFFFFF)
        self.assertEqual(partitioner.pixel(UnitVector3d(1.0, -1.0, -1.0)), 0xFAAAAA)

    def test_time_partition(self) -> None:
        """Test time_partition() method."""
        partitioner = self.make_partitioner()

        astrotime = Time("2025-06-01T00:00:00", format="isot", scale="tai")
        self.assertEqual(partitioner.time_partition(astrotime), 674)
        astrotime = Time("2025-07-01T00:00:00", format="isot", scale="tai")
        self.assertEqual(partitioner.time_partition(astrotime), 675)

        self.assertEqual(partitioner.time_partition(60827.0), 674)
        self.assertEqual(partitioner.time_partition(60857.0), 675)

    def test_spatial_where(self) -> None:
        """Test spatial_where() method."""
        region = Box.fromDegrees(-0.05, 0.05, 0.05, 0.15)

        partitioner = self.make_partitioner()
        result = partitioner.spatial_where(region)
        self.assertEqual(result, [('"apdb_part" IN (12058622,12058623,12058624,12058625)', ())])
        result = partitioner.spatial_where(region, use_ranges=True)
        self.assertEqual(result, [('"apdb_part" >= %s AND "apdb_part" <= %s', (12058622, 12058625))])

        partitioner = self.make_partitioner(query_per_spatial_part=True)
        result = partitioner.spatial_where(region)
        self.assertEqual(
            set(result),
            {
                ('"apdb_part" = %s', (12058622,)),
                ('"apdb_part" = %s', (12058623,)),
                ('"apdb_part" = %s', (12058624,)),
                ('"apdb_part" = %s', (12058625,)),
            },
        )
        result = partitioner.spatial_where(region, for_prepare=True)
        self.assertEqual(
            set(result),
            {
                ('"apdb_part" = ?', (12058622,)),
                ('"apdb_part" = ?', (12058623,)),
                ('"apdb_part" = ?', (12058624,)),
                ('"apdb_part" = ?', (12058625,)),
            },
        )
        result = partitioner.spatial_where(region, use_ranges=True, for_prepare=True)
        self.assertEqual(result, [('"apdb_part" >= ? AND "apdb_part" <= ?', (12058622, 12058625))])

    def test_temporal_where(self) -> None:
        """Test temporal_where() method."""
        start_time = Time("2025-01-01T00:00:00", format="isot", scale="tai")
        end_time = Time("2025-06-01T00:00:00", format="isot", scale="tai")

        partitioner = self.make_partitioner()

        tables, where = partitioner.temporal_where(ApdbTables.DiaSource, start_time, end_time)
        self.assertEqual(tables, ["DiaSource"])
        self.assertEqual(where, [('"apdb_time_part" IN (669,670,671,672,673,674)', ())])

        tables, where = partitioner.temporal_where(
            ApdbTables.DiaSource, start_time, end_time, query_per_time_part=True
        )
        self.assertEqual(tables, ["DiaSource"])
        self.assertEqual(
            where,
            [
                ('"apdb_time_part" = %s', (669,)),
                ('"apdb_time_part" = %s', (670,)),
                ('"apdb_time_part" = %s', (671,)),
                ('"apdb_time_part" = %s', (672,)),
                ('"apdb_time_part" = %s', (673,)),
                ('"apdb_time_part" = %s', (674,)),
            ],
        )

        tables, where = partitioner.temporal_where(
            ApdbTables.DiaSource,
            start_time,
            end_time,
            query_per_time_part=True,
            for_prepare=True,
        )
        self.assertEqual(tables, ["DiaSource"])
        self.assertEqual(
            where,
            [
                ('"apdb_time_part" = ?', (669,)),
                ('"apdb_time_part" = ?', (670,)),
                ('"apdb_time_part" = ?', (671,)),
                ('"apdb_time_part" = ?', (672,)),
                ('"apdb_time_part" = ?', (673,)),
                ('"apdb_time_part" = ?', (674,)),
            ],
        )

        partitioner = self.make_partitioner(query_per_time_part=True)

        tables, where = partitioner.temporal_where(ApdbTables.DiaSource, start_time, end_time)
        self.assertEqual(tables, ["DiaSource"])
        self.assertEqual(
            where,
            [
                ('"apdb_time_part" = %s', (669,)),
                ('"apdb_time_part" = %s', (670,)),
                ('"apdb_time_part" = %s', (671,)),
                ('"apdb_time_part" = %s', (672,)),
                ('"apdb_time_part" = %s', (673,)),
                ('"apdb_time_part" = %s', (674,)),
            ],
        )

        partitioner = self.make_partitioner(time_partition_tables=True)
        tables, where = partitioner.temporal_where(ApdbTables.DiaSource, start_time, end_time)
        self.assertEqual(
            tables,
            [
                "DiaSource_669",
                "DiaSource_670",
                "DiaSource_671",
                "DiaSource_672",
                "DiaSource_673",
                "DiaSource_674",
            ],
        )
        self.assertEqual(where, [])


if __name__ == "__main__":
    unittest.main()
