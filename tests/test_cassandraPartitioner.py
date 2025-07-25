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
from lsst.dax.apdb.cassandra import (
    ApdbCassandraConfig,
    ApdbCassandraPartitioningConfig,
    ApdbCassandraTimePartitionRange,
)
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

    def test_partition_period(self) -> None:
        """Test partition_period() method."""
        partitioner = self.make_partitioner()

        self.assertEqual(
            partitioner.partition_period(674),
            (
                Time("2025-05-12T00:00:00", format="isot", scale="tai"),
                Time("2025-06-11T00:00:00", format="isot", scale="tai"),
            ),
        )
        self.assertEqual(
            partitioner.partition_period(800),
            (
                Time("2035-09-17T00:00:00", format="isot", scale="tai"),
                Time("2035-10-17T00:00:00", format="isot", scale="tai"),
            ),
        )

    def test_spatial_where(self) -> None:
        """Test spatial_where() method."""
        region = Box.fromDegrees(-0.05, 0.05, 0.05, 0.15)

        partitioner = self.make_partitioner()
        result, count = partitioner.spatial_where(region)
        self.assertEqual(count, 4)
        self.assertEqual(result, [('"apdb_part" IN (12058622,12058623,12058624,12058625)', ())])
        result, count = partitioner.spatial_where(region, use_ranges=True)
        self.assertEqual(count, 4)
        self.assertEqual(result, [('"apdb_part" >= %s AND "apdb_part" <= %s', (12058622, 12058625))])

        partitioner = self.make_partitioner(query_per_spatial_part=True)
        result, count = partitioner.spatial_where(region)
        self.assertEqual(count, 4)
        self.assertEqual(
            set(result),
            {
                ('"apdb_part" = %s', (12058622,)),
                ('"apdb_part" = %s', (12058623,)),
                ('"apdb_part" = %s', (12058624,)),
                ('"apdb_part" = %s', (12058625,)),
            },
        )
        result, count = partitioner.spatial_where(region, for_prepare=True)
        self.assertEqual(count, 4)
        self.assertEqual(
            set(result),
            {
                ('"apdb_part" = ?', (12058622,)),
                ('"apdb_part" = ?', (12058623,)),
                ('"apdb_part" = ?', (12058624,)),
                ('"apdb_part" = ?', (12058625,)),
            },
        )
        result, count = partitioner.spatial_where(region, use_ranges=True, for_prepare=True)
        self.assertEqual(count, 4)
        self.assertEqual(result, [('"apdb_part" >= ? AND "apdb_part" <= ?', (12058622, 12058625))])

    def _check_temporal_where(
        self,
        tables: list[str],
        where: list[tuple],
        part_start: int,
        part_end: int,
        *,
        time_partition_tables: bool = False,
        query_per_time_part: bool = False,
        for_prepare: bool = False,
    ) -> None:
        if part_start > part_end:
            self.assertEqual(tables, [])
            self.assertEqual(where, [])
        elif time_partition_tables:
            expect_tables = [f"DiaSource_{part}" for part in range(part_start, part_end + 1)]
            self.assertEqual(tables, expect_tables)
            self.assertEqual(where, [])
        elif query_per_time_part:
            where_str = '"apdb_time_part" = ?' if for_prepare else '"apdb_time_part" = %s'
            expect_where = [(where_str, (part,)) for part in range(part_start, part_end + 1)]
            self.assertEqual(tables, ["DiaSource"])
            self.assertEqual(where, expect_where)
        else:
            parts_str = ",".join(str(part) for part in range(part_start, part_end + 1))
            self.assertEqual(tables, ["DiaSource"])
            self.assertEqual(where, [(f'"apdb_time_part" IN ({parts_str})', ())])

    def test_temporal_where(self) -> None:
        """Test temporal_where() method."""
        start_time = Time("2025-01-01T00:00:00", format="isot", scale="tai")
        end_time = Time("2025-06-01T00:00:00", format="isot", scale="tai")

        partitioner = self.make_partitioner()

        tables, where = partitioner.temporal_where(ApdbTables.DiaSource, start_time, end_time)
        self._check_temporal_where(tables, where, 669, 674)

        tables, where = partitioner.temporal_where(
            ApdbTables.DiaSource, start_time, end_time, query_per_time_part=True
        )
        self._check_temporal_where(tables, where, 669, 674, query_per_time_part=True)

        tables, where = partitioner.temporal_where(
            ApdbTables.DiaSource,
            start_time,
            end_time,
            query_per_time_part=True,
            for_prepare=True,
        )
        self._check_temporal_where(tables, where, 669, 674, query_per_time_part=True, for_prepare=True)

        partitioner = self.make_partitioner(query_per_time_part=True)

        tables, where = partitioner.temporal_where(ApdbTables.DiaSource, start_time, end_time)
        self._check_temporal_where(tables, where, 669, 674, query_per_time_part=True)

        partitioner = self.make_partitioner(time_partition_tables=True)
        tables, where = partitioner.temporal_where(ApdbTables.DiaSource, start_time, end_time)
        self._check_temporal_where(tables, where, 669, 674, time_partition_tables=True)

        # Check additional partition range constraint.
        ranges = [
            ((0, 1000), (669, 674)),
            ((0, 1), (0, -1)),
            ((600, 670), (669, 670)),
            ((670, 770), (670, 674)),
            ((671, 672), (671, 672)),
        ]

        partitioner = self.make_partitioner()
        for (range_start, range_end), (result_start, result_end) in ranges:
            part_range = ApdbCassandraTimePartitionRange(start=range_start, end=range_end)
            tables, where = partitioner.temporal_where(
                ApdbTables.DiaSource, start_time, end_time, partitons_range=part_range
            )
            self._check_temporal_where(tables, where, result_start, result_end)

        partitioner = self.make_partitioner(time_partition_tables=True)
        for (range_start, range_end), (result_start, result_end) in ranges:
            part_range = ApdbCassandraTimePartitionRange(start=range_start, end=range_end)
            tables, where = partitioner.temporal_where(
                ApdbTables.DiaSource, start_time, end_time, partitons_range=part_range
            )
            self._check_temporal_where(tables, where, result_start, result_end, time_partition_tables=True)


if __name__ == "__main__":
    unittest.main()
