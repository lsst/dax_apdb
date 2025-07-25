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

__all__ = ["Partitioner"]


import astropy.time

from lsst import sphgeom

from ..apdbSchema import ApdbTables
from ..pixelization import Pixelization
from .config import ApdbCassandraConfig, ApdbCassandraTimePartitionRange


class Partitioner:
    """Logic for temporal and spacial partitiong of APDB tables.

    Parameters
    ----------
    config : `ApdbCassandraConfig`
        Configuration object.
    """

    partition_zero_epoch = astropy.time.Time(0, format="unix_tai")
    """Start time for partition 0, this should never be changed."""

    def __init__(self, config: ApdbCassandraConfig):
        self._config = config
        self.pixelization = Pixelization(
            config.partitioning.part_pixelization,
            config.partitioning.part_pix_level,
            config.partitioning.part_pix_max_ranges,
        )
        self._epoch = float(self.partition_zero_epoch.mjd)

    def pixel(self, direction: sphgeom.UnitVector3d) -> int:
        """Compute the index of the pixel for given direction.

        Parameters
        ----------
        direction : `lsst.sphgeom.UnitVector3d`
            Spatial position.

        Returns
        -------
        pixel : `int`
            Pixel index.
        """
        return self.pixelization.pixel(direction)

    def time_partition(self, time: float | astropy.time.Time) -> int:
        """Calculate time partition number for a given time.

        Parameters
        ----------
        time : `float` or `astropy.time.Time`
            Time for which to calculate partition number. Can be float to mean
            MJD or `astropy.time.Time`

        Returns
        -------
        partition : `int`
            Partition number for a given time.
        """
        if isinstance(time, astropy.time.Time):
            mjd = float(time.mjd)
        else:
            mjd = time
        days_since_epoch = mjd - self._epoch
        partition = int(days_since_epoch) // self._config.partitioning.time_partition_days
        return partition

    def partition_period(self, time_partition: int) -> tuple[astropy.time.Time, astropy.time.Time]:
        """Return time period for specified taime partition.

        Parameters
        ----------
        time_partition : `int`
            Time partition.

        Returns
        -------
        start : `astropy.time.Time`
            Start of the period, inclusive boundary.
        end : `astropy.time.Time`
            Start of the period, exclusive boundary.
        """
        partition_days = self._config.partitioning.time_partition_days
        start_mjd = self._epoch + partition_days * time_partition
        end_mjd = self._epoch + partition_days * (time_partition + 1)
        start = astropy.time.Time(start_mjd, format="mjd", scale="tai")
        end = astropy.time.Time(end_mjd, format="mjd", scale="tai")
        return (start, end)

    def spatial_where(
        self, region: sphgeom.Region | None, *, use_ranges: bool = False, for_prepare: bool = False
    ) -> tuple[list[tuple[str, tuple]], int]:
        """Generate expressions for spatial part of WHERE clause.

        Parameters
        ----------
        region : `sphgeom.Region`
            Spatial region for query results.
        use_ranges : `bool`, optional
            If True then use pixel ranges ("apdb_part >= p1 AND apdb_part <=
            p2") instead of exact list of pixels. Should be set to True for
            large regions covering very many pixels.
        for_prepare : `bool`, optional
            If True then use placeholders for prepared statement (?), otherwise
            produce regulr statement placeholders (%s).

        Returns
        -------
        expressions : `list` [ `tuple` ]
            Empty list is returned if ``region`` is `None`, otherwise a list
            of one or more ``(expression: str, parameters: tuple)`` tuples.
        partition_count : `int`
            Number of spatial partitions in the result.
        """
        if region is None:
            return [], 0

        token = "?" if for_prepare else "%s"

        count = 0
        expressions: list[tuple[str, tuple]] = []
        if use_ranges:
            pixel_ranges = self.pixelization.envelope(region)
            for lower, upper in pixel_ranges:
                upper -= 1
                if lower == upper:
                    expressions.append((f'"apdb_part" = {token}', (lower,)))
                    count += 1
                elif lower + 1 == upper:
                    expressions.append((f'"apdb_part" = {token}', (lower,)))
                    expressions.append((f'"apdb_part" = {token}', (upper,)))
                    count += 2
                else:
                    count += upper - lower + 1
                    expressions.append((f'"apdb_part" >= {token} AND "apdb_part" <= {token}', (lower, upper)))
        else:
            pixels = self.pixelization.pixels(region)
            count = len(pixels)
            if self._config.partitioning.query_per_spatial_part:
                expressions.extend((f'"apdb_part" = {token}', (pixel,)) for pixel in pixels)
            else:
                pixels_str = ",".join([str(pix) for pix in pixels])
                expressions.append((f'"apdb_part" IN ({pixels_str})', ()))

        return expressions, count

    def temporal_where(
        self,
        table: ApdbTables,
        start_time: float | astropy.time.Time,
        end_time: float | astropy.time.Time,
        *,
        query_per_time_part: bool | None = None,
        for_prepare: bool = False,
        partitons_range: ApdbCassandraTimePartitionRange | None = None,
    ) -> tuple[list[str], list[tuple[str, tuple]]]:
        """Generate table names and expressions for temporal part of WHERE
        clauses.

        Parameters
        ----------
        table : `ApdbTables`
            Table to select from.
        start_time : `astropy.time.Time` or `float`
            Starting Datetime of MJD value of the time range.
        end_time : `astropy.time.Time` or `float`
            Starting Datetime of MJD value of the time range.
        query_per_time_part : `bool`, optional
            If None then use ``query_per_time_part`` from configuration.
        for_prepare : `bool`, optional
            If True then use placeholders for prepared statement (?), otherwise
            produce regulr statement placeholders (%s).
        partitons_range : `ApdbCassandraTimePartitionRange` or `None`
            Partitions range to further restrict time range.

        Returns
        -------
        tables : `list` [ `str` ]
            List of the table names to query. Empty list is returned when time
            range does not overlap ``partitons_range``.
        expressions : `list` [ `tuple` ]
            A list of zero or more ``(expression: str, parameters: tuple)``
            tuples.
        """
        tables: list[str]
        temporal_where: list[tuple[str, tuple]] = []
        # First and last partition.
        time_part_start = self.time_partition(start_time)
        time_part_end = self.time_partition(end_time)
        if partitons_range:
            # Check for non-overlapping ranges.
            if time_part_start > partitons_range.end or time_part_end < partitons_range.start:
                return [], []
            if time_part_start < partitons_range.start:
                time_part_start = partitons_range.start
            if time_part_end > partitons_range.end:
                time_part_end = partitons_range.end
        # Inclusive range.
        time_parts = list(range(time_part_start, time_part_end + 1))
        if self._config.partitioning.time_partition_tables:
            tables = [table.table_name(self._config.prefix, part) for part in time_parts]
        else:
            token = "?" if for_prepare else "%s"
            tables = [table.table_name(self._config.prefix)]
            if query_per_time_part is None:
                query_per_time_part = self._config.partitioning.query_per_time_part
            if query_per_time_part:
                temporal_where = [(f'"apdb_time_part" = {token}', (time_part,)) for time_part in time_parts]
            else:
                time_part_list = ",".join([str(part) for part in time_parts])
                temporal_where = [(f'"apdb_time_part" IN ({time_part_list})', ())]

        return tables, temporal_where
