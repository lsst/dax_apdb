# This file is part of dax_ppdb
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ["partition_extend_temporal", "partition_show_temporal"]

import sys

import astropy.time

from ..apdb import Apdb
from ..cassandra import ApdbCassandra


def partition_show_temporal(apdb_config: str) -> int:
    """Print range of temporal partitions.

    Parameters
    ----------
    apdb_config : `str`
        URL for APDB configuration file.
    """
    apdb = Apdb.from_uri(apdb_config)
    if not isinstance(apdb, ApdbCassandra):
        print("ERROR: Non-Cassandra APDB does not use time-partitioned tables.", file=sys.stderr)
        return 1

    admin = apdb.admin
    try:
        part_range = admin.time_partitions()
    except TypeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    start_time, _ = admin.partitioner.partition_period(part_range.start)
    _, end_time = admin.partitioner.partition_period(part_range.end)

    print(
        f"Current time partition range: {part_range.start} - {part_range.end} "
        f"[{start_time.tai.isot}, {end_time.tai.isot})"
    )

    return 0


def partition_extend_temporal(apdb_config: str, time: str, past: bool, max_days: int) -> int:
    """Extend the range of temporal partitions.

    Parameters
    ----------
    apdb_config : `str`
        URL for APDB configuration file.
    time : `str`
        Timestamps in ISOT format and TAI scale.
    past : `bool`
        If `True` extend the range in the past.
    max_days : `int`
        Max. number of days for extension.
    """
    try:
        astro_time = astropy.time.Time(time, format="isot", scale="tai")
        max_delta = astropy.time.TimeDelta(float(max_days), format="jd", scale="tai")
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    apdb = Apdb.from_uri(apdb_config)
    if not isinstance(apdb, ApdbCassandra):
        print("ERROR: Non-Cassandra APDB does not use time-partitioned tables.", file=sys.stderr)
        return 1

    admin = apdb.admin
    try:
        result = admin.extend_time_partitions(astro_time, forward=not past, max_delta=max_delta)
    except (TypeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    part_range = admin.time_partitions()
    start_time, _ = admin.partitioner.partition_period(part_range.start)
    _, end_time = admin.partitioner.partition_period(part_range.end)

    if result:
        print(
            "Time partition range succesfully extended.\n"
            f"New time partition range: {part_range.start} - {part_range.end} "
            f"[{start_time.tai.isot}, {end_time.tai.isot})"
        )
    else:
        print(
            "Time partition range was not extended.\n"
            f"Current time partition range: {part_range.start} - {part_range.end} "
            f"[{start_time.tai.isot}, {end_time.tai.isot})"
        )

    return 0
