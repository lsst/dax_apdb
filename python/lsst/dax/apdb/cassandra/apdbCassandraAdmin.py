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

__all__ = ["ApdbCassandraAdmin"]

import itertools
import logging
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING

from lsst.sphgeom import LonLat, UnitVector3d
from lsst.utils.iteration import chunk_iterable

from ..apdbAdmin import ApdbAdmin, DiaForcedSourceLocator, DiaObjectLocator, DiaSourceLocator
from ..monitor import MonAgent
from ..timer import Timer
from .cassandra_utils import execute_concurrent

if TYPE_CHECKING:
    from .apdbCassandra import ApdbCassandra

_LOG = logging.getLogger(__name__)

_MON = MonAgent(__name__)


class ApdbCassandraAdmin(ApdbAdmin):
    """Implementation of `ApdbAdmin` for Cassandra backend.

    Parameters
    ----------
    apdb : `ApdbCassandra`
        APDB implementation.
    """

    def __init__(self, apdb: ApdbCassandra):
        self._apdb = apdb

    def _timer(self, name: str, *, tags: Mapping[str, str | int] | None = None) -> Timer:
        """Create `Timer` instance given its name."""
        return Timer(name, _MON, _LOG, tags=tags)

    def apdb_part(self, ra: float, dec: float) -> int:
        # docstring is inherited from a base class
        context = self._apdb._context

        uv3d = UnitVector3d(LonLat.fromDegrees(ra, dec))
        return context.pixelization.pixel(uv3d)

    def apdb_time_part(self, midpointMjdTai: float) -> int:
        # docstring is inherited from a base class
        return self._apdb._time_partition(midpointMjdTai)

    def delete_records(
        self,
        objects: Iterable[DiaObjectLocator],
        sources: Iterable[DiaSourceLocator],
        forced_sources: Iterable[DiaForcedSourceLocator],
    ) -> None:
        # docstring is inherited from a base class
        context = self._apdb._context
        config = context.config
        keyspace = self._apdb._keyspace
        has_dia_object_table = not (config.enable_replica and config.replica_skips_diaobjects)

        # Group objects by partition.
        partitions = defaultdict(list)
        for object in objects:
            apdb_part = self.apdb_part(object.ra, object.dec)
            partitions[apdb_part].append(object.diaObjectId)
        object_ids = set(itertools.chain.from_iterable(partitions.values()))

        # Group sources by associated object ID.
        source_groups = defaultdict(list)
        for source in sources:
            if source.diaObjectId in object_ids:
                source_groups[source.diaObjectId].append(source)

        object_deletes = []
        object_count = 0
        # Delete from DiaObjectLast table.
        for apdb_part, oids in partitions.items():
            oids = sorted(oids)
            object_count += len(oids)
            for oid_chunk in chunk_iterable(oids, 1000):
                oids_str = ",".join(str(oid) for oid in oid_chunk)
                object_deletes.append(
                    (
                        f'DELETE FROM "{keyspace}"."DiaObjectLast" '
                        f'WHERE apdb_part = {apdb_part} and "diaObjectId" IN ({oids_str});',
                        (),
                    )
                )

                # If DiaObject is in use then delete from that too.
                if has_dia_object_table:
                    # Need temporal partitions for DiaObject, the only source
                    # for that is the timestamp of the associated DiaSource.
                    # Problem here is that DiaObject temporal partitioning is
                    # based on validityStart, which is "visit_time"", but
                    # DiaSource does not record visit_time, it is partitioned
                    # on midpointMjdTai. There is time_processed defiend for
                    # DiaSource but it does not match "visit_time" though it is
                    # close. I use midpointMjdTai as approximation for
                    # validityStart, this may skip some DiaObjects, but in
                    # production we are not going to have DiaObjects table at
                    # all. There is also a chance that DiaObject moves from one
                    # spatial partition to another with the same consequences,
                    # which we also ignore.
                    for oid in oid_chunk:
                        temporal_partitions = {
                            self.apdb_time_part(src.midpointMjdTai) for src in source_groups.get(oid, [])
                        }
                        if temporal_partitions:
                            apdb_time_partitions = ",".join(str(part) for part in temporal_partitions)
                            object_deletes.append(
                                (
                                    f'DELETE FROM "{keyspace}"."DiaObject" '
                                    f"WHERE apdb_part = {apdb_part} "
                                    f"AND apdb_time_part IN ({apdb_time_partitions}) "
                                    f'AND "diaObjectId" = {oid}',
                                    (),
                                )
                            )

        # Delete from DiaObjectLastToPartition table.
        for oid_chunk in chunk_iterable(sorted(object_ids), 1000):
            oids_str = ",".join(str(oid) for oid in oid_chunk)
            object_deletes.append(
                (
                    f'DELETE FROM "{keyspace}"."DiaObjectLastToPartition" '
                    f'WHERE "diaObjectId" IN ({oids_str})',
                    (),
                )
            )

        # Group sources by partition.
        source_partitions = defaultdict(list)
        for source in itertools.chain.from_iterable(source_groups.values()):
            apdb_part = self.apdb_part(source.ra, source.dec)
            apdb_time_part = self.apdb_time_part(source.midpointMjdTai)
            source_partitions[(apdb_part, apdb_time_part)].append(source)

        source_deletes = []
        source_count = 0
        for (apdb_part, apdb_time_part), source_list in source_partitions.items():
            source_ids = sorted(source.diaSourceId for source in source_list)
            source_count += len(source_ids)
            for id_chunk in chunk_iterable(source_ids, 1000):
                ids_str = ",".join(str(id) for id in id_chunk)
                if config.partitioning.time_partition_tables:
                    source_deletes.append(
                        (
                            f'DELETE FROM "{keyspace}"."DiaSource_{apdb_time_part}" '
                            f'WHERE apdb_part = {apdb_part} and "diaSourceId" IN ({ids_str})',
                            (),
                        )
                    )
                else:
                    source_deletes.append(
                        (
                            f'DELETE FROM "{keyspace}"."DiaSource" '
                            f"WHERE apdb_part = {apdb_part} AND apdb_time_part = {apdb_time_part} "
                            f'AND "diaSourceId" IN ({ids_str})',
                            (),
                        )
                    )

        # Group forced sources by partition.
        forced_source_partitions = defaultdict(list)
        for forced_source in forced_sources:
            if forced_source.diaObjectId in object_ids:
                apdb_part = self.apdb_part(forced_source.ra, forced_source.dec)
                apdb_time_part = self.apdb_time_part(forced_source.midpointMjdTai)
                forced_source_partitions[(apdb_part, apdb_time_part)].append(forced_source)

        forced_source_deletes = []
        forced_source_count = 0
        for (apdb_part, apdb_time_part), forced_source_list in forced_source_partitions.items():
            clustering_keys = sorted(
                (fsource.diaObjectId, fsource.visit, fsource.detector) for fsource in forced_source_list
            )
            forced_source_count += len(clustering_keys)
            for key_chunk in chunk_iterable(clustering_keys, 1000):
                cl_str = ",".join(f"({oid}, {v}, {d})" for oid, v, d in key_chunk)
                if config.partitioning.time_partition_tables:
                    forced_source_deletes.append(
                        (
                            f'DELETE FROM "{keyspace}"."DiaForcedSource_{apdb_time_part}" '
                            f"WHERE apdb_part = {apdb_part}"
                            f'AND ("diaObjectId", visit, detector) IN ({cl_str})',
                            (),
                        )
                    )
                else:
                    forced_source_deletes.append(
                        (
                            f'DELETE FROM "{keyspace}"."DiaForcedSource" '
                            f"WHERE apdb_part = {apdb_part} "
                            f"AND apdb_time_part = {apdb_time_part} "
                            f'AND ("diaObjectId", visit, detector) IN ({cl_str})',
                            (),
                        )
                    )

        _LOG.info(
            "Deleting %d objects, %d sources, and %d forced sources",
            object_count,
            source_count,
            forced_source_count,
        )

        # Now run all queries.
        with self._timer("delete_forced_sources"):
            execute_concurrent(context.session, forced_source_deletes)
        with self._timer("delete_sources"):
            execute_concurrent(context.session, source_deletes)
        with self._timer("delete_objects"):
            execute_concurrent(context.session, object_deletes)
