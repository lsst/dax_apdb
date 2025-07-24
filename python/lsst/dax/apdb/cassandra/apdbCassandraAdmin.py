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

import dataclasses
import itertools
import logging
import warnings
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Protocol

import astropy.time

from lsst.sphgeom import LonLat, UnitVector3d
from lsst.utils.iteration import chunk_iterable

try:
    import cassandra
except ImportError:
    pass

from ..apdbAdmin import ApdbAdmin, DiaForcedSourceLocator, DiaObjectLocator, DiaSourceLocator
from ..apdbSchema import ApdbTables
from ..monitor import MonAgent
from ..timer import Timer
from .cassandra_utils import execute_concurrent, quote_id
from .config import ApdbCassandraConfig, ApdbCassandraTimePartitionRange
from .sessionFactory import SessionContext

if TYPE_CHECKING:
    from .apdbCassandra import ApdbCassandra
    from .partitioner import Partitioner

_LOG = logging.getLogger(__name__)

_MON = MonAgent(__name__)


class ConfirmDeletePartitions(Protocol):
    """Protocol for callable which confirms deletion of partitions."""

    def __call__(self, *, partitions: list[int], tables: list[str], partitioner: Partitioner) -> bool: ...


@dataclasses.dataclass
class DatabaseInfo:
    """Collection of information about a specific database."""

    name: str
    """Keyspace name."""

    permissions: dict[str, set[str]] | None = None
    """Roles that can access the database and their permissions.

    `None` means that authentication information is not accessible due to
    system table permissions. If anonymous access is enabled then dictionary
    will be empty but not `None`.
    """


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

    @classmethod
    def list_databases(cls, host: str) -> Iterable[DatabaseInfo]:
        """Return the list of keyspaces with APDB databases.

        Parameters
        ----------
        host : `str`
            Name of one of the hosts in Cassandra cluster.

        Returns
        -------
        databases : `~collections.abc.Iterable` [`DatabaseInfo`]
            Information about databases that contain APDB instance.
        """
        # For DbAuth we need to use database name "*" to try to match any
        # database.
        config = ApdbCassandraConfig(contact_points=(host,), keyspace="*")
        with SessionContext(config) as session:
            # Get names of all keyspaces containing DiaSource table
            table_name = ApdbTables.DiaSource.table_name()
            query = "select keyspace_name from system_schema.tables where table_name = %s ALLOW FILTERING"
            result = session.execute(query, (table_name,))
            keyspaces = [row[0] for row in result.all()]

            if not keyspaces:
                return []

            # Retrieve roles for each keyspace.
            template = ", ".join(["%s"] * len(keyspaces))
            query = (
                "SELECT resource, role, permissions FROM system_auth.role_permissions "
                f"WHERE resource IN ({template}) ALLOW FILTERING"
            )
            resources = [f"data/{keyspace}" for keyspace in keyspaces]
            try:
                result = session.execute(query, resources)
                # If anonymous access is enabled then result will be empty,
                # set infos to have empty permissions dict in that case.
                infos = {keyspace: DatabaseInfo(name=keyspace, permissions={}) for keyspace in keyspaces}
                for row in result:
                    _, _, keyspace = row[0].partition("/")
                    role: str = row[1]
                    role_permissions: set[str] = set(row[2])
                    infos[keyspace].permissions[role] = role_permissions  # type: ignore[index]
            except cassandra.Unauthorized as exc:
                # Likely that access to role_permissions is not granted for
                # current user.
                warnings.warn(
                    f"Authentication information is not accessible to current user - {exc}", stacklevel=2
                )
                infos = {keyspace: DatabaseInfo(name=keyspace) for keyspace in keyspaces}

            # Would be nice to get size estimate, but this is not available
            # via CQL queries.
            return infos.values()

    @classmethod
    def delete_database(cls, host: str, keyspace: str, *, timeout: int = 3600) -> None:
        """Delete APDB database by dropping its keyspace.

        Parameters
        ----------
        host : `str`
            Name of one of the hosts in Cassandra cluster.
        keyspace : `str`
            Name of keyspace to delete.
        timeout : `int`, optional
            Timeout for delete operation in seconds. Dropping a large keyspace
            can be a long operation, but this default value of one hour should
            be sufficient for most or all cases.
        """
        # For DbAuth we need to use database name "*" to try to match any
        # database.
        config = ApdbCassandraConfig(contact_points=(host,), keyspace="*")
        with SessionContext(config) as session:
            query = f"DROP KEYSPACE {quote_id(keyspace)}"
            session.execute(query, timeout=timeout)

    @property
    def partitioner(self) -> Partitioner:
        """Partitoner used by this APDB instance (`Partitioner`)."""
        context = self._apdb._context
        return context.partitioner

    def apdb_part(self, ra: float, dec: float) -> int:
        # docstring is inherited from a base class
        uv3d = UnitVector3d(LonLat.fromDegrees(ra, dec))
        return self.partitioner.pixel(uv3d)

    def apdb_time_part(self, midpointMjdTai: float) -> int:
        # docstring is inherited from a base class
        return self.partitioner.time_partition(midpointMjdTai)

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
            # Need temporal partitions for DiaObject, the only source for that
            # is the timestamp of the associated DiaSource. Problem here is
            # that DiaObject temporal partitioning is based on validityStart,
            # which is "visit_time"", but DiaSource does not record visit_time,
            # it is partitioned on midpointMjdTai. There is time_processed
            # defined for DiaSource but it does not match "visit_time" though
            # it is close. I use midpointMjdTai as approximation for
            # validityStart, this may skip some DiaObjects, but in production
            # we are not going to have DiaObjects table at all. There is also
            # a chance that DiaObject moves from one spatial partition to
            # another with the same consequences, which we also ignore.
            oids_by_partition: dict[tuple[int, int], list[int]] = defaultdict(list)
            for apdb_part, oids in partitions.items():
                for oid in oids:
                    temporal_partitions = {
                        self.apdb_time_part(src.midpointMjdTai) for src in source_groups.get(oid, [])
                    }
                    for time_part in temporal_partitions:
                        oids_by_partition[(apdb_part, time_part)].append(oid)
            for (apdb_part, time_part), oids in oids_by_partition.items():
                for oid_chunk in chunk_iterable(oids, 1000):
                    oids_str = ",".join(str(oid) for oid in oid_chunk)
                    if config.partitioning.time_partition_tables:
                        table_name = context.schema.tableName(ApdbTables.DiaObject, time_part)
                        object_deletes.append(
                            (
                                f'DELETE FROM "{keyspace}"."{table_name}" '
                                f'WHERE apdb_part = {apdb_part} AND "diaObjectId" IN ({oids_str})',
                                (),
                            )
                        )
                    else:
                        table_name = context.schema.tableName(ApdbTables.DiaObject)
                        object_deletes.append(
                            (
                                f'DELETE FROM "{keyspace}"."{table_name}" '
                                f"WHERE apdb_part = {apdb_part} AND apdb_time_part = {time_part} "
                                f'AND "diaObjectId" IN ({oids_str})',
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
                    table_name = context.schema.tableName(ApdbTables.DiaSource, apdb_time_part)
                    source_deletes.append(
                        (
                            f'DELETE FROM "{keyspace}"."{table_name}" '
                            f'WHERE apdb_part = {apdb_part} and "diaSourceId" IN ({ids_str})',
                            (),
                        )
                    )
                else:
                    table_name = context.schema.tableName(ApdbTables.DiaSource)
                    source_deletes.append(
                        (
                            f'DELETE FROM "{keyspace}"."{table_name}" '
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
                    table_name = context.schema.tableName(ApdbTables.DiaForcedSource, apdb_time_part)
                    forced_source_deletes.append(
                        (
                            f'DELETE FROM "{keyspace}"."{table_name}" '
                            f"WHERE apdb_part = {apdb_part}"
                            f'AND ("diaObjectId", visit, detector) IN ({cl_str})',
                            (),
                        )
                    )
                else:
                    table_name = context.schema.tableName(ApdbTables.DiaForcedSource)
                    forced_source_deletes.append(
                        (
                            f'DELETE FROM "{keyspace}"."{table_name}" '
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

    def time_partitions(self) -> ApdbCassandraTimePartitionRange:
        """Return range of existing time partitions.

        Returns
        -------
        range : `ApdbCassandraTimePartitionRange`
            Time partition range.

        Raises
        ------
        TypeError
            Raised if APDB instance does not use time-partition tables.
        """
        context = self._apdb._context
        part_range = context.time_partitions_range
        if not part_range:
            raise TypeError("This APDB instance does not use time-partitioned tables.")
        return part_range

    def extend_time_partitions(
        self,
        time: astropy.time.Time,
        forward: bool = True,
        max_delta: astropy.time.TimeDelta | None = None,
    ) -> list[int]:
        """Extend set of time-partitioned tables to include specified time.

        Parameters
        ----------
        time : `astropy.time.Time`
            Time to which to extend partitions.
        forward : `bool`, optional
            If `True` then extend partitions into the future, time should be
            later than the end time of the last existing partition. If `False`
            then extend partitions into the past, time should be earlier than
            the start time of the first existing partition.
        max_delta : `astropy.time.TimeDelta`, optional
            Maximum possible extension of the aprtitions, default is 365 days.

        Returns
        -------
        partitions : `list` [`int`]
            List of partitons added to the database, empty list returned if
            ``time`` is already in the existing partition range.

        Raises
        ------
        TypeError
            Raised if APDB instance does not use time-partition tables.
        ValueError
            Raised if extension request exceeds time limit of ``max_delta``.
        """
        if max_delta is None:
            max_delta = astropy.time.TimeDelta(365, format="jd")

        context = self._apdb._context

        # Get current partitions.
        part_range = context.time_partitions_range
        if not part_range:
            raise TypeError("This APDB instance does not use time-partitioned tables.")

        # Partitions that we need to create.
        partitions = self._partitions_to_add(time, forward, max_delta)
        if not partitions:
            return []

        _LOG.debug("New partitions to create: %s", partitions)

        # Tables that are time-partitioned.
        keyspace = self._apdb._keyspace
        tables = context.schema.time_partitioned_tables()

        # Easiest way to create new tables is to take DDL from existing one
        # and update table name.
        table_name_token = "%TABLE_NAME%"
        table_schemas = {}
        for table in tables:
            existing_table_name = context.schema.tableName(table, part_range.end)
            query = f'DESCRIBE TABLE "{keyspace}"."{existing_table_name}"'
            result = context.session.execute(query).one()
            if not result:
                raise LookupError(f'Failed to read schema for table "{keyspace}"."{existing_table_name}"')
            schema: str = result.create_statement
            schema = schema.replace(existing_table_name, table_name_token)
            table_schemas[table] = schema

        # Be paranoid and check that none of the new tables exist.
        exsisting_tables = context.schema.existing_tables(*tables)
        for table in tables:
            new_tables = {context.schema.tableName(table, partition) for partition in partitions}
            old_tables = new_tables.intersection(exsisting_tables[table])
            if old_tables:
                raise ValueError(f"Some to be created tables already exist: {old_tables}")

        # Now can create all of them.
        for table, schema in table_schemas.items():
            for partition in partitions:
                new_table_name = context.schema.tableName(table, partition)
                _LOG.debug("Creating table %s", new_table_name)
                new_ddl = schema.replace(table_name_token, new_table_name)
                context.session.execute(new_ddl)

        # Update metadata.
        if context.has_time_partition_meta:
            if forward:
                part_range.end = max(partitions)
            else:
                part_range.start = min(partitions)
            part_range.save_to_meta(context.metadata)

        return partitions

    def _partitions_to_add(
        self,
        time: astropy.time.Time,
        forward: bool,
        max_delta: astropy.time.TimeDelta,
    ) -> list[int]:
        """Make the list of time partitions to add to current range."""
        context = self._apdb._context
        part_range = context.time_partitions_range
        assert part_range is not None

        new_partition = context.partitioner.time_partition(time)
        if forward:
            if new_partition <= part_range.end:
                _LOG.debug(
                    "Partition for time=%s (%d) is below existing end (%d)",
                    time,
                    new_partition,
                    part_range.end,
                )
                return []
            _, end = context.partitioner.partition_period(part_range.end)
            if time - end > max_delta:
                raise ValueError(
                    f"Extension exceeds limit: current end time = {end.isot}, new end time = {time.isot}, "
                    f"limit = {max_delta.jd} days"
                )
            partitions = list(range(part_range.end + 1, new_partition + 1))
        else:
            if new_partition >= part_range.start:
                _LOG.debug(
                    "Partition for time=%s (%d) is above existing start (%d)",
                    time,
                    new_partition,
                    part_range.start,
                )
                return []
            start, _ = context.partitioner.partition_period(part_range.start)
            if start - time > max_delta:
                raise ValueError(
                    f"Extension exceeds limit: current start time = {start.isot}, "
                    f"new start time = {time.isot}, "
                    f"limit = {max_delta.jd} days"
                )
            partitions = list(range(new_partition, part_range.start))

        return partitions

    def delete_time_partitions(
        self, time: astropy.time.Time, after: bool = False, *, confirm: ConfirmDeletePartitions | None = None
    ) -> list[int]:
        """Delete time-partitioned tables before or after specified time.

        Parameters
        ----------
        time : `astropy.time.Time`
            Time before or after which to remove partitions. Partition that
            includes this time is not deleted.
        after : `bool`, optional
            If `True` then delete partitions after the specified time. Default
            is to delete partitions before this time.
        confirm : `~collections.abc.Callable`, optional
            A callable that will be called to confirm deletion of the
            partitions. The callable needs to accept three keyword arguments:

                - `partitions` - a list of partition numbers to be deleted,
                - `tables` - a list of table names to be deleted,
                - `partitioner` - a `Partitioner` instance.

            Partitions are deleted only if callable returns `True`.

        Returns
        -------
        partitions : `list` [`int`]
            List of partitons deleted from the database, empty list returned if
            nothing is deleted.

        Raises
        ------
        TypeError
            Raised if APDB instance does not use time-partition tables.
        ValueError
            Raised if requested to delete all partitions.
        """
        context = self._apdb._context

        # Get current partitions.
        part_range = context.time_partitions_range
        if not part_range:
            raise TypeError("This APDB instance does not use time-partitioned tables.")

        partitions = self._partitions_to_delete(time, after)
        if not partitions:
            return []

        # Cannot delete all partitions.
        if min(partitions) == part_range.start and max(partitions) == part_range.end:
            raise ValueError("Cannot delete all partitions.")

        # Tables that are time-partitioned.
        keyspace = self._apdb._keyspace
        tables = context.schema.time_partitioned_tables()

        table_names = []
        for table in tables:
            for partition in partitions:
                table_names.append(context.schema.tableName(table, partition))

        if confirm is not None:
            # It can raise an exception, but at this point it's completely
            # harmless.
            answer = confirm(partitions=partitions, tables=table_names, partitioner=context.partitioner)
            if not answer:
                return []

        for table_name in table_names:
            _LOG.debug("Dropping table %s", table_name)
            # Use IF EXISTS just in case.
            query = f'DROP TABLE IF EXISTS "{keyspace}"."{table_name}"'
            context.session.execute(query)

        # Update metadata.
        if context.has_time_partition_meta:
            if after:
                part_range.end = min(partitions) - 1
            else:
                part_range.start = max(partitions) + 1
            part_range.save_to_meta(context.metadata)

        return partitions

    def _partitions_to_delete(
        self,
        time: astropy.time.Time,
        after: bool = False,
    ) -> list[int]:
        """Make the list of time partitions to delete."""
        context = self._apdb._context
        part_range = context.time_partitions_range
        assert part_range is not None

        partition = context.partitioner.time_partition(time)
        if after:
            return list(range(max(partition + 1, part_range.start), part_range.end + 1))
        else:
            return list(range(part_range.start, min(partition, part_range.end + 1)))
