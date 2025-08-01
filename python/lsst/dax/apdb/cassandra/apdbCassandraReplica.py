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

__all__ = ["ApdbCassandraReplica"]

import logging
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, cast

import astropy.time

from ..apdbReplica import ApdbReplica, ApdbTableData, ReplicaChunk
from ..apdbSchema import ApdbTables
from ..monitor import MonAgent
from ..timer import Timer
from ..versionTuple import VersionTuple
from .apdbCassandraSchema import ExtraTables
from .cassandra_utils import (
    ApdbCassandraTableData,
    execute_concurrent,
    select_concurrent,
)

if TYPE_CHECKING:
    from .apdbCassandra import ApdbCassandra

_LOG = logging.getLogger(__name__)

_MON = MonAgent(__name__)

VERSION = VersionTuple(1, 1, 0)
"""Version for the code controlling replication tables. This needs to be
updated following compatibility rules when schema produced by this code
changes.
"""


class ApdbCassandraReplica(ApdbReplica):
    """Implementation of `ApdbReplica` for Cassandra backend.

    Parameters
    ----------
    apdb : `ApdbCassandra`
        Instance of ApbdCassandra for database.
    """

    def __init__(self, apdb: ApdbCassandra):
        # Note that ApdbCassandra instance must stay alive while this object
        # exists, so we keep reference to it.
        self._apdb = apdb

    def _timer(self, name: str, *, tags: Mapping[str, str | int] | None = None) -> Timer:
        """Create `Timer` instance given its name."""
        return Timer(name, _MON, tags=tags)

    def schemaVersion(self) -> VersionTuple:
        # Docstring inherited from base class.
        context = self._apdb._context
        return context.db_versions.schema_version

    @classmethod
    def apdbReplicaImplementationVersion(cls) -> VersionTuple:
        # Docstring inherited from base class.
        return VERSION

    @classmethod
    def hasChunkSubPartitions(cls, version: VersionTuple) -> bool:
        """Return True if replica chunk tables have sub-partitions."""
        return version >= VersionTuple(1, 1, 0)

    def getReplicaChunks(self) -> list[ReplicaChunk] | None:
        # docstring is inherited from a base class
        context = self._apdb._context
        config = context.config

        if not context.schema.replication_enabled:
            return None

        # everything goes into a single partition
        partition = 0

        table_name = context.schema.tableName(ExtraTables.ApdbReplicaChunks)
        # We want to avoid timezone mess so return timestamps as milliseconds.
        query = (
            "SELECT toUnixTimestamp(last_update_time), apdb_replica_chunk, unique_id "
            f'FROM "{config.keyspace}"."{table_name}" WHERE partition = %s'
        )

        with self._timer("chunks_select_time") as timer:
            result = context.session.execute(
                query,
                (partition,),
                timeout=config.connection_config.read_timeout,
                execution_profile="read_tuples",
            )
            # order by last_update_time
            rows = sorted(result)
            timer.add_values(row_count=len(rows))
        return [
            ReplicaChunk(
                id=row[1],
                last_update_time=astropy.time.Time(row[0] / 1000, format="unix_tai"),
                unique_id=row[2],
            )
            for row in rows
        ]

    def deleteReplicaChunks(self, chunks: Iterable[int]) -> None:
        # docstring is inherited from a base class
        context = self._apdb._context
        config = context.config

        if not context.schema.replication_enabled:
            raise ValueError("APDB is not configured for replication")

        # everything goes into a single partition
        partition = 0

        # Iterable can be single pass, make everything that we need from it
        # in a single loop.
        repl_table_params = []
        chunk_table_params: list[tuple] = []
        for chunk in chunks:
            repl_table_params.append((partition, chunk))
            if context.schema.has_chunk_sub_partitions:
                for subchunk in range(config.replica_sub_chunk_count):
                    chunk_table_params.append((chunk, subchunk))
            else:
                chunk_table_params.append((chunk,))
        # Anything to do att all?
        if not repl_table_params:
            return

        table_name = context.schema.tableName(ExtraTables.ApdbReplicaChunks)
        query = (
            f'DELETE FROM "{config.keyspace}"."{table_name}" WHERE partition = ? AND apdb_replica_chunk = ?'
        )
        statement = context.preparer.prepare(query)

        queries = [(statement, param) for param in repl_table_params]
        with self._timer("chunks_delete_time") as timer:
            execute_concurrent(context.session, queries)
            timer.add_values(row_count=len(queries))

        # Also remove those chunk_ids from Dia*Chunks tables.
        tables = list(ExtraTables.replica_chunk_tables(context.schema.has_chunk_sub_partitions).values())
        for table in tables:
            table_name = context.schema.tableName(table)
            query = f'DELETE FROM "{config.keyspace}"."{table_name}" WHERE apdb_replica_chunk = ?'
            if context.schema.has_chunk_sub_partitions:
                query += " AND apdb_replica_subchunk = ?"
            statement = context.preparer.prepare(query)

            queries = [(statement, param) for param in chunk_table_params]
            with self._timer("table_chunk_detele_time", tags={"table": table_name}) as timer:
                execute_concurrent(context.session, queries)
                timer.add_values(row_count=len(queries))

    def getTableDataChunks(self, table: ApdbTables, chunks: Iterable[int]) -> ApdbTableData:
        # docstring is inherited from a base class
        context = self._apdb._context
        config = context.config

        if not context.schema.replication_enabled:
            raise ValueError("APDB is not configured for replication")
        if table not in ExtraTables.replica_chunk_tables(False):
            raise ValueError(f"Table {table} does not support replica chunks.")

        # We need to iterate few times.
        chunks = list(chunks)

        # If schema was migrated then a chunk can appear in either old or new
        # chunk table (e.g. DiaObjectChunks or DiaObjectChunks2). Chunk table
        # has a column which will be set to true for new table.
        has_chunk_sub_partitions: dict[int, bool] = {}
        if context.schema.has_chunk_sub_partitions:
            table_name = context.schema.tableName(ExtraTables.ApdbReplicaChunks)
            chunks_str = ",".join(str(chunk_id) for chunk_id in chunks)
            query = (
                f'SELECT apdb_replica_chunk, has_subchunks FROM "{config.keyspace}"."{table_name}" '
                f"WHERE partition = %s and apdb_replica_chunk IN ({chunks_str})"
            )
            partition = 0
            result = context.session.execute(
                query,
                (partition,),
                timeout=config.connection_config.read_timeout,
                execution_profile="read_tuples",
            )
            has_chunk_sub_partitions = dict(result)
        else:
            has_chunk_sub_partitions = dict.fromkeys(chunks, False)

        # Check what kind of tables we want to query, if chunk list is empty
        # then use tbales which should exist in the schema.
        if has_chunk_sub_partitions:
            have_subchunks = any(has_chunk_sub_partitions.values())
            have_non_subchunks = not all(has_chunk_sub_partitions.values())
        else:
            have_subchunks = context.schema.has_chunk_sub_partitions
            have_non_subchunks = not have_subchunks

        # NOTE: if an existing database is migrated and has both types of chunk
        # tables (e.g. DiaObjectChunks and DiaObjectChunks2) it is possible
        # that the same chunk can appear in both tables. In reality schema
        # migration should only happen during the downtime, so there will be
        # suffient gap and a different chunk ID will be used for new chunks.

        table_data: ApdbCassandraTableData | None = None
        table_data_subchunk: ApdbCassandraTableData | None = None

        table_name = context.schema.tableName(ExtraTables.replica_chunk_tables(False)[table])
        with self._timer("table_chunk_select_time", tags={"table": table_name}) as timer:
            if have_subchunks:
                replica_table = ExtraTables.replica_chunk_tables(True)[table]
                table_name = context.schema.tableName(replica_table)
                query = (
                    f'SELECT * FROM "{config.keyspace}"."{table_name}" '
                    "WHERE apdb_replica_chunk = ? AND apdb_replica_subchunk = ?"
                )
                statement = context.preparer.prepare(query)

                queries: list[tuple] = []
                for chunk in chunks:
                    if has_chunk_sub_partitions.get(chunk, False):
                        for subchunk in range(config.replica_sub_chunk_count):
                            queries.append((statement, (chunk, subchunk)))
                if not queries and not have_non_subchunks:
                    # Add a dummy query to return correct set of columns.
                    queries.append((statement, (-1, -1)))

                if queries:
                    table_data_subchunk = cast(
                        ApdbCassandraTableData,
                        select_concurrent(
                            context.session,
                            queries,
                            "read_raw_multi",
                            config.connection_config.read_concurrency,
                        ),
                    )

            if have_non_subchunks:
                replica_table = ExtraTables.replica_chunk_tables(False)[table]
                table_name = context.schema.tableName(replica_table)
                query = f'SELECT * FROM "{config.keyspace}"."{table_name}" WHERE apdb_replica_chunk = ?'
                statement = context.preparer.prepare(query)

                queries = []
                for chunk in chunks:
                    if not has_chunk_sub_partitions.get(chunk, True):
                        queries.append((statement, (chunk,)))
                if not queries and not table_data_subchunk:
                    # Add a dummy query to return correct set of columns.
                    queries.append((statement, (-1,)))

                if queries:
                    table_data = cast(
                        ApdbCassandraTableData,
                        select_concurrent(
                            context.session,
                            queries,
                            "read_raw_multi",
                            config.connection_config.read_concurrency,
                        ),
                    )

            # Merge if both are non-empty.
            if table_data and table_data_subchunk:
                table_data_subchunk.project(drop=["apdb_replica_subchunk"])
                table_data.append(table_data_subchunk)
            elif table_data_subchunk:
                table_data = table_data_subchunk
            elif not table_data:
                raise AssertionError("above logic is incorrect")

            timer.add_values(row_count=len(table_data.rows()))

        return table_data

    def getTableUpdateChunks(self, table: ApdbTables, chunks: Iterable[int]) -> ApdbTableData:
        # docstring is inherited from a base class
        raise NotImplementedError()
