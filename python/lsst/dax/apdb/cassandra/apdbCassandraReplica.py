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
from typing import TYPE_CHECKING, Any, cast

import astropy.time

from ..apdbReplica import ApdbReplica, ApdbTableData, ReplicaChunk
from ..apdbSchema import ApdbTables
from ..monitor import MonAgent
from ..timer import Timer
from ..versionTuple import VersionTuple
from .apdbCassandraSchema import ApdbCassandraSchema, ExtraTables
from .cassandra_utils import PreparedStatementCache, execute_concurrent, select_concurrent

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
    schema : `ApdbCassandraSchema`
        Instance of ApdbCassandraSchema for database.
    session
        Instance of cassandra session type.
    """

    def __init__(self, apdb: ApdbCassandra, schema: ApdbCassandraSchema, session: Any):
        # Note that ApdbCassandra instance must stay alive while this object
        # exists, so we keep reference to it.
        self._apdb = apdb
        self._schema = schema
        self._session = session
        self._config = apdb.config

        # Cache for prepared statements
        self._preparer = PreparedStatementCache(self._session)

    def _timer(self, name: str, *, tags: Mapping[str, str | int] | None = None) -> Timer:
        """Create `Timer` instance given its name."""
        return Timer(name, _MON, tags=tags)

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
        if not self._schema.replication_enabled:
            return None

        # everything goes into a single partition
        partition = 0

        table_name = self._schema.tableName(ExtraTables.ApdbReplicaChunks)
        # We want to avoid timezone mess so return timestamps as milliseconds.
        query = (
            "SELECT toUnixTimestamp(last_update_time), apdb_replica_chunk, unique_id "
            f'FROM "{self._config.keyspace}"."{table_name}" WHERE partition = %s'
        )

        with self._timer("chunks_select_time") as timer:
            result = self._session.execute(
                query,
                (partition,),
                timeout=self._config.connection_config.read_timeout,
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
        if not self._schema.replication_enabled:
            raise ValueError("APDB is not configured for replication")

        # everything goes into a single partition
        partition = 0

        # Iterable can be single pass, make everything that we need from it
        # in a single loop.
        repl_table_params = []
        chunk_table_params: list[tuple] = []
        for chunk in chunks:
            repl_table_params.append((partition, chunk))
            if self._schema.has_chunk_sub_partitions:
                for subchunk in range(self._config.replica_sub_chunk_count):
                    chunk_table_params.append((chunk, subchunk))
            else:
                chunk_table_params.append((chunk,))
        # Anything to do att all?
        if not repl_table_params:
            return

        table_name = self._schema.tableName(ExtraTables.ApdbReplicaChunks)
        query = (
            f'DELETE FROM "{self._config.keyspace}"."{table_name}" '
            f"WHERE partition = ? AND apdb_replica_chunk = ?"
        )
        statement = self._preparer.prepare(query)

        queries = [(statement, param) for param in repl_table_params]
        with self._timer("chunks_delete_time") as timer:
            execute_concurrent(self._session, queries)
            timer.add_values(row_count=len(queries))

        # Also remove those chunk_ids from Dia*Chunks tables.
        tables = list(ExtraTables.replica_chunk_tables(self._schema.has_chunk_sub_partitions).values())
        for table in tables:
            table_name = self._schema.tableName(table)
            query = f'DELETE FROM "{self._config.keyspace}"."{table_name}" WHERE apdb_replica_chunk = ?'
            if self._schema.has_chunk_sub_partitions:
                query += " AND apdb_replica_subchunk = ?"
            statement = self._preparer.prepare(query)

            queries = [(statement, param) for param in chunk_table_params]
            with self._timer("table_chunk_detele_time", tags={"table": table_name}) as timer:
                execute_concurrent(self._session, queries)
                timer.add_values(row_count=len(queries))

    def getDiaObjectsChunks(self, chunks: Iterable[int]) -> ApdbTableData:
        # docstring is inherited from a base class
        table = ExtraTables.replica_chunk_tables(self._schema.has_chunk_sub_partitions)[ApdbTables.DiaObject]
        return self._get_chunks(table, chunks)

    def getDiaSourcesChunks(self, chunks: Iterable[int]) -> ApdbTableData:
        # docstring is inherited from a base class
        table = ExtraTables.replica_chunk_tables(self._schema.has_chunk_sub_partitions)[ApdbTables.DiaSource]
        return self._get_chunks(table, chunks)

    def getDiaForcedSourcesChunks(self, chunks: Iterable[int]) -> ApdbTableData:
        # docstring is inherited from a base class
        table = ExtraTables.replica_chunk_tables(self._schema.has_chunk_sub_partitions)[
            ApdbTables.DiaForcedSource
        ]
        return self._get_chunks(table, chunks)

    def _get_chunks(self, table: ExtraTables, chunks: Iterable[int]) -> ApdbTableData:
        """Return records from a particular table given set of insert IDs."""
        if not self._schema.replication_enabled:
            raise ValueError("APDB is not configured for replication")

        # NOTE: if an existing database is migrated and has both types of chunk
        # tables (e.g. DiaObjectChunks and DiaObjectChunks2) it is possible
        # that the same chunk can appear in both tables. In reality schema
        # migration should only happen during the downtime, so there will be
        # suffient gap and a different chunk ID will be used for new chunks.

        table_name = self._schema.tableName(table)
        # I know that chunk table schema has only regular APDB columns plus
        # apdb_replica_chunk column, and this is exactly what we need to return
        # from this method, so selecting a star is fine here.
        query = f'SELECT * FROM "{self._config.keyspace}"."{table_name}" WHERE apdb_replica_chunk = ?'
        if self._schema.has_chunk_sub_partitions:
            query += " AND apdb_replica_subchunk = ?"
        statement = self._preparer.prepare(query)

        queries: list[tuple] = []
        if self._schema.has_chunk_sub_partitions:
            for chunk in chunks:
                for subchunk in range(self._config.replica_sub_chunk_count):
                    queries.append((statement, (chunk, subchunk)))
            if not queries:
                # Add a dummy query to return correct set of columns.
                queries.append((statement, (-1, -1)))
        else:
            for chunk in chunks:
                queries.append((statement, (chunk,)))
            if not queries:
                # Add a dummy query to return correct set of columns.
                queries.append((statement, (-1,)))

        with self._timer("table_chunk_select_time", tags={"table": table_name}) as timer:
            table_data = cast(
                ApdbTableData,
                select_concurrent(
                    self._session, queries, "read_raw_multi", self._config.connection_config.read_concurrency
                ),
            )
            timer.add_values(row_count=len(table_data.rows()))

        return table_data
