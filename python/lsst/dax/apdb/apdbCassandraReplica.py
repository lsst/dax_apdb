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
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, cast

import astropy.time
from lsst.utils.iteration import chunk_iterable

from .apdbCassandraSchema import ApdbCassandraSchema, ExtraTables
from .apdbReplica import ApdbInsertId, ApdbReplica, ApdbTableData
from .cassandra_utils import ApdbCassandraTableData, PreparedStatementCache
from .timer import Timer
from .versionTuple import VersionTuple

if TYPE_CHECKING:
    from .apdbCassandra import ApdbCassandra

_LOG = logging.getLogger(__name__)

VERSION = VersionTuple(1, 0, 0)
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

    @classmethod
    def apdbReplicaImplementationVersion(cls) -> VersionTuple:
        # Docstring inherited from base class.
        return VERSION

    def getInsertIds(self) -> list[ApdbInsertId] | None:
        # docstring is inherited from a base class
        if not self._schema.has_insert_id:
            return None

        # everything goes into a single partition
        partition = 0

        table_name = self._schema.tableName(ExtraTables.DiaInsertId)
        # We want to avoid timezone mess so return timestamps as milliseconds.
        query = (
            "SELECT toUnixTimestamp(insert_time), insert_id, unique_id "
            f'FROM "{self._config.keyspace}"."{table_name}" WHERE partition = ?'
        )

        result = self._session.execute(
            self._preparer.prepare(query),
            (partition,),
            timeout=self._config.read_timeout,
            execution_profile="read_tuples",
        )
        # order by insert_time
        rows = sorted(result)
        return [
            ApdbInsertId(
                id=row[1],
                insert_time=astropy.time.Time(row[0] / 1000, format="unix_tai"),
                unique_id=row[2],
            )
            for row in rows
        ]

    def deleteInsertIds(self, ids: Iterable[ApdbInsertId]) -> None:
        # docstring is inherited from a base class
        if not self._schema.has_insert_id:
            raise ValueError("APDB is not configured for history storage")

        all_insert_ids = [id.id for id in ids]
        # There is 64k limit on number of markers in Cassandra CQL
        for insert_ids in chunk_iterable(all_insert_ids, 20_000):
            params = ",".join("?" * len(insert_ids))

            # everything goes into a single partition
            partition = 0

            table_name = self._schema.tableName(ExtraTables.DiaInsertId)
            query = (
                f'DELETE FROM "{self._config.keyspace}"."{table_name}" '
                f"WHERE partition = ? AND insert_id IN ({params})"
            )

            self._session.execute(
                self._preparer.prepare(query),
                [partition] + list(insert_ids),
                timeout=self._config.remove_timeout,
            )

            # Also remove those insert_ids from Dia*InsertId tables.
            for table in (
                ExtraTables.DiaObjectInsertId,
                ExtraTables.DiaSourceInsertId,
                ExtraTables.DiaForcedSourceInsertId,
            ):
                table_name = self._schema.tableName(table)
                query = f'DELETE FROM "{self._config.keyspace}"."{table_name}" WHERE insert_id IN ({params})'
                self._session.execute(
                    self._preparer.prepare(query),
                    insert_ids,
                    timeout=self._config.remove_timeout,
                )

    def getDiaObjectsHistory(self, ids: Iterable[ApdbInsertId]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_history(ExtraTables.DiaObjectInsertId, ids)

    def getDiaSourcesHistory(self, ids: Iterable[ApdbInsertId]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_history(ExtraTables.DiaSourceInsertId, ids)

    def getDiaForcedSourcesHistory(self, ids: Iterable[ApdbInsertId]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_history(ExtraTables.DiaForcedSourceInsertId, ids)

    def _get_history(self, table: ExtraTables, ids: Iterable[ApdbInsertId]) -> ApdbTableData:
        """Return records from a particular table given set of insert IDs."""
        if not self._schema.has_insert_id:
            raise ValueError("APDB is not configured for history retrieval")

        insert_ids = [id.id for id in ids]
        params = ",".join("?" * len(insert_ids))

        table_name = self._schema.tableName(table)
        # I know that history table schema has only regular APDB columns plus
        # an insert_id column, and this is exactly what we need to return from
        # this method, so selecting a star is fine here.
        query = f'SELECT * FROM "{self._config.keyspace}"."{table_name}" WHERE insert_id IN ({params})'
        statement = self._preparer.prepare(query)

        with Timer(f"{table_name} history", self._config.timer):
            result = self._session.execute(statement, insert_ids, execution_profile="read_raw")
            table_data = cast(ApdbCassandraTableData, result._current_rows)
        return table_data
