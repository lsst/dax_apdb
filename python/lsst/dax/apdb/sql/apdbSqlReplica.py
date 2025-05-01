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

"""Module defining Apdb class and related methods."""

from __future__ import annotations

__all__ = ["ApdbSqlReplica"]

import logging
from collections.abc import Collection, Iterable, Mapping, Sequence
from typing import TYPE_CHECKING, cast

import astropy.time
import sqlalchemy
from sqlalchemy import sql

from ..apdbReplica import ApdbReplica, ApdbTableData, ReplicaChunk
from ..apdbSchema import ApdbTables
from ..monitor import MonAgent
from ..timer import Timer
from ..versionTuple import VersionTuple
from .apdbSqlSchema import ExtraTables

if TYPE_CHECKING:
    from .apdbSqlSchema import ApdbSqlSchema


_LOG = logging.getLogger(__name__)

_MON = MonAgent(__name__)

VERSION = VersionTuple(1, 0, 0)
"""Version for the code controlling replication tables. This needs to be
updated following compatibility rules when schema produced by this code
changes.
"""


class ApdbSqlTableData(ApdbTableData):
    """Implementation of ApdbTableData that wraps sqlalchemy Result."""

    def __init__(self, result: sqlalchemy.engine.Result):
        self._keys = list(result.keys())
        self._rows: list[tuple] = cast(list[tuple], list(result.fetchall()))

    def column_names(self) -> Sequence[str]:
        return self._keys

    def rows(self) -> Collection[tuple]:
        return self._rows


class ApdbSqlReplica(ApdbReplica):
    """Implementation of `ApdbReplica` for SQL backend.

    Parameters
    ----------
    schema : `ApdbSqlSchema`
        Instance of `ApdbSqlSchema` class for APDB database.
    engine : `sqlalchemy.engine.Engine`
        Engine for database access.
    timer : `bool`, optional
        If `True` then log timing information.
    """

    def __init__(self, schema: ApdbSqlSchema, engine: sqlalchemy.engine.Engine, timer: bool = False):
        self._schema = schema
        self._engine = engine

        self._timer_args: list[MonAgent | logging.Logger] = [_MON]
        if timer:
            self._timer_args.append(_LOG)

    def _timer(self, name: str, *, tags: Mapping[str, str | int] | None = None) -> Timer:
        """Create `Timer` instance given its name."""
        return Timer(name, *self._timer_args, tags=tags)

    def schemaVersion(self) -> VersionTuple:
        # Docstring inherited from base class.
        return self._schema.schemaVersion()

    @classmethod
    def apdbReplicaImplementationVersion(cls) -> VersionTuple:
        # Docstring inherited from base class.
        return VERSION

    def getReplicaChunks(self) -> list[ReplicaChunk] | None:
        # docstring is inherited from a base class
        if not self._schema.replication_enabled:
            return None

        table = self._schema.get_table(ExtraTables.ApdbReplicaChunks)
        assert table is not None, "replication_enabled=True means it must be defined"
        query = sql.select(
            table.columns["apdb_replica_chunk"], table.columns["last_update_time"], table.columns["unique_id"]
        ).order_by(table.columns["last_update_time"])
        with self._timer("chunks_select_time") as timer:
            with self._engine.connect() as conn:
                result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
                ids = []
                for row in result:
                    last_update_time = astropy.time.Time(row[1].timestamp(), format="unix_tai")
                    ids.append(ReplicaChunk(id=row[0], last_update_time=last_update_time, unique_id=row[2]))
                timer.add_values(row_count=len(ids))
                return ids

    def deleteReplicaChunks(self, chunks: Iterable[int]) -> None:
        # docstring is inherited from a base class
        if not self._schema.replication_enabled:
            raise ValueError("APDB is not configured for replication")

        table = self._schema.get_table(ExtraTables.ApdbReplicaChunks)
        chunk_list = list(chunks)
        where_clause = table.columns["apdb_replica_chunk"].in_(chunk_list)
        stmt = table.delete().where(where_clause)
        with self._timer("chunks_delete_time") as timer:
            with self._engine.begin() as conn:
                conn.execute(stmt)
            timer.add_values(row_count=len(chunk_list))

    def getDiaObjectsChunks(self, chunks: Iterable[int]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_chunks(chunks, ApdbTables.DiaObject, ExtraTables.DiaObjectChunks)

    def getDiaSourcesChunks(self, chunks: Iterable[int]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_chunks(chunks, ApdbTables.DiaSource, ExtraTables.DiaSourceChunks)

    def getDiaForcedSourcesChunks(self, chunks: Iterable[int]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_chunks(chunks, ApdbTables.DiaForcedSource, ExtraTables.DiaForcedSourceChunks)

    def _get_chunks(
        self,
        chunks: Iterable[int],
        table_enum: ApdbTables,
        chunk_table_enum: ExtraTables,
    ) -> ApdbTableData:
        """Return catalog of records for given insert identifiers, common
        implementation for all DIA tables.
        """
        if not self._schema.replication_enabled:
            raise ValueError("APDB is not configured for replication")

        table = self._schema.get_table(table_enum)
        chunk_table = self._schema.get_table(chunk_table_enum)

        join = table.join(chunk_table)
        chunk_id_column = chunk_table.columns["apdb_replica_chunk"]
        apdb_columns = self._schema.get_apdb_columns(table_enum)
        where_clause = chunk_id_column.in_(chunks)
        query = sql.select(chunk_id_column, *apdb_columns).select_from(join).where(where_clause)

        # execute select
        with self._timer("table_chunk_select_time", tags={"table": table.name}) as timer:
            with self._engine.begin() as conn:
                result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
                table_data = ApdbSqlTableData(result)
                timer.add_values(row_count=len(table_data.rows()))
                return table_data
