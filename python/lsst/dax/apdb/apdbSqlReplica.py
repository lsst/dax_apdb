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

"""Module defining Apdb class and related methods.
"""

from __future__ import annotations

__all__ = ["ApdbSqlReplica"]

import logging
from collections.abc import Collection, Iterable, Sequence
from typing import TYPE_CHECKING, cast

import astropy.time
import sqlalchemy
from sqlalchemy import sql

from .apdbReplica import ApdbInsertId, ApdbReplica, ApdbTableData
from .apdbSchema import ApdbTables
from .apdbSqlSchema import ExtraTables
from .timer import Timer
from .versionTuple import VersionTuple

if TYPE_CHECKING:
    from .apdbSqlSchema import ApdbSqlSchema


_LOG = logging.getLogger(__name__)

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
        self._timer = timer

    @classmethod
    def apdbReplicaImplementationVersion(cls) -> VersionTuple:
        # Docstring inherited from base class.
        return VERSION

    def getInsertIds(self) -> list[ApdbInsertId] | None:
        # docstring is inherited from a base class
        if not self._schema.has_insert_id:
            return None

        table = self._schema.get_table(ExtraTables.DiaInsertId)
        assert table is not None, "has_insert_id=True means it must be defined"
        query = sql.select(
            table.columns["insert_id"], table.columns["insert_time"], table.columns["unique_id"]
        ).order_by(table.columns["insert_time"])
        with Timer("DiaObject insert id select", self._timer):
            with self._engine.connect() as conn:
                result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
                ids = []
                for row in result:
                    insert_time = astropy.time.Time(row[1].timestamp(), format="unix_tai")
                    ids.append(ApdbInsertId(id=row[0], insert_time=insert_time, unique_id=row[2]))
                return ids

    def deleteInsertIds(self, ids: Iterable[ApdbInsertId]) -> None:
        # docstring is inherited from a base class
        if not self._schema.has_insert_id:
            raise ValueError("APDB is not configured for history storage")

        table = self._schema.get_table(ExtraTables.DiaInsertId)

        insert_ids = [id.id for id in ids]
        where_clause = table.columns["insert_id"].in_(insert_ids)
        stmt = table.delete().where(where_clause)
        with self._engine.begin() as conn:
            conn.execute(stmt)

    def getDiaObjectsHistory(self, ids: Iterable[ApdbInsertId]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_history(ids, ApdbTables.DiaObject, ExtraTables.DiaObjectInsertId)

    def getDiaSourcesHistory(self, ids: Iterable[ApdbInsertId]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_history(ids, ApdbTables.DiaSource, ExtraTables.DiaSourceInsertId)

    def getDiaForcedSourcesHistory(self, ids: Iterable[ApdbInsertId]) -> ApdbTableData:
        # docstring is inherited from a base class
        return self._get_history(ids, ApdbTables.DiaForcedSource, ExtraTables.DiaForcedSourceInsertId)

    def _get_history(
        self,
        ids: Iterable[ApdbInsertId],
        table_enum: ApdbTables,
        history_table_enum: ExtraTables,
    ) -> ApdbTableData:
        """Return catalog of records for given insert identifiers, common
        implementation for all DIA tables.
        """
        if not self._schema.has_insert_id:
            raise ValueError("APDB is not configured for history retrieval")

        table = self._schema.get_table(table_enum)
        history_table = self._schema.get_table(history_table_enum)

        join = table.join(history_table)
        insert_ids = [id.id for id in ids]
        history_id_column = history_table.columns["insert_id"]
        apdb_columns = self._schema.get_apdb_columns(table_enum)
        where_clause = history_id_column.in_(insert_ids)
        query = sql.select(history_id_column, *apdb_columns).select_from(join).where(where_clause)

        # execute select
        with Timer(f"{table.name} history select", self._timer):
            with self._engine.begin() as conn:
                result = conn.execution_options(stream_results=True, max_row_buffer=10000).execute(query)
                return ApdbSqlTableData(result)
