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

__all__ = ["ApdbMetadataCassandra"]

from collections.abc import Generator
from typing import Any

from ..apdbMetadata import ApdbMetadata
from .cassandra_utils import PreparedStatementCache, quote_id


class ApdbMetadataCassandra(ApdbMetadata):
    """Implementation of `ApdbMetadata` for Cassandra backend.

    Parameters
    ----------
    session : `cassandra.cluster.Session`
        Cassandra session instance.
    schema : `ApdbSqlSchema`
        Object providing access to schema details.
    """

    def __init__(self, session: Any, table_name: str, keyspace: str, read_profile: str, write_profile: str):
        self._session = session
        self._read_profile = read_profile
        self._write_profile = write_profile
        self._part = 0  # Partition for all rows
        self._preparer = PreparedStatementCache(session)
        # _table_clause will be None when metadata table is not configured
        self._table_clause: str | None = None

        query = "SELECT count(*) FROM system_schema.tables WHERE keyspace_name = %s and table_name = %s"
        result = self._session.execute(query, (keyspace, table_name), execution_profile=read_profile)
        exists = bool(result.one()[0])
        if exists:
            self._table_clause = f"{quote_id(keyspace)}.{quote_id(table_name)}"

    def get(self, key: str, default: str | None = None) -> str | None:
        # Docstring is inherited.
        if self._table_clause is None:
            return default
        query = f"SELECT value FROM {self._table_clause} WHERE meta_part = ? AND name = ?"
        result = self._session.execute(
            self._preparer.prepare(query), (self._part, key), execution_profile=self._read_profile
        )
        if (row := result.one()) is not None:
            return row[0]
        else:
            return default

    def set(self, key: str, value: str, *, force: bool = False) -> None:
        # Docstring is inherited.
        if self._table_clause is None:
            raise RuntimeError("Metadata table does not exist")
        if not key or not value:
            raise ValueError("name and value cannot be empty")
        query = f"INSERT INTO {self._table_clause} (meta_part, name, value) VALUES (?, ?, ?)"
        if not force and self.get(key) is not None:
            raise KeyError(f"Metadata key {key!r} already exists")
        # Race is still possible between check and insert.
        self._session.execute(
            self._preparer.prepare(query), (self._part, key, value), execution_profile=self._write_profile
        )

    def delete(self, key: str) -> bool:
        # Docstring is inherited.
        if self._table_clause is None:
            # Missing table means nothing to delete.
            return False
        if not key:
            raise ValueError("name cannot be empty")
        query = f"DELETE FROM {self._table_clause} WHERE meta_part = ? AND name = ?"
        # Cassandra cannot tell how many rows are deleted, just check if row
        # exists now.
        exists = self.get(key) is not None
        # Race is still possible between check and remove.
        self._session.execute(
            self._preparer.prepare(query), (self._part, key), execution_profile=self._write_profile
        )
        return exists

    def items(self) -> Generator[tuple[str, str], None, None]:
        # Docstring is inherited.
        if self._table_clause is None:
            # Missing table means nothing to return.
            return
        query = f"SELECT name, value FROM {self._table_clause} WHERE meta_part = ?"
        result = self._session.execute(
            self._preparer.prepare(query), (self._part,), execution_profile=self._read_profile
        )
        for row in result:
            yield tuple(row)

    def empty(self) -> bool:
        # Docstring is inherited.
        if self._table_clause is None:
            # Missing table means empty.
            return True
        query = f"SELECT count(*) FROM {self._table_clause} WHERE meta_part = ?"
        result = self._session.execute(
            self._preparer.prepare(query), (self._part,), execution_profile=self._read_profile
        )
        row = result.one()
        return row[0] == 0

    def table_exists(self) -> bool:
        """Return `True` if metadata table exists."""
        return self._table_clause is not None
