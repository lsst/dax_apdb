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

__all__ = ["ApdbMetadataSql"]

from collections.abc import Generator

import sqlalchemy

from .apdbMetadata import ApdbMetadata


class ApdbMetadataSql(ApdbMetadata):
    """Implementation of `ApdbMetadata` for SQL backend.

    Parameters
    ----------
    engine : `sqlalchemy.engine.Engine`
        Database access engine.
    table : `sqlalchemy.schema.Table` or `None`
        Database table holding metadata. If table does not exists then `None`
        should be specified.
    """

    def __init__(self, engine: sqlalchemy.engine.Engine, table: sqlalchemy.schema.Table | None):
        self._engine = engine
        self._table = table

    def get(self, key: str, default: str | None = None) -> str | None:
        # Docstring is inherited.
        if self._table is None:
            return default
        sql = sqlalchemy.sql.select(self._table.columns.value).where(self._table.columns.name == key)
        with self._engine.begin() as conn:
            result = conn.execute(sql)
            value = result.scalar()
        if value is not None:
            return value
        return default

    def set(self, key: str, value: str, *, force: bool = False) -> None:
        # Docstring is inherited.
        if self._table is None:
            raise RuntimeError("Metadata table does not exist")
        if not key or not value:
            raise ValueError("name and value cannot be empty")
        try:
            insert = sqlalchemy.sql.insert(self._table).values(name=key, value=value)
            with self._engine.begin() as conn:
                conn.execute(insert)
        except sqlalchemy.exc.IntegrityError as exc:
            # Try to update if it exists.
            if not force:
                raise KeyError(f"Metadata key {key!r} already exists") from exc
            update = (
                sqlalchemy.sql.update(self._table).where(self._table.columns.name == key).values(value=value)
            )
            with self._engine.begin() as conn:
                result = conn.execute(update)
                if result.rowcount != 1:
                    raise RuntimeError(f"Metadata update failed unexpectedly, count={result.rowcount}")

    def delete(self, key: str) -> bool:
        # Docstring is inherited.
        if self._table is None:
            # Missing table means nothing to delete.
            return False
        stmt = sqlalchemy.sql.delete(self._table).where(self._table.columns.name == key)
        with self._engine.begin() as conn:
            result = conn.execute(stmt)
            return result.rowcount > 0

    def items(self) -> Generator[tuple[str, str], None, None]:
        # Docstring is inherited.
        if self._table is None:
            # Missing table means nothing to return.
            return
        stmt = sqlalchemy.sql.select(self._table.columns.name, self._table.columns.value)
        with self._engine.begin() as conn:
            result = conn.execute(stmt)
            for row in result:
                yield row._tuple()

    def empty(self) -> bool:
        # Docstring is inherited.
        if self._table is None:
            # Missing table means empty.
            return True
        stmt = sqlalchemy.sql.select(sqlalchemy.sql.func.count()).select_from(self._table)
        with self._engine.begin() as conn:
            result = conn.execute(stmt)
            count = result.scalar()
        return count == 0

    def table_exists(self) -> bool:
        """Return `True` if metadata table exists."""
        return self._table is not None
