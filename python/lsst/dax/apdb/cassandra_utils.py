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

__all__ = [
    "ApdbCassandraTableData",
    "PreparedStatementCache",
    "literal",
    "pandas_dataframe_factory",
    "quote_id",
    "raw_data_factory",
    "select_concurrent",
]

import logging
from collections.abc import Iterable, Iterator
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import numpy as np
import pandas

# If cassandra-driver is not there the module can still be imported
# but things will not work.
try:
    from cassandra.cluster import EXEC_PROFILE_DEFAULT, Session
    from cassandra.concurrent import execute_concurrent
    from cassandra.query import PreparedStatement

    CASSANDRA_IMPORTED = True
except ImportError:
    CASSANDRA_IMPORTED = False

from .apdb import ApdbTableData

_LOG = logging.getLogger(__name__)


if CASSANDRA_IMPORTED:

    class SessionWrapper:
        """Special wrapper class to workaround ``execute_concurrent()`` issue
        which does not allow non-default execution profile.

        Instance of this class can be passed to execute_concurrent() instead
        of `Session` instance. This class implements a small set of methods
        that are needed by ``execute_concurrent()``. When
        ``execute_concurrent()`` is fixed to accept exectution profiles, this
        wrapper can be dropped.
        """

        def __init__(self, session: Session, execution_profile: Any = EXEC_PROFILE_DEFAULT):
            self._session = session
            self._execution_profile = execution_profile

        def execute_async(
            self,
            *args: Any,
            execution_profile: Any = EXEC_PROFILE_DEFAULT,
            **kwargs: Any,
        ) -> Any:
            # explicit parameter can override our settings
            if execution_profile is EXEC_PROFILE_DEFAULT:
                execution_profile = self._execution_profile
            return self._session.execute_async(*args, execution_profile=execution_profile, **kwargs)

        def submit(self, *args: Any, **kwargs: Any) -> Any:
            # internal method
            return self._session.submit(*args, **kwargs)


class ApdbCassandraTableData(ApdbTableData):
    """Implementation of ApdbTableData that wraps Cassandra raw data."""

    def __init__(self, columns: list[str], rows: list[tuple]):
        self._columns = columns
        self._rows = rows

    def column_names(self) -> list[str]:
        # docstring inherited
        return self._columns

    def rows(self) -> Iterable[tuple]:
        # docstring inherited
        return self._rows

    def append(self, other: ApdbCassandraTableData) -> None:
        """Extend rows in this table with rows in other table"""
        if self._columns != other._columns:
            raise ValueError(f"Different columns returned by queries: {self._columns} and {other._columns}")
        self._rows.extend(other._rows)

    def __iter__(self) -> Iterator[tuple]:
        """Make it look like a row iterator, needed for some odd logic."""
        return iter(self._rows)


class PreparedStatementCache:
    """Cache for prepared Cassandra statements"""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._prepared_statements: dict[str, PreparedStatement] = {}

    def prepare(self, query: str) -> PreparedStatement:
        """Convert query string into prepared statement."""
        stmt = self._prepared_statements.get(query)
        if stmt is None:
            stmt = self._session.prepare(query)
            self._prepared_statements[query] = stmt
        return stmt


def pandas_dataframe_factory(colnames: list[str], rows: list[tuple]) -> pandas.DataFrame:
    """Create pandas DataFrame from Cassandra result set.

    Parameters
    ----------
    colnames : `list` [ `str` ]
        Names of the columns.
    rows : `list` of `tuple`
        Result rows.

    Returns
    -------
    catalog : `pandas.DataFrame`
        DataFrame with the result set.

    Notes
    -----
    When using this method as row factory for Cassandra, the resulting
    DataFrame should be accessed in a non-standard way using
    `ResultSet._current_rows` attribute.
    """
    return pandas.DataFrame.from_records(rows, columns=colnames)


def raw_data_factory(colnames: list[str], rows: list[tuple]) -> ApdbCassandraTableData:
    """Make 2-element tuple containing unmodified data: list of column names
    and list of rows.

    Parameters
    ----------
    colnames : `list` [ `str` ]
        Names of the columns.
    rows : `list` of `tuple`
        Result rows.

    Returns
    -------
    data : `ApdbCassandraTableData`
        Input data wrapped into ApdbCassandraTableData.

    Notes
    -----
    When using this method as row factory for Cassandra, the resulting
    object should be accessed in a non-standard way using
    `ResultSet._current_rows` attribute.
    """
    return ApdbCassandraTableData(colnames, rows)


def select_concurrent(
    session: Session, statements: list[tuple], execution_profile: str, concurrency: int
) -> pandas.DataFrame | ApdbCassandraTableData | list:
    """Execute bunch of queries concurrently and merge their results into
    a single result.

    Parameters
    ----------
    statements : `list` [ `tuple` ]
        List of statements and their parameters, passed directly to
        ``execute_concurrent()``.
    execution_profile : `str`
        Execution profile name.

    Returns
    -------
    result
        Combined result of multiple statements, type of the result depends on
        specific row factory defined in execution profile. If row factory is
        `pandas_dataframe_factory` then pandas DataFrame is created from a
        combined result. If row factory is `raw_data_factory` then
        `ApdbCassandraTableData` is built from all records. Otherwise a list of
        rows is returned, type of each row is determined by the row factory.

    Notes
    -----
    This method can raise any exception that is raised by one of the provided
    statements.
    """
    session_wrap = SessionWrapper(session, execution_profile)
    results = execute_concurrent(
        session_wrap,
        statements,
        results_generator=True,
        raise_on_first_error=False,
        concurrency=concurrency,
    )

    ep = session.get_execution_profile(execution_profile)
    if ep.row_factory is raw_data_factory:
        # Collect rows into a single list and build Dataframe out of that
        _LOG.debug("making pandas data frame out of rows/columns")
        table_data: ApdbCassandraTableData | None = None
        for success, result in results:
            if success:
                data = result._current_rows
                assert isinstance(data, ApdbCassandraTableData)
                if table_data is None:
                    table_data = data
                else:
                    table_data.append(data)
            else:
                _LOG.error("error returned by query: %s", result)
                raise result
        if table_data is None:
            table_data = ApdbCassandraTableData([], [])
        return table_data

    elif ep.row_factory is pandas_dataframe_factory:
        # Merge multiple DataFrames into one
        _LOG.debug("making pandas data frame out of set of data frames")
        dataframes = []
        for success, result in results:
            if success:
                dataframes.append(result._current_rows)
            else:
                _LOG.error("error returned by query: %s", result)
                raise result
        # concatenate all frames
        if len(dataframes) == 1:
            catalog = dataframes[0]
        else:
            catalog = pandas.concat(dataframes)
        _LOG.debug("pandas catalog shape: %s", catalog.shape)
        return catalog

    else:
        # Just concatenate all rows into a single collection.
        rows = []
        for success, result in results:
            if success:
                rows.extend(result)
            else:
                _LOG.error("error returned by query: %s", result)
                raise result
        _LOG.debug("number of rows: %s", len(rows))
        return rows


def literal(v: Any) -> Any:
    """Transform object into a value for the query."""
    if v is None:
        pass
    elif isinstance(v, datetime):
        v = int((v - datetime(1970, 1, 1)) / timedelta(seconds=1)) * 1000
    elif isinstance(v, (bytes, str, UUID, int)):
        pass
    else:
        try:
            if not np.isfinite(v):
                v = None
        except TypeError:
            pass
    return v


def quote_id(columnName: str) -> str:
    """Smart quoting for column names. Lower-case names are not quoted."""
    if not columnName.islower():
        columnName = '"' + columnName + '"'
    return columnName
