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

__all__ = ["ColumnExpr", "QExpr", "Query", "Select"]

import abc
import itertools
from collections.abc import Generator, Iterable
from typing import Any, overload


class ColumnExpr(str):
    """A wrapper for a string representing an expression that can be used
    where column name is expected, for example "select(*)". This string will be
    rendered in a query without quoting.
    """

    pass


def CSP(n: int) -> str:
    """Generate a string of the comma-separate placeholders."""
    return ",".join(["{}"] * n)


def _quote_id(columnName: str) -> str:
    """Smart quoting for column names. Lower-case names are not quoted."""
    if columnName != "*":
        if not isinstance(columnName, ColumnExpr):
            if not columnName.islower():
                columnName = '"' + columnName + '"'
    return columnName


class Query(abc.ABC):
    """Abstract base class for all query types."""

    @property
    @abc.abstractmethod
    def can_prepare(self) -> bool:
        """If `False` then this query should not be prepared."""
        raise NotImplementedError()

    @property
    @abc.abstractmethod
    def parameters(self) -> tuple:
        """All query parameters in the same order as in query text."""
        raise NotImplementedError()

    @abc.abstractmethod
    def render(self, placeholder: str | None = None) -> str:
        """Generate query string with placeholders for parameters.

        Parameters
        ----------
        placeholder : `str`, optional
            String to replace ``{}`` in the query text with (e.g. "?" or "%s").
            If not specified then original text is returned.
        """
        raise NotImplementedError()

    def __str__(self) -> str:
        """Generate query string with placeholders for parameters."""
        return self.render()


class QExpr:
    """Class representing a part of query string, such as WHEREexpression or
    column list.

    Parameters
    ----------
    expression : `str`
        An expression, arbitrary text with placeholders for parameters
        represented by ``{}``. If expression contains "{*}" string it will be
        replaced by comma-separated placeholders matching the number of
        parameters.
    parameters : `tuple`, optional
        Parameter values, order must match placeholders in expression.
    can_prepare : `bool`, optional
        If `False` then the statement which includes this expression should not
        be prepared. If not specified as True or False then it is determined by
        the presence of parameters, assumed to be False if no parameters are
        present.
    """

    def __init__(self, expression: str, parameters: Iterable = (), *, can_prepare: bool | None = None):
        # Check that number of placeholders matches number of parameters.
        self.parameters = tuple(parameters)
        if "{*}" in expression:
            expression = expression.replace("{*}", CSP(len(self.parameters)))
        if expression.count("{}") != len(self.parameters):
            raise ValueError(
                f"Number of placeholders in expression ({expression}) "
                f"does not match number of parameters {parameters}"
            )
        self.expression = expression
        self._can_prepare = can_prepare

    @property
    def can_prepare(self) -> bool:
        """If `False` then this query should not be prepared."""
        # If there are no parameters then it's likely to contain rendered
        # values and we do not want to prepare it.
        if self._can_prepare is None:
            return bool(self.parameters)
        return self._can_prepare

    def join(self, other: QExpr, separator: str) -> QExpr:
        """Combine two clauses using given separator."""
        return QExpr(
            expression=f"{self.expression}{separator}{other.expression}",
            parameters=self.parameters + other.parameters,
            can_prepare=self.can_prepare and other.can_prepare,
        )

    def __and__(self, other: object) -> QExpr:
        """Combine two clauses using AND expression."""
        if not isinstance(other, QExpr):
            return NotImplemented
        return self.join(other, " AND ")

    def __eq__(self, other: object) -> bool:
        """Compare two clauses for equality."""
        if not isinstance(other, QExpr):
            return NotImplemented
        return (
            self.expression == other.expression
            and self.parameters == other.parameters
            and self.can_prepare == other.can_prepare
        )

    def __str__(self) -> str:
        return f"QExpr({self.expression!r}, {self.parameters}, {self._can_prepare})"

    def __repr__(self) -> str:
        return str(self)

    @staticmethod
    def combine(*products: list[QExpr], extra: QExpr | None = None) -> Generator[QExpr]:
        """Combine multiple clauses using AND expression.

        Parameters
        ----------
        *products : `Iterable` [ `QExpr` ]
            One or more iterables of clauses to be combined. The result will
            include all combinations of clauses from these iterables. If any of
            the products is empty it is ignored.
        extra : `QExpr`, optional
            An extra clause to be added to each combination of clauses from
            ``products``. If `None` then no extra clause is added.

        Yields
        ------
        clause : `QExpr`
            Combined clause for one of the combinations of clauses from
            ``products`` and the extra clause if it is not `None`.
        """
        for clauses in itertools.product(*[product for product in products if product]):
            if extra is not None:
                clauses = (*clauses, extra)
            result = None
            for clause in clauses:
                if result is None:
                    result = clause
                else:
                    result &= clause
            if result is not None:
                yield result

    @classmethod
    def _from_args(cls, *args: Any, **kwargs: Any) -> QExpr:
        """Construct QExpr from a bunch of parameters that can be passed
        to Select.where().
        """
        match args:
            case (QExpr() as clause,):
                if kwargs:
                    raise TypeError("No keyword arguments expected when QExpr is passed")
                return clause
            case (str() as expr,):
                params = ()
            case (str() as expr, Iterable() as params):
                pass
            case _:
                raise TypeError(f"Unexpected arguments: {args}")

        can_prepare = kwargs.pop("can_prepare", None)
        if kwargs:
            raise TypeError(f"Unexpected keyword arguments: {kwargs}")

        return cls(expr, params, can_prepare=can_prepare)


class Column:
    """Class representing a table column, its main purpose is to generate
    expressions (`QExpr`).
    """

    def __init__(self, column: str):
        self._column = column

    def __str__(self) -> str:
        return _quote_id(self._column)

    def __eq__(self, other: Any) -> QExpr:  # type: ignore[override]
        return QExpr(f"{self} = {{}}", [other])

    def __ne__(self, other: Any) -> QExpr:  # type: ignore[override]
        return QExpr(f"{self} != {{}}", [other])

    def __lt__(self, other: Any) -> QExpr:
        return QExpr(f"{self} < {{}}", [other])

    def __le__(self, other: Any) -> QExpr:
        return QExpr(f"{self} <= {{}}", [other])

    def __gt__(self, other: Any) -> QExpr:
        return QExpr(f"{self} > {{}}", [other])

    def __ge__(self, other: Any) -> QExpr:
        return QExpr(f"{self} >= {{}}", [other])

    def in_(self, other: Iterable, *, can_prepare: bool | None = None) -> QExpr:
        """Generate IN operator."""
        return QExpr(f"{self} IN ({{*}})", other, can_prepare=can_prepare)

    def update(self, other: Any) -> QExpr:
        """Generate column = value expression for UPDATE statement."""
        return QExpr(f"{self} = {{}}", [other])


class Select(Query):
    """Class representing SELECT query.

    Parameters
    ----------
    keyspace : `str`
        Keyspace name.
    table : `str`
        Table name.
    columns : `~collections.abc.Iterable` [`str`]
        Names of the columns to return.
    where_clause : `QExpr`, optional
        WHERE clause to be added to the query.
    extra_clause : `str`, optional
        Extra clause to be added to the query, e.g. for GROUP BY or ORDER BY.
    can_prepare : `bool`, optional
        If `False` then the statement should not be prepared.
    """

    def __init__(
        self,
        keyspace: str,
        table: str,
        columns: Iterable[str],
        *,
        where_clause: QExpr | None = None,
        extra_clause: str | None = None,
        can_prepare: bool = True,
    ):
        self._keyspace = keyspace
        self._table = table
        self._columns = tuple(columns)
        self._where_clause = where_clause
        self._extra_clause = extra_clause
        self._can_prepare = can_prepare

    @property
    def can_prepare(self) -> bool:
        """If `False` then this query should not be prepared."""
        if self._where_clause:
            return self._where_clause.can_prepare and self._can_prepare
        else:
            return self._can_prepare

    @property
    def parameters(self) -> tuple:
        """Complete list of all query parameters."""
        if self._where_clause is not None:
            return self._where_clause.parameters
        return ()

    @overload
    def where(self, where_clause: QExpr) -> Select: ...

    @overload
    def where(
        self, expression: str, parameters: Iterable = (), *, can_prepare: bool | None = None
    ) -> Select: ...

    def where(self, *args: Any, **kwargs: Any) -> Select:
        """Add another QExpr to the query."""
        where_clause = QExpr._from_args(*args, **kwargs)
        if self._where_clause is None:
            where = where_clause
        else:
            where = self._where_clause & where_clause
        return Select(
            keyspace=self._keyspace,
            table=self._table,
            columns=self._columns,
            where_clause=where,
            extra_clause=self._extra_clause,
            can_prepare=self._can_prepare,
        )

    def render(self, placeholder: str | None = None) -> str:
        """Generate query string with placeholders for parameters."""
        select = ",".join(_quote_id(column) for column in self._columns)

        result = f"SELECT {select} FROM {_quote_id(self._keyspace)}.{_quote_id(self._table)}"
        if self._where_clause is not None:
            result += " WHERE " + self._where_clause.expression
        if self._extra_clause is not None:
            result += " " + self._extra_clause
        if placeholder:
            result = result.replace("{}", placeholder)
        return result


class Insert(Query):
    """Class representing INSERT query.

    Parameters
    ----------
    keyspace : `str`
        Keyspace name.
    table : `str`
        Table name.
    columns : `~collections.abc.Iterable` [`str`]
        Names of the columns to return.
    can_prepare : `bool`, optional
        If `False` then the statement should not be prepared.
    """

    def __init__(
        self,
        keyspace: str,
        table: str,
        columns: Iterable[str],
        *,
        can_prepare: bool = True,
    ):
        self._keyspace = keyspace
        self._table = table
        self._columns = tuple(columns)
        self._can_prepare = can_prepare

    @property
    def can_prepare(self) -> bool:
        """If `False` then this query should not be prepared."""
        return self._can_prepare

    @property
    def parameters(self) -> tuple:
        """Complete list of all query parameters."""
        return ()

    def render(self, placeholder: str | None = None) -> str:
        """Generate query string with placeholders for parameters."""
        columns = ",".join(_quote_id(column) for column in self._columns)
        query = (
            f"INSERT INTO {_quote_id(self._keyspace)}.{_quote_id(self._table)} ({columns}) "
            f"VALUES ({CSP(len(self._columns))})"
        )
        if placeholder:
            query = query.replace("{}", placeholder)
        return query


class Delete(Query):
    """Class representing DELETE query.

    Parameters
    ----------
    keyspace : `str`
        Keyspace name.
    table : `str`
        Table name.
    where_clause : `QExpr`, optional
        WHERE clause to be added to the query.
    can_prepare : `bool`, optional
        If `False` then the statement should not be prepared.
    """

    def __init__(
        self,
        keyspace: str,
        table: str,
        *,
        where_clause: QExpr | None = None,
        can_prepare: bool = True,
    ):
        self._keyspace = keyspace
        self._table = table
        self._where_clause = where_clause
        self._can_prepare = can_prepare

    @property
    def can_prepare(self) -> bool:
        """If `False` then this query should not be prepared."""
        if self._where_clause:
            return self._where_clause.can_prepare and self._can_prepare
        else:
            return self._can_prepare

    @property
    def parameters(self) -> tuple:
        """Complete list of all query parameters."""
        if self._where_clause is not None:
            return self._where_clause.parameters
        return ()

    @overload
    def where(self, where_clause: QExpr) -> Delete: ...

    @overload
    def where(
        self, expression: str, parameters: Iterable = (), *, can_prepare: bool | None = None
    ) -> Delete: ...

    def where(self, *args: Any, **kwargs: Any) -> Delete:
        """Add another QExpr to the query."""
        where_clause = QExpr._from_args(*args, **kwargs)
        if self._where_clause is None:
            where = where_clause
        else:
            where = self._where_clause & where_clause
        return Delete(
            keyspace=self._keyspace,
            table=self._table,
            where_clause=where,
            can_prepare=self._can_prepare,
        )

    def render(self, placeholder: str | None = None) -> str:
        """Generate query string with placeholders for parameters."""
        # DELETE without WHERE is very likely an error.
        if self._where_clause is None:
            raise RuntimeError("DELETE statement without WHERE clause is dangerous.")

        result = f"DELETE FROM {_quote_id(self._keyspace)}.{_quote_id(self._table)}"
        if self._where_clause is not None:
            result += " WHERE " + self._where_clause.expression
        if placeholder:
            result = result.replace("{}", placeholder)
        return result


class Update(Query):
    """Class representing UPDATE query.

    Parameters
    ----------
    keyspace : `str`
        Keyspace name.
    table : `str`
        Table name.
    where_clause : `QExpr`, optional
        WHERE clause to be added to the query.
    values : `QExpr`, optional
        SET clause to be added to the query.
    can_prepare : `bool`, optional
        If `False` then the statement should not be prepared.
    """

    def __init__(
        self,
        keyspace: str,
        table: str,
        *,
        where_clause: QExpr | None = None,
        values: QExpr | None = None,
        can_prepare: bool = True,
    ):
        self._keyspace = keyspace
        self._table = table
        self._where_clause = where_clause
        self._values = values
        self._can_prepare = can_prepare

    @property
    def can_prepare(self) -> bool:
        """If `False` then this query should not be prepared."""
        if self._where_clause:
            return self._where_clause.can_prepare and self._can_prepare
        else:
            return self._can_prepare

    @property
    def parameters(self) -> tuple:
        """Complete list of all query parameters."""
        set_params = () if self._values is None else self._values.parameters
        where_params = () if self._where_clause is None else self._where_clause.parameters
        return set_params + where_params

    @overload
    def where(self, where_clause: QExpr) -> Update: ...

    @overload
    def where(
        self, expression: str, parameters: Iterable = (), *, can_prepare: bool | None = None
    ) -> Update: ...

    def where(self, *args: Any, **kwargs: Any) -> Update:
        """Add another QExpr to the query."""
        where_clause = QExpr._from_args(*args, **kwargs)
        if self._where_clause is None:
            where = where_clause
        else:
            where = self._where_clause & where_clause
        return Update(
            keyspace=self._keyspace,
            table=self._table,
            where_clause=where,
            values=self._values,
            can_prepare=self._can_prepare,
        )

    def values(self, *updates: QExpr) -> Update:
        """Define SET clause.

        Parameters
        ----------
        updates : `QExpr`
            One ot more SET clauses, e.g. "x = {}", "y = 10".
        """
        if not updates:
            return self
        values = self._values
        if values is None:
            values = updates[0]
            updates = updates[1:]
        for update in updates:
            values = values.join(update, ", ")
        return Update(
            keyspace=self._keyspace,
            table=self._table,
            where_clause=self._where_clause,
            values=values,
            can_prepare=self._can_prepare,
        )

    def render(self, placeholder: str | None = None) -> str:
        """Generate query string with placeholders for parameters."""
        # UPDATE without WHERE is very likely an error.
        if self._where_clause is None:
            raise RuntimeError("UPDATE statement without WHERE clause is dangerous.")
        if self._values is None:
            raise RuntimeError("UPDATE statement without SET clause.")

        result = f"UPDATE {_quote_id(self._keyspace)}.{_quote_id(self._table)} SET "
        result += self._values.expression
        result += " WHERE " + self._where_clause.expression
        if placeholder:
            result = result.replace("{}", placeholder)
        return result
