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

__all__ = ["LoggingMonHandler", "MonAgent", "MonService"]

import contextlib
import json
import logging
import os
import time
import warnings
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping
from typing import TYPE_CHECKING, Any

from lsst.utils.classes import Singleton

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

_TagsType = Mapping[str, str | int]


_CONFIG_ENV = "DAX_APDB_MONITOR_CONFIG"
"""Name of the envvar specifying service configuration."""


class MonHandler(ABC):
    """Interface for handlers of the monitoring records.

    Handlers are responsible for delivering monitoring records to their final
    destination, for example log file or time-series database.
    """

    @abstractmethod
    def handle(
        self, name: str, timestamp: float, tags: _TagsType, values: Mapping[str, Any], agent_name: str
    ) -> None:
        """Handle one monitoring record.

        Parameters
        ----------
        name : `str`
            Record name, arbitrary string.
        timestamp : `str`
            Time in seconds since UNIX epoch when record originated.
        tags : `~collections.abc.Mapping` [`str`, `str` or `int`]
            Tags associated with the record, may be empty.
        values : `~collections.abc.Mapping` [`str`, `Any`]
            Values associated with the record, usually never empty.
        agent_name `str`
            Name of a client agent that produced this record.
        """
        raise NotImplementedError()


class MonAgent:
    """Client-side interface for adding monitoring records to the monitoring
    service.

    Parameters
    ----------
    name : `str`
        Client agent name, this is used for filtering of the records by the
        service and is also passed to monitoring handler as ``agent_name``.
    """

    def __init__(self, name: str = ""):
        self._name = name
        self._service = MonService()

    def add_record(
        self,
        name: str,
        *,
        values: Mapping[str, Any],
        tags: Mapping[str, str | int] | None = None,
        timestamp: float | None = None,
    ) -> None:
        """Send one record to monitoring service.

        Parameters
        ----------
        name : `str`
            Record name, arbitrary string.
        values : `~collections.abc.Mapping` [`str`, `Any`]
            Values associated with the record, usually never empty.
        tags : `~collections.abc.Mapping` [`str`, `str` or `int`]
            Tags associated with the record, may be empty.
        timestamp : `str`
            Time in seconds since UNIX epoch when record originated.
        """
        self._service._add_record(
            agent_name=self._name,
            record_name=name,
            tags=tags,
            values=values,
            timestamp=timestamp,
        )

    def context_tags(self, tags: _TagsType) -> AbstractContextManager[None]:
        """Context manager that adds a set of tags to all records created
        inside the context.

        Parameters
        ----------
        tags : `~collections.abc.Mapping` [`str`, `str` or `int`]
            Tags associated with the records.

        Notes
        -----
        All calls to `add_record` that happen inside the corresponding context
        will add tags specified in this call. Tags specified in `add_record`
        will override matching tag names that are passed to this method. On
        exit from context a previous tag context is restored (which may be
        empty).
        """
        return self._service.context_tags(tags)


class MonFilter:
    """Filter for the names associated with client agents.

    Parameters
    ----------
    rule : `str`
        String specifying filtering rule for a single name, or catch-all rule.
        The rule consist of the agent name prefixed by minus or optional plus
        sign. Catch-all rule uses name "any". If the rule starts with minus
        sign then matching agent will be rejected. Otherwise matching agent
        is accepted.
    """

    def __init__(self, rule: str):
        self._accept = True
        if rule.startswith("-"):
            self._accept = False
            rule = rule[1:]
        elif rule.startswith("+"):
            rule = rule[1:]
        self.agent_name = "" if rule == "any" else rule

    def is_match_all(self) -> bool:
        """Return `True` if this rule is a catch-all rule.

        Returns
        -------
        is_match_all : `bool`
            `True` if rule name is `-any`, `+any`, or `any`.
        """
        return not self.agent_name

    def accept(self, agent_name: str) -> bool | None:
        """Return filtering decision for specified agent name.

        Parameters
        ----------
        agent_name : `str`
            Name of the client agent that produces monitoring record.

        Returns
        -------
        decision : `bool` or `None`
            `True` if the agent is accepted, `False` if agent is rejected.
            `None` is returned if this rule does not match agent name and
            decision should be made by the next rule.
        """
        if not self.agent_name or agent_name == self.agent_name:
            return self._accept
        return None


class MonService(metaclass=Singleton):
    """Class implementing monitoring service functionality.

    Notes
    -----
    This is a singleton class which serves all client agents in an application.
    It accepts records from agents, filters it based on a set of configured
    rules and forwards them to one or more configured handlers. By default
    there are no handlers defined which means that all records are discarded.
    Default set of filtering rules is empty which accepts all agent names.

    To produce a useful output from this service one has to add at least one
    handler using `add_handler` method (e.g. `LoggingMonHandler` instance).
    The `set_filters` methods can be used to specify the set of filtering
    rules.
    """

    _handlers: list[MonHandler] = []
    """List of active handlers."""

    _context_tags: _TagsType | None = None
    """Current tag context, these tags are added to each new record."""

    _filters: list[MonFilter] = []
    """Sequence of filters for agent names."""

    _initialized: bool = False
    """False before initialization."""

    def set_filters(self, rules: Iterable[str]) -> None:
        """Define a sequence of rules for filtering of the agent names.

        Parameters
        ----------
        rules : `~collections.abc.Iterable` [`str`]
            Ordered collection of rules.  Each string specifies filtering rule
            for a single name, or catch-all rule.  The rule consist of the
            agent name prefixed by minus or optional plus sign. Catch-all rule
            uses name "any". If the rule starts with minus sign then matching
            agent will be rejected. Otherwise matching agent is accepted.

        Notes
        -----
        The catch-all rule (`-any`, `+any`, or `any`) can be specified in any
        location in the sequence but it is always applied last. E.g.
        `["-any", "+agent1"]` behaves the same as `["+agent1", "-any"]`.
        If the set of rues does not include catch-all rule, filtering behaves
        as if it is added implicitly as `+any`.

        Filtering code evaluates each rule in order. First rule that matches
        the agent name wins. Agent names are matched literally, wildcards are
        not supported and there are no parent/child relations between agent
        names (e.g `lsst.dax.apdb` and `lsst.dax.apdb.sql` are treated as
        independent names).
        """
        match_all: MonFilter | None = None
        self._filters = []
        for rule in rules:
            mon_filter = MonFilter(rule)
            if mon_filter.is_match_all():
                match_all = mon_filter
            else:
                self._filters.append(mon_filter)
        if match_all:
            self._filters.append(match_all)

    def _add_record(
        self,
        *,
        agent_name: str,
        record_name: str,
        values: Mapping[str, Any],
        tags: Mapping[str, str | int] | None = None,
        timestamp: float | None = None,
    ) -> None:
        """Add one monitoring record, this method is for use by agents only."""
        if not self._initialized:
            try:
                self._default_init()
                self._initialized = True
            except Exception as exc:
                # Complain but continue.
                message = f"Error in configuration of monitoring service: {exc}"
                # Stack level does not really matter.
                warnings.warn(message, stacklevel=3)
        if self._handlers:
            accept: bool | None = None
            # Check every filter, accept if none makes any decision.
            for filter in self._filters:
                accept = filter.accept(agent_name)
                if accept is False:
                    return
                if accept is True:
                    break
            if timestamp is None:
                timestamp = time.time()
            if tags is None:
                tags = self._context_tags or {}
            else:
                if self._context_tags:
                    all_tags = dict(self._context_tags)
                    all_tags.update(tags)
                    tags = all_tags
            for handler in self._handlers:
                handler.handle(record_name, timestamp, tags, values, agent_name)

    def _default_init(self) -> None:
        """Perform default initialization of the service."""
        if env := os.environ.get(_CONFIG_ENV):
            # Configuration is specified as colon-separated list of key:value
            # pairs or simple values. Simple values are treated as filters
            # (see set_filters for syntax). key-values pairs pairs specify
            # handlers, for now the only supported handler is logging, it
            # is specified as "logging:<logger-name>[:<level>]".
            filters = []
            handlers: list[MonHandler] = []
            for item in env.split(","):
                pieces = item.split(":")
                if len(pieces) in (2, 3) and pieces[0] == "logging":
                    logger_name = pieces[1]
                    if len(pieces) == 3:
                        level_name = pieces[2]
                        level = logging.getLevelNamesMapping().get(level_name.upper())
                        if level is None:
                            raise ValueError(
                                f"Unknown logging level name {level_name!r} in {_CONFIG_ENV}={env!r}"
                            )
                    else:
                        level = logging.INFO
                    handlers.append(LoggingMonHandler(logger_name, level))
                elif len(pieces) == 1:
                    filters.extend(pieces)
                else:
                    raise ValueError(f"Unexpected format of item {item!r} in {_CONFIG_ENV}={env!r}")
            for handler in handlers:
                self.add_handler(handler)
            self.set_filters(filters)

    @property
    def handlers(self) -> Iterable[MonHandler]:
        """Set of handlers defined currently."""
        return self._handlers

    def add_handler(self, handler: MonHandler) -> None:
        """Add one monitoring handler.

        Parameters
        ----------
        handler : `MonHandler`
            Handler instance.
        """
        # Manually adding handler means default initialization should be
        # skipped.
        self._initialized = True
        if handler not in self._handlers:
            self._handlers.append(handler)

    def remove_handler(self, handler: MonHandler) -> None:
        """Remove a monitoring handler.

        Parameters
        ----------
        handler : `MonHandler`
            Handler instance.
        """
        if handler in self._handlers:
            self._handlers.remove(handler)

    def _add_context_tags(self, tags: _TagsType) -> _TagsType | None:
        """Extend the tag context with new tags, overriding any tags that may
        already exist in a current context.
        """
        old_tags = self._context_tags
        if not self._context_tags:
            self._context_tags = tags
        else:
            all_tags = dict(self._context_tags)
            all_tags.update(tags)
            self._context_tags = all_tags
        return old_tags

    @contextlib.contextmanager
    def context_tags(self, tags: _TagsType) -> Iterator[None]:
        """Context manager that adds a set of tags to all records created
        inside the context.

        Typically clients will be using `MonAgent.context_tags`, which forwards
        to this method.
        """
        old_context = self._add_context_tags(tags)
        try:
            yield
        finally:
            # Restore old context.
            self._context_tags = old_context


class LoggingMonHandler(MonHandler):
    """Implementation of the monitoring handler which dumps records formatted
    as JSON objects to `logging`.

    Parameters
    ----------
    logger_name : `str`
        Name of the `logging` logger to use for output.
    log_level : `int`, optional
        Logging level to use for output, default is `INFO`

    Notes
    -----
    The attributes of the formatted JSON object correspond to the parameters
    of `handle` method, except for `agent_name` which is mapped to `source`.
    The `tags` and `values` become JSON sub-objects with corresponding keys.
    """

    def __init__(self, logger_name: str, log_level: int = logging.INFO):
        self._logger = logging.getLogger(logger_name)
        self._level = log_level

    def handle(
        self, name: str, timestamp: float, tags: _TagsType, values: Mapping[str, Any], agent_name: str
    ) -> None:
        # Docstring is inherited from base class.
        record = {
            "name": name,
            "timestamp": timestamp,
            "tags": tags,
            "values": values,
            "source": agent_name,
        }
        msg = json.dumps(record)
        self._logger.log(self._level, msg)
