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

"""Module with methods to return timing information.

This was developed as a part of a prototype for performance studies. It
could probably be removed in the production system.
"""

from __future__ import annotations

import logging
import resource
import time
from collections.abc import Mapping
from typing import Any

from .monitor import MonAgent

_TagsType = Mapping[str, str | int]


class Timer:
    """Instance of this class can be used to track consumed time.

    Parameters
    ----------
    name : `str`
        Timer name, will be use for reporting to both monitoring and logging.
        Typically the name should look like an identifier for ease of use with
        downstream monitoring software.
    *args : `MonAgent` or `logging.Logger`
        Positional arguments can include a combination of `MonAgent` and
        `logging.Logger` instances. They will be used to report accumulated
        times on exit from context or by calling `dump` method directly.
    tags : `~collections.abc.Mapping` [`str`, `str` or `int`], optional
        Keyword argument, additional tags added to monitoring report and
        logging output.
    log_level : `int`, optional
        Keyword argument, level used for logging output, default is
        `logging.INFO`.

    Notes
    -----
    This class is also a context manager and can be used in a `with` statement.
    By default it prints consumed CPU time and real time spent in a context.

    Example:

        with Timer("SelectTimer", logger):
            engine.execute('SELECT ...')

    """

    def __init__(
        self,
        name: str,
        *args: MonAgent | logging.Logger,
        tags: _TagsType | None = None,
        log_level: int = logging.INFO,
    ):
        self._name = name
        self._mon_agents: list[MonAgent] = []
        self._loggers: list[logging.Logger] = []
        self._tags = tags
        self._log_level = log_level

        for arg in args:
            if isinstance(arg, MonAgent):
                self._mon_agents.append(arg)
            elif isinstance(arg, logging.Logger):
                self._loggers.append(arg)

        self._startReal = -1.0
        self._startUser = -1.0
        self._startSys = -1.0
        self._sumReal = 0.0
        self._sumUser = 0.0
        self._sumSys = 0.0

        self._extra_values: dict[str, int | float] = {}

    def add_values(self, **values: int | float) -> None:
        """Add values to dump together with timing information.

        Parameters
        ----------
        **values : int | float
            Key/values to add to timer information.
        """
        self._extra_values.update(values)

    def start(self) -> Timer:
        """Start timer."""
        self._startReal = time.time()
        ru = resource.getrusage(resource.RUSAGE_SELF)
        self._startUser = ru.ru_utime
        self._startSys = ru.ru_stime
        return self

    def stop(self) -> Timer:
        """Stop timer."""
        if self._startReal > 0:
            self._sumReal += time.time() - self._startReal
            ru = resource.getrusage(resource.RUSAGE_SELF)
            self._sumUser += ru.ru_utime - self._startUser
            self._sumSys += ru.ru_stime - self._startSys
            self._startReal = -1.0
            self._startUser = -1.0
            self._startSys = -1.0
        return self

    def dump(self) -> Timer:
        """Dump timer statistics"""
        for logger in self._loggers:
            logger.log(self._log_level, "%s", self)
        for agent in self._mon_agents:
            agent.add_record(self._name, values=self.as_dict(), tags=self._tags)
        return self

    def accumulated(self) -> tuple[float, float, float]:
        """Return accumulated real, user, and system times in seconds."""
        real = self._sumReal
        user = self._sumUser
        sys = self._sumSys
        if self._startReal > 0:
            real += time.time() - self._startReal
            ru = resource.getrusage(resource.RUSAGE_SELF)
            user += ru.ru_utime - self._startUser
            sys += ru.ru_stime - self._startSys
        return (real, user, sys)

    def as_dict(self, prefix: str = "") -> dict[str, int | float]:
        """Return timers and extra values as dictionary."""
        real, user, sys = self.accumulated()
        values = {
            f"{prefix}real": real,
            f"{prefix}user": user,
            f"{prefix}sys": sys,
        }
        values.update(self._extra_values)
        return values

    def __str__(self) -> str:
        real, user, sys = self.accumulated()
        info = "real=%.3f user=%.3f sys=%.3f" % (real, user, sys)
        if self._name:
            info = self._name + ": " + info
        if self._tags:
            info += f" (tags={self._tags})"
        return info

    def __enter__(self) -> Timer:
        """Enter context, start timer"""
        self.start()
        return self

    def __exit__(self, exc_type: type | None, exc_val: Any, exc_tb: Any) -> Any:
        """Exit context, stop and dump timer"""
        self.stop()
        if exc_type is None:
            self.dump()
        return False
