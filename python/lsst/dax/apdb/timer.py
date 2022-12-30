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
from typing import Any, Optional, Type

_LOG = logging.getLogger(__name__)


class Timer:
    """
    Instance of this class can be used to track consumed time.

    This class is also a context manager and can be used in
    a `with` statement. By default it prints consumed CPU time
    and real time spent in a context.

    Example:

        with Timer('SelectTimer'):
            engine.execute('SELECT ...')

    """

    def __init__(self, name: str = "", doPrint: bool = True):
        """
        Parameters
        ----------
        name : `str`
            Timer name, will be printed together with statistics.
        doPrint : `bool`
            If True then print statistics on exist from context.
        """
        self._name = name
        self._print = doPrint

        self._startReal = -1.0
        self._startUser = -1.0
        self._startSys = -1.0
        self._sumReal = 0.0
        self._sumUser = 0.0
        self._sumSys = 0.0

    def start(self) -> Timer:
        """
        Start timer.
        """
        self._startReal = time.time()
        ru = resource.getrusage(resource.RUSAGE_SELF)
        self._startUser = ru.ru_utime
        self._startSys = ru.ru_stime
        return self

    def stop(self) -> Timer:
        """
        Stop timer.
        """
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
        """
        Dump timer statistics
        """
        _LOG.info("%s", self)
        return self

    def __str__(self) -> str:
        real = self._sumReal
        user = self._sumUser
        sys = self._sumSys
        if self._startReal > 0:
            real += time.time() - self._startReal
            ru = resource.getrusage(resource.RUSAGE_SELF)
            user += ru.ru_utime - self._startUser
            sys += ru.ru_stime - self._startSys
        info = "real=%.3f user=%.3f sys=%.3f" % (real, user, sys)
        if self._name:
            info = self._name + ": " + info
        return info

    def __enter__(self) -> Timer:
        """
        Enter context, start timer
        """
        self.start()
        return self

    def __exit__(self, exc_type: Optional[Type], exc_val: Any, exc_tb: Any) -> Any:
        """
        Exit context, stop and dump timer
        """
        if exc_type is None:
            self.stop()
            if self._print:
                self.dump()
        return False
