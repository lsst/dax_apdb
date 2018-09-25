# This file is part of dax_ppdb.
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


import logging
import resource
import time


_LOG = logging.getLogger(__name__.partition(".")[2])  # strip leading "lsst."


class Timer(object):
    """
    Instance of this class can be used to track consumed time.

    This class is also a context manager and can be used in
    a `with` statement. By default it prints consumed CPU time
    and real time spent in a context.

    Example:

        with Timer('SelectTimer'):
            engine.execute('SELECT ...')

    """
    def __init__(self, name="", doPrint=True):
        """
        @param name:  Time name, will be printed together with statistics
        @param doPrint: if True then print statistics on exist from context
        """
        self._name = name
        self._print = doPrint

        self._startReal = None
        self._startUser = None
        self._startSys = None
        self._sumReal = 0.
        self._sumUser = 0.
        self._sumSys = 0.

    def start(self):
        """
        Start timer.
        """
        self._startReal = time.time()
        ru = resource.getrusage(resource.RUSAGE_SELF)
        self._startUser = ru.ru_utime
        self._startSys = ru.ru_stime
        return self

    def stop(self):
        """
        Stop timer.
        """
        if self._startReal is not None:
            self._sumReal += time.time() - self._startReal
            ru = resource.getrusage(resource.RUSAGE_SELF)
            self._sumUser += ru.ru_utime - self._startUser
            self._sumSys += ru.ru_stime - self._startSys
            self._startReal = None
            self._startUser = None
            self._startSys = None
        return self

    def dump(self):
        """
        Dump timer statistics
        """
        _LOG.info("%s", self)
        return self

    def __str__(self):
        real = self._sumReal
        user = self._sumUser
        sys = self._sumSys
        if self._startReal is not None:
            real += time.time() - self._startReal
            ru = resource.getrusage(resource.RUSAGE_SELF)
            user += ru.ru_utime - self._startUser
            sys += ru.ru_stime - self._startSys
        info = "real=%.3f user=%.3f sys=%.3f" % (real, user, sys)
        if self._name:
            info = self._name + ": " + info
        return info

    def __enter__(self):
        """
        Enter context, start timer
        """
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit context, stop and dump timer
        """
        if exc_type is None:
            self.stop()
            if self._print:
                self.dump()
        return False
