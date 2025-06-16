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

__all__ = ["ApdbAdmin", "DiaForcedSourceLocator", "DiaObjectLocator", "DiaSourceLocator"]

from abc import ABC, abstractmethod
from collections.abc import Iterable
from typing import NamedTuple


class DiaObjectLocator(NamedTuple):
    """Collection of facts necessary to locate DiaObject."""

    diaObjectId: int
    ra: float  # degrees
    dec: float  # degrees


class DiaSourceLocator(NamedTuple):
    """Collection of facts necessary to locate DiaSource."""

    diaSourceId: int
    diaObjectId: int  # This is not identifying, but needed for other reasons.
    ra: float  # degrees
    dec: float  # degrees
    midpointMjdTai: float


class DiaForcedSourceLocator(NamedTuple):
    """Collection of facts necessary to locate DiaForcedSource."""

    diaObjectId: int
    visit: int
    detector: int
    ra: float  # degrees
    dec: float  # degrees
    midpointMjdTai: float


class ApdbAdmin(ABC):
    """Abstract interface for administrative tasks for APDB."""

    @abstractmethod
    def apdb_part(self, ra_deg: float, dec_deg: float) -> int:
        """Return spatial partition index for given coordinates.

        Parameters
        ----------
        ra_deg, dec_deg : `float`
            Spatial coordinates, in degrees.

        Returns
        -------
        index : `int`
            Spatial partition index.
        """
        raise NotImplementedError()

    @abstractmethod
    def apdb_time_part(self, midpointMjdTai: float) -> int:
        """Return temporal partition index for given time.

        Parameters
        ----------
        midpointMjdTai : `float`
            Time in MJD.

        Returns
        -------
        index : `int`
            Temporal partition index.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete_records(
        self,
        objects: Iterable[DiaObjectLocator],
        sources: Iterable[DiaSourceLocator],
        forced_sources: Iterable[DiaForcedSourceLocator],
    ) -> None:
        """Remove DiaObjects and all their associated DiaSources and
        DiaForcedSources from database.

        Parameters
        ----------
        objects : `~collections.abc.Iterable` [`DiaObjectLocator`]
            Locators for DiaObject records to remove.
        sources : `~collections.abc.Iterable` [`DiaSourceLocator`]
            Locators for DiaSource records to remove.
        forced_sources : `~collections.abc.Iterable` [`DiaSourceLocator`]
            Locators for DiaForcedSource records to remove.

        Notes
        -----
        The list of sources has to include all sources associated with objects
        in ``objects`` list. Sources that are not associated with any object
        from ``objects`` are not removed.
        """
        raise NotImplementedError()
