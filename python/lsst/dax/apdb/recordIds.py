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

__all__ = ["DiaForcedSourceId", "DiaObjectId", "DiaSourceId"]

from dataclasses import dataclass
from typing import Any


@dataclass(kw_only=True, slots=True)
class DiaObjectId:
    """Collection of data items identifying DIAObject records.

    DIAObject primary key is ``diaObjectId``, in Cassandra the table is
    partitioned by spatial coordinates (``ra`` and ``dec``).
    """

    diaObjectId: int
    """ID of DIAObject record."""

    ra: float
    """DIAObject ra, in degrees. Not required to be exact, but needs to be
    close to the value in database record.
    """

    dec: float
    """DIAObject dec, in degrees. Not required to be exact, but needs to be
    close to the value in database record.
    """

    @classmethod
    def from_named_tuple(cls, named_tuple: Any) -> DiaObjectId:
        """Construct DiaObjectId from a named tuple.

        Parameters
        ----------
        named_tuple :
            Named tuple which includes the same attributes as this class, e.g.
            a tuple returned from ``pandas.DataFrame.itertuples()``. Any
            additional attributes are ignored.

        Returns
        -------
        object_id : `DiaObjectId`
            Instance of this class.
        """
        # Input tuple most likely comes from Pandas DataFrame, in that case
        # items may have numpy types, need to convert them to Python types.
        return cls(diaObjectId=int(named_tuple.diaObjectId), ra=named_tuple.ra, dec=named_tuple.dec)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DiaObjectId):
            return self.diaObjectId == other.diaObjectId
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.diaObjectId)


@dataclass(kw_only=True, slots=True)
class DiaSourceId:
    """Collection of data items identifying DIASource records.

    DIASource primary key is ``diaSourceId``, in Cassandra the table is
    partitioned by both spatial coordinates (``ra`` and ``dec``) and time
    (``midpointMjdTai``).
    """

    diaSourceId: int
    """ID of DIASource record."""

    ra: float
    """DIASource ra, in degrees. Not required to be exact, but needs to be
    close to the value in database record.
    """

    dec: float
    """DIASource dec, in degrees. Not required to be exact, but needs to be
    close to the value in database record.
    """

    midpointMjdTai: float
    """DIASource midpointMjdTai, not required to be exact, but needs to be
    close to the value in database record.
    """

    @classmethod
    def from_named_tuple(cls, named_tuple: Any) -> DiaSourceId:
        """Construct DiaSourceId from a named tuple.

        Parameters
        ----------
        named_tuple :
            Named tuple which includes the same attributes as this class, e.g.
            a tuple returned from ``pandas.DataFrame.itertuples()``. Any
            additional attributes are ignored.

        Returns
        -------
        object_id : `DiaSourceId`
            Instance of this class.
        """
        # Input tuple most likely comes from Pandas DataFrame, in that case
        # items may have numpy types, need to convert them to Python types.
        return cls(
            diaSourceId=int(named_tuple.diaSourceId),
            ra=named_tuple.ra,
            dec=named_tuple.dec,
            midpointMjdTai=named_tuple.midpointMjdTai,
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DiaSourceId):
            return self.diaSourceId == other.diaSourceId
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.diaSourceId)


@dataclass(kw_only=True, slots=True)
class DiaForcedSourceId:
    """Collection of data items identifying DIAForcedSource records.

    DIAForcedSource primary key is ``(diaObjectId, visit, detector)``, in
    Cassandra the table is partitioned by both spatial coordinates (``ra`` and
    ``dec``) and time (``midpointMjdTai``).
    """

    diaObjectId: int
    """ID of parent DIAObject record."""

    visit: int
    """Visit ID."""

    detector: int
    """Detector ID."""

    ra: float
    """DIAForcedSource ra, in degrees. Not required to be exact, but needs to
    be close to the value in database record.
    """

    dec: float
    """DIAForcedSource dec, in degrees. Not required to be exact, but needs to
    be close to the value in database record.
    """

    midpointMjdTai: float
    """DIAForcedSource midpointMjdTai, not required to be exact, but needs to
    be close to the value in database record.
    """

    @classmethod
    def from_named_tuple(cls, named_tuple: Any) -> DiaForcedSourceId:
        """Construct DiaForcedSourceId from a named tuple.

        Parameters
        ----------
        named_tuple :
            Named tuple which includes the same attributes as this class, e.g.
            a tuple returned from ``pandas.DataFrame.itertuples()``. Any
            additional attributes are ignored.

        Returns
        -------
        object_id : `DiaForcedSourceId`
            Instance of this class.
        """
        # Input tuple most likely comes from Pandas DataFrame, in that case
        # items may have numpy types, need to convert them to Python types.
        return cls(
            diaObjectId=int(named_tuple.diaObjectId),
            visit=int(named_tuple.visit),
            detector=int(named_tuple.detector),
            ra=named_tuple.ra,
            dec=named_tuple.dec,
            midpointMjdTai=named_tuple.midpointMjdTai,
        )

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DiaForcedSourceId):
            return (
                self.diaObjectId == other.diaObjectId
                and self.visit == other.visit
                and self.detector == other.detector
            )
        return NotImplemented

    def __hash__(self) -> int:
        return hash((self.diaObjectId, self.visit, self.detector))
