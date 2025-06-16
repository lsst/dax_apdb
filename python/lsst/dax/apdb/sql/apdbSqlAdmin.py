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

__all__ = ["ApdbSqlAdmin"]

import logging
from collections.abc import Iterable, Mapping

from lsst.sphgeom import HtmPixelization, LonLat, UnitVector3d

from ..apdbAdmin import ApdbAdmin, DiaForcedSourceLocator, DiaObjectLocator, DiaSourceLocator
from ..monitor import MonAgent
from ..timer import Timer

_LOG = logging.getLogger(__name__)

_MON = MonAgent(__name__)


class ApdbSqlAdmin(ApdbAdmin):
    """Implementation of `ApdbAdmin` for SQL backend.

    Parameters
    ----------
    pixelator : `HtmPixelization`
        Pixelization scheme.
    """

    def __init__(self, pixelator: HtmPixelization):
        self._pixelator = pixelator

    def _timer(self, name: str, *, tags: Mapping[str, str | int] | None = None) -> Timer:
        """Create `Timer` instance given its name."""
        return Timer(name, _MON, tags=tags)

    def apdb_part(self, ra: float, dec: float) -> int:
        # docstring is inherited from a base class
        uv3d = UnitVector3d(LonLat.fromDegrees(ra, dec))
        return self._pixelator.index(uv3d)

    def apdb_time_part(self, midpointMjdTai: float) -> int:
        # docstring is inherited from a base class
        # SQL implementation does not partition temporally.
        return 0

    def delete_records(
        self,
        objects: Iterable[DiaObjectLocator],
        sources: Iterable[DiaSourceLocator],
        forced_sources: Iterable[DiaForcedSourceLocator],
    ) -> None:
        # docstring is inherited from a base class
        raise NotImplementedError("not implemented yet")
