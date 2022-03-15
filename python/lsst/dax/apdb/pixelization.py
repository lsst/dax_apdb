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

__all__ = ["Pixelization"]

import logging
from typing import List, Tuple

from lsst import sphgeom


_LOG = logging.getLogger(__name__)


class Pixelization:
    """Wrapper for pixelization classes from `sphgeom` with configurable
    pixelization type and parameters.

    Parameters
    ----------
    pixelization : `str`
        Name of a pixelization type, one of ""htm", "q3c", or "mq3c"
    pix_level : `int`
        Pixelization level.
    pix_max_ranges : `int`
        Maximum number of ranges returned from `envelope()` method.
    """

    def __init__(self, pixelization: str, pix_level: int, pix_max_ranges: int):
        if pixelization == "htm":
            self.pixelator = sphgeom.HtmPixelization(pix_level)
        elif pixelization == "q3c":
            self.pixelator = sphgeom.Q3cPixelization(pix_level)
        elif pixelization == "mq3c":
            self.pixelator = sphgeom.Mq3cPixelization(pix_level)
        else:
            raise ValueError(f"unknown pixelization: {pixelization}")
        self._pix_max_ranges = pix_max_ranges

    def pixels(self, region: sphgeom.Region) -> List[int]:
        """Compute set of the pixel indices for given region.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
        """
        # we want finest set of pixels, so ask as many pixel as possible
        ranges = self.pixelator.envelope(region, 1_000_000)
        indices = []
        for lower, upper in ranges:
            indices += list(range(lower, upper))
        return indices

    def pixel(self, direction: sphgeom.UnitVector3d) -> int:
        """Compute the index of the pixel for given direction.

        Parameters
        ----------
        direction : `lsst.sphgeom.UnitVector3d`
        """
        index = self.pixelator.index(direction)
        return index

    def envelope(self, region: sphgeom.Region) -> List[Tuple[int, int]]:
        """Generate a set of HTM indices covering specified region.

        Parameters
        ----------
        region: `sphgeom.Region`
            Region that needs to be indexed.

        Returns
        -------
        ranges : `list` of `tuple`
            Sequence of ranges, range is a tuple (minHtmID, maxHtmID).
        """
        _LOG.debug("region: %s", region)
        indices = self.pixelator.envelope(region, self._pix_max_ranges)

        if _LOG.isEnabledFor(logging.DEBUG):
            for irange in indices.ranges():
                _LOG.debug(
                    "range: %s %s",
                    self.pixelator.toString(irange[0]),
                    self.pixelator.toString(irange[1]),
                )

        return indices.ranges()
