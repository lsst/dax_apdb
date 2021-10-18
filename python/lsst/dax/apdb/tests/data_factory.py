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

import numpy
import pandas
import random
from typing import Iterator

from lsst.daf.base import DateTime
from lsst.sphgeom import LonLat, Region, UnitVector3d
from lsst.geom import SpherePoint


def _genPointsInRegion(region: Region, count: int) -> Iterator[SpherePoint]:
    """Generate bunch of SpherePoints inside given region.

    Parameters
    ----------
    region : `lsst.sphgeom.Region`
        Spherical region.
    count : `int`
        Number of points to generate.

    Notes
    -----
    Returned points are random but not necessarily uniformly distributed.
    """
    bbox = region.getBoundingBox()
    center = bbox.getCenter()
    center_lon = center.getLon().asRadians()
    center_lat = center.getLat().asRadians()
    width = bbox.getWidth().asRadians()
    height = bbox.getHeight().asRadians()
    while count > 0:
        lon = random.uniform(center_lon - width / 2, center_lon + width / 2)
        lat = random.uniform(center_lat - height / 2, center_lat + height / 2)
        lonlat = LonLat.fromRadians(lon, lat)
        uv3d = UnitVector3d(lonlat)
        if region.contains(uv3d):
            yield SpherePoint(lonlat)
            count -= 1


def makeObjectCatalog(region: Region, count: int) -> pandas.DataFrame:
    """Make a catalog containing a bunch of DiaObjects inside a region.

    Parameters
    ----------
    region : `lsst.sphgeom.Region`
        Spherical region.
    count : `int`
        Number of records to generate.

    Returns
    -------
    catalog : `pandas.DataFrame`
        Catalog of DiaObjects records.

    Notes
    -----
    Returned catalog only contains three columns - ``diaObjectId`, ``ra``, and
    ``decl`` (in degrees).
    """
    points = list(_genPointsInRegion(region, count))
    # diaObjectId=0 may be used in some code for DiaSource foreign key to mean
    # the same as ``None``.
    ids = numpy.arange(1, len(points) + 1, dtype=numpy.int64)
    ras = numpy.array([sp.getRa().asDegrees() for sp in points], dtype=numpy.float64)
    decls = numpy.array([sp.getDec().asDegrees() for sp in points], dtype=numpy.float64)
    df = pandas.DataFrame({"diaObjectId": ids,
                           "ra": ras,
                           "decl": decls})
    return df


def makeSourceCatalog(objects: pandas.DataFrame, visit_time: DateTime,
                      start_id: int = 0, ccdVisitId: int = 1) -> pandas.DataFrame:
    """Make a catalog containing a bunch of DiaSources associated with the
    input DiaObjects.

    Parameters
    ----------
    objects : `pandas.DataFrame`
        Catalog of DiaObject records.
    visit_time : `lsst.daf.base.DateTime`
        Time of the visit.
    start_id : `int`
        Starting value for ``diaObjectId``.
    ccdVisitId : `int`
        Value for ``ccdVisitId`` field.

    Returns
    -------
    catalog : `pandas.DataFrame`
        Catalog of DiaSource records.

    Notes
    -----
    Returned catalog only contains small number of columns needed for tests.
    """
    nrows = len(objects)
    midPointTai = visit_time.get(system=DateTime.MJD)
    df = pandas.DataFrame({
        "diaSourceId": numpy.arange(start_id, start_id + nrows, dtype=numpy.int64),
        "diaObjectId": objects["diaObjectId"],
        "ccdVisitId": numpy.full(nrows, ccdVisitId, dtype=numpy.int64),
        "parentDiaSourceId": 0,
        "ra": objects["ra"],
        "decl": objects["decl"],
        "midPointTai": numpy.full(nrows, midPointTai, dtype=numpy.float64),
        "flags": numpy.full(nrows, 0, dtype=numpy.int64),
    })
    return df


def makeForcedSourceCatalog(objects: pandas.DataFrame, visit_time: DateTime,
                            ccdVisitId: int = 1) -> pandas.DataFrame:
    """Make a catalog containing a bunch of DiaFourceSources associated with
    the input DiaObjects.

    Parameters
    ----------
    objects : `pandas.DataFrame`
        Catalog of DiaObject records.
    visit_time : `lsst.daf.base.DateTime`
        Time of the visit.
    ccdVisitId : `int`
        Value for ``ccdVisitId`` field.

    Returns
    -------
    catalog : `pandas.DataFrame`
        Catalog of DiaForcedSource records.

    Notes
    -----
    Returned catalog only contains small number of columns needed for tests.
    """
    nrows = len(objects)
    midPointTai = visit_time.get(system=DateTime.MJD)
    df = pandas.DataFrame({
        "diaObjectId": objects["diaObjectId"],
        "ccdVisitId": numpy.full(nrows, ccdVisitId, dtype=numpy.int64),
        "midPointTai": numpy.full(nrows, midPointTai, dtype=numpy.float64),
        "flags": numpy.full(nrows, 0, dtype=numpy.int64),
    })
    return df