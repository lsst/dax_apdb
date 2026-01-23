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

import datetime
import random
from collections.abc import Iterator
from typing import Any

import astropy.time
import numpy
import pandas

from lsst.sphgeom import LonLat, Region, UnitVector3d


def _genPointsInRegion(region: Region, count: int) -> Iterator[LonLat]:
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
            yield lonlat
            count -= 1


def makeObjectCatalog(
    region: Region | LonLat, count: int, *, start_id: int = 1, **kwargs: Any
) -> pandas.DataFrame:
    """Make a catalog containing a bunch of DiaObjects inside a region.

    Parameters
    ----------
    region : `lsst.sphgeom.Region` or `lsst.sphgeom.LonLat`
        Spherical region or spherical coordinate.
    count : `int`
        Number of records to generate.
    start_id : `int`
        Starting diaObjectId.
    **kwargs : `Any`
        Additional columns and their values to add to catalog.

    Returns
    -------
    catalog : `pandas.DataFrame`
        Catalog of DiaObjects records.

    Notes
    -----
    Returned catalog only contains three columns - ``diaObjectId`, ``ra``, and
    ``dec`` (in degrees).
    """
    if isinstance(region, Region):
        points = list(_genPointsInRegion(region, count))
    else:
        points = [region] * count
    # diaObjectId=0 may be used in some code for DiaSource foreign key to mean
    # the same as ``None``.
    ids = numpy.arange(start_id, len(points) + start_id, dtype=numpy.int64)
    ras = numpy.array([lonlat.getLon().asDegrees() for lonlat in points], dtype=numpy.float64)
    decs = numpy.array([lonlat.getLat().asDegrees() for lonlat in points], dtype=numpy.float64)
    nDiaSources = numpy.ones(len(points), dtype=numpy.int32)
    firstDiaSourceMjdTai = numpy.full(len(points), 60000.0, dtype=numpy.float64)
    data = dict(
        kwargs,
        diaObjectId=ids,
        ra=ras,
        dec=decs,
        nDiaSources=nDiaSources,
        firstDiaSourceMjdTai=firstDiaSourceMjdTai,
    )
    df = pandas.DataFrame(data)
    return df


def makeTimestamp(time: astropy.time.Time, use_mjd: bool, offset_ms: int = 0) -> float | datetime.datetime:
    """Return timestamp in either MJD TAI or datetime format.

    Parameters
    ----------
    time : `astropy.time.Time`
        Time value to convert to timestamp.
    use_mjd : `bool`
        If True return time as MJD TAI, otherwise as datetime.
    offset_ms : `int`, optional
        Additional offset in milliseconds to add to current timestamp.

    Returns
    -------
    timestamp : `float` or `datetime.datetime`
        Resulting timestamp.
    """
    if use_mjd:
        ts = time.tai.mjd
        if offset_ms != 0:
            ts += offset_ms / (24 * 3600 * 1_000)
        return ts
    else:
        # TODO: Note that for now we use naive datetime for time_processed, to
        # have it consistent with ap_association, this is being replaces with
        # MJD TAI in the new APDB schema.
        dt = time.datetime
        if offset_ms != 0:
            dt += datetime.timedelta(milliseconds=offset_ms)
        return dt


def makeTimestampColumn(column: str, use_mjd: bool = True) -> str:
    """Return column name before/after schema migration to MJD TAI."""
    if use_mjd:
        if column == "time_processed":
            return "timeProcessedMjdTai"
        elif column == "time_withdrawn":
            return "timeWithdrawnMjdTai"
        else:
            return f"{column}MjdTai"
    else:
        return column


def makeSourceCatalog(
    objects: pandas.DataFrame,
    visit_time: astropy.time.Time,
    start_id: int = 0,
    visit: int = 1,
    detector: int = 1,
    *,
    use_mjd: bool = True,
    processing_time: astropy.time.Time | None = None,
) -> pandas.DataFrame:
    """Make a catalog containing a bunch of DiaSources associated with the
    input DiaObjects.

    Parameters
    ----------
    objects : `pandas.DataFrame`
        Catalog of DiaObject records.
    visit_time : `astropy.time.Time`
        Time of the visit.
    start_id : `int`
        Starting value for ``diaObjectId``.
    visit, detector : `int`
        Value for ``visit`` and ``detector`` fields.
    use_mjd : `bool`
        If True use MJD TAI for timestamp columns.
    processing_time : `astropy.time.Time` or `None`
        Processing time, if `None` the value of ``visit_time`` is used.

    Returns
    -------
    catalog : `pandas.DataFrame`
        Catalog of DiaSource records.

    Notes
    -----
    Returned catalog only contains small number of columns needed for tests.
    """
    if processing_time is None:
        processing_time = visit_time
    nrows = len(objects)
    midpointMjdTai = visit_time.tai.mjd
    centroid_flag: list[bool | None] = [True] * nrows
    if nrows > 1:
        centroid_flag[-1] = None
    df = pandas.DataFrame(
        {
            "diaSourceId": numpy.arange(start_id, start_id + nrows, dtype=numpy.int64),
            "diaObjectId": pandas.Series(objects["diaObjectId"], dtype="Int64"),
            "visit": numpy.full(nrows, visit, dtype=numpy.int64),
            "detector": numpy.full(nrows, detector, dtype=numpy.int16),
            "parentDiaSourceId": 0,
            "ra": objects["ra"],
            "dec": objects["dec"],
            "midpointMjdTai": numpy.full(nrows, midpointMjdTai, dtype=numpy.float64),
            "centroid_flag": pandas.Series(centroid_flag, dtype="boolean"),
            "ssObjectId": pandas.NA,
            makeTimestampColumn("time_processed", use_mjd): makeTimestamp(processing_time, use_mjd),
        }
    )
    return df


def makeForcedSourceCatalog(
    objects: pandas.DataFrame,
    visit_time: astropy.time.Time,
    visit: int = 1,
    detector: int = 1,
    *,
    use_mjd: bool = True,
    processing_time: astropy.time.Time | None = None,
) -> pandas.DataFrame:
    """Make a catalog containing a bunch of DiaForcedSources associated with
    the input DiaObjects.

    Parameters
    ----------
    objects : `pandas.DataFrame`
        Catalog of DiaObject records.
    visit_time : `astropy.time.Time`
        Time of the visit.
    visit, detector : `int`
        Value for ``visit`` and ``detector`` fields.
    use_mjd : `bool`
        If True use MJD TAI for timestamp columns.
    processing_time : `astropy.time.Time` or `None`
        Processing time, if `None` the value of ``visit_time`` is used.

    Returns
    -------
    catalog : `pandas.DataFrame`
        Catalog of DiaForcedSource records.

    Notes
    -----
    Returned catalog only contains small number of columns needed for tests.
    """
    if processing_time is None:
        processing_time = visit_time
    nrows = len(objects)
    midpointMjdTai = visit_time.mjd
    df = pandas.DataFrame(
        {
            "diaObjectId": objects["diaObjectId"],
            "visit": numpy.full(nrows, visit, dtype=numpy.int64),
            "detector": numpy.full(nrows, detector, dtype=numpy.int16),
            "ra": objects["ra"],
            "dec": objects["dec"],
            "midpointMjdTai": numpy.full(nrows, midpointMjdTai, dtype=numpy.float64),
            "flags": numpy.full(nrows, 0, dtype=numpy.int64),
            makeTimestampColumn("time_processed", use_mjd): makeTimestamp(processing_time, use_mjd),
        }
    )
    return df
