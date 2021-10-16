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

"""Unit test for Apdb class.
"""

import pandas
import random
from typing import Iterator
import unittest

from lsst.daf.base import DateTime
from lsst.dax.apdb import ApdbSql, ApdbSqlConfig, ApdbTables
from lsst.sphgeom import Angle, Circle, LonLat, Region, UnitVector3d
from lsst.geom import SpherePoint
import lsst.utils.tests


def _makeRegion() -> Region:
    """Generate pixel ID ranges for some envelope region"""
    pointing_v = UnitVector3d(1., 1., -1.)
    fov = 0.05  # radians
    region = Circle(pointing_v, Angle(fov/2))
    return region


def _makeVectors(region: Region, count: int = 1) -> Iterator[SpherePoint]:
    """Generate bunch of SpherePoints inside given region.

    Returned vectors are random but not necessarily uniformly distributed.
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


def _makeObjectCatalogPandas(region, count: int, config: ApdbSqlConfig):
    """Make a catalog containing a bunch of DiaObjects inside region.

    The number of created records will be equal to the number of ranges (one
    object per pixel range). Coordinates of the created objects are not usable.
    """
    data_list = []
    # 0 id'ed DiaObjects don't exist and is used as a Null value for the id.
    for oid, sp in enumerate(_makeVectors(region, count)):
        tmp_dict = {"diaObjectId": oid + 1,
                    "ra": sp.getRa().asDegrees(),
                    "decl": sp.getDec().asDegrees()}
        data_list.append(tmp_dict)

    df = pandas.DataFrame(data=data_list)
    return df


def _makeSourceCatalogPandas(objects, visit_time, start_id=0):
    """Make a catalog containing a bunch of DiaSources associated with the
    input diaObjects.
    """
    # make some sources
    catalog = []
    midPointTai = visit_time.get(system=DateTime.MJD)
    for obj in objects.itertuples(index=False):
        catalog.append({"diaSourceId": start_id,
                        "ccdVisitId": 1,
                        "diaObjectId": obj.diaObjectId,
                        "parentDiaSourceId": 0,
                        "ra": obj.ra,
                        "decl": obj.decl,
                        "midPointTai": midPointTai,
                        "flags": 0})
        start_id += 1
    return pandas.DataFrame(data=catalog)


def _makeForcedSourceCatalogPandas(objects, visit_time, ccdVisitId=1):
    """Make a catalog containing a bunch of DiaFourceSources associated with
    the input diaObjects.
    """
    # make some sources
    catalog = []
    midPointTai = visit_time.get(system=DateTime.MJD)
    for obj in objects.itertuples(index=False):
        catalog.append({"diaObjectId": obj.diaObjectId,
                        "ccdVisitId": ccdVisitId,
                        "midPointTai": midPointTai,
                        "flags": 0})
    return pandas.DataFrame(data=catalog)


class ApdbTestCase(unittest.TestCase):
    """A test case for Apdb class
    """

    data_type = pandas.DataFrame

    def _assertCatalog(self, catalog, size, type=pandas.DataFrame):
        """Validate catalog type and size

        Parameters
        ----------
        calalog : `object`
            Expected type of this is ``type``.
        size : int
            Expected catalog size
        type : `type`, optional
            Expected catalog type
        """
        self.assertIsInstance(catalog, type)
        self.assertEqual(len(catalog), size)

    def test_makeSchema(self):
        """Test for making an instance of Apdb using in-memory sqlite engine.
        """
        # sqlite does not support default READ_COMMITTED, for in-memory
        # database have to use connection pool
        config = ApdbSqlConfig(db_url="sqlite://")
        apdb = ApdbSql(config)
        # the essence of a test here is that there are no exceptions.
        apdb.makeSchema()
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObject))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObjectLast))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaSource))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaForcedSource))

    def test_emptyGetsBaseline0months(self):
        """Test for getting data from empty database.

        All get() methods should return empty results, only useful for
        checking that code is not broken.
        """

        # set read_sources_months to 0 so that Forced/Sources are None
        config = ApdbSqlConfig(db_url="sqlite:///",
                               read_sources_months=0,
                               read_forced_sources_months=0)
        apdb = ApdbSql(config)
        apdb.makeSchema()

        region = _makeRegion()
        visit_time = DateTime.now()

        # get objects by region
        res = apdb.getDiaObjects(region)
        self._assertCatalog(res, 0, type=self.data_type)

        # get sources by region
        res = apdb.getDiaSources(region, None, visit_time)
        self.assertIs(res, None)

        # get sources by object ID, empty object list
        res = apdb.getDiaSources(region, [], visit_time)
        self.assertIs(res, None)

        # get forced sources by object ID, empty object list
        res = apdb.getDiaForcedSources(region, [], visit_time)
        self.assertIs(res, None)

    def test_emptyGetsBaseline(self):
        """Test for getting data from empty database.

        All get() methods should return empty results, only useful for
        checking that code is not broken.
        """

        # use non-zero months for Forced/Source fetching
        config = ApdbSqlConfig(db_url="sqlite:///",
                               read_sources_months=12,
                               read_forced_sources_months=12)
        apdb = ApdbSql(config)
        apdb.makeSchema()

        region = _makeRegion()
        visit_time = DateTime.now()

        # get objects by region
        res = apdb.getDiaObjects(region)
        self._assertCatalog(res, 0, type=self.data_type)

        # get sources by region
        res = apdb.getDiaSources(region, None, visit_time)
        self._assertCatalog(res, 0, type=self.data_type)

        res = apdb.getDiaSources(region, [], visit_time)
        self._assertCatalog(res, 0, type=self.data_type)

        # get sources by object ID, non-empty object list
        res = apdb.getDiaSources(region, [1, 2, 3], visit_time)
        self._assertCatalog(res, 0, type=self.data_type)

        # get forced sources by object ID, empty object list
        res = apdb.getDiaForcedSources(region, [], visit_time)
        self._assertCatalog(res, 0, type=self.data_type)

        # get sources by object ID, non-empty object list
        res = apdb.getDiaForcedSources(region, [1, 2, 3], visit_time)
        self._assertCatalog(res, 0, type=self.data_type)

        # SQL implementation needs ID list
        with self.assertRaises(NotImplementedError):
            apdb.getDiaForcedSources(region, None, visit_time)

    def test_emptyGetsObjectLast(self):
        """Test for getting DiaObjects from empty database using DiaObjectLast
        table.

        All get() methods should return empty results, only useful for
        checking that code is not broken.
        """

        # don't care about sources.
        config = ApdbSqlConfig(db_url="sqlite:///",
                               dia_object_index="last_object_table")
        apdb = ApdbSql(config)
        apdb.makeSchema()

        region = _makeRegion()

        # get objects by region
        res = apdb.getDiaObjects(region)
        self._assertCatalog(res, 0, type=self.data_type)

    def test_storeObjectsBaseline(self):
        """Store and retrieve DiaObjects."""

        # don't care about sources.
        config = ApdbSqlConfig(db_url="sqlite:///",
                               dia_object_index="baseline")
        apdb = ApdbSql(config)
        apdb.makeSchema()

        region = _makeRegion()
        visit_time = DateTime.now()

        # make catalog with Objects
        catalog = _makeObjectCatalogPandas(region, 100, config)

        # store catalog
        apdb.store(visit_time, catalog)

        # read it back and check sizes
        res = apdb.getDiaObjects(region)
        self._assertCatalog(res, len(catalog), type=self.data_type)

    def test_storeObjectsLast(self):
        """Store and retrieve DiaObjects using DiaObjectLast table."""
        # don't care about sources.
        config = ApdbSqlConfig(db_url="sqlite:///",
                               dia_object_index="last_object_table",
                               object_last_replace=True)
        apdb = ApdbSql(config)
        apdb.makeSchema()

        region = _makeRegion()
        visit_time = DateTime.now()

        # make catalog with Objects
        catalog = _makeObjectCatalogPandas(region, 100, config)

        # store catalog
        apdb.store(visit_time, catalog)

        # read it back and check sizes
        res = apdb.getDiaObjects(region)
        self._assertCatalog(res, len(catalog), type=self.data_type)

    def test_storeSources(self):
        """Store and retrieve DiaSources."""
        config = ApdbSqlConfig(db_url="sqlite:///",
                               read_sources_months=12,
                               read_forced_sources_months=12)
        apdb = ApdbSql(config)
        apdb.makeSchema()

        region = _makeRegion()
        visit_time = DateTime.now()

        # have to store Objects first
        objects = _makeObjectCatalogPandas(region, 100, config)
        oids = list(objects["diaObjectId"])
        sources = _makeSourceCatalogPandas(objects, visit_time)

        # save the objects and sources
        apdb.store(visit_time, objects, sources)

        # read it back, no ID filtering
        res = apdb.getDiaSources(region, None, visit_time)
        self._assertCatalog(res, len(sources), type=self.data_type)

        # read it back and filter by ID
        res = apdb.getDiaSources(region, oids, visit_time)
        self._assertCatalog(res, len(sources), type=self.data_type)

        # read it back to get schema
        res = apdb.getDiaSources(region, [], visit_time)
        self._assertCatalog(res, 0, type=self.data_type)

    def test_storeForcedSources(self):
        """Store and retrieve DiaForcedSources."""

        config = ApdbSqlConfig(db_url="sqlite:///",
                               read_sources_months=12,
                               read_forced_sources_months=12)
        apdb = ApdbSql(config)
        apdb.makeSchema()

        region = _makeRegion()
        visit_time = DateTime.now()

        # have to store Objects first
        objects = _makeObjectCatalogPandas(region, 100, config)
        oids = list(objects["diaObjectId"])
        catalog = _makeForcedSourceCatalogPandas(objects, visit_time)

        apdb.store(visit_time, objects, forced_sources=catalog)

        # read it back and check sizes
        res = apdb.getDiaForcedSources(region, oids, visit_time)
        self._assertCatalog(res, len(catalog), type=self.data_type)

        # read it back to get schema
        res = apdb.getDiaForcedSources(region, [], visit_time)
        self._assertCatalog(res, 0, type=self.data_type)

    def test_midPointTai_src(self):
        """Test for time filtering of DiaSources.
        """
        config = ApdbSqlConfig(db_url="sqlite:///",
                               read_sources_months=12,
                               read_forced_sources_months=12)
        apdb = ApdbSql(config)
        apdb.makeSchema()

        region = _makeRegion()
        # 2021-01-01 plus 360 days is 2021-12-27
        src_time1 = DateTime(2021, 1, 1, 0, 0, 0, DateTime.TAI)
        src_time2 = DateTime(2021, 1, 1, 0, 0, 2, DateTime.TAI)
        visit_time0 = DateTime(2021, 12, 26, 23, 59, 59, DateTime.TAI)
        visit_time1 = DateTime(2021, 12, 27, 0, 0, 1, DateTime.TAI)
        visit_time2 = DateTime(2021, 12, 27, 0, 0, 3, DateTime.TAI)

        objects = _makeObjectCatalogPandas(region, 100, config)
        oids = list(objects["diaObjectId"])
        sources = _makeSourceCatalogPandas(objects, src_time1, 0)
        apdb.store(src_time1, objects, sources)

        sources = _makeSourceCatalogPandas(objects, src_time2, 100)
        apdb.store(src_time2, objects, sources)

        # reading at time of last save should read all
        res = apdb.getDiaSources(region, oids, src_time2)
        self._assertCatalog(res, 200, type=self.data_type)

        # one second before 12 months
        res = apdb.getDiaSources(region, oids, visit_time0)
        self._assertCatalog(res, 200, type=self.data_type)

        # reading at later time of last save should only read a subset
        res = apdb.getDiaSources(region, oids, visit_time1)
        self._assertCatalog(res, 100, type=self.data_type)

        # reading at later time of last save should only read a subset
        res = apdb.getDiaSources(region, oids, visit_time2)
        self._assertCatalog(res, 0, type=self.data_type)

    def test_midPointTai_fsrc(self):
        """Test for time filtering of DiaForcedSources.
        """
        config = ApdbSqlConfig(db_url="sqlite:///",
                               read_sources_months=12,
                               read_forced_sources_months=12)
        apdb = ApdbSql(config)
        apdb.makeSchema()

        region = _makeRegion()
        src_time1 = DateTime(2021, 1, 1, 0, 0, 0, DateTime.TAI)
        src_time2 = DateTime(2021, 1, 1, 0, 0, 2, DateTime.TAI)
        visit_time0 = DateTime(2021, 12, 26, 23, 59, 59, DateTime.TAI)
        visit_time1 = DateTime(2021, 12, 27, 0, 0, 1, DateTime.TAI)
        visit_time2 = DateTime(2021, 12, 27, 0, 0, 3, DateTime.TAI)

        objects = _makeObjectCatalogPandas(region, 100, config)
        oids = list(objects["diaObjectId"])
        sources = _makeForcedSourceCatalogPandas(objects, src_time1, 1)
        apdb.store(src_time1, objects, forced_sources=sources)

        sources = _makeForcedSourceCatalogPandas(objects, src_time2, 2)
        apdb.store(src_time2, objects, forced_sources=sources)

        # reading at time of last save should read all
        res = apdb.getDiaForcedSources(region, oids, src_time2)
        self._assertCatalog(res, 200, type=self.data_type)

        # one second before 12 months
        res = apdb.getDiaForcedSources(region, oids, visit_time0)
        self._assertCatalog(res, 200, type=self.data_type)

        # reading at later time of last save should only read a subset
        res = apdb.getDiaForcedSources(region, oids, visit_time1)
        self._assertCatalog(res, 100, type=self.data_type)

        # reading at later time of last save should only read a subset
        res = apdb.getDiaForcedSources(region, oids, visit_time2)
        self._assertCatalog(res, 0, type=self.data_type)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
