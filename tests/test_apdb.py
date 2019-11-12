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

import datetime
import pandas
import unittest

import lsst.afw.table as afwTable
from lsst.dax.apdb import (Apdb, ApdbConfig, make_minimal_dia_object_schema,
                           make_minimal_dia_source_schema)
from lsst.sphgeom import Angle, Circle, HtmPixelization, Vector3d, UnitVector3d
from lsst.geom import SpherePoint
import lsst.utils.tests


# HTM indexing level used in the unit tests
HTM_LEVEL = 20


def _makePixelRanges():
    """Generate pixel ID ranges for some envelope region"""
    pointing_v = UnitVector3d(1., 1., -1.)
    fov = 0.05  # radians
    region = Circle(pointing_v, Angle(fov/2))
    pixelator = HtmPixelization(HTM_LEVEL)
    indices = pixelator.envelope(region, 128)
    return indices.ranges()


def _makeObjectCatalog(pixel_ranges):
    """Make a catalog containing a bunch of DiaObjects inside pixel envelope.

    The number of created records will be equal number of ranges (one object
    per pixel range). Coordinates of the created objects are not usable.
    """
    # make afw catalog
    schema = make_minimal_dia_object_schema()
    catalog = afwTable.SourceCatalog(schema)

    # make small bunch of records, one entry per one pixel range,
    # we do not care about coordinates here, in current implementation
    # they are not used in any query
    v3d = Vector3d(1., 1., -1.)
    sp = SpherePoint(v3d)
    for oid, (start, end) in enumerate(pixel_ranges):
        record = catalog.addNew()
        record.set("id", oid)
        record.set("pixelId", start)
        record.set("coord_ra", sp.getRa())
        record.set("coord_dec", sp.getDec())

    return catalog


def _makeObjectCatalogPandas(pixel_ranges):
    """Make a catalog containing a bunch of DiaObjects inside pixel envelope.

    The number of created records will be equal to the number of ranges (one
    object per pixel range). Coordinates of the created objects are not usable.
    """
    v3d = Vector3d(1., 1., -1.)
    sp = SpherePoint(v3d)
    data_list = []
    for oid, (start, end) in enumerate(pixel_ranges):
        tmp_dict = {"diaObjectId": oid,
                    "pixelId": start,
                    "ra": sp.getRa().asDegrees(),
                    "decl": sp.getDec().asDegrees()}
        data_list.append(tmp_dict)

    df = pandas.DataFrame(data=data_list)
    return df


def _makeSourceCatalog(objects):
    """Make a catalog containing a bunch of DiaSources associated with the
    input diaObjects.
    """
    # make some sources
    schema = make_minimal_dia_source_schema()
    catalog = afwTable.BaseCatalog(schema)
    oids = []
    for sid, obj in enumerate(objects):
        record = catalog.addNew()
        record.set("id", sid)
        record.set("ccdVisitId", 1)
        record.set("diaObjectId", obj["id"])
        record.set("parent", 0)
        record.set("coord_ra", obj["coord_ra"])
        record.set("coord_dec", obj["coord_dec"])
        record.set("flags", 0)
        record.set("pixelId", obj["pixelId"])
        oids.append(obj["id"])

    return catalog, oids


def _makeSourceCatalogPandas(objects):
    """Make a catalog containing a bunch of DiaSources associated with the
    input diaObjects.
    """
    # make some sources
    catalog = []
    oids = []
    for sid, (index, obj) in enumerate(objects.iterrows()):
        catalog.append({"diaSourceId": sid,
                        "ccdVisitId": 1,
                        "diaObjectId": obj["diaObjectId"],
                        "parentDiaSourceId": 0,
                        "ra": obj["ra"],
                        "decl": obj["decl"],
                        "flags": 0,
                        "pixelId": obj["pixelId"]})
        oids.append(obj["diaObjectId"])
    return pandas.DataFrame(data=catalog), oids


def _makeForcedSourceCatalog(objects):
    """Make a catalog containing a bunch of DiaFourceSources associated with
    the input diaObjects.
    """
    # make some sources
    schema = afwTable.Schema()
    schema.addField("diaObjectId", "L")
    schema.addField("ccdVisitId", "L")
    schema.addField("flags", "L")
    catalog = afwTable.BaseCatalog(schema)
    oids = []
    for obj in objects:
        record = catalog.addNew()
        record.set("diaObjectId", obj["id"])
        record.set("ccdVisitId", 1)
        record.set("flags", 0)
        oids.append(obj["id"])

    return catalog, oids


def _makeForcedSourceCatalogPandas(objects):
    """Make a catalog containing a bunch of DiaFourceSources associated with
    the input diaObjects.
    """
    # make some sources
    catalog = []
    oids = []
    for index, obj in objects.iterrows():
        catalog.append({"diaObjectId": obj["diaObjectId"],
                        "ccdVisitId": 1,
                        "flags": 0})
        oids.append(obj["diaObjectId"])
    return pandas.DataFrame(data=catalog), oids


class ApdbTestCase(unittest.TestCase):
    """A test case for Apdb class
    """

    use_pandas = False
    data_type = afwTable.SourceCatalog

    def _assertCatalog(self, catalog, size, type=afwTable.SourceCatalog):
        """Validate catalog type and size

        Parameters
        ----------
        calalog : `lsst.afw.table.SourceCatalog`
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
        config = ApdbConfig(db_url="sqlite://",
                            isolation_level="READ_UNCOMMITTED")
        apdb = Apdb(config)
        # the essence of a test here is that there are no exceptions.
        apdb.makeSchema()

    def test_emptyGetsBaseline0months(self):
        """Test for getting data from empty database.

        All get() methods should return empty results, only useful for
        checking that code is not broken.
        """

        # set read_sources_months to 0 so that Forced/Sources are None
        config = ApdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            read_sources_months=0,
                            read_forced_sources_months=0)
        apdb = Apdb(config)
        apdb.makeSchema()

        pixel_ranges = _makePixelRanges()
        visit_time = datetime.datetime.now()

        # get objects by region
        res = apdb.getDiaObjects(pixel_ranges, return_pandas=self.use_pandas)
        self._assertCatalog(res, 0, type=self.data_type)

        # get sources by region
        res = apdb.getDiaSourcesInRegion(pixel_ranges, visit_time, return_pandas=self.use_pandas)
        self.assertIs(res, None)

        # get sources by object ID, empty object list
        res = apdb.getDiaSources([], visit_time, return_pandas=self.use_pandas)

        # get forced sources by object ID, empty object list
        res = apdb.getDiaForcedSources([], visit_time, return_pandas=self.use_pandas)
        self.assertIs(res, None)

    def test_emptyGetsBaseline(self):
        """Test for getting data from empty database.

        All get() methods should return empty results, only useful for
        checking that code is not broken.
        """

        # use non-zero months for Forced/Source fetching
        config = ApdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            read_sources_months=12,
                            read_forced_sources_months=12)
        apdb = Apdb(config)
        apdb.makeSchema()

        pixel_ranges = _makePixelRanges()
        visit_time = datetime.datetime.now()

        # get objects by region
        res = apdb.getDiaObjects(pixel_ranges, return_pandas=self.use_pandas)
        self._assertCatalog(res, 0, type=self.data_type)

        # get sources by region
        res = apdb.getDiaSourcesInRegion(pixel_ranges, visit_time, return_pandas=self.use_pandas)
        self._assertCatalog(res, 0, type=self.data_type)

        # get sources by object ID, empty object list, should return None
        res = apdb.getDiaSources([], visit_time, return_pandas=self.use_pandas)
        self.assertIs(res, None)

        # get sources by object ID, non-empty object list
        res = apdb.getDiaSources([1, 2, 3], visit_time, return_pandas=self.use_pandas)
        self._assertCatalog(res, 0, type=self.data_type)

        # get forced sources by object ID, empty object list
        res = apdb.getDiaForcedSources([], visit_time, return_pandas=self.use_pandas)
        self.assertIs(res, None)

        # get sources by object ID, non-empty object list
        res = apdb.getDiaForcedSources([1, 2, 3], visit_time, return_pandas=self.use_pandas)
        self._assertCatalog(res, 0, type=self.data_type)

    def test_emptyGetsObjectLast(self):
        """Test for getting DiaObjects from empty database using DiaObjectLast
        table.

        All get() methods should return empty results, only useful for
        checking that code is not broken.
        """

        # don't care about sources.
        config = ApdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            dia_object_index="last_object_table")
        apdb = Apdb(config)
        apdb.makeSchema()

        pixel_ranges = _makePixelRanges()

        # get objects by region
        res = apdb.getDiaObjects(pixel_ranges, return_pandas=self.use_pandas)
        self._assertCatalog(res, 0, type=self.data_type)

    def test_storeObjectsBaseline(self):
        """Store and retrieve DiaObjects."""

        # don't care about sources.
        config = ApdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            dia_object_index="baseline")
        apdb = Apdb(config)
        apdb.makeSchema()

        pixel_ranges = _makePixelRanges()
        visit_time = datetime.datetime.now()

        # make afw catalog with Objects
        if self.use_pandas:
            catalog = _makeObjectCatalogPandas(pixel_ranges)
        else:
            catalog = _makeObjectCatalog(pixel_ranges)

        # store catalog
        apdb.storeDiaObjects(catalog, visit_time)

        # read it back and check sizes
        res = apdb.getDiaObjects(pixel_ranges, return_pandas=self.use_pandas)
        self._assertCatalog(res, len(catalog), type=self.data_type)

    def test_storeObjectsLast(self):
        """Store and retrieve DiaObjects using DiaObjectLast table."""
        # don't care about sources.
        config = ApdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            dia_object_index="last_object_table",
                            object_last_replace=True)
        apdb = Apdb(config)
        apdb.makeSchema()

        pixel_ranges = _makePixelRanges()
        visit_time = datetime.datetime.now()

        # make afw catalog with Objects
        if self.use_pandas:
            catalog = _makeObjectCatalogPandas(pixel_ranges)
        else:
            catalog = _makeObjectCatalog(pixel_ranges)

        # store catalog
        apdb.storeDiaObjects(catalog, visit_time)

        # read it back and check sizes
        res = apdb.getDiaObjects(pixel_ranges, return_pandas=self.use_pandas)
        self._assertCatalog(res, len(catalog), type=self.data_type)

    def test_storeSources(self):
        """Store and retrieve DiaSources."""
        config = ApdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            read_sources_months=12,
                            read_forced_sources_months=12)
        apdb = Apdb(config)
        apdb.makeSchema()

        pixel_ranges = _makePixelRanges()
        visit_time = datetime.datetime.now()

        # have to store Objects first
        if self.use_pandas:
            objects = _makeObjectCatalogPandas(pixel_ranges)
            catalog, oids = _makeSourceCatalogPandas(objects)
        else:
            objects = _makeObjectCatalog(pixel_ranges)
            catalog, oids = _makeSourceCatalog(objects)

        # save the objects
        apdb.storeDiaObjects(objects, visit_time)

        # save the sources
        apdb.storeDiaSources(catalog)

        # read it back and check sizes
        res = apdb.getDiaSourcesInRegion(pixel_ranges, visit_time, self.use_pandas)
        self._assertCatalog(res, len(catalog), type=self.data_type)

        # read it back using different method
        res = apdb.getDiaSources(oids, visit_time, self.use_pandas)
        self._assertCatalog(res, len(catalog), type=self.data_type)

    def test_storeForcedSources(self):
        """Store and retrieve DiaForcedSources."""

        config = ApdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            read_sources_months=12,
                            read_forced_sources_months=12)
        apdb = Apdb(config)
        apdb.makeSchema()

        pixel_ranges = _makePixelRanges()
        visit_time = datetime.datetime.now()

        # have to store Objects first
        if self.use_pandas:
            objects = _makeObjectCatalogPandas(pixel_ranges)
            catalog, oids = _makeForcedSourceCatalogPandas(objects)
        else:
            objects = _makeObjectCatalog(pixel_ranges)
            catalog, oids = _makeForcedSourceCatalog(objects)

        apdb.storeDiaObjects(objects, visit_time)

        # save them
        apdb.storeDiaForcedSources(catalog)

        # read it back and check sizes
        res = apdb.getDiaForcedSources(oids, visit_time, return_pandas=self.use_pandas)
        self._assertCatalog(res, len(catalog), type=self.data_type)


class ApdbPandasTestCase(ApdbTestCase):
    """A test case for Apdb using Pandas as the input/output"""

    use_pandas = True
    data_type = pandas.DataFrame


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
