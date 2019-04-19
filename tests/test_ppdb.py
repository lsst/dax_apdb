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

"""Unit test for Ppdb class.
"""

import datetime
import pandas
import unittest

import lsst.afw.table as afwTable
from lsst.dax.ppdb import (Ppdb, PpdbConfig, make_minimal_dia_object_schema,
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


class PpdbTestCase(unittest.TestCase):
    """A test case for Ppdb class
    """

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
        """Test for making an instance of Ppdb using in-memory sqlite engine.
        """
        # sqlite does not support default READ_COMMITTED, for in-memory
        # database have to use connection pool
        config = PpdbConfig(db_url="sqlite://",
                            isolation_level="READ_UNCOMMITTED")
        ppdb = Ppdb(config)
        # the essence of a test here is that there are no exceptions.
        ppdb.makeSchema()

    def test_emptyGetsBaseline0months(self):
        """Test for getting data from empty database.

        All get() methods should return empty results, only useful for
        checking that code is not broken.
        """
        self._emptyGetsBaseline0months(test_pandas=False)
        self._emptyGetsBaseline0months(test_pandas=True)

    def _emptyGetsBaseline0months(self, test_pandas=False):
        # set read_sources_months to 0 so that Forced/Sources are None
        config = PpdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            read_sources_months=0,
                            read_forced_sources_months=0)
        ppdb = Ppdb(config)
        ppdb.makeSchema()

        pixel_ranges = _makePixelRanges()
        visit_time = datetime.datetime.now()

        if test_pandas:
            data_type = pandas.DataFrame
        else:
            data_type = afwTable.SourceCatalog

        # get objects by region
        res = ppdb.getDiaObjects(pixel_ranges, return_pandas=test_pandas)
        self._assertCatalog(res, 0, type=data_type)

        # get sources by region
        res = ppdb.getDiaSourcesInRegion(pixel_ranges, visit_time, return_pandas=test_pandas)
        self.assertIs(res, None)

        # get sources by object ID, empty object list
        res = ppdb.getDiaSources([], visit_time, return_pandas=test_pandas)

        # get forced sources by object ID, empty object list
        res = ppdb.getDiaForcedSources([], visit_time, return_pandas=test_pandas)
        self.assertIs(res, None)

    def test_emptyGetsBaseline(self):
        """Test for getting data from empty database.

        All get() methods should return empty results, only useful for
        checking that code is not broken.
        """
        self._emptyGetsBaseline(test_pandas=False)
        self._emptyGetsBaseline(test_pandas=True)

    def _emptyGetsBaseline(self, test_pandas=False):
        # use non-zero months for Forced/Source fetching
        config = PpdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            read_sources_months=12,
                            read_forced_sources_months=12)
        ppdb = Ppdb(config)
        ppdb.makeSchema()

        pixel_ranges = _makePixelRanges()
        visit_time = datetime.datetime.now()

        if test_pandas:
            data_type = pandas.DataFrame
        else:
            data_type = afwTable.SourceCatalog

        # get objects by region
        res = ppdb.getDiaObjects(pixel_ranges, return_pandas=test_pandas)
        self._assertCatalog(res, 0, type=data_type)

        # get sources by region
        res = ppdb.getDiaSourcesInRegion(pixel_ranges, visit_time, return_pandas=test_pandas)
        self._assertCatalog(res, 0, type=data_type)

        # get sources by object ID, empty object list, should return None
        res = ppdb.getDiaSources([], visit_time, return_pandas=test_pandas)
        self.assertIs(res, None)

        # get sources by object ID, non-empty object list
        res = ppdb.getDiaSources([1, 2, 3], visit_time, return_pandas=test_pandas)
        self._assertCatalog(res, 0, type=data_type)

        # get forced sources by object ID, empty object list
        res = ppdb.getDiaForcedSources([], visit_time, return_pandas=test_pandas)
        self.assertIs(res, None)

        # get sources by object ID, non-empty object list
        res = ppdb.getDiaForcedSources([1, 2, 3], visit_time, return_pandas=test_pandas)
        self._assertCatalog(res, 0, type=data_type)

    def test_emptyGetsObjectLast(self):
        """Test for getting DiaObjects from empty database using DiaObjectLast
        table.

        All get() methods should return empty results, only useful for
        checking that code is not broken.
        """
        self._emptyGetsObjectLast(test_pandas=False)
        self._emptyGetsObjectLast(test_pandas=True)

    def _emptyGetsObjectLast(self, test_pandas=False):
        # don't care about sources.
        config = PpdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            dia_object_index="last_object_table")
        ppdb = Ppdb(config)
        ppdb.makeSchema()

        pixel_ranges = _makePixelRanges()

        if test_pandas:
            data_type = pandas.DataFrame
        else:
            data_type = afwTable.SourceCatalog

        # get objects by region
        res = ppdb.getDiaObjects(pixel_ranges, return_pandas=test_pandas)
        self._assertCatalog(res, 0, type=data_type)

    def test_storeObjectsBaseline(self):
        """Store and retrieve DiaObjects."""
        self._storeObjectsBaseline(test_pandas=False)
        self._storeObjectsBaseline(test_pandas=True)

    def _storeObjectsBaseline(self, test_pandas=False):
        # don't care about sources.
        config = PpdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            dia_object_index="baseline")
        ppdb = Ppdb(config)
        ppdb.makeSchema()

        pixel_ranges = _makePixelRanges()
        visit_time = datetime.datetime.now()

        # make afw catalog with Objects
        if test_pandas:
            data_type = pandas.DataFrame
            catalog = _makeObjectCatalogPandas(pixel_ranges)
        else:
            data_type = afwTable.SourceCatalog
            catalog = _makeObjectCatalog(pixel_ranges)

        # store catalog
        ppdb.storeDiaObjects(catalog, visit_time)

        # read it back and check sizes
        res = ppdb.getDiaObjects(pixel_ranges, return_pandas=test_pandas)
        self._assertCatalog(res, len(catalog), type=data_type)

    def test_storeObjectsLast(self):
        """Store and retrieve DiaObjects using DiaObjectLast table."""
        # don't care about sources.
        self._storeObjectsLast(test_pandas=False)
        self._storeObjectsLast(test_pandas=True)

    def _storeObjectsLast(self, test_pandas=False):
        config = PpdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            dia_object_index="last_object_table",
                            object_last_replace=True)
        ppdb = Ppdb(config)
        ppdb.makeSchema()

        pixel_ranges = _makePixelRanges()
        visit_time = datetime.datetime.now()

        # make afw catalog with Objects
        if test_pandas:
            data_type = pandas.DataFrame
            catalog = _makeObjectCatalogPandas(pixel_ranges)
        else:
            data_type = afwTable.SourceCatalog
            catalog = _makeObjectCatalog(pixel_ranges)

        # store catalog
        ppdb.storeDiaObjects(catalog, visit_time)

        # read it back and check sizes
        res = ppdb.getDiaObjects(pixel_ranges, return_pandas=test_pandas)
        self._assertCatalog(res, len(catalog), type=data_type)

    def test_storeSources(self):
        """Store and retrieve DiaSources."""
        self._storeSources(test_pandas=False)
        self._storeSources(test_pandas=True)

    def _storeSources(self, test_pandas=False):
        config = PpdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            read_sources_months=12,
                            read_forced_sources_months=12)
        ppdb = Ppdb(config)
        ppdb.makeSchema()

        pixel_ranges = _makePixelRanges()
        visit_time = datetime.datetime.now()

        # have to store Objects first
        if test_pandas:
            data_type = pandas.DataFrame
            objects = _makeObjectCatalogPandas(pixel_ranges)
            catalog, oids = _makeSourceCatalogPandas(objects)
        else:
            data_type = afwTable.SourceCatalog
            objects = _makeObjectCatalog(pixel_ranges)
            catalog, oids = _makeSourceCatalog(objects)

        # save the objects
        ppdb.storeDiaObjects(objects, visit_time)

        # save the sources
        ppdb.storeDiaSources(catalog)

        # read it back and check sizes
        res = ppdb.getDiaSourcesInRegion(pixel_ranges, visit_time, test_pandas)
        self._assertCatalog(res, len(catalog), type=data_type)

        # read it back using different method
        res = ppdb.getDiaSources(oids, visit_time, test_pandas)
        self._assertCatalog(res, len(catalog), type=data_type)

    def test_storeForcedSources(self):
        """Store and retrieve DiaForcedSources."""
        self._storeForcedSources(test_pandas=False)
        self._storeForcedSources(test_pandas=True)

    def _storeForcedSources(self, test_pandas=False):
        config = PpdbConfig(db_url="sqlite:///",
                            isolation_level="READ_UNCOMMITTED",
                            read_sources_months=12,
                            read_forced_sources_months=12)
        ppdb = Ppdb(config)
        ppdb.makeSchema()

        pixel_ranges = _makePixelRanges()
        visit_time = datetime.datetime.now()

        # have to store Objects first
        if test_pandas:
            data_type = pandas.DataFrame
            objects = _makeObjectCatalogPandas(pixel_ranges)
            catalog, oids = _makeForcedSourceCatalogPandas(objects)
        else:
            data_type = afwTable.SourceCatalog
            objects = _makeObjectCatalog(pixel_ranges)
            catalog, oids = _makeForcedSourceCatalog(objects)

        ppdb.storeDiaObjects(objects, visit_time)

        # save them
        ppdb.storeDiaForcedSources(catalog)

        # read it back and check sizes
        res = ppdb.getDiaForcedSources(oids, visit_time, return_pandas=test_pandas)
        self._assertCatalog(res, len(catalog), type=data_type)


class MyMemoryTestCase(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
