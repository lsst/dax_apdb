# This file is part of dax_apdb.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import unittest.mock
import lsst.utils.tests

import lsst.afw.table as afwTable
import lsst.afw.image as afwImage
import lsst.geom as geom
import lsst.daf.base as dafBase
from lsst.dax.apdb import Apdb, ApdbConfig


def createTestObjects(n_objects, extra_fields):
    """Create test objects to store in the Apdb.

    Parameters
    ----------
    n_objects : `int`
        Number of objects to create.
    extra_fields : `dict`
        A `dict` whose keys are field names and whose values are their types.

    Returns
    -------
    sources : `lsst.afw.table.SourceCatalog`
        Tests sources with filled values.
    """
    schema = afwTable.SourceTable.makeMinimalSchema()
    for field, type in extra_fields.items():
        schema.addField(field, type=type)
    sources = afwTable.SourceCatalog(schema)

    for src_idx in range(n_objects):
        src = sources.addNew()
        for subSchema in schema:
            if subSchema.getField().getTypeString() == "Angle":
                src[subSchema.getField().getName()] = 1 * geom.degrees
            else:
                src[subSchema.getField().getName()] = 1
        src['id'] = src_idx

    return sources


class TestApVerifyQueries(unittest.TestCase):

    def setUp(self):
        self.apdbCfg = ApdbConfig()
        # Create DB in memory.
        self.apdbCfg.db_url = 'sqlite://'
        self.apdbCfg.isolation_level = "READ_UNCOMMITTED"
        self.apdbCfg.dia_object_index = "baseline"
        self.apdbCfg.dia_object_columns = []
        self.apdb = Apdb(
            config=self.apdbCfg,
            afw_schemas=dict(DiaObject=afwTable.SourceTable.makeMinimalSchema(),
                             DiaSource=afwTable.SourceTable.makeMinimalSchema()))
        self.apdb._schema.makeSchema()

    def tearDown(self):
        del self.apdb

    def test_count_zero_objects(self):
        value = self.apdb.countUnassociatedObjects()
        self.assertEqual(value, 0)

    def test_count_objects(self):
        n_created = 5
        sources = createTestObjects(n_created, {'nDiaSources': 'I'})
        sources[-1]['nDiaSources'] = 2

        # nsecs must be an integer, not 1.4e18
        dateTime = dafBase.DateTime(nsecs=1400000000 * 10**9)
        self.apdb.storeDiaObjects(sources, dateTime.toPython())

        value = self.apdb.countUnassociatedObjects()
        self.assertEqual(n_created - 1, value)

    @staticmethod
    def _makeVisitInfo(exposureId):
        # Real VisitInfo hard to create
        visitInfo = unittest.mock.NonCallableMock(
            afwImage.VisitInfo,
            **{"getExposureId.return_value": exposureId}
        )
        return visitInfo

    def test_isExposureProcessed(self):
        n_created = 5
        sources = createTestObjects(n_created, {'ccdVisitId': 'I'})
        for source in sources:
            source['ccdVisitId'] = 2381

        self.apdb.storeDiaSources(sources)

        self.assertTrue(self.apdb.isVisitProcessed(TestApVerifyQueries._makeVisitInfo(2381)))
        self.assertFalse(self.apdb.isVisitProcessed(TestApVerifyQueries._makeVisitInfo(42)))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
