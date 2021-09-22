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

import numpy
import pandas
import unittest.mock
import lsst.utils.tests

from lsst.afw.image import VisitInfo
import lsst.geom as geom
from lsst.daf.base import DateTime
from lsst.dax.apdb import ApdbSql, ApdbSqlConfig


def createTestObjects(n_objects, id_column_name, extra_fields):
    """Create test objects to store in the ApdbSql.

    Parameters
    ----------
    n_objects : `int`
        Number of objects to create.
    id_column_name : `str`
        Name of the ID column.
    extra_fields : `dict`
        A `dict` whose keys are field names and whose values are their types.

    Returns
    -------
    sources : `pandas.DataFrame`
        Tests sources with filled values.
    """
    data = {
        id_column_name: numpy.arange(n_objects, dtype=numpy.int64),
        "ra": numpy.full(n_objects, 1 * geom.degrees, dtype=numpy.float64),
        "decl": numpy.full(n_objects, 1 * geom.degrees, dtype=numpy.float64),
    }
    for field, type in extra_fields.items():
        data[field] = numpy.ones(n_objects, dtype=type)
    df = pandas.DataFrame(data)
    return df


class TestApVerifyQueries(unittest.TestCase):

    def setUp(self):
        self.apdbCfg = ApdbSqlConfig()
        # Create DB in memory.
        self.apdbCfg.db_url = 'sqlite://'
        self.apdbCfg.dia_object_index = "baseline"
        self.apdbCfg.dia_object_columns = []
        self.apdb = ApdbSql(config=self.apdbCfg)
        self.apdb._schema.makeSchema()

    def tearDown(self):
        del self.apdb

    def test_count_zero_objects(self):
        value = self.apdb.countUnassociatedObjects()
        self.assertEqual(value, 0)

    def test_count_objects(self):
        n_created = 5
        objects = createTestObjects(n_created, "diaObjectId", {'nDiaSources': int})
        objects.at[n_created - 1, "nDiaSources"] = 2

        # nsecs must be an integer, not 1.4e18
        dateTime = DateTime(nsecs=1400000000 * 10**9)
        self.apdb.store(dateTime, objects)

        value = self.apdb.countUnassociatedObjects()
        self.assertEqual(n_created - 1, value)

    @staticmethod
    def _makeVisitInfo(exposureId):
        # Real VisitInfo hard to create
        visitInfo = unittest.mock.NonCallableMock(
            VisitInfo,
            **{"getExposureId.return_value": exposureId}
        )
        return visitInfo

    def test_isExposureProcessed(self):
        n_created = 5
        objects = createTestObjects(n_created, "diaObjectId", {'nDiaSources': int})
        sources = createTestObjects(n_created, "diaSourceId", {'ccdVisitId': int, "diaObjectId": int})
        sources.loc[:, "diaObjectId"] = 0
        sources.loc[:, "ccdVisitId"] = 2381

        # nsecs must be an integer, not 1.4e18
        dateTime = DateTime(nsecs=1400000000 * 10**9)
        self.apdb.store(dateTime, objects, sources)

        self.assertTrue(self.apdb.isVisitProcessed(TestApVerifyQueries._makeVisitInfo(2381)))
        self.assertFalse(self.apdb.isVisitProcessed(TestApVerifyQueries._makeVisitInfo(42)))


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
