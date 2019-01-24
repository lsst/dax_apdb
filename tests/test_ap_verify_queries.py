# This file is part of dax_ppdb.
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

import unittest
import lsst.utils.tests

import lsst.afw.table as afwTable
import lsst.geom as geom
import lsst.daf.base as dafBase
from lsst.dax.ppdb import Ppdb, PpdbConfig, countUnassociatedObjects


def createTestObjects(n_objects, extra_fields):
    """Create test objects to store in the Ppdb.

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
        self.ppdbCfg = PpdbConfig()
        # Create DB in memory.
        self.ppdbCfg.db_url = 'sqlite://'
        self.ppdbCfg.isolation_level = "READ_UNCOMMITTED"
        self.ppdbCfg.dia_object_index = "baseline"
        self.ppdbCfg.dia_object_columns = []
        self.ppdb = Ppdb(
            config=self.ppdbCfg,
            afw_schemas=dict(DiaObject=afwTable.SourceTable.makeMinimalSchema(),
                             DiaSource=afwTable.SourceTable.makeMinimalSchema()))
        self.ppdb._schema.makeSchema()

    def tearDown(self):
        del self.ppdb

    def test_count_zero_objects(self):
        value = countUnassociatedObjects(self.ppdb)
        self.assertEqual(value, 0)

    def test_count_objects(self):
        n_created = 5
        sources = createTestObjects(n_created, {'nDiaSources': 'I'})
        sources[-1]['nDiaSources'] = 2

        # nsecs must be an integer, not 1.4e18
        dateTime = dafBase.DateTime(nsecs=1400000000 * 10**9)
        self.ppdb.storeDiaObjects(sources, dateTime.toPython())

        value = countUnassociatedObjects(self.ppdb)
        self.assertEqual(n_created - 1, value)


class MemoryTester(lsst.utils.tests.MemoryTestCase):
    pass


def setup_module(module):
    lsst.utils.tests.init()


if __name__ == "__main__":
    lsst.utils.tests.init()
    unittest.main()
