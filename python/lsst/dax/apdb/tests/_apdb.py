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

__all__ = ["ApdbSchemaUpdateTest", "ApdbTest", "update_schema_yaml"]

import contextlib
import logging.config
import os
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Iterator
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any

import astropy.time
import felis.datamodel
import pandas
import yaml

from lsst.sphgeom import Angle, Circle, LonLat, Region, UnitVector3d

from .. import (
    Apdb,
    ApdbConfig,
    ApdbReassignDiaSourceToSSObjectRecord,
    ApdbReplica,
    ApdbTableData,
    ApdbTables,
    ApdbUpdateRecord,
    ApdbWithdrawDiaSourceRecord,
    DiaObjectId,
    DiaSourceId,
    IncompatibleVersionError,
    ReplicaChunk,
    VersionTuple,
)
from .data_factory import (
    makeForcedSourceCatalog,
    makeObjectCatalog,
    makeSourceCatalog,
    makeTimestamp,
    makeTimestampColumn,
)
from .utils import TestCaseMixin

if TYPE_CHECKING:
    from ..pixelization import Pixelization


# Optionally configure logging from a config file.
if log_config := os.environ.get("DAX_APDB_TEST_LOG_CONFIG"):
    logging.config.fileConfig(log_config)


def _make_region(xyz: tuple[float, float, float] = (1.0, 1.0, -1.0)) -> Region:
    """Make a region to use in tests"""
    pointing_v = UnitVector3d(*xyz)
    fov = 0.0013  # radians
    region = Circle(pointing_v, Angle(fov / 2))
    return region


@contextlib.contextmanager
def update_schema_yaml(
    schema_file: str,
    drop_metadata: bool = False,
    version: str | None = None,
) -> Iterator[str]:
    """Update schema definition and return name of the new schema file.

    Parameters
    ----------
    schema_file : `str`
        Path for the existing YAML file with APDB schema.
    drop_metadata : `bool`
        If `True` then remove metadata table from the list of tables.
    version : `str` or `None`
        If non-empty string then set schema version to this string, if empty
        string then remove schema version from config, if `None` - don't change
        the version in config.

    Yields
    ------
    Path for the updated configuration file.
    """
    with open(schema_file) as yaml_stream:
        schemas_list = list(yaml.load_all(yaml_stream, Loader=yaml.SafeLoader))
    # Edit YAML contents.
    for schema in schemas_list:
        # Optionally drop metadata table.
        if drop_metadata:
            schema["tables"] = [table for table in schema["tables"] if table["name"] != "metadata"]
        if version is not None:
            if version == "":
                del schema["version"]
            else:
                schema["version"] = version

    with TemporaryDirectory(ignore_cleanup_errors=True) as tmpdir:
        output_path = os.path.join(tmpdir, "schema.yaml")
        with open(output_path, "w") as yaml_stream:
            yaml.dump_all(schemas_list, stream=yaml_stream)
        yield output_path


class ApdbTest(TestCaseMixin, ABC):
    """Base class for Apdb tests that can be specialized for concrete
    implementation.

    This can only be used as a mixin class for a unittest.TestCase and it
    calls various assert methods.
    """

    visit_time = astropy.time.Time("2021-01-01T00:00:00", format="isot", scale="tai")

    processing_time = astropy.time.Time("2021-01-01T12:00:00", format="isot", scale="tai")

    fsrc_requires_id_list = False
    """Should be set to True if getDiaForcedSources requires object IDs"""

    enable_replica: bool = False
    """Set to true when support for replication is configured"""

    use_mjd: bool = True
    """If True then timestamp columns are MJD TAI."""

    extra_chunk_columns = 1
    """Number of additional columns in chunk tables."""

    meta_row_count = 3
    """Initial row count in metadata table."""

    # number of columns as defined in tests/config/schema.yaml
    table_column_count = {
        ApdbTables.DiaObject: 8,
        ApdbTables.DiaObjectLast: 6,
        ApdbTables.DiaSource: 12,
        ApdbTables.DiaForcedSource: 8,
        ApdbTables.SSObject: 3,
    }

    @abstractmethod
    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make database instance and return configuration for it."""
        raise NotImplementedError()

    @abstractmethod
    def getDiaObjects_table(self) -> ApdbTables:
        """Return type of table returned from getDiaObjects method."""
        raise NotImplementedError()

    @abstractmethod
    def pixelization(self, config: ApdbConfig) -> Pixelization:
        """Return pixelization used by implementation."""
        raise NotImplementedError()

    def assert_catalog(self, catalog: Any, rows: int, table: ApdbTables) -> None:
        """Validate catalog type and size

        Parameters
        ----------
        catalog : `object`
            Expected type of this is ``pandas.DataFrame``.
        rows : `int`
            Expected number of rows in a catalog.
        table : `ApdbTables`
            APDB table type.
        """
        self.assertIsInstance(catalog, pandas.DataFrame)
        self.assertEqual(catalog.shape[0], rows)
        self.assertEqual(catalog.shape[1], self.table_column_count[table])

    def assert_table_data(self, catalog: Any, rows: int, table: ApdbTables) -> None:
        """Validate catalog type and size

        Parameters
        ----------
        catalog : `object`
            Expected type of this is `ApdbTableData`.
        rows : `int`
            Expected number of rows in a catalog.
        table : `ApdbTables`
            APDB table type.
        extra_columns : `int`
            Count of additional columns expected in ``catalog``.
        """
        self.assertIsInstance(catalog, ApdbTableData)
        n_rows = sum(1 for row in catalog.rows())
        self.assertEqual(n_rows, rows)
        # One extra column for replica chunk id
        self.assertEqual(
            len(catalog.column_names()), self.table_column_count[table] + self.extra_chunk_columns
        )

    def assert_column_types(self, catalog: Any, types: dict[str, felis.datamodel.DataType]) -> None:
        column_defs = dict(catalog.column_defs())
        for column, datatype in types.items():
            self.assertEqual(column_defs[column], datatype)

    def make_region(self, xyz: tuple[float, float, float] = (1.0, 1.0, -1.0)) -> Region:
        """Make a region to use in tests"""
        return _make_region(xyz)

    def test_makeSchema(self) -> None:
        """Test for making APDB schema."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObject))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObjectLast))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaSource))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaForcedSource))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.metadata))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.SSObject))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.SSSource))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObject_To_Object_Match))

        # Test from_uri factory method with the same config.
        with tempfile.NamedTemporaryFile() as tmpfile:
            config.save(tmpfile.name)
            apdb = Apdb.from_uri(tmpfile.name)

        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObject))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObjectLast))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaSource))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaForcedSource))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.metadata))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.SSObject))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.SSSource))
        self.assertIsNotNone(apdb.tableDef(ApdbTables.DiaObject_To_Object_Match))

    def test_empty_gets(self) -> None:
        """Test for getting data from empty database.

        All get() methods should return empty results, only useful for
        checking that code is not broken.
        """
        # use non-zero months for Forced/Source fetching
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = self.make_region()
        visit_time = self.visit_time

        res: pandas.DataFrame | None

        # get objects by region
        res = apdb.getDiaObjects(region)
        self.assert_catalog(res, 0, self.getDiaObjects_table())

        # get sources by region
        res = apdb.getDiaSources(region, None, visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaSource)

        res = apdb.getDiaSources(region, [], visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaSource)

        # get sources by object ID, non-empty object list
        res = apdb.getDiaSources(region, [1, 2, 3], visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaSource)

        # get forced sources by object ID, empty object list
        res = apdb.getDiaForcedSources(region, [], visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaForcedSource)

        # get sources by object ID, non-empty object list
        res = apdb.getDiaForcedSources(region, [1, 2, 3], visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaForcedSource)

        # data_factory's ccdVisitId generation corresponds to (1, 1)
        res = apdb.containsVisitDetector(visit=1, detector=1, region=region, visit_time=visit_time)
        self.assertFalse(res)

        # get sources by region
        if self.fsrc_requires_id_list:
            with self.assertRaises(NotImplementedError):
                apdb.getDiaForcedSources(region, None, visit_time)
        else:
            res = apdb.getDiaForcedSources(region, None, visit_time)
            self.assert_catalog(res, 0, ApdbTables.DiaForcedSource)

    def test_empty_gets_0months(self) -> None:
        """Test for getting data from empty database.

        All get() methods should return empty DataFrame or None.
        """
        # set read_sources_months to 0 so that Forced/Sources are None
        config = self.make_instance(read_sources_months=0, read_forced_sources_months=0)
        apdb = Apdb.from_config(config)

        region = self.make_region()
        visit_time = self.visit_time

        res: pandas.DataFrame | None

        # get objects by region
        res = apdb.getDiaObjects(region)
        self.assert_catalog(res, 0, self.getDiaObjects_table())

        # get sources by region
        res = apdb.getDiaSources(region, None, visit_time)
        self.assertIs(res, None)

        # get sources by object ID, empty object list
        res = apdb.getDiaSources(region, [], visit_time)
        self.assertIs(res, None)

        # get forced sources by object ID, empty object list
        res = apdb.getDiaForcedSources(region, [], visit_time)
        self.assertIs(res, None)

        # Database is empty, no images exist.
        res = apdb.containsVisitDetector(visit=1, detector=1, region=region, visit_time=visit_time)
        self.assertFalse(res)

    def test_storeObjects(self) -> None:
        """Store and retrieve DiaObjects."""
        # don't care about sources.
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = self.make_region()
        visit_time = self.visit_time

        # make catalog with Objects
        catalog = makeObjectCatalog(region, 100)

        # store catalog
        apdb.store(visit_time, catalog)

        # read it back and check sizes
        res = apdb.getDiaObjects(region)
        self.assert_catalog(res, len(catalog), self.getDiaObjects_table())

        # TODO: test apdb.contains with generic implementation from DM-41671

    def test_storeObjects_empty(self) -> None:
        """Test calling storeObject when there are no objects: see DM-43270."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)
        region = self.make_region()
        visit_time = self.visit_time
        # make catalog with no Objects
        catalog = makeObjectCatalog(region, 0)

        with self.assertLogs("lsst.dax.apdb", level="DEBUG") as cm:
            apdb.store(visit_time, catalog)
        self.assertIn("No objects", "\n".join(cm.output))

    def test_storeMovingObject(self) -> None:
        """Store and retrieve DiaObject which changes its position."""
        # don't care about sources.
        config = self.make_instance()
        apdb = Apdb.from_config(config)
        pixelization = self.pixelization(config)

        lon_deg, lat_deg = 0.0, 0.0
        lonlat1 = LonLat.fromDegrees(lon_deg - 1.0, lat_deg)
        lonlat2 = LonLat.fromDegrees(lon_deg + 1.0, lat_deg)
        uv1 = UnitVector3d(lonlat1)
        uv2 = UnitVector3d(lonlat2)

        # Check that they fall into different pixels.
        self.assertNotEqual(pixelization.pixel(uv1), pixelization.pixel(uv2))

        # Store one object at two different positions.
        visit_time1 = self.visit_time
        catalog1 = makeObjectCatalog(lonlat1, 1)
        apdb.store(visit_time1, catalog1)

        visit_time2 = visit_time1 + astropy.time.TimeDelta(120.0, format="sec")
        catalog1 = makeObjectCatalog(lonlat2, 1)
        apdb.store(visit_time2, catalog1)

        # Make region covering both points.
        region = Circle(UnitVector3d(LonLat.fromDegrees(lon_deg, lat_deg)), Angle.fromDegrees(1.1))
        self.assertTrue(region.contains(uv1))
        self.assertTrue(region.contains(uv2))

        # Read it back, must return the latest one.
        res = apdb.getDiaObjects(region)
        self.assert_catalog(res, 1, self.getDiaObjects_table())

    def test_storeSources(self) -> None:
        """Store and retrieve DiaSources."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = self.make_region()
        visit_time = self.visit_time

        # have to store Objects first
        objects = makeObjectCatalog(region, 100)
        oids = list(objects["diaObjectId"])
        sources = makeSourceCatalog(objects, visit_time, use_mjd=self.use_mjd)

        # save the objects and sources
        apdb.store(visit_time, objects, sources)

        # read it back, no ID filtering
        res = apdb.getDiaSources(region, None, visit_time)
        self.assert_catalog(res, len(sources), ApdbTables.DiaSource)

        # read it back and filter by ID
        res = apdb.getDiaSources(region, oids, visit_time)
        self.assert_catalog(res, len(sources), ApdbTables.DiaSource)

        # read it back to get schema
        res = apdb.getDiaSources(region, [], visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaSource)

        # test if a visit is present
        # data_factory's ccdVisitId generation corresponds to (1, 1)
        res = apdb.containsVisitDetector(visit=1, detector=1, region=region, visit_time=visit_time)
        self.assertTrue(res)
        # non-existent image
        res = apdb.containsVisitDetector(visit=2, detector=42, region=region, visit_time=visit_time)
        self.assertFalse(res)

    def test_storeForcedSources(self) -> None:
        """Store and retrieve DiaForcedSources."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = self.make_region()
        visit_time = self.visit_time

        # have to store Objects first
        objects = makeObjectCatalog(region, 100)
        oids = list(objects["diaObjectId"])
        catalog = makeForcedSourceCatalog(objects, visit_time, use_mjd=self.use_mjd)

        apdb.store(visit_time, objects, forced_sources=catalog)

        # read it back and check sizes
        res = apdb.getDiaForcedSources(region, oids, visit_time)
        self.assert_catalog(res, len(catalog), ApdbTables.DiaForcedSource)

        # read it back to get schema
        res = apdb.getDiaForcedSources(region, [], visit_time)
        self.assert_catalog(res, 0, ApdbTables.DiaForcedSource)

        # data_factory's ccdVisitId generation corresponds to (1, 1)
        res = apdb.containsVisitDetector(visit=1, detector=1, region=region, visit_time=visit_time)
        self.assertTrue(res)
        # non-existent image
        res = apdb.containsVisitDetector(visit=2, detector=42, region=region, visit_time=visit_time)
        self.assertFalse(res)

    def test_null_integer_type(self) -> None:
        """Test that integer column with NULLs correct type on select."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = self.make_region()
        visit_time = self.visit_time

        # have to store Objects first
        objects = makeObjectCatalog(region, 100)
        sources = makeSourceCatalog(objects, visit_time, use_mjd=self.use_mjd)
        # Reset some diaObjectIds to NULL.
        sources.loc[0:10, "diaObjectId"] = None

        # save the objects and sources
        apdb.store(visit_time, objects, sources)

        # read it back, no ID filtering
        res = apdb.getDiaSources(region, None, visit_time)
        self.assert_catalog(res, len(sources), ApdbTables.DiaSource)
        assert res is not None, "Expecting catalog, not None"
        self.assertEqual(res.dtypes["diaObjectId"], pandas.Int64Dtype())

    def test_timestamps(self) -> None:
        """Check that timestamp return type is as expected."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = self.make_region()
        visit_time = self.visit_time

        # Cassandra has a millisecond precision, so subtract 1ms to allow for
        # truncated returned values.
        time_before = makeTimestamp(self.processing_time, self.use_mjd, -1)
        objects = makeObjectCatalog(region, 100)
        oids = list(objects["diaObjectId"])
        catalog = makeForcedSourceCatalog(
            objects, visit_time, processing_time=self.processing_time, use_mjd=self.use_mjd
        )
        time_after = makeTimestamp(self.processing_time, self.use_mjd)

        apdb.store(visit_time, objects, forced_sources=catalog)

        # read it back and check sizes
        res = apdb.getDiaForcedSources(region, oids, visit_time)
        assert res is not None
        self.assert_catalog(res, len(catalog), ApdbTables.DiaForcedSource)

        time_processed_column = makeTimestampColumn("time_processed", self.use_mjd)
        self.assertIn(time_processed_column, res.dtypes)
        dtype = res.dtypes[time_processed_column]
        timestamp_type_names = (
            ("float64",) if self.use_mjd else ("datetime64[ms]", "datetime64[us]", "datetime64[ns]")
        )
        self.assertIn(dtype.name, timestamp_type_names)
        # Verify that returned time is sensible.
        self.assertTrue(all(time_before <= dt <= time_after for dt in res[time_processed_column]))

    def test_getDiaObjectsForDedup(self) -> None:
        """Test getDiaObjectsForDedup() method."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region1 = self.make_region((1.0, 1.0, -1.0))
        region2 = self.make_region((-1.0, 1.0, -1.0))
        region3 = self.make_region((-1.0, -1.0, -1.0))
        nobj = 100
        objects1 = makeObjectCatalog(region1, nobj)
        objects2 = makeObjectCatalog(region2, nobj, start_id=nobj * 2)
        objects3 = makeObjectCatalog(region3, nobj, start_id=nobj * 4)

        visits = [
            (astropy.time.Time("2021-01-01T00:00:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-01-01T00:10:00", format="isot", scale="tai"), objects2),
            (astropy.time.Time("2021-01-01T00:20:00", format="isot", scale="tai"), objects3),
        ]

        for visit_time, objects in visits:
            apdb.store(visit_time, objects)

        catalog = apdb.getDiaObjectsForDedup()
        self.assertEqual(len(catalog), 300)

        catalog = apdb.getDiaObjectsForDedup(visits[0][0])
        self.assertEqual(len(catalog), 300)

        catalog = apdb.getDiaObjectsForDedup(visits[1][0])
        self.assertEqual(len(catalog), 200)

        catalog = apdb.getDiaObjectsForDedup(visits[2][0])
        self.assertEqual(len(catalog), 100)

        time = astropy.time.Time("2021-01-01T00:30:00", format="isot", scale="tai")
        catalog = apdb.getDiaObjectsForDedup(time)
        self.assertEqual(len(catalog), 0)

    def test_getDiaSourcesForDiaObjects(self) -> None:
        """Test getDiaSourcesForDiaObjects() method."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)
        # Monkey-patch APDB instance to set current time.
        apdb._current_time = lambda: self.processing_time  # type: ignore[method-assign]

        region1 = self.make_region((1.0, 1.0, -1.0))
        region2 = self.make_region((-1.0, 1.0, -1.0))
        region3 = self.make_region((-1.0, -1.0, -1.0))
        nobj = 100
        objects1 = makeObjectCatalog(region1, nobj)
        objects2 = makeObjectCatalog(region2, nobj, start_id=nobj * 2)
        objects3 = makeObjectCatalog(region3, nobj, start_id=nobj * 4)

        visits = [
            (astropy.time.Time("2021-01-01T00:00:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-01-01T00:10:00", format="isot", scale="tai"), objects2),
            (astropy.time.Time("2021-01-01T00:20:00", format="isot", scale="tai"), objects3),
        ]

        start_id = 1_000_000
        for visit_time, objects in visits:
            sources = makeSourceCatalog(objects, visit_time, start_id=start_id, use_mjd=self.use_mjd)
            apdb.store(visit_time, objects, sources)
            start_id += 1_000_000

        # Take a small number of objects from different regions.
        object_ids = [
            DiaObjectId.from_named_tuple(next(objects1.itertuples())),
            DiaObjectId.from_named_tuple(next(objects2.itertuples())),
            DiaObjectId.from_named_tuple(next(objects3.itertuples())),
        ]

        catalog = apdb.getDiaSourcesForDiaObjects(object_ids, visits[0][0])
        self.assertEqual(len(catalog), 3)
        self.assertEqual(set(catalog["diaObjectId"]), {1, 200, 400})
        self.assertEqual(set(catalog["diaSourceId"]), {1_000_000, 2_000_000, 3_000_000})

        catalog = apdb.getDiaSourcesForDiaObjects(object_ids, visits[2][0])
        self.assertEqual(len(catalog), 1)
        self.assertEqual(set(catalog["diaObjectId"]), {400})
        self.assertEqual(set(catalog["diaSourceId"]), {3_000_000})

    def test_reassignDiaSourcesToDiaObjects(self) -> None:
        """Test reassignDiaSourcesToDiaObjects() method."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)
        apdb._current_time = lambda: self.processing_time  # type: ignore[method-assign]
        apdb_replica = ApdbReplica.from_config(config)

        visit_time = self.visit_time
        lonlat1 = LonLat.fromDegrees(0.0, 0.0)
        lonlat2 = LonLat.fromDegrees(180.0, 0.0)
        # regons around lonlat1/2
        region1 = self.make_region(xyz=(1.0, 0.0, 0.0))
        region2 = self.make_region(xyz=(-1.0, 0.0, 0.0))

        # Store 3 objects and sources at the same position in each region.
        objects = makeObjectCatalog(lonlat1, 3, start_id=100)
        sources = makeSourceCatalog(objects, visit_time, start_id=1000, use_mjd=self.use_mjd)
        apdb.store(visit_time, objects, sources)

        objects = makeObjectCatalog(lonlat2, 3, start_id=200)
        sources = makeSourceCatalog(objects, visit_time, start_id=2000, use_mjd=self.use_mjd)
        apdb.store(visit_time, objects, sources)

        # check that everything as we think it is.
        objects = apdb.getDiaObjects(region1)
        self.assertEqual(set(objects["diaObjectId"]), {100, 101, 102})
        self.assertEqual(list(objects["nDiaSources"]), [1, 1, 1])
        sources = apdb.getDiaSources(region1, [100, 101, 102], visit_time)
        assert sources is not None
        self.assertEqual(set(sources["diaSourceId"]), {1000, 1001, 1002})
        self.assertEqual(set(sources["diaObjectId"]), {100, 101, 102})

        dia_source_ids = [DiaSourceId.from_named_tuple(row) for row in sources.itertuples()]

        # Reassign sources in region1 and increment/decrement nDiaSources.
        reassign = {
            dia_source_id: 100
            for dia_source_id in dia_source_ids
            if dia_source_id.diaSourceId in (1001, 1002)
        }
        apdb.reassignDiaSourcesToDiaObjects(reassign)

        objects = apdb.getDiaObjects(region1)
        self.assertEqual(set(objects["nDiaSources"]), {0, 3})
        sources = apdb.getDiaSources(region1, [100], visit_time)
        assert sources is not None
        self.assertEqual(set(sources["diaSourceId"]), {1000, 1001, 1002})
        self.assertEqual(set(sources["diaObjectId"]), {100})

        sources = apdb.getDiaSources(region2, [201, 202], visit_time)
        assert sources is not None
        self.assertEqual(set(sources["diaSourceId"]), {2001, 2002})
        dia_source_ids = [DiaSourceId.from_named_tuple(row) for row in sources.itertuples()]

        # Reassign but do not increment/decrement nDiaSources.
        reassign = {
            dia_source_id: 200
            for dia_source_id in dia_source_ids
            if dia_source_id.diaSourceId in (2001, 2002)
        }
        apdb.reassignDiaSourcesToDiaObjects(
            reassign, increment_nDiaSources=False, decrement_nDiaSources=False
        )

        objects = apdb.getDiaObjects(region2)
        self.assertEqual(set(objects["nDiaSources"]), {1})
        sources = apdb.getDiaSources(region2, [200], visit_time)
        assert sources is not None
        self.assertEqual(set(sources["diaSourceId"]), {2000, 2001, 2002})
        self.assertEqual(set(sources["diaObjectId"]), {200})

        replica_chunks = apdb_replica.getReplicaChunks()
        if not self.enable_replica:
            self.assertIsNone(replica_chunks)
        else:
            assert replica_chunks is not None

            # There could be one or two chunks.
            self.assertTrue(1 <= len(replica_chunks) <= 2)

            update_records = apdb_replica.getUpdateRecordChunks([chunk.id for chunk in replica_chunks])
            # Two reassignments for region1, three increments/decrements for
            # that region, plus two reassignments for region2 without
            # increments/decrements.
            self.assertEqual(len(update_records), 2 + 3 + 2)

    def test_setValidityEnd(self) -> None:
        """Store DiaObjects and truncate validity for some."""
        # don't care about sources.
        config = self.make_instance()
        apdb = Apdb.from_config(config)
        apdb._current_time = lambda: self.processing_time  # type: ignore[method-assign]
        apdb_replica = ApdbReplica.from_config(config)

        region = self.make_region()
        visit_time = self.visit_time

        # make catalog with Objects
        catalog = makeObjectCatalog(region, 100)

        # store catalog
        apdb.store(visit_time, catalog)

        # read it back and check sizes
        res = apdb.getDiaObjects(region)
        self.assert_catalog(res, 100, self.getDiaObjects_table())

        # Select first 10 objects.
        object_ids = [DiaObjectId.from_named_tuple(row) for row in catalog.iloc[:10].itertuples()]
        count = apdb.setValidityEnd(object_ids, self.processing_time)
        self.assertEqual(count, 10)

        res = apdb.getDiaObjects(region)
        self.assert_catalog(res, 90, self.getDiaObjects_table())

        replica_chunks = apdb_replica.getReplicaChunks()
        if not self.enable_replica:
            self.assertIsNone(replica_chunks)
        else:
            # Check that there are 10 update records in replica tables.
            assert replica_chunks is not None

            # There could be one or two chunks.
            self.assertTrue(1 <= len(replica_chunks) <= 2)

            update_records = apdb_replica.getUpdateRecordChunks([chunk.id for chunk in replica_chunks])
            self.assertEqual(len(update_records), 10)

        # Check that empty list works.
        count = apdb.setValidityEnd(object_ids, self.processing_time)
        self.assertEqual(count, 0)

        # Try with non-existing object.
        object_ids = [DiaObjectId.from_named_tuple(row) for row in catalog.iloc[10:12].itertuples()]
        object_ids += [DiaObjectId(diaObjectId=1_000_000, ra=0.0, dec=0.0)]
        with self.assertRaises(LookupError):
            apdb.setValidityEnd(object_ids, self.processing_time, raise_on_missing_id=True)

        count = apdb.setValidityEnd(object_ids, self.processing_time)
        self.assertEqual(count, 2)

    def test_resetDedup(self) -> None:
        """Test resetDedup method."""
        # don't care about sources.
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = self.make_region()

        # make catalog with Objects
        objects = makeObjectCatalog(region, 100)

        visit_time1 = astropy.time.Time("2021-01-01T00:00:00", format="isot", scale="tai")
        dedup_time1 = astropy.time.Time("2021-01-01T12:00:00", format="isot", scale="tai")
        visit_time2 = astropy.time.Time("2021-01-02T00:00:00", format="isot", scale="tai")
        dedup_time2 = astropy.time.Time("2021-01-02T12:00:00", format="isot", scale="tai")

        # store catalog
        apdb.store(visit_time1, objects)

        catalog = apdb.getDiaObjectsForDedup()
        self.assertEqual(len(catalog), 100)

        catalog = apdb.getDiaObjectsForDedup(visit_time1)
        self.assertEqual(len(catalog), 100)

        apdb.resetDedup(dedup_time1)

        catalog = apdb.getDiaObjectsForDedup(visit_time1)
        self.assertEqual(len(catalog), self._count_after_reset_dedup(100))

        apdb.store(visit_time2, objects)

        catalog = apdb.getDiaObjectsForDedup()
        self.assertEqual(len(catalog), 100)

        catalog = apdb.getDiaObjectsForDedup(dedup_time1)
        self.assertEqual(len(catalog), 100)

        apdb.resetDedup(dedup_time2)

        catalog = apdb.getDiaObjectsForDedup(dedup_time1)
        self.assertEqual(len(catalog), self._count_after_reset_dedup(100))

        catalog = apdb.getDiaObjectsForDedup()
        self.assertEqual(len(catalog), 0)

    def _count_after_reset_dedup(self, count_before: int) -> int:
        """Return the number of rows that will be returned by
        getDiaObjectsForDedup() after resetDedup() was called. For SQL backend
        deduplication data comes from a regular table, and it is not removed
        by resetDedup().
        """
        raise NotImplementedError()

    def test_getChunks(self) -> None:
        """Store and retrieve replica chunks."""
        # don't care about sources.
        config = self.make_instance()
        apdb = Apdb.from_config(config)
        apdb_replica = ApdbReplica.from_config(config)
        visit_time = self.visit_time

        region1 = self.make_region((1.0, 1.0, -1.0))
        region2 = self.make_region((-1.0, -1.0, -1.0))
        nobj = 100
        objects1 = makeObjectCatalog(region1, nobj)
        objects2 = makeObjectCatalog(region2, nobj, start_id=nobj * 2)

        # With the default 10 minutes replica chunk window we should have 4
        # records.
        visits = [
            (astropy.time.Time("2021-01-01T00:01:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-01-01T00:02:00", format="isot", scale="tai"), objects2),
            (astropy.time.Time("2021-01-01T00:11:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-01-01T00:12:00", format="isot", scale="tai"), objects2),
            (astropy.time.Time("2021-01-01T00:45:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-01-01T00:46:00", format="isot", scale="tai"), objects2),
            (astropy.time.Time("2021-03-01T00:01:00", format="isot", scale="tai"), objects1),
            (astropy.time.Time("2021-03-01T00:02:00", format="isot", scale="tai"), objects2),
        ]

        start_id = 0
        for visit_time, objects in visits:
            sources = makeSourceCatalog(objects, visit_time, start_id=start_id, use_mjd=self.use_mjd)
            fsources = makeForcedSourceCatalog(objects, visit_time, visit=start_id, use_mjd=self.use_mjd)
            apdb.store(visit_time, objects, sources, fsources)
            start_id += nobj

        replica_chunks = apdb_replica.getReplicaChunks()
        if not self.enable_replica:
            self.assertIsNone(replica_chunks)

            with self.assertRaisesRegex(ValueError, "APDB is not configured for replication"):
                apdb_replica.getTableDataChunks(ApdbTables.DiaObject, [])

        else:
            assert replica_chunks is not None
            self.assertEqual(len(replica_chunks), 4)

            with self.assertRaisesRegex(ValueError, "does not support replica chunks"):
                apdb_replica.getTableDataChunks(ApdbTables.SSObject, [])

            def _check_chunks(replica_chunks: list[ReplicaChunk], n_records: int | None = None) -> None:
                if n_records is None:
                    n_records = len(replica_chunks) * nobj
                res = apdb_replica.getTableDataChunks(
                    ApdbTables.DiaObject, (chunk.id for chunk in replica_chunks)
                )
                self.assert_table_data(res, n_records, ApdbTables.DiaObject)
                validityStartColumn = "validityStartMjdTai" if self.use_mjd else "validityStart"
                validityStartType = (
                    felis.datamodel.DataType.double if self.use_mjd else felis.datamodel.DataType.timestamp
                )
                self.assert_column_types(
                    res,
                    {
                        "apdb_replica_chunk": felis.datamodel.DataType.long,
                        "diaObjectId": felis.datamodel.DataType.long,
                        validityStartColumn: validityStartType,
                        "ra": felis.datamodel.DataType.double,
                        "dec": felis.datamodel.DataType.double,
                        "parallax": felis.datamodel.DataType.float,
                        "nDiaSources": felis.datamodel.DataType.int,
                    },
                )

                res = apdb_replica.getTableDataChunks(
                    ApdbTables.DiaSource, (chunk.id for chunk in replica_chunks)
                )
                self.assert_table_data(res, n_records, ApdbTables.DiaSource)
                self.assert_column_types(
                    res,
                    {
                        "apdb_replica_chunk": felis.datamodel.DataType.long,
                        "diaSourceId": felis.datamodel.DataType.long,
                        "visit": felis.datamodel.DataType.long,
                        "detector": felis.datamodel.DataType.short,
                    },
                )

                res = apdb_replica.getTableDataChunks(
                    ApdbTables.DiaForcedSource, (chunk.id for chunk in replica_chunks)
                )
                self.assert_table_data(res, n_records, ApdbTables.DiaForcedSource)
                self.assert_column_types(
                    res,
                    {
                        "apdb_replica_chunk": felis.datamodel.DataType.long,
                        "diaObjectId": felis.datamodel.DataType.long,
                        "visit": felis.datamodel.DataType.long,
                        "detector": felis.datamodel.DataType.short,
                    },
                )

            # read it back and check sizes
            _check_chunks(replica_chunks, 800)
            _check_chunks(replica_chunks[1:], 600)
            _check_chunks(replica_chunks[1:-1], 400)
            _check_chunks(replica_chunks[2:3], 200)
            _check_chunks([])

            # try to remove some of those
            deleted_chunks = replica_chunks[:1]
            apdb_replica.deleteReplicaChunks(chunk.id for chunk in deleted_chunks)

            # All queries on deleted ids should return empty set.
            _check_chunks(deleted_chunks, 0)

            replica_chunks = apdb_replica.getReplicaChunks()
            assert replica_chunks is not None
            self.assertEqual(len(replica_chunks), 3)

            _check_chunks(replica_chunks, 600)

    def test_reassignObjects(self) -> None:
        """Reassign DiaObjects."""
        # don't care about sources.
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = self.make_region()
        visit_time = self.visit_time
        objects = makeObjectCatalog(region, 100)
        oids = list(objects["diaObjectId"])
        sources = makeSourceCatalog(objects, visit_time, use_mjd=self.use_mjd)
        apdb.store(visit_time, objects, sources)

        # read it back and filter by ID
        res = apdb.getDiaSources(region, oids, visit_time)
        self.assert_catalog(res, len(sources), ApdbTables.DiaSource)

        apdb.reassignDiaSources({1: 1, 2: 2, 5: 5})
        res = apdb.getDiaSources(region, oids, visit_time)
        self.assert_catalog(res, len(sources) - 3, ApdbTables.DiaSource)

        with self.assertRaisesRegex(ValueError, r"do not exist.*\D1000"):
            apdb.reassignDiaSources(
                {
                    1000: 1,
                    7: 3,
                }
            )
        self.assert_catalog(res, len(sources) - 3, ApdbTables.DiaSource)

    def test_storeUpdateRecord(self) -> None:
        """Test _storeUpdateRecord() method."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        # Times are totally arbitrary.
        update_time_ns1 = 2_000_000_000_000_000_000
        update_time_ns2 = 2_000_000_001_000_000_000
        records = [
            ApdbReassignDiaSourceToSSObjectRecord(
                update_time_ns=update_time_ns1,
                update_order=0,
                diaSourceId=1,
                ssObjectId=1,
                ssObjectReassocTimeMjdTai=60000.0,
                ra=45.0,
                dec=-45.0,
                midpointMjdTai=60000.0,
            ),
            ApdbWithdrawDiaSourceRecord(
                update_time_ns=update_time_ns1,
                update_order=1,
                diaSourceId=123456,
                timeWithdrawnMjdTai=61000.0,
                ra=45.0,
                dec=-45.0,
                midpointMjdTai=60000.0,
            ),
            ApdbReassignDiaSourceToSSObjectRecord(
                update_time_ns=update_time_ns1,
                update_order=3,
                diaSourceId=2,
                ssObjectId=3,
                ssObjectReassocTimeMjdTai=60000.0,
                ra=45.0,
                dec=-45.0,
                midpointMjdTai=60000.0,
            ),
            ApdbWithdrawDiaSourceRecord(
                update_time_ns=update_time_ns2,
                update_order=0,
                diaSourceId=123456,
                timeWithdrawnMjdTai=61000.0,
                ra=45.0,
                dec=-45.0,
                midpointMjdTai=60000.0,
            ),
        ]

        update_time = astropy.time.Time("2021-01-01T00:00:00", format="isot", scale="tai")
        chunk = ReplicaChunk.make_replica_chunk(update_time, 600)

        if not self.enable_replica:
            with self.assertRaises(TypeError):
                self.store_update_records(apdb, records, chunk)
        else:
            self.store_update_records(apdb, records, chunk)

            apdb_replica = ApdbReplica.from_config(config)
            records_returned = apdb_replica.getUpdateRecordChunks([chunk.id])

            # Input records are ordered, output will be ordered too.
            self.assertEqual(records_returned, records)

    @abstractmethod
    def store_update_records(self, apdb: Apdb, records: list[ApdbUpdateRecord], chunk: ReplicaChunk) -> None:
        """Store update records in database, must be overriden in subclass."""
        raise NotImplementedError()

    def test_midpointMjdTai_src(self) -> None:
        """Test for time filtering of DiaSources."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = self.make_region()
        # 2021-01-01 plus 360 days is 2021-12-27
        src_time1 = astropy.time.Time("2021-01-01T00:00:00", format="isot", scale="tai")
        src_time2 = astropy.time.Time("2021-01-01T00:00:02", format="isot", scale="tai")
        visit_time0 = astropy.time.Time("2021-12-26T23:59:59", format="isot", scale="tai")
        visit_time1 = astropy.time.Time("2021-12-27T00:00:01", format="isot", scale="tai")
        visit_time2 = astropy.time.Time("2021-12-27T00:00:03", format="isot", scale="tai")
        one_sec = astropy.time.TimeDelta(1.0, format="sec")

        objects = makeObjectCatalog(region, 100)
        oids = list(objects["diaObjectId"])
        sources = makeSourceCatalog(objects, src_time1, 0, use_mjd=self.use_mjd)
        apdb.store(src_time1, objects, sources)

        sources = makeSourceCatalog(objects, src_time2, 100, use_mjd=self.use_mjd)
        apdb.store(src_time2, objects, sources)

        # reading at time of last save should read all
        res = apdb.getDiaSources(region, oids, src_time2)
        self.assert_catalog(res, 200, ApdbTables.DiaSource)

        # one second before 12 months
        res = apdb.getDiaSources(region, oids, visit_time0)
        self.assert_catalog(res, 200, ApdbTables.DiaSource)

        # reading at later time of last save should only read a subset
        res = apdb.getDiaSources(region, oids, visit_time1)
        self.assert_catalog(res, 100, ApdbTables.DiaSource)

        # reading at later time of last save should only read a subset
        res = apdb.getDiaSources(region, oids, visit_time2)
        self.assert_catalog(res, 0, ApdbTables.DiaSource)

        # Use explicit start time argument instead of 12 month window, visit
        # time does not matter in this case, set it to before all data.
        res = apdb.getDiaSources(region, oids, src_time1 - one_sec, src_time1 - one_sec)
        self.assert_catalog(res, 200, ApdbTables.DiaSource)

        res = apdb.getDiaSources(region, oids, src_time1 - one_sec, src_time2 - one_sec)
        self.assert_catalog(res, 100, ApdbTables.DiaSource)

        res = apdb.getDiaSources(region, oids, src_time1 - one_sec, src_time2 + one_sec)
        self.assert_catalog(res, 0, ApdbTables.DiaSource)

    def test_midpointMjdTai_fsrc(self) -> None:
        """Test for time filtering of DiaForcedSources."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        region = self.make_region()
        src_time1 = astropy.time.Time("2021-01-01T00:00:00", format="isot", scale="tai")
        src_time2 = astropy.time.Time("2021-01-01T00:00:02", format="isot", scale="tai")
        visit_time0 = astropy.time.Time("2021-12-26T23:59:59", format="isot", scale="tai")
        visit_time1 = astropy.time.Time("2021-12-27T00:00:01", format="isot", scale="tai")
        visit_time2 = astropy.time.Time("2021-12-27T00:00:03", format="isot", scale="tai")
        one_sec = astropy.time.TimeDelta(1.0, format="sec")

        objects = makeObjectCatalog(region, 100)
        oids = list(objects["diaObjectId"])
        sources = makeForcedSourceCatalog(objects, src_time1, 1, use_mjd=self.use_mjd)
        apdb.store(src_time1, objects, forced_sources=sources)

        sources = makeForcedSourceCatalog(objects, src_time2, 2, use_mjd=self.use_mjd)
        apdb.store(src_time2, objects, forced_sources=sources)

        # reading at time of last save should read all
        res = apdb.getDiaForcedSources(region, oids, src_time2)
        self.assert_catalog(res, 200, ApdbTables.DiaForcedSource)

        # one second before 12 months
        res = apdb.getDiaForcedSources(region, oids, visit_time0)
        self.assert_catalog(res, 200, ApdbTables.DiaForcedSource)

        # reading at later time of last save should only read a subset
        res = apdb.getDiaForcedSources(region, oids, visit_time1)
        self.assert_catalog(res, 100, ApdbTables.DiaForcedSource)

        # reading at later time of last save should only read a subset
        res = apdb.getDiaForcedSources(region, oids, visit_time2)
        self.assert_catalog(res, 0, ApdbTables.DiaForcedSource)

        # Use explicit start time argument instead of 12 month window, visit
        # time does not matter in this case, set it to before all data.
        res = apdb.getDiaForcedSources(region, oids, src_time1 - one_sec, src_time1 - one_sec)
        self.assert_catalog(res, 200, ApdbTables.DiaForcedSource)

        res = apdb.getDiaForcedSources(region, oids, src_time1 - one_sec, src_time2 - one_sec)
        self.assert_catalog(res, 100, ApdbTables.DiaForcedSource)

        res = apdb.getDiaForcedSources(region, oids, src_time1 - one_sec, src_time2 + one_sec)
        self.assert_catalog(res, 0, ApdbTables.DiaForcedSource)

    def test_metadata(self) -> None:
        """Simple test for writing/reading metadata table"""
        config = self.make_instance()
        apdb = Apdb.from_config(config)
        metadata = apdb.metadata

        # APDB should write two or three metadata items with version numbers
        # and a frozen JSON config.
        self.assertFalse(metadata.empty())
        self.assertEqual(len(list(metadata.items())), self.meta_row_count)

        metadata.set("meta", "data")
        metadata.set("data", "meta")

        self.assertFalse(metadata.empty())
        self.assertTrue(set(metadata.items()) >= {("meta", "data"), ("data", "meta")})

        with self.assertRaisesRegex(KeyError, "Metadata key 'meta' already exists"):
            metadata.set("meta", "data1")

        metadata.set("meta", "data2", force=True)
        self.assertTrue(set(metadata.items()) >= {("meta", "data2"), ("data", "meta")})

        self.assertTrue(metadata.delete("meta"))
        self.assertIsNone(metadata.get("meta"))
        self.assertFalse(metadata.delete("meta"))

        self.assertEqual(metadata.get("data"), "meta")
        self.assertEqual(metadata.get("meta", "meta"), "meta")

    def test_schemaVersionFromYaml(self) -> None:
        """Check version number handling for reading schema from YAML."""
        config = self.make_instance()
        default_schema = config.schema_file
        apdb = Apdb.from_config(config)
        self.assertEqual(apdb.schema.schemaVersion(), VersionTuple(0, 1, 1))

        with update_schema_yaml(default_schema, version="") as schema_file:
            config = self.make_instance(schema_file=schema_file)
            apdb = Apdb.from_config(config)
            self.assertEqual(
                apdb.schema.schemaVersion(),
                VersionTuple(0, 1, 0),
            )

        with update_schema_yaml(default_schema, version="99.0.0") as schema_file:
            config = self.make_instance(schema_file=schema_file)
            apdb = Apdb.from_config(config)
            self.assertEqual(
                apdb.schema.schemaVersion(),
                VersionTuple(99, 0, 0),
            )

    def test_config_freeze(self) -> None:
        """Test that some config fields are correctly frozen in database."""
        config = self.make_instance()

        # `enable_replica` is the only parameter that is frozen in all
        # implementations.
        config.enable_replica = not self.enable_replica
        apdb = Apdb.from_config(config)
        frozen_config = apdb.getConfig()
        self.assertEqual(frozen_config.enable_replica, self.enable_replica)


class ApdbSchemaUpdateTest(TestCaseMixin, ABC):
    """Base class for unit tests that verify how schema changes work."""

    visit_time = astropy.time.Time("2021-01-01T00:00:00", format="isot", scale="tai")

    @abstractmethod
    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make config class instance used in all tests.

        This method should return configuration that point to the identical
        database instance on each call (i.e. ``db_url`` must be the same,
        which also means for sqlite it has to use on-disk storage).
        """
        raise NotImplementedError()

    def make_region(self, xyz: tuple[float, float, float] = (1.0, 1.0, -1.0)) -> Region:
        """Make a region to use in tests"""
        return _make_region(xyz)

    def test_schema_add_replica(self) -> None:
        """Check that new code can work with old schema without replica
        tables.
        """
        # Make schema without replica tables.
        config = self.make_instance(enable_replica=False)
        apdb = Apdb.from_config(config)
        apdb_replica = ApdbReplica.from_config(config)

        # Make APDB instance configured for replication.
        config.enable_replica = True
        apdb = Apdb.from_config(config)

        # Try to insert something, should work OK.
        region = self.make_region()
        visit_time = self.visit_time

        # have to store Objects first
        objects = makeObjectCatalog(region, 100)
        sources = makeSourceCatalog(objects, visit_time)
        fsources = makeForcedSourceCatalog(objects, visit_time)
        apdb.store(visit_time, objects, sources, fsources)

        # There should be no replica chunks.
        replica_chunks = apdb_replica.getReplicaChunks()
        self.assertIsNone(replica_chunks)

    def test_schemaVersionCheck(self) -> None:
        """Check version number compatibility."""
        config = self.make_instance()
        apdb = Apdb.from_config(config)

        self.assertEqual(apdb.schema.schemaVersion(), VersionTuple(0, 1, 1))

        # Claim that schema version is now 99.0.0, must raise an exception.
        with update_schema_yaml(config.schema_file, version="99.0.0") as schema_file:
            config.schema_file = schema_file
            with self.assertRaises(IncompatibleVersionError):
                apdb = Apdb.from_config(config)
                # Version is checked only when we try to do connect.
                apdb.metadata.items()
