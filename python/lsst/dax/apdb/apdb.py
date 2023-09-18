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

__all__ = ["ApdbConfig", "Apdb", "ApdbInsertId", "ApdbTableData"]

import os
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Optional
from uuid import UUID, uuid4

import lsst.daf.base as dafBase
import pandas
from felis.simple import Table
from lsst.pex.config import Config, ConfigurableField, Field
from lsst.sphgeom import Region

from .apdbSchema import ApdbTables


def _data_file_name(basename: str) -> str:
    """Return path name of a data file in sdm_schemas package."""
    return os.path.join("${SDM_SCHEMAS_DIR}", "yml", basename)


class ApdbConfig(Config):
    """Part of Apdb configuration common to all implementations."""

    read_sources_months = Field[int](doc="Number of months of history to read from DiaSource", default=12)
    read_forced_sources_months = Field[int](
        doc="Number of months of history to read from DiaForcedSource", default=12
    )
    schema_file = Field[str](
        doc="Location of (YAML) configuration file with standard schema", default=_data_file_name("apdb.yaml")
    )
    schema_name = Field[str](doc="Name of the schema in YAML configuration file.", default="ApdbSchema")
    extra_schema_file = Field[str](
        doc="Location of (YAML) configuration file with extra schema, "
        "definitions in this file are merged with the definitions in "
        "'schema_file', extending or replacing parts of the schema.",
        default=None,
        optional=True,
        deprecated="This field is deprecated, its value is not used.",
    )
    use_insert_id = Field[bool](
        doc=(
            "If True, make and fill additional tables used for getHistory methods. "
            "Databases created with earlier versions of APDB may not have these tables, "
            "and corresponding methods will not work for them."
        ),
        default=False,
    )


class ApdbTableData(ABC):
    """Abstract class for representing table data."""

    @abstractmethod
    def column_names(self) -> list[str]:
        """Return ordered sequence of column names in the table.

        Returns
        -------
        names : `list` [`str`]
            Column names.
        """
        raise NotImplementedError()

    @abstractmethod
    def rows(self) -> Iterable[tuple]:
        """Return table rows, each row is a tuple of values.

        Returns
        -------
        rows : `iterable` [`tuple`]
            Iterable of tuples.
        """
        raise NotImplementedError()


@dataclass(frozen=True)
class ApdbInsertId:
    """Class used to identify single insert operation.

    Instances of this class are used to identify the units of transfer from
    APDB to PPDB. Usually single `ApdbInsertId` corresponds to a single call to
    `store` method.
    """

    id: UUID

    @classmethod
    def new_insert_id(cls) -> ApdbInsertId:
        """Generate new unique insert identifier."""
        return ApdbInsertId(id=uuid4())


class Apdb(ABC):
    """Abstract interface for APDB."""

    ConfigClass = ApdbConfig

    @abstractmethod
    def tableDef(self, table: ApdbTables) -> Optional[Table]:
        """Return table schema definition for a given table.

        Parameters
        ----------
        table : `ApdbTables`
            One of the known APDB tables.

        Returns
        -------
        tableSchema : `felis.simple.Table` or `None`
            Table schema description, `None` is returned if table is not
            defined by this implementation.
        """
        raise NotImplementedError()

    @abstractmethod
    def makeSchema(self, drop: bool = False) -> None:
        """Create or re-create whole database schema.

        Parameters
        ----------
        drop : `bool`
            If True then drop all tables before creating new ones.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDiaObjects(self, region: Region) -> pandas.DataFrame:
        """Return catalog of DiaObject instances from a given region.

        This method returns only the last version of each DiaObject. Some
        records in a returned catalog may be outside the specified region, it
        is up to a client to ignore those records or cleanup the catalog before
        futher use.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region to search for DIAObjects.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog containing DiaObject records for a region that may be a
            superset of the specified region.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDiaSources(
        self, region: Region, object_ids: Optional[Iterable[int]], visit_time: dafBase.DateTime
    ) -> Optional[pandas.DataFrame]:
        """Return catalog of DiaSource instances from a given region.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region to search for DIASources.
        object_ids : iterable [ `int` ], optional
            List of DiaObject IDs to further constrain the set of returned
            sources. If `None` then returned sources are not constrained. If
            list is empty then empty catalog is returned with a correct
            schema.
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit.

        Returns
        -------
        catalog : `pandas.DataFrame`, or `None`
            Catalog containing DiaSource records. `None` is returned if
            ``read_sources_months`` configuration parameter is set to 0.

        Notes
        -----
        This method returns DiaSource catalog for a region with additional
        filtering based on DiaObject IDs. Only a subset of DiaSource history
        is returned limited by ``read_sources_months`` config parameter, w.r.t.
        ``visit_time``. If ``object_ids`` is empty then an empty catalog is
        always returned with the correct schema (columns/types). If
        ``object_ids`` is `None` then no filtering is performed and some of the
        returned records may be outside the specified region.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDiaForcedSources(
        self, region: Region, object_ids: Optional[Iterable[int]], visit_time: dafBase.DateTime
    ) -> Optional[pandas.DataFrame]:
        """Return catalog of DiaForcedSource instances from a given region.

        Parameters
        ----------
        region : `lsst.sphgeom.Region`
            Region to search for DIASources.
        object_ids : iterable [ `int` ], optional
            List of DiaObject IDs to further constrain the set of returned
            sources. If list is empty then empty catalog is returned with a
            correct schema. If `None` then returned sources are not
            constrained. Some implementations may not support latter case.
        visit_time : `lsst.daf.base.DateTime`
            Time of the current visit.

        Returns
        -------
        catalog : `pandas.DataFrame`, or `None`
            Catalog containing DiaSource records. `None` is returned if
            ``read_forced_sources_months`` configuration parameter is set to 0.

        Raises
        ------
        NotImplementedError
            May be raised by some implementations if ``object_ids`` is `None`.

        Notes
        -----
        This method returns DiaForcedSource catalog for a region with
        additional filtering based on DiaObject IDs. Only a subset of DiaSource
        history is returned limited by ``read_forced_sources_months`` config
        parameter, w.r.t. ``visit_time``. If ``object_ids`` is empty then an
        empty catalog is always returned with the correct schema
        (columns/types). If ``object_ids`` is `None` then no filtering is
        performed and some of the returned records may be outside the specified
        region.
        """
        raise NotImplementedError()

    @abstractmethod
    def getInsertIds(self) -> list[ApdbInsertId] | None:
        """Return collection of insert identifiers known to the database.

        Returns
        -------
        ids : `list` [`ApdbInsertId`] or `None`
            List of identifiers, they may be time-ordered if database supports
            ordering. `None` is returned if database is not configured to store
            insert identifiers.
        """
        raise NotImplementedError()

    @abstractmethod
    def deleteInsertIds(self, ids: Iterable[ApdbInsertId]) -> None:
        """Remove insert identifiers from the database.

        Parameters
        ----------
        ids : `iterable` [`ApdbInsertId`]
            Insert identifiers, can include items returned from `getInsertIds`.

        Notes
        -----
        This method causes Apdb to forget about specified identifiers. If there
        are any auxiliary data associated with the identifiers, it is also
        removed from database (but data in regular tables is not removed).
        This method should be called after successful transfer of data from
        APDB to PPDB to free space used by history.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDiaObjectsHistory(self, ids: Iterable[ApdbInsertId]) -> ApdbTableData:
        """Return catalog of DiaObject instances from a given time period
        including the history of each DiaObject.

        Parameters
        ----------
        ids : `iterable` [`ApdbInsertId`]
            Insert identifiers, can include items returned from `getInsertIds`.

        Returns
        -------
        data : `ApdbTableData`
            Catalog containing DiaObject records. In addition to all regular
            columns it will contain ``insert_id`` column.

        Notes
        -----
        This part of API may not be very stable and can change before the
        implementation finalizes.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDiaSourcesHistory(self, ids: Iterable[ApdbInsertId]) -> ApdbTableData:
        """Return catalog of DiaSource instances from a given time period.

        Parameters
        ----------
        ids : `iterable` [`ApdbInsertId`]
            Insert identifiers, can include items returned from `getInsertIds`.

        Returns
        -------
        data : `ApdbTableData`
            Catalog containing DiaSource records. In addition to all regular
            columns it will contain ``insert_id`` column.

        Notes
        -----
        This part of API may not be very stable and can change before the
        implementation finalizes.
        """
        raise NotImplementedError()

    @abstractmethod
    def getDiaForcedSourcesHistory(self, ids: Iterable[ApdbInsertId]) -> ApdbTableData:
        """Return catalog of DiaForcedSource instances from a given time
        period.

        Parameters
        ----------
        ids : `iterable` [`ApdbInsertId`]
            Insert identifiers, can include items returned from `getInsertIds`.

        Returns
        -------
        data : `ApdbTableData`
            Catalog containing DiaForcedSource records. In addition to all
            regular columns it will contain ``insert_id`` column.

        Notes
        -----
        This part of API may not be very stable and can change before the
        implementation finalizes.
        """
        raise NotImplementedError()

    @abstractmethod
    def getSSObjects(self) -> pandas.DataFrame:
        """Return catalog of SSObject instances.

        Returns
        -------
        catalog : `pandas.DataFrame`
            Catalog containing SSObject records, all existing records are
            returned.
        """
        raise NotImplementedError()

    @abstractmethod
    def store(
        self,
        visit_time: dafBase.DateTime,
        objects: pandas.DataFrame,
        sources: Optional[pandas.DataFrame] = None,
        forced_sources: Optional[pandas.DataFrame] = None,
    ) -> None:
        """Store all three types of catalogs in the database.

        Parameters
        ----------
        visit_time : `lsst.daf.base.DateTime`
            Time of the visit.
        objects : `pandas.DataFrame`
            Catalog with DiaObject records.
        sources : `pandas.DataFrame`, optional
            Catalog with DiaSource records.
        forced_sources : `pandas.DataFrame`, optional
            Catalog with DiaForcedSource records.

        Notes
        -----
        This methods takes DataFrame catalogs, their schema must be
        compatible with the schema of APDB table:

          - column names must correspond to database table columns
          - types and units of the columns must match database definitions,
            no unit conversion is performed presently
          - columns that have default values in database schema can be
            omitted from catalog
          - this method knows how to fill interval-related columns of DiaObject
            (validityStart, validityEnd) they do not need to appear in a
            catalog
          - source catalogs have ``diaObjectId`` column associating sources
            with objects
        """
        raise NotImplementedError()

    @abstractmethod
    def storeSSObjects(self, objects: pandas.DataFrame) -> None:
        """Store or update SSObject catalog.

        Parameters
        ----------
        objects : `pandas.DataFrame`
            Catalog with SSObject records.

        Notes
        -----
        If SSObjects with matching IDs already exist in the database, their
        records will be updated with the information from provided records.
        """
        raise NotImplementedError()

    @abstractmethod
    def reassignDiaSources(self, idMap: Mapping[int, int]) -> None:
        """Associate DiaSources with SSObjects, dis-associating them
        from DiaObjects.

        Parameters
        ----------
        idMap : `Mapping`
            Maps DiaSource IDs to their new SSObject IDs.

        Raises
        ------
        ValueError
            Raised if DiaSource ID does not exist in the database.
        """
        raise NotImplementedError()

    @abstractmethod
    def dailyJob(self) -> None:
        """Implement daily activities like cleanup/vacuum.

        What should be done during daily activities is determined by
        specific implementation.
        """
        raise NotImplementedError()

    @abstractmethod
    def countUnassociatedObjects(self) -> int:
        """Return the number of DiaObjects that have only one DiaSource
        associated with them.

        Used as part of ap_verify metrics.

        Returns
        -------
        count : `int`
            Number of DiaObjects with exactly one associated DiaSource.

        Notes
        -----
        This method can be very inefficient or slow in some implementations.
        """
        raise NotImplementedError()

    @classmethod
    def makeField(cls, doc: str) -> ConfigurableField:
        """Make a `~lsst.pex.config.ConfigurableField` for Apdb.

        Parameters
        ----------
        doc : `str`
            Help text for the field.

        Returns
        -------
        configurableField : `lsst.pex.config.ConfigurableField`
            A `~lsst.pex.config.ConfigurableField` for Apdb.
        """
        return ConfigurableField(doc=doc, target=cls)
