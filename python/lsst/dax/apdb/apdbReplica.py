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

__all__ = ["ApdbReplica", "ApdbInsertId", "ApdbTableData"]

from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast
from uuid import UUID, uuid4

import astropy.time
from lsst.pex.config import Config
from lsst.resources import ResourcePath, ResourcePathExpression

from .apdb import ApdbConfig
from .apdbIndex import ApdbIndex
from .factory import make_apdb_replica

if TYPE_CHECKING:
    from .versionTuple import VersionTuple


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
    insert_time: astropy.time.Time
    """Time of this insert, usually corresponds to visit time
    (`astropy.time.Time`).
    """

    @classmethod
    def new_insert_id(cls, insert_time: astropy.time.Time) -> ApdbInsertId:
        """Generate new unique insert identifier."""
        return ApdbInsertId(id=uuid4(), insert_time=insert_time)


class ApdbReplica(ABC):
    """Abstract interface for APDB replication methods."""

    @classmethod
    def from_config(cls, config: ApdbConfig) -> ApdbReplica:
        """Create ApdbReplica instance from configuration object.

        Parameters
        ----------
        config : `ApdbConfig`
            Configuration object, type of this object determines type of the
            ApdbReplica implementation.

        Returns
        -------
        replica : `ApdbReplica`
            Instance of `ApdbReplica` class.
        """
        return make_apdb_replica(config)

    @classmethod
    def from_uri(cls, uri: ResourcePathExpression) -> ApdbReplica:
        """Make ApdbReplica instance from a serialized configuration.

        Parameters
        ----------
        uri : `~lsst.resources.ResourcePathExpression`
            URI or local file path pointing to a file with serialized
            configuration, or a string with a "label:" prefix. In the latter
            case, the configuration will be looked up from an APDB index file
            using the label name that follows the prefix. The APDB index file's
            location is determined by the ``DAX_APDB_INDEX_URI`` environment
            variable.

        Returns
        -------
        replica : `ApdbReplica`
            Instance of `ApdbReplica` class, the type of the returned instance
            is determined by configuration.
        """
        if isinstance(uri, str) and uri.startswith("label:"):
            tag, _, label = uri.partition(":")
            index = ApdbIndex()
            # Current format for config files is "pex_config"
            format = "pex_config"
            uri = index.get_apdb_uri(label, format)
        path = ResourcePath(uri)
        config_str = path.read().decode()
        # Assume that this is ApdbConfig, make_apdb will raise if not.
        config = cast(ApdbConfig, Config._fromPython(config_str))
        return make_apdb_replica(config)

    @classmethod
    @abstractmethod
    def apdbReplicaImplementationVersion(cls) -> VersionTuple:
        """Return version number for current ApdbReplica implementation.

        Returns
        -------
        version : `VersionTuple`
            Version of the code defined in implementation class.
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
        """Return catalog of DiaObject records from a given time period
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
        """Return catalog of DiaSource records from a given time period.

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
        """Return catalog of DiaForcedSource records from a given time
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
