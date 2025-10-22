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

__all__ = ["ApdbReplica", "ApdbTableData", "ReplicaChunk"]

import uuid
from abc import ABC, abstractmethod
from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import astropy.time
import felis.datamodel

from lsst.resources import ResourcePathExpression

from .apdb import ApdbConfig, ApdbTables
from .factory import make_apdb_replica

if TYPE_CHECKING:
    from .apdbUpdateRecord import ApdbUpdateRecord
    from .versionTuple import VersionTuple


class ApdbTableData(ABC):
    """Abstract class for representing table data."""

    @abstractmethod
    def column_names(self) -> Sequence[str]:
        """Return ordered sequence of column names in the table.

        Returns
        -------
        names : `~collections.abc.Sequence` [`str`]
            Column names.
        """
        raise NotImplementedError()

    @abstractmethod
    def column_defs(self) -> Sequence[tuple[str, felis.datamodel.DataType]]:
        """Return ordered sequence of column names and their types.

        Returns
        -------
        columns : `~collections.abc.Sequence` \
            [`tuple`[`str`, `felis.datamodel.DataType`]]
            Sequence of 2-tuples, each tuple consists of column name and its
            type.
        """
        raise NotImplementedError()

    @abstractmethod
    def rows(self) -> Collection[tuple]:
        """Return table rows, each row is a tuple of values.

        Returns
        -------
        rows : `~collections.abc.Collection` [`tuple`]
            Collection of tuples.
        """
        raise NotImplementedError()


@dataclass(frozen=True)
class ReplicaChunk:
    """Class used for identification of replication chunks.

    Instances of this class are used to identify the units of transfer from
    APDB to PPDB. Usually single `ReplicaChunk` corresponds to multiple
    consecutive calls to `Apdb.store` method.

    Every ``store`` with the same ``id`` value will update ``unique_id`` with
    some unique value so that it can be verified on PPDB side.
    """

    id: int
    """A number identifying replication chunk (`int`)."""

    last_update_time: astropy.time.Time
    """Time of last insert for this chunk, usually corresponds to visit time
    (`astropy.time.Time`).
    """

    unique_id: uuid.UUID
    """Unique value updated on each new store (`uuid.UUID`)."""

    @classmethod
    def make_replica_chunk(
        cls, last_update_time: astropy.time.Time, chunk_window_seconds: int
    ) -> ReplicaChunk:
        """Generate new unique insert identifier."""
        seconds = int(last_update_time.unix_tai)
        seconds = (seconds // chunk_window_seconds) * chunk_window_seconds
        unique_id = uuid.uuid4()
        return ReplicaChunk(id=seconds, last_update_time=last_update_time, unique_id=unique_id)

    def __str__(self) -> str:
        class_name = self.__class__.__name__
        time_str = str(self.last_update_time.tai.isot)
        return f"{class_name}(id={self.id:10d}, last_update_time={time_str}/tai, unique_id={self.unique_id})"


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
        config = ApdbConfig.from_uri(uri)
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
    def schemaVersion(self) -> VersionTuple:
        """Return version number of the database schema.

        Returns
        -------
        version : `VersionTuple`
            Version of the database schema.
        """
        raise NotImplementedError()

    @abstractmethod
    def getReplicaChunks(self) -> list[ReplicaChunk] | None:
        """Return collection of replication chunks known to the database.

        Returns
        -------
        chunks : `list` [`ReplicaChunk`] or `None`
            List of chunks, they may be time-ordered if database supports
            ordering. `None` is returned if database is not configured for
            replication.
        """
        raise NotImplementedError()

    @abstractmethod
    def deleteReplicaChunks(self, chunks: Iterable[int]) -> None:
        """Remove replication chunks from the database.

        Parameters
        ----------
        chunks : `~collections.abc.Iterable` [`int`]
            Chunk identifiers to remove.

        Notes
        -----
        This method causes Apdb to forget about specified chunks. If there
        are any auxiliary data associated with the identifiers, it is also
        removed from database (but data in regular tables is not removed).
        This method should be called after successful transfer of data from
        APDB to PPDB to free space used by replicas.
        """
        raise NotImplementedError()

    @abstractmethod
    def getTableDataChunks(self, table: ApdbTables, chunks: Iterable[int]) -> ApdbTableData:
        """Return catalog of new records for a table from given replica chunks.

        Parameters
        ----------
        table : `ApdbTables`
            Table for which to return the data. Acceptable tables are
            `ApdbTables.DiaObject`, `ApdbTables.DiaSource`, and
            `ApdbTables.DiaForcedSource`.
        chunks : `~collections.abc.Iterable` [`int`]
            Chunk identifiers to return.

        Returns
        -------
        data : `ApdbTableData`
            Catalog containing table records. In addition to all regular
            columns it will contain ``apdb_replica_chunk`` column.

        Notes
        -----
        This method returns new records that have been added to the table by
        `Apdb.store()` method. Updates to the records that happen at later time
        are available from `getTableUpdateChunks` method.

        This part of API may not be very stable and can change before the
        implementation finalizes.
        """
        raise NotImplementedError()

    @abstractmethod
    def getUpdateRecordChunks(self, chunks: Iterable[int]) -> Sequence[ApdbUpdateRecord]:
        """Return the list of record updates from given replica chunks.

        Parameters
        ----------
        chunks : `~collections.abc.Iterable` [`int`]
            Chunk identifiers to return.

        Returns
        -------
        records : `~collections.abc.Sequence` [`ApdbUpdateRecord`]
            Collection of update records. Records will be sorted according
            their update time and update order.
        """
        raise NotImplementedError()
