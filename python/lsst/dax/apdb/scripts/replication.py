# This file is part of dax_ppdb
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

from __future__ import annotations

__all__ = ["replication_delete_chunks", "replication_list_chunks"]

import sys
from collections.abc import Collection

from ..apdbReplica import ApdbReplica, ReplicaChunk


def replication_list_chunks(apdb_config: str) -> None:
    """Print full list of replica chunks existing in APDB.

    Parameters
    ----------
    apdb_config : `str`
        URL for APDB configuration file.
    """
    apdb = ApdbReplica.from_uri(apdb_config)
    chunks = apdb.getReplicaChunks()
    if chunks is not None:
        chunks = sorted(chunks, key=lambda chunk: chunk.id)
        _print_chunks(chunks)
    else:
        print("APDB instance does not support replication")


def replication_delete_chunks(apdb_config: str, chunk_id: int, force: bool, print_only: bool) -> int:
    """Delete replication chunks from APDB.

    Parameters
    ----------
    apdb_config : `str`
        URL for APDB configuration file.
    chunk_id : `int`
        Chunk to delete, all earlier chunks are deleted as well.
    force : `bool`
        If `True` do not ask confirmation.
    print_only : `bool`
        If `True` print the list of chunks, but do not delete anything.
    """
    apdb = ApdbReplica.from_uri(apdb_config)
    chunks = apdb.getReplicaChunks()
    if chunks is None:
        print("APDB instance does not support replications")
    else:
        chunks = sorted(chunks, key=lambda chunk: chunk.id)
        # Chunks to delete.
        chunks = [chunk for chunk in chunks if chunk.id <= chunk_id]
        # Check that given chunk ID actually exists.
        if not chunks or chunks[-1].id != chunk_id:
            print(f"ERROR: Replication chunk with ID={chunk_id} does not exist", file=sys.stderr)
            return 1

        if print_only:
            print("Following chunks will be deleted:")
            _print_chunks(chunks)
            return 0

        if not force:
            try:
                response = input(f"{len(chunks)} chunks will be removed, y[n]? ")
            except EOFError:
                response = ""
            if response not in ("y", "Y"):
                return 0

        apdb.deleteReplicaChunks(chunk.id for chunk in chunks)

    return 0


def _print_chunks(chunks: Collection[ReplicaChunk]) -> None:
    """Print the list of chunks.

    Parameters
    ----------
    chunks : `~collections.abc.Collection` [`ReplicaChunk`]
        Chunks to print.
    """
    print(" Chunk Id            Update time                      Unique Id")
    sep = "-" * 77
    print(sep)
    for chunk in chunks:
        insert_time = chunk.last_update_time
        print(f"{chunk.id:10d}  {insert_time.tai.isot}/tai  {chunk.unique_id}")
    print(sep)
    print(f"Total: {len(chunks)}")
