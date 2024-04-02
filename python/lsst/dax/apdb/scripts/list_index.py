# This file is part of dax_apdb
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

__all__ = ["list_index"]

from ..apdbIndex import ApdbIndex


def list_index(index_path: str | None) -> None:
    """List contents of APDB index file.

    Parameters
    ----------
    index_path : `str`, optional
        Location of index file, if missing then $DAX_APDB_INDEX_URI is used.
    """
    index = ApdbIndex(index_path)
    entries = index.get_entries()
    for label, uri in sorted(entries.items()):
        print(f"{label}: {uri}")
