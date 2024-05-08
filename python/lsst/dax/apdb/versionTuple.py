# This file is part of dax_apdb.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
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

__all__ = [
    "IncompatibleVersionError",
    "VersionTuple",
]

from typing import NamedTuple


class IncompatibleVersionError(RuntimeError):
    """Exception raised when version numbers are not compatible."""


class VersionTuple(NamedTuple):
    """Class representing a version number.

    Parameters
    ----------
    major, minor, patch : `int`
        Version number components
    """

    major: int
    minor: int
    patch: int

    @classmethod
    def fromString(cls, versionStr: str) -> VersionTuple:
        """Extract version number from a string.

        Parameters
        ----------
        versionStr : `str`
            Version number in string form "X.Y.Z", all components must be
            present.

        Returns
        -------
        version : `VersionTuple`
            Parsed version tuple.

        Raises
        ------
        ValueError
            Raised if string has an invalid format.
        """
        try:
            version = tuple(int(v) for v in versionStr.split("."))
        except ValueError as exc:
            raise ValueError(f"Invalid version  string '{versionStr}'") from exc
        if len(version) != 3:
            raise ValueError(f"Invalid version  string '{versionStr}', must consist of three numbers")
        return cls(*version)

    def checkCompatibility(self, database_version: VersionTuple) -> bool:
        """Compare implementation schema version with schema version in
        database.

        Parameters
        ----------
        database_version : `VersionTuple`
            Version of the database schema.

        Returns
        -------
        compatible : `bool`
            True if schema versions are compatible.

        Notes
        -----
        This method implements default rules for checking schema compatibility:

            - if major numbers differ, schemas are not compatible;
            - otherwise, if minor versions are different then newer version can
              read and write schema made by older version; older version can
              neither read nor write into newer schema;
            - otherwise, different patch versions are totally compatible.
        """
        if self.major != database_version.major:
            # different major versions are not compatible at all
            return False
        if self.minor != database_version.minor:
            # Different minor versions are backward compatible.
            return self.minor > database_version.minor
        # patch difference does not matter
        return True

    def __str__(self) -> str:
        """Transform version tuple into a canonical string form."""
        return f"{self.major}.{self.minor}.{self.patch}"
