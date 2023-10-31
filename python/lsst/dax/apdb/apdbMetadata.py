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

__all__ = ["ApdbMetadata"]

from abc import ABC, abstractmethod
from collections.abc import Generator


class ApdbMetadata(ABC):
    """Interface for accessing APDB metadata.

    Metadata is a collection of key/value items usually stored in a special
    table in the database. This abstract interface provides methods for
    accessing and modifying it.
    """

    @abstractmethod
    def get(self, key: str, default: str | None = None) -> str | None:
        """Retrieve value of a given metadata record.

        Parameters
        ----------
        key : `str`
            Metadata key, arbitrary non-empty string.
        default : `str`, optional
            Default value returned when key does not exist, can be string
            or `None`.

        Returns
        -------
        value : `str` or `None`
            Metadata value, if key does not exist then ``default`` is returned.
        """
        raise NotImplementedError()

    @abstractmethod
    def set(self, key: str, value: str, *, force: bool = False) -> None:
        """Set value for a given metadata record.

        Parameters
        ----------
        key : `str`
            Metadata key, arbitrary non-empty string.
        value : `str`
            New metadata value, an arbitrary string. Due to deficiencies of
            some database engines we are not allowing empty strings to be
            stored in the database, and ``value`` cannot be an empty string.
        force : `bool`, optional
            Controls handling of existing metadata. With default `False`
            value an exception is raised if ``key`` already exists, if `True`
            is passed then value of the existing key will be updated.

        Raises
        ------
        KeyError
            Raised if key already exists but ``force`` option is false.
        ValueError
            Raised if key or value parameters are empty.
        """
        raise NotImplementedError()

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete metadata record.

        Parameters
        ----------
        key : `str`
            Metadata key, arbitrary non-empty string.

        Returns
        -------
        existed : `bool`
            `True` is returned if attribute existed before it was deleted.
        """
        raise NotImplementedError()

    @abstractmethod
    def items(self) -> Generator[tuple[str, str], None, None]:
        """Iterate over records and yield their keys and values.

        Yields
        ------
        key : `str`
            Metadata key.
        value : `str`
            Corresponding metadata value.
        """
        raise NotImplementedError()

    @abstractmethod
    def empty(self) -> bool:
        """Check whether attributes set is empty.

        Returns
        -------
        empty : `bool`
            True if there are no any attributes defined.
        """
        raise NotImplementedError()
