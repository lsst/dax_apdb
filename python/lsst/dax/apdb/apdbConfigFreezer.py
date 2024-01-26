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

__all__ = ["ApdbConfigFreezer"]

import json
from collections.abc import Iterable
from typing import Generic, TypeVar

from .apdb import ApdbConfig

_Config = TypeVar("_Config", bound=ApdbConfig)


class ApdbConfigFreezer(Generic[_Config]):
    """Class that handles freezing of the configuration parameters, this is
    an implementation detail for use in Apdb subclasses.

    Parameters
    ----------
    field_names : `~collections.abc.Iterable` [`str`]
        Names of configuration fields to be frozen.
    """

    def __init__(self, field_names: Iterable[str]):
        self._field_names = list(field_names)

    def to_json(self, config: ApdbConfig) -> str:
        """Convert part of the configuration object to JSON string.

        Parameters
        ----------
        config : `ApdbConfig`
            Configuration object.

        Returns
        -------
        json_str : `str`
            JSON representation of the frozen part of the config.
        """
        config_dict = config.toDict()
        json_dict = {name: config_dict[name] for name in self._field_names}
        return json.dumps(json_dict)

    def update(self, config: _Config, json_str: str) -> _Config:
        """Update configuration field values from a JSON string.

        Parameters
        ----------
        config : `ApdbConfig`
            Configuration object.
        json_str : str
            String containing JSON representation of configuration.

        Returns
        -------
        updated : `ApdbConfig`
            Copy of the ``config`` with some fields updated from JSON object.

        Raises
        ------
        TypeError
            Raised if JSON string does not represent JSON object.
        ValueError
            Raised if JSON object contains key which is not present in
            ``field_names``.
        """
        data = json.loads(json_str)
        if not isinstance(data, dict):
            raise TypeError(f"JSON string must be convertible to object: {json_str!r}")

        new_config = type(config)(**config.toDict())
        for key, value in data.items():
            if key not in self._field_names:
                raise ValueError(f"JSON object contains unknown key: {key}")
            setattr(new_config, key, value)

        return new_config
