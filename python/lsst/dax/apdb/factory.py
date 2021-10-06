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

__all__ = ["make_apdb"]

from .apdb import Apdb, ApdbConfig
from .apdbSql import ApdbSql, ApdbSqlConfig


def make_apdb(config: ApdbConfig) -> Apdb:
    """Create Apdb instance based on Apdb configuration.

    Parameters
    ----------
    config : `ApdbConfig`
        Configuration object, sub-class of ApdbConfig.

    Returns
    -------
    apdb : `Apdb`
        Instance of a specific Apdb sub-class.

    Raises
    ------
    TypeError
        Raised if type of ``config`` does not match any known types.
    """
    if type(config) is ApdbSqlConfig:
        return ApdbSql(config)
    raise TypeError(f"Unknown type of config object: {type(config)}")
