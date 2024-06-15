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

__all__ = ["list_cassandra"]

from ..cassandra import ApdbCassandra


def list_cassandra(host: str) -> None:
    """List APDB instances in Cassandra cluster.

    Parameters
    ----------
    host : `str`
        Name of one of the hosts in Cassandra cluster.
    """
    databases = ApdbCassandra.list_databases(host=host)
    for database in sorted(databases):
        print(database)
