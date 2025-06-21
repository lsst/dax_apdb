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

__all__ = ["delete_cassandra"]

from ..cassandra.apdbCassandraAdmin import ApdbCassandraAdmin


def delete_cassandra(host: str, keyspace: str, confirm: bool) -> None:
    """Delete APDB instance from Cassandra cluster.

    Parameters
    ----------
    host : `str`
        Name of one of the hosts in Cassandra cluster.
    keyspace : `str`
        Cassandra keyspace name containing APDB tables.
    confirm : `bool`
        If True do not ask for confirmation.
    """
    if not confirm:
        try:
            answer = input(f"Type 'yes' to confirm deletion of keyspace {keyspace!r} and all of its data: ")
            if answer != "yes":
                return
        except EOFError:
            return
    ApdbCassandraAdmin.delete_database(host=host, keyspace=keyspace)
