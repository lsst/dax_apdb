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

from astropy.table import Table

from ..cassandra import ApdbCassandra


def list_cassandra(host: str, verbose: bool) -> None:
    """List APDB instances in Cassandra cluster.

    Parameters
    ----------
    host : `str`
        Name of one of the hosts in Cassandra cluster.
    verbose : `bool`
        If `True` provide detailed output.
    """
    databases = ApdbCassandra.list_databases(host=host)
    if verbose:
        columns = ["Keyspace", "Role", "Permissions"]
        sort_columns = columns[:2]
    else:
        columns = ["Keyspace", "Roles[access]"]
        sort_columns = columns[:1]
    if databases:
        table = Table(names=columns, dtype=[str] * len(columns))
        for database in databases:
            if database.permissions:
                if verbose:
                    for role, perm_list in database.permissions.items():
                        permissions = ", ".join(sorted(perm_list))
                        table.add_row([database.name, role, permissions])
                else:
                    roles = []
                    for role, perm_list in database.permissions.items():
                        access = _access(perm_list)
                        roles.append(f"{role}[{access}]")
                    table.add_row([database.name, " ".join(sorted(roles))])
            else:
                table.add_row([database.name] + ["-"] * (len(columns) - 1))
        table.sort(sort_columns)
        table.pprint_all(align="<")


def _access(permissions: list[str]) -> str:
    """Convert list of Cassandra permissions into access mode string"""
    perms = set(permissions)
    if {"CREATE", "DROP"} & perms:
        return "manage"
    elif "MODIFY" in perms:
        return "update"
    elif "SELECT" in perms:
        return "read"
    else:
        return "none"
