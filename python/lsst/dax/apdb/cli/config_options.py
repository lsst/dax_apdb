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

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import click

from ..apdb import ApdbConfig
from ..apdbSql import ApdbSqlConfig

if TYPE_CHECKING:
    from lsst.pex.config import Field


def _option_from_pex_field(
    field: Field,
    *,
    name: str | None = None,
    type: Any | None = None,
    help: str | None = None,
    default: Any | None = None,
    metavar: str | None = None,
    is_flag: bool | None = None,
) -> Callable:
    """Convert pex_config Field into a click option decorator."""
    if name is None:
        name = "--" + field.name.replace("_", "-")
    if help is None:
        help = field.doc
    return click.option(
        name, field.name, type=type, help=help, default=default, metavar=metavar, is_flag=is_flag
    )


read_sources_months = _option_from_pex_field(ApdbConfig.read_sources_months, type=int)

read_forced_sources_months = _option_from_pex_field(ApdbConfig.read_forced_sources_months, type=int)

schema_file = _option_from_pex_field(ApdbConfig.schema_file, metavar="URL")

schema_name = _option_from_pex_field(ApdbConfig.schema_name)

use_insert_id = _option_from_pex_field(ApdbConfig.use_insert_id, is_flag=True)


def common_config_options(f: Callable) -> Callable:
    """Apply decorators for common configuration options."""
    decorators = [
        schema_file,
        schema_name,
        read_sources_months,
        read_forced_sources_months,
        use_insert_id,
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f


# Options for fields in ApdbSqlConfig, db_url is not included.

connection_timeout = _option_from_pex_field(ApdbSqlConfig.connection_timeout, type=int, metavar="SECONDS")

dia_object_index = _option_from_pex_field(
    ApdbSqlConfig.dia_object_index,
    type=click.Choice(choices=["baseline", "pix_id_iov", "last_object_table"]),
)

htm_level = _option_from_pex_field(ApdbSqlConfig.htm_level, type=int)

htm_index_column = _option_from_pex_field(ApdbSqlConfig.htm_index_column)

ra_dec_columns = _option_from_pex_field(
    ApdbSqlConfig.ra_dec_columns,
    help="Names of ra/dec columns in DiaObject table, comma-separated.",
    metavar="COLUMN[,COLUMN,...]",
)

prefix = _option_from_pex_field(ApdbSqlConfig.prefix)

namespace = _option_from_pex_field(ApdbSqlConfig.namespace, metavar="NAME")


def sql_config_options(f: Callable) -> Callable:
    """Apply decorators for configuration options for SQL database."""
    decorators = [
        connection_timeout,
        dia_object_index,
        htm_level,
        htm_index_column,
        ra_dec_columns,
        prefix,
        namespace,
    ]
    for decorator in reversed(decorators):
        f = decorator(f)
    return f
