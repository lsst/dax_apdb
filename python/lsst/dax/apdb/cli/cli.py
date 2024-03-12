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

__all__ = ["cli"]

import logging
from typing import Any

import click

from .. import scripts
from . import config_options, options

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(context_settings=CONTEXT_SETTINGS)
def cli() -> None:
    """APDB command line tools."""
    logging.basicConfig(level=logging.INFO)


@cli.command(short_help="Create new APDB instance in SQL database.")
@click.argument("db-url")
@click.argument("config-path", type=click.Path(exists=False, dir_okay=False, writable=True))
@config_options.common_config_options
@config_options.sql_config_options
@options.drop
def create_sql(*args: Any, **kwargs: Any) -> None:
    """Create new APDB database and generate its configuration file.

    DB_URL is database URL in SQLAlchemy format for APDB instance. CONFIG_PATH
    is the name of the new configuration file for created APDB instance.

    Options specify a subset of ApdbSql configuration options, some of the
    options are used for database initialization and are also stored in the
    database. Other options are simply written into output configuration file.
    Configuration file can be edited later, but options that are stored in the
    database always override configuration file.
    """
    scripts.create_sql(*args, **kwargs)
