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

import argparse
from collections.abc import Sequence

from .. import scripts
from . import options
from .logging_cli import LoggingCli


def cli(args: Sequence | None = None) -> None:
    """APDB command line tools."""
    parser = argparse.ArgumentParser(description="APDB command line tools")
    log_cli = LoggingCli(parser)

    subparsers = parser.add_subparsers(title="available subcommands", required=True)
    _create_sql_subcommand(subparsers)
    _create_cassandra_subcommand(subparsers)
    _list_index_subcommand(subparsers)

    parsed_args = parser.parse_args(args)
    log_cli.process_args(parsed_args)

    kwargs = vars(parsed_args)
    # Strip keywords not understood by scripts.
    method = kwargs.pop("method")
    method(**kwargs)


def _create_sql_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("create-sql", help="Create new APDB instance in SQL database.")
    parser.add_argument("db_url", help="Database URL in SQLAlchemy format for APDB instance.")
    parser.add_argument("config_path", help="Name of the new configuration file for created APDB instance.")
    options.common_apdb_options(parser)
    options.sql_config_options(parser)
    parser.add_argument(
        "--drop", help="If True then drop existing tables.", default=False, action="store_true"
    )
    parser.set_defaults(method=scripts.create_sql)


def _create_cassandra_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("create-cassandra", help="Create new APDB instance in Cassandra cluster.")
    parser.add_argument("host", help="One or more host names for Cassandra cluster.", nargs="+")
    parser.add_argument(
        "keyspace", help="Cassandra keyspace name for APDB tables, will be created if does not exist."
    )
    parser.add_argument("config_path", help="Name of the new configuration file for created APDB instance.")
    options.common_apdb_options(parser)
    options.cassandra_config_options(parser)
    parser.add_argument(
        "--drop", help="If True then drop existing tables.", default=False, action="store_true"
    )
    parser.set_defaults(method=scripts.create_cassandra)


def _list_index_subcommand(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("list-index", help="List contents of APDB index file.")
    parser.add_argument(
        "index_path", help="Location of index file, if missing then $DAX_APDB_INDEX_URI is used.", nargs="?"
    )
    parser.set_defaults(method=scripts.list_index)
