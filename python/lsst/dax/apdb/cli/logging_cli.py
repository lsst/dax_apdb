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

__all__ = ["LoggingCli"]

import argparse
import logging

_log_format = "%(asctime)s %(levelname)s %(name)s - %(message)s"


class LoggingCli:
    """Class that handles CLI logging options and logging configuration.

    Parameters
    ----------
    parser : `argparse.ArgumentParser`
        Parser for which to define logging options.
    """

    def __init__(self, parser: argparse.ArgumentParser):
        self._parser = parser
        parser.add_argument(
            "-l",
            "--log-level",
            dest="log_level",
            action="append",
            metavar="LEVEL|LOGGER=LEVEL[,...]",
            help="Global or per-logger logging level, comma-separated and can be specified multiple times",
            default=[],
        )

    def process_args(self, args: argparse.Namespace) -> None:
        """Configure Python logging based on command line options."""
        global_level = logging.INFO
        # Suppress chatty cassandra.cluster logger by default.
        logger_levels: dict[str, int] = {
            "cassandra.cluster": logging.WARNING,
            "botocore": logging.WARNING,
        }
        for level_str in args.log_level:
            for spec in level_str.split(","):
                logger_name, sep, level_name = spec.rpartition("=")
                level = logging.getLevelNamesMapping().get(level_name.upper())
                if level is None:
                    self._parser.error(f"Unknown logging level {level_name!r} in {level_str!r}")
                if logger_name:
                    logger_levels[logger_name] = level
                else:
                    global_level = level

        logging.basicConfig(level=global_level, format=_log_format)
        for logger_name, level in logger_levels.items():
            logging.getLogger(logger_name).setLevel(level)

        # We want to remove `log_level` so that namespace can be converted to
        # a dict and passed as kwargs to scripts.
        del args.log_level
