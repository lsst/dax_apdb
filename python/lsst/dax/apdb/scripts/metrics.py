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

__all__ = ["metrics_log_to_influx"]

import io
import json
import re
import sys
from collections.abc import Iterable
from datetime import datetime
from typing import Any, TextIO

import yaml

_LOG_LINE_RE_PIPELINE = re.compile(
    r"""
    ^
    INFO .* lsst[.]dax[.]apdb[.]monitor
    \ \(\w+:(?P<MDC>\{[^}]*\})\)
    \([^)]*\)[ ]-[ ](?P<metric>.*)
    $
    """,
    re.VERBOSE,
)

_LOG_LINE_RE_REPLICATION = re.compile(
    r"""
    ^
    .*[ ]INFO[ ]lsst[.]dax[.]ppdb[.]monitor
    [ ]-[ ](?P<metric>.*)
    $
    """,
    re.VERBOSE,
)

# Error or warning message from cassandra logger.
_LOG_LINE_CASSANDRA_RE = re.compile(
    r"""
    ^
    (?P<level>ERROR|WARNING)[ ](?P<datetime>\d{4}-\d{2}-\d{2}[ T]\S+)[ ]cassandra[.]cluster
    \ \((\w+:(?P<MDC>\{[^}]*\}))?\)
    \([^)]*\)[ ]-[ ](?P<message>.*)
    $
    """,
    re.VERBOSE,
)

_AP_PIPE_DIAOBJECTS_RE = re.compile(r"Calculating summary stats for (?P<count>\d+) DiaObjects")
_AP_PIPE_DIASOURCES_RE = re.compile(
    r"(?P<count1>\d+) updated and \d+ unassociated diaObjects. Creating (?P<count2>\d+) new diaObjects"
)
_AP_PIPE_DIAFORCED_RE = re.compile(r"Updating (?P<count>\d+) diaForcedSources in the APDB")

_CASSNDRA_MESSAGES_RE = (
    (re.compile(r"^Error preparing query for host (?P<host>\S+):$"), "error_prepare_query"),
    (re.compile(r"^Control connection failed to connect"), "error_control_connect"),
    (
        re.compile(r"^Unexpected failure handling node (?P<host>\S+) being marked up:$"),
        "error_failure_marking_up",
    ),
    (re.compile(r"^Failed to submit task to executor$"), "error_submit_task"),
    (re.compile(r"^Failed to create connection pool for new host (?P<host>\S+):$"), "warn_create_pool"),
    (re.compile(r"^Error attempting to reconnect to (?P<host>\S+),"), "warn_reconnect"),
    (re.compile(r"^Host (?P<host>\S+) has been marked down"), "warn_host_down"),
)


# Some metrics are not usefult for replication
_SKIP_METRICS_REPLICATION = {
    "read_metadata_config",
    "version_check",
}


def metrics_log_to_influx(
    file: Iterable[str],
    context_keys: str,
    extra_tags: str,
    fix_row_count: bool,
    replication: bool,
    prefix: str,
    no_header: bool,
    header_database: str,
) -> None:
    """Extract metrics from log file and dump as InfluxDB data.

    Parameters
    ----------
    file : `~collections.abc.Iterable` [`str`]
        Names of the files to parse for metrics.
    context_keys : `str`
        Names of keys to extract from message context, comma-separated.
    extra_tags : `str`
        Additional tags to add to each record, comma-separated key=value pairs.
    fix_row_count : `bool`
        If True then extract records counts from pipeline messages instead of
        metrics. A workaround for broken metrics.
    replication : `bool`
        If True then the log is from replication service, otherwise it is a log
        from AP pipeline.
    prefix : `str`
        Prefix to add to each tag name.
    no_header : `bool`
        If False then do not print DML header.
    header_database : `str`
        Name of the database for DML header.
    """
    context_names = [name for name in context_keys.split(",") if name]
    tags: dict[str, Any] = {}
    for tag_val in extra_tags.split(","):
        if tag_val:
            tag, _, val = tag_val.partition("=")
            tags[tag] = val

    if not no_header:
        print(
            f"""\
# DML

# CONTEXT-DATABASE: {header_database}
"""
        )

    if not file:
        file = ["-"]
    for file_name in file:
        if file_name == "-":
            _metrics_log_to_influx(sys.stdin, context_names, tags, fix_row_count, replication, prefix)
        else:
            with open(file_name) as file_obj:
                _metrics_log_to_influx(file_obj, context_names, tags, fix_row_count, replication, prefix)


def _metrics_log_to_influx(
    file: TextIO,
    context_keys: Iterable[str],
    extra_tags: dict[str, Any],
    fix_row_count: bool,
    replication: bool,
    prefix: str,
) -> None:
    """Parse metrics from a single file."""
    objects_count = -1
    sources_count = -1
    forced_sources_count = -1

    line_re = _LOG_LINE_RE_REPLICATION if replication else _LOG_LINE_RE_PIPELINE

    for line in file:
        line = line.strip()
        if fix_row_count and not replication:
            if match := _AP_PIPE_DIAOBJECTS_RE.search(line):
                objects_count = int(match.group("count"))
            elif match := _AP_PIPE_DIASOURCES_RE.search(line):
                sources_count = int(match.group("count1")) + int(match.group("count2"))
            elif match := _AP_PIPE_DIAFORCED_RE.search(line):
                forced_sources_count = int(match.group("count"))

        if match := line_re.match(line):
            metric_str = match.group("metric")
            metric: dict[str, Any] = json.loads(metric_str)
            tags = dict(extra_tags)

            name: str = metric["name"]
            if replication and name in _SKIP_METRICS_REPLICATION:
                continue

            timestamp: float = metric["timestamp"]
            for tag, tag_val in metric["tags"].items():
                tags[tag] = tag_val
            values: dict[str, Any] = metric["values"]

            if fix_row_count and name == "insert_time":
                if tags["table"].startswith("DiaObject"):
                    values["row_count"] = objects_count
                elif tags["table"].startswith("DiaSource"):
                    values["row_count"] = sources_count
                elif tags["table"].startswith("DiaForcedSource"):
                    values["row_count"] = forced_sources_count

            if not replication and context_keys:
                tags.update(_extract_mdc(match, context_keys))

            _print_metrics(prefix + name, tags, values, timestamp)

        elif match := _LOG_LINE_CASSANDRA_RE.match(line):
            tags = dict(extra_tags)
            tags["level"] = match.group("level").lower()
            dt = datetime.fromisoformat(match.group("datetime"))
            timestamp = dt.timestamp()
            tags.update(_extract_mdc(match, context_keys))
            values = {"count": 1}

            message = match.group("message")
            for message_re, name in _CASSNDRA_MESSAGES_RE:
                if (message_match := message_re.search(message)) is not None:
                    tags.update(message_match.groupdict())
                    _print_metrics(prefix + name, tags, values, timestamp)
                    break


def _print_metrics(name: str, tags: dict[str, Any], values: dict[str, Any], timestamp: float) -> None:
    tags_str = ",".join([name] + [f"{key}={val}" for key, val in tags.items()])
    values_str = ",".join(f"{key}={val}" for key, val in values.items())
    print(f"{tags_str} {values_str} {int(timestamp * 1e9)}")


def _extract_mdc(match: re.Match, context_keys: Iterable[str]) -> dict[str, Any]:
    tags: dict[str, Any] = {}
    mdc_str = match.group("MDC")
    if mdc_str:
        mdc_str = mdc_str.replace("'", '"')
        mdc: dict[str, Any] = yaml.safe_load(io.StringIO(mdc_str))
        for tag in context_keys:
            if (tag_val := mdc.get(tag)) is not None:
                tags[tag] = tag_val
    return tags
