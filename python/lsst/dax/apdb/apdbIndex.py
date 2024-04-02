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

__all__ = ["ApdbIndex"]

import io
import logging
import os
from collections.abc import Mapping
from typing import ClassVar

import yaml
from lsst.resources import ResourcePath
from pydantic import TypeAdapter, ValidationError

_LOG = logging.getLogger(__name__)


class ApdbIndex:
    """Index of well-known Apdb instances.

    Parameters
    ----------
    index_path : `str`, optional
        Path to the index configuration file, if not provided then the value
        of ``DAX_APDB_INDEX_URI`` environment variable is used to locate
        configuration file.

    The index is configured from a simple YAML file whose location is
    determined from ``DAX_APDB_INDEX_URI`` environment variable. The content
    of the index file is a mapping of the labels to URIs in YAML format, e.g.:

    .. code-block:: yaml

       dev: "/path/to/config-file.yaml"
       "prod/pex_config": "s3://bucket/apdb-prod.py"
       "prod/yaml": "s3://bucket/apdb-prod.yaml"

    The labels in the index file consists of the label name and an optional
    format name separated from label by slash. `get_apdb_uri` method can
    use its ``format`` argument to return either a format-specific
    configuration or a label-only configuration if format-specific is not
    defined.
    """

    index_env_var: ClassVar[str] = "DAX_APDB_INDEX_URI"
    """The name of the environment variable containing the URI of the index
    configuration file.
    """

    _cache: Mapping[str, str] | None = None
    """Contents of the index file cached in memory."""

    def __init__(self, index_path: str | None = None):
        self._index_path = index_path

    def _read_index(self, index_path: str | None = None) -> Mapping[str, str]:
        """Return contents of the index file.

        Parameters
        ----------
        index_path : `str`, optional
            Location of the index file, if not provided then default location
            is used.

        Returns
        -------
        entries : `~collections.abc.Mapping` [`str`, `str`]
            All known entries. Can be empty if no index can be found.

        Raises
        ------
        RuntimeError
            Raised if ``index_path`` is not provided and environment variable
            is not set.
        TypeError
            Raised if content of the configuration file is incorrect.
        """
        if self._cache is not None:
            return self._cache
        if index_path is None:
            index_path = os.getenv(self.index_env_var)
            if not index_path:
                raise RuntimeError(
                    f"No repository index defined, environment variable {self.index_env_var} is not set."
                )
        index_uri = ResourcePath(index_path)
        _LOG.debug("Opening YAML index file: %s", index_uri.geturl())
        try:
            content = index_uri.read()
        except IsADirectoryError as exc:
            raise FileNotFoundError(f"Index file {index_uri.geturl()} is a directory") from exc
        stream = io.BytesIO(content)
        if index_data := yaml.load(stream, Loader=yaml.SafeLoader):
            try:
                self._cache = TypeAdapter(dict[str, str]).validate_python(index_data)
            except ValidationError as e:
                raise TypeError(f"Repository index {index_uri.geturl()} not in expected format") from e
        else:
            self._cache = {}
        return self._cache

    def get_apdb_uri(self, label: str, format: str | None = None) -> ResourcePath:
        """Return URI for APDB configuration file given its label.

        Parameters
        ----------
        label : `str`
            Label of APDB instance.
        format : `str`
            Format of the APDB configuration file, arbitrary string. This can
            be used to support an expected migration from pex_config to YAML
            configuration for APDB, code that uses pex_config could provide
            "pex_config" for ``format``. The actual key in the index is
            either a slash-separated label and format, or, if that is missing,
            just a label.

        Returns
        -------
        uri : `~lsst.resources.ResourcePath`
            URI for the configuration file for APDB instance.

        Raises
        ------
        FileNotFoundError
            Raised if an index is defined in the environment but it
            can not be found.
        ValueError
            Raised if the label is not found in the index.
        RuntimeError
            Raised if ``index_path`` is not provided and environment variable
            is not set.
        TypeError
            Raised if the format of the index file is incorrect.
        """
        index = self._read_index(self._index_path)
        labels: list[str] = [label]
        if format:
            labels.insert(0, f"{label}/{format}")
        for label in labels:
            if (uri_str := index.get(label)) is not None:
                return ResourcePath(uri_str)
        if len(labels) == 1:
            message = f"Label {labels[0]} is not defined in index file"
        else:
            labels_str = ", ".join(labels)
            message = f"None of labels {labels_str} is defined in index file"
        all_labels = set(index)
        raise ValueError(f"{message}, labels known to index: {all_labels}")

    def get_entries(self) -> Mapping[str, str]:
        """Retrieve all entries defined in index.

        Returns
        -------
        entries : `~collections.abc.Mapping` [`str`, `str`]
            All known index entries.

        Raises
        ------
        RuntimeError
            Raised if ``index_path`` is not provided and environment variable
            is not set.
        TypeError
            Raised if content of the configuration file is incorrect.
        """
        return self._read_index(self._index_path)
