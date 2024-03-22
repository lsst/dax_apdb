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

    """

    index_env_var: ClassVar[str] = "DAX_APDB_INDEX_URI"
    """The name of the environment variable containing the URI of the index
    configuration file.
    """

    _cache: Mapping[str, str] | None = None
    """Contents of the index file cached in memory."""

    def __init__(self, index_path: str | None = None):
        self._index_path = index_path

    @classmethod
    def _read_index(cls, index_path: str | None = None) -> Mapping[str, str]:
        if index_path is None:
            index_path = os.getenv(cls.index_env_var)
            if index_path is None:
                raise RuntimeError(
                    f"No repository index defined, environment variable {cls.index_env_var} is not set."
                )
        index_uri = ResourcePath(index_path)
        _LOG.debug("Opening YAML index file: %s", index_uri.geturl())
        content = index_uri.read()
        # Use a stream so we can name it
        stream = io.BytesIO(content)
        if index_data := yaml.load(stream, Loader=yaml.SafeLoader):
            try:
                return TypeAdapter(dict[str, str]).validate_python(index_data)
            except ValidationError as e:
                raise TypeError(f"Repository index {index_uri.geturl()} not in expected format") from e
        return {}

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
        KeyError
            Raised if the label is not found in the index.
        TypeError
            Raised if the format of the index file is incorrect.
        """
        if self._cache is None:
            self._cache = self._read_index(self._index_path)
        labels: list[str] = [label]
        if format:
            labels.insert(0, f"{label}/{format}")
        for label in labels:
            if (uri_str := self._cache.get(label)) is not None:
                return ResourcePath(uri_str)
        if len(labels) == 1:
            raise KeyError(f"Label {labels[0]} is not defined in index file.")
        else:
            labels_str = ", ".join(labels)
            raise KeyError(f"None of labels {labels_str} is defined in index file.")

    def get_known_labels(self) -> set[str]:
        """Retrieve the set of labels defined in index.

        Returns
        -------
        repos : `set` of `str`
            All known labels. Can be empty if no index can be found.
        """
        if self._cache is not None:
            return set(self._cache)
        # If have not read yet then try to read but ignore any errors.
        try:
            return set(self._read_index(self._index_path))
        except Exception:
            return set()

    def get_entries(self) -> Mapping[str, str]:
        """Retrieve all entries defined in index.

        Returns
        -------
        entries : `~collections.abc.Mapping` [`str`, `str`]
            All known entries. Can be empty if no index can be found.
        """
        if self._cache is not None:
            return self._cache
        # If have not read yet then try to read but ignore any errors.
        try:
            return self._read_index(self._index_path)
        except Exception:
            return {}
