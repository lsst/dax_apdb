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

__all__ = ["ApdbConfig"]

import warnings
from collections.abc import Mapping
from typing import Any, ClassVar, cast

import yaml
from pydantic import BaseModel, Field

from lsst.resources import ResourcePath, ResourcePathExpression

from .apdbIndex import ApdbIndex
from .factory import config_type_for_name


class ApdbConfig(BaseModel):
    """Base class for APDB configuration types.

    This class contains a set of parameters that are common to all
    implementations. Implementation-specific parameters are declared in
    sub-classes.
    """

    _implementation_type: ClassVar[str]

    schema_file: str = Field(
        default="resource://lsst.sdm.schemas/apdb.yaml",
        description="Location of (YAML) configuration file with standard APDB schema.",
    )

    ss_schema_file: str = Field(
        default="resource://lsst.sdm.schemas/sso.yaml",
        description=(
            "Location of (YAML) configuration file with SSO schema. "
            "This file is only loaded if SSObject/SSSource tables are not present in APDB schema file. "
            "Can be set to empty string to avoid loading even if tables are not in APDB schema."
        ),
    )

    schema_name: str = Field(
        default="ApdbSchema",
        description="Name of the schema in YAML configuration file (not used and deprecated).",
    )

    read_sources_months: int = Field(
        default=12,
        description="Number of months of history to read from DiaSource.",
    )

    read_forced_sources_months: int = Field(
        default=12,
        description="Number of months of history to read from DiaForcedSource",
    )

    enable_replica: bool = Field(
        default=False,
        description="If True, make and fill additional tables used for replication.",
    )

    replica_chunk_seconds: int = Field(
        default=600,
        description=(
            "Time extent for replica chunks, new chunks are created every specified number of seconds."
        ),
    )

    @classmethod
    def from_uri(cls, uri: ResourcePathExpression) -> ApdbConfig:
        """Load configuration object from external file.

        Parameters
        ----------
        uri : `~lsst.resources.ResourcePathExpression`
            Location of the file containing serialized configuration in YAML
            format.

        Returns
        -------
        config : `ApdbConfig`
            Apdb configuration object.
        """
        if isinstance(uri, str) and uri.startswith("label:"):
            tag, _, label = uri.partition(":")
            index = ApdbIndex()
            # Try to find YAML format first, and pex_config if YAML is not
            # found. During transitional period we support conversion of
            # pex_config format to a new pydantic format.
            try:
                uri = index.get_apdb_uri(label, "yaml")
            except ValueError as yaml_exc:
                try:
                    uri = index.get_apdb_uri(label, "pex_config")
                except ValueError:
                    # If none is found then re-raise exception from yaml
                    # attempt, but add a note that pex_config is missing too.
                    yaml_exc.add_note(f"Legacy label {label}/pex_config is also missing.")
                    raise yaml_exc from None

        path = ResourcePath(uri)
        config_bytes = path.read()

        # During transitional period we support loading of configurations from
        # both pex_config and pydantic/YAML formats. We have to look at the
        # contents for figure out which is which.
        if config_bytes.startswith(b"import lsst.dax.apdb"):
            from . import legacy_config

            pex_config = legacy_config.ApdbConfig.legacy_load(config_bytes)
            new_config = pex_config.to_model()
            warnings.warn(
                (
                    f"APDB is instantiated using legacy pex_config format from file {path}. "
                    "Support for pex_config format will be removed after v29. Please update "
                    "configuration to new YAML format with `apdb-cli convert-legacy-config` command."
                ),
                category=FutureWarning,
                stacklevel=2,
            )
            return new_config

        config_object = yaml.full_load(config_bytes)
        if not isinstance(config_object, Mapping):
            raise TypeError("YAML configuration file does not represent valid object")
        config_dict: dict[str, Any] = dict(config_object)
        type_name = config_dict.pop("implementation_type", None)
        if not type_name:
            raise LookupError("YAML configuration file does not have `implementation_type` key")
        klass = config_type_for_name(cast(str, type_name))
        return klass.model_validate(config_dict)

    def save(self, uri: ResourcePathExpression) -> None:
        """Save configuration to a specified location in YAML format.

        Parameters
        ----------
        uri : `ResourcePathExpression`
            Location to save configuration
        """
        config_dict = self.model_dump(exclude_unset=True, exclude_defaults=True)
        config_dict["implementation_type"] = self._implementation_type
        config_yaml = yaml.dump(config_dict)

        path = ResourcePath(uri)
        path.write(config_yaml.encode())
