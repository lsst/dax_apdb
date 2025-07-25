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

__all__ = [
    "ApdbCassandraConfig",
    "ApdbCassandraConnectionConfig",
    "ApdbCassandraPartitioningConfig",
    "ApdbCassandraTimePartitionRange",
]

import json
from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, ClassVar, Self

from pydantic import BaseModel, Field, field_validator

# If cassandra-driver is not there the module can still be imported.
try:
    import cassandra

    CASSANDRA_IMPORTED = True
except ImportError:
    CASSANDRA_IMPORTED = False


from ..config import ApdbConfig

if TYPE_CHECKING:
    from .apdbMetadataCassandra import ApdbMetadataCassandra


class ApdbCassandraConnectionConfig(BaseModel):
    """Connection configuration for Cassandra APDB."""

    port: int = Field(
        default=9042,
        description="Port number to connect to.",
    )

    private_ips: tuple[str, ...] = Field(
        default=(),
        description="List of internal IP addresses for contact_points.",
    )

    username: str = Field(
        default="",
        description=(
            "Cassandra user name, if empty then db-auth.yaml has to provide it together with a password."
        ),
    )

    read_consistency: str = Field(
        default="QUORUM",
        description="Name for consistency level of read operations, default: QUORUM, can be ONE.",
    )

    write_consistency: str = Field(
        default="QUORUM",
        description="Name for consistency level of write operations, default: QUORUM, can be ONE.",
    )

    read_timeout: float = Field(
        default=120.0,
        description="Timeout in seconds for read operations.",
    )

    write_timeout: float = Field(
        default=60.0,
        description="Timeout in seconds for write operations.",
    )

    remove_timeout: float = Field(
        default=600.0,
        description="Timeout in seconds for remove operations.",
    )

    read_concurrency: int = Field(
        default=500,
        description="Concurrency level for read operations.",
    )

    protocol_version: int = Field(
        default=cassandra.ProtocolVersion.V4 if CASSANDRA_IMPORTED else 4,
        description="Cassandra protocol version to use, default is V4.",
    )

    extra_parameters: dict[str, Any] = Field(
        default={}, description="Additional keyword parameters passed to connect() method verbatim."
    )


class ApdbCassandraPartitioningConfig(BaseModel):
    """Partitioning configuration for Cassandra APDB."""

    part_pixelization: str = Field(
        default="mq3c",
        description="Pixelization used for partitioning index.",
    )

    part_pix_level: int = Field(
        default=11,
        description="Pixelization level used for partitioning index.",
    )

    part_pix_max_ranges: int = Field(
        default=128,
        description="Max number of ranges in pixelization envelope",
    )

    time_partition_tables: bool = Field(
        default=False,
        description="Use per-partition tables for sources instead of partitioning by time",
    )

    time_partition_days: int = Field(
        default=30,
        description=(
            "Time partitioning granularity in days, this value must not be changed after database is "
            "initialized"
        ),
    )

    time_partition_start: str = Field(
        default="2018-12-01T00:00:00",
        description=(
            "Starting time for per-partition tables, in yyyy-mm-ddThh:mm:ss format, in TAI. "
            "This is used only when time_partition_tables is True."
        ),
    )

    time_partition_end: str = Field(
        default="2030-01-01T00:00:00",
        description=(
            "Ending time for per-partition tables, in yyyy-mm-ddThh:mm:ss format, in TAI. "
            "This is used only when time_partition_tables is True."
        ),
    )

    query_per_time_part: bool = Field(
        default=False,
        description=(
            "If True then build separate query for each time partition, otherwise build one single query. "
            "This is only used when time_partition_tables is False in schema config."
        ),
    )

    query_per_spatial_part: bool = Field(
        default=False,
        description="If True then build one query per spatial partition, otherwise build single query.",
    )

    @field_validator("part_pixelization")
    @classmethod
    def check_pixelization(cls, v: str) -> str:
        allowed = {"htm", "q3c", "mq3c", "healpix"}
        if v not in allowed:
            raise ValueError(f"Unexpected value for part_pixelization: {v}, allowed values: {allowed}")
        return v


class ApdbCassandraConfig(ApdbConfig):
    """Configuration class for Cassandra-based APDB implementation."""

    _implementation_type: ClassVar[str] = "cassandra"

    contact_points: tuple[str, ...] = Field(
        default=("127.0.0.1",),
        description="The list of contact points to try connecting for cluster discovery.",
    )

    keyspace: str = Field(
        default="apdb",
        description="Keyspace name for APDB tables.",
    )

    connection_config: ApdbCassandraConnectionConfig = Field(
        default_factory=ApdbCassandraConnectionConfig,
        description="Database connection configuration",
    )

    partitioning: ApdbCassandraPartitioningConfig = Field(
        default_factory=ApdbCassandraPartitioningConfig,
        description="Configuration for partitioning.",
    )

    dia_object_columns: list[str] = Field(
        default=[],
        description="List of columns to read from DiaObject[Last], by default read all columns.",
    )

    prefix: str = Field(
        default="",
        description="Prefix to add to table names.",
    )

    ra_dec_columns: tuple[str, str] = Field(
        default=("ra", "dec"),
        description="Names of ra/dec columns in DiaObject table",
    )

    replica_skips_diaobjects: bool = Field(
        default=False,
        description=(
            "If True then do not store DiaObjects when enable_replica is True "
            "(DiaObjectsChunks has the same data)."
        ),
    )

    replica_sub_chunk_count: int = Field(
        default=64,
        description="Number of sub-partitions in replica chunk tables.",
    )

    batch_statement_limit: int = Field(
        default=65_535,
        description=(
            "Limit on a number of rows in a BatchStatement. Default is the same as Cassandra limit of 65535."
        ),
    )

    batch_size_limit: int = Field(
        default=1_000_000,
        description=(
            "Limit on a size of BatchStatement in bytes. Batch size is estimated approximately. "
            "Set to 0 or negative to disable this limit. "
            "Server-side batch size warning threshold needs to be set to at least this value."
        ),
    )

    @field_validator("ra_dec_columns")
    @classmethod
    def check_ra_dec(cls, v: Iterable[str]) -> tuple[str, str]:
        # This validation method is needed in case we initialize model from
        # JSON in strict mode, in that mode JSON list is rejected by default.
        vtup = tuple(v)
        if len(vtup) != 2:
            raise ValueError("ra_dec_columns must have exactly two column names")
        return vtup


class ApdbCassandraTimePartitionRange(BaseModel):
    """Configuration of the time partitions, this is not user-configurable,
    but it is reflected in metadata.
    """

    metadataTimePartitionKey: ClassVar[str] = "config:time-partition-range.json"
    """Name of the metadata key to store time partition range."""

    start: int = Field(
        description="Start partition number for per-time-partition tables that exist in the schema."
    )

    end: int = Field(
        description="End partition number (inclusive) for per-time-partition tables that exist in the schema."
    )

    def range(self) -> Iterator[int]:
        """Generate a sequence of partition numbers."""
        yield from range(self.start, self.end + 1)

    @classmethod
    def from_meta(cls, metadata: ApdbMetadataCassandra) -> Self:
        """Read this configuration object from metadata table.

        Parameters
        ----------
        metadata : `ApdbMetadataCassandra`
            Metadata table.

        Returns
        -------
        range : `ApdbCassandraTimePartitionRange`
            Configuration retrieved from database.
        """
        time_partitions_str = metadata.get(cls.metadataTimePartitionKey)
        if time_partitions_str is None:
            raise LookupError(f"Key '{cls.metadataTimePartitionKey}' is missing from metadata table.")
        time_partitions_json = json.loads(time_partitions_str)
        return cls.model_validate(time_partitions_json)

    def save_to_meta(self, metadata: ApdbMetadataCassandra) -> None:
        """Save this configuration to metadata table.

        Parameters
        ----------
        metadata : `ApdbMetadataCassandra`
            Metadata table.
        """
        metadata.set(self.metadataTimePartitionKey, json.dumps(self.model_dump()), force=True)
