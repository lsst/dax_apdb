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

__all__ = ["ConnectionContext", "DbVersions"]

import dataclasses
import logging
from collections.abc import Mapping

# If cassandra-driver is not there the module can still be imported
# but ApdbCassandra cannot be instantiated.
try:
    from cassandra.cluster import Session
except ImportError:
    pass

from ..apdbConfigFreezer import ApdbConfigFreezer
from ..apdbSchema import ApdbTables
from ..monitor import MonAgent
from ..pixelization import Pixelization
from ..schema_model import Table
from ..versionTuple import VersionTuple
from .apdbCassandraReplica import ApdbCassandraReplica
from .apdbCassandraSchema import ApdbCassandraSchema
from .apdbMetadataCassandra import ApdbMetadataCassandra
from .cassandra_utils import PreparedStatementCache
from .config import ApdbCassandraConfig

_LOG = logging.getLogger(__name__)

_MON = MonAgent(__name__)


@dataclasses.dataclass
class DbVersions:
    """Versions defined in APDB metadata table."""

    schema_version: VersionTuple
    """Version of the schema from which database was created."""

    code_version: VersionTuple
    """Version of ApdbCassandra with which database was created."""

    replica_version: VersionTuple | None
    """Version of ApdbCassandraReplica with which database was created, None
    if replication was not configured.
    """


class ConnectionContext:
    """Container for all kinds ob objects that are instantiated once the
    connection to Cassandra is established.

    Parameters
    ----------
    session : `cassandra.cluster.Sesion`
        Cassandra session.
    config : `ApdbCassandraConfig`
        Configuration object.
    table_schemas : `~collection.abc.Mapping` [`ApdbTables`, `Table`]
        Schema definitions for regular APDB tables.
    """

    metadataSchemaVersionKey = "version:schema"
    """Name of the metadata key to store schema version number."""

    metadataCodeVersionKey = "version:ApdbCassandra"
    """Name of the metadata key to store code version number."""

    metadataReplicaVersionKey = "version:ApdbCassandraReplica"
    """Name of the metadata key to store replica code version number."""

    metadataConfigKey = "config:apdb-cassandra.json"
    """Name of the metadata key to store frozen part of the configuration."""

    frozen_parameters = (
        "enable_replica",
        "ra_dec_columns",
        "replica_skips_diaobjects",
        "replica_sub_chunk_count",
        "partitioning.part_pixelization",
        "partitioning.part_pix_level",
        "partitioning.time_partition_tables",
        "partitioning.time_partition_days",
    )
    """Names of the config parameters to be frozen in metadata table."""

    def __init__(
        self, session: Session, config: ApdbCassandraConfig, table_schemas: Mapping[ApdbTables, Table]
    ):
        self.session = session

        meta_table_name = ApdbTables.metadata.table_name(config.prefix)
        self.metadata = ApdbMetadataCassandra(
            self.session, meta_table_name, config.keyspace, "read_tuples", "write"
        )

        # Read frozen config from metadata.
        config_json = self.metadata.get(self.metadataConfigKey)
        if config_json is not None:
            # Update config from metadata.
            freezer = ApdbConfigFreezer[ApdbCassandraConfig](self.frozen_parameters)
            self.config = freezer.update(config, config_json)
        else:
            self.config = config
        del config

        # Read versions stored in database.
        self.db_versions = self._readVersions(self.metadata)
        _LOG.debug("Database versions: %s", self.db_versions)

        # Since replica version 1.1.0 we use finer partitioning for replica
        # chunk tables.
        self.has_chunk_sub_partitions = False
        if self.config.enable_replica:
            assert self.db_versions.replica_version is not None, "Replica version must be defined"
            self.has_chunk_sub_partitions = ApdbCassandraReplica.hasChunkSubPartitions(
                self.db_versions.replica_version
            )

        # Since version 0.1.2 we have an extra table for visit/detector.
        self.has_visit_detector_table = self.db_versions.code_version >= VersionTuple(0, 1, 2)

        # Support for DiaObjectLastToPartition was added at code version 0.1.1
        # in a backward-compatible way (we only use the table if it is there).
        self.has_dia_object_last_to_partition = self.db_versions.code_version >= VersionTuple(0, 1, 1)

        # Cache for prepared statements
        self.preparer = PreparedStatementCache(self.session)

        self.pixelization = Pixelization(
            self.config.partitioning.part_pixelization,
            self.config.partitioning.part_pix_level,
            self.config.partitioning.part_pix_max_ranges,
        )

        self.schema = ApdbCassandraSchema(
            session=self.session,
            keyspace=self.config.keyspace,
            table_schemas=table_schemas,
            prefix=self.config.prefix,
            time_partition_tables=self.config.partitioning.time_partition_tables,
            enable_replica=self.config.enable_replica,
            has_chunk_sub_partitions=self.has_chunk_sub_partitions,
            has_visit_detector_table=self.has_visit_detector_table,
        )

    def _readVersions(self, metadata: ApdbMetadataCassandra) -> DbVersions:
        """Read versions of all objects from metadata."""

        def _get_version(key: str) -> VersionTuple:
            """Retrieve version number from given metadata key."""
            version_str = metadata.get(key)
            if version_str is None:
                # Should not happen with existing metadata table.
                raise RuntimeError(f"Version key {key!r} does not exist in metadata table.")
            return VersionTuple.fromString(version_str)

        db_schema_version = _get_version(self.metadataSchemaVersionKey)
        db_code_version = _get_version(self.metadataCodeVersionKey)

        # Check replica code version only if replica is enabled.
        db_replica_version: VersionTuple | None = None
        if self.config.enable_replica:
            db_replica_version = _get_version(self.metadataReplicaVersionKey)

        return DbVersions(
            schema_version=db_schema_version, code_version=db_code_version, replica_version=db_replica_version
        )
