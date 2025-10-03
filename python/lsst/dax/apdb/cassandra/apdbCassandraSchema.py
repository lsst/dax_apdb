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

__all__ = ["ApdbCassandraSchema", "CreateTableOptions", "TableOptions"]

import enum
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

import felis.datamodel
import pydantic

from .. import schema_model
from ..apdbSchema import ApdbTables

if TYPE_CHECKING:
    import cassandra.cluster

    from ..schema_model import Table
    from .config import ApdbCassandraTimePartitionRange


_LOG = logging.getLogger(__name__)


class InconsistentSchemaError(RuntimeError):
    """Exception raised when schema state is inconsistent."""


class TableOptions(pydantic.BaseModel):
    """Set of per-table options for creating Cassandra tables."""

    model_config = pydantic.ConfigDict(extra="forbid")

    tables: list[str]
    """List of table names for which the options should be applied."""

    options: str


class CreateTableOptions(pydantic.BaseModel):
    """Set of options for creating Cassandra tables."""

    model_config = pydantic.ConfigDict(extra="forbid")

    table_options: list[TableOptions] = pydantic.Field(default_factory=list)
    """Collection of per-table options."""

    default_table_options: str = ""
    """Default options used for tables that are not in the above list."""

    def get_options(self, table_name: str) -> str:
        """Find table options for a given table name."""
        for table_options in self.table_options:
            if table_name in table_options.tables:
                return table_options.options
        return self.default_table_options


@enum.unique
class ExtraTables(enum.Enum):
    """Names of the extra tables used by Cassandra implementation.

    Chunk tables exist in two versions now to support both old and new schema.
    Eventually we will drop support for old tables.
    """

    ApdbReplicaChunks = "ApdbReplicaChunks"
    """Name of the table for replica chunk records."""

    DiaObjectChunks = "DiaObjectChunks"
    """Name of the table for DIAObject chunk data."""

    DiaSourceChunks = "DiaSourceChunks"
    """Name of the table for DIASource chunk data."""

    DiaForcedSourceChunks = "DiaForcedSourceChunks"
    """Name of the table for DIAForcedSource chunk data."""

    DiaObjectChunks2 = "DiaObjectChunks2"
    """Name of the table for DIAObject chunk data."""

    DiaSourceChunks2 = "DiaSourceChunks2"
    """Name of the table for DIASource chunk data."""

    DiaForcedSourceChunks2 = "DiaForcedSourceChunks2"
    """Name of the table for DIAForcedSource chunk data."""

    ApdbUpdateRecordChunks = "ApdbUpdateRecordChunks"
    """Name of the table for ApdbUpdateRecord chunk data."""

    DiaSourceToPartition = "DiaSourceToPartition"
    """Maps diaSourceId to its partition values (pixel and time)."""

    DiaObjectLastToPartition = "DiaObjectLastToPartition"
    """Maps last diaObjectId version to its partition (pixel)."""

    ApdbVisitDetector = "ApdbVisitDetector"
    """Records attempted processing of visit/detector."""

    def table_name(self, prefix: str = "", time_partition: int | None = None) -> str:
        """Return full table name.

        Parameters
        ----------
        prefix : `str`, optional
            Optional prefix for table name.
        time_partition : `int`, optional
            Optional time partition, only used for tables that support time
            patitioning.
        """
        return f"{prefix}{self.value}"

    @classmethod
    def replica_chunk_tables(cls, has_subchunks: bool) -> Mapping[ApdbTables, ExtraTables]:
        """Return mapping of APDB tables to corresponding replica chunks
        tables.
        """
        if has_subchunks:
            return {
                ApdbTables.DiaObject: cls.DiaObjectChunks2,
                ApdbTables.DiaSource: cls.DiaSourceChunks2,
                ApdbTables.DiaForcedSource: cls.DiaForcedSourceChunks2,
            }
        else:
            return {
                ApdbTables.DiaObject: cls.DiaObjectChunks,
                ApdbTables.DiaSource: cls.DiaSourceChunks,
                ApdbTables.DiaForcedSource: cls.DiaForcedSourceChunks,
            }


class ApdbCassandraSchema:
    """Class for management of APDB schema.

    Parameters
    ----------
    session : `cassandra.cluster.Session`
        Cassandra session object
    keyspace : `str`
        Keyspace name for all tables.
    schema_file : `str`
        Name of the YAML schema file.
    schema_name : `str`, optional
        Name of the schema in YAML files.
    prefix : `str`, optional
        Prefix to add to all schema elements.
    time_partition_tables : `bool`
        If `True` then schema will have a separate table for each time
        partition.
    enable_replica : `bool`, optional
        If `True` then use additional tables for replica chunks.
    has_chunk_sub_partitions : `bool`, optional
        If `True` then replica chunk tables have sub-partition columns. Only
        used if ``enable_replica`` is `True`.
    """

    _type_map = {
        felis.datamodel.DataType.double: "DOUBLE",
        felis.datamodel.DataType.float: "FLOAT",
        felis.datamodel.DataType.timestamp: "TIMESTAMP",
        felis.datamodel.DataType.long: "BIGINT",
        felis.datamodel.DataType.int: "INT",
        felis.datamodel.DataType.short: "SMALLINT",
        felis.datamodel.DataType.byte: "TINYINT",
        felis.datamodel.DataType.binary: "BLOB",
        felis.datamodel.DataType.char: "TEXT",
        felis.datamodel.DataType.string: "TEXT",
        felis.datamodel.DataType.unicode: "TEXT",
        felis.datamodel.DataType.text: "TEXT",
        felis.datamodel.DataType.boolean: "BOOLEAN",
        schema_model.ExtraDataTypes.UUID: "UUID",
    }
    """Map YAML column types to Cassandra"""

    _time_partitioned_tables = [
        ApdbTables.DiaObject,
        ApdbTables.DiaSource,
        ApdbTables.DiaForcedSource,
    ]
    _spatially_partitioned_tables = [ApdbTables.DiaObjectLast]

    def __init__(
        self,
        session: cassandra.cluster.Session,
        keyspace: str,
        table_schemas: Mapping[ApdbTables, Table],
        prefix: str = "",
        time_partition_tables: bool = False,
        enable_replica: bool = False,
        replica_skips_diaobjects: bool = False,
        has_chunk_sub_partitions: bool = True,
        has_visit_detector_table: bool = True,
    ):
        self._session = session
        self._keyspace = keyspace
        self._table_schemas = table_schemas
        self._prefix = prefix
        self._time_partition_tables = time_partition_tables
        self._enable_replica = enable_replica
        self._replica_skips_diaobjects = replica_skips_diaobjects
        self._has_chunk_sub_partitions = has_chunk_sub_partitions
        self._has_visit_detector_table = has_visit_detector_table

        self._apdb_tables = self._apdb_tables_schema(time_partition_tables)
        self._extra_tables = self._extra_tables_schema()

    def _apdb_tables_schema(self, time_partition_tables: bool) -> Mapping[ApdbTables, schema_model.Table]:
        """Generate schema for regular APDB tables."""
        apdb_tables: dict[ApdbTables, schema_model.Table] = {}

        # add columns and index for partitioning.
        for table, apdb_table_def in self._table_schemas.items():
            part_columns = []
            add_columns = []
            primary_key = apdb_table_def.primary_key[:]
            if table in self._spatially_partitioned_tables:
                # DiaObjectLast does not need temporal partitioning
                part_columns = ["apdb_part"]
                add_columns = part_columns
            elif table in self._time_partitioned_tables:
                if time_partition_tables:
                    part_columns = ["apdb_part"]
                else:
                    part_columns = ["apdb_part", "apdb_time_part"]
                add_columns = part_columns
            elif table is ApdbTables.SSObject:
                # For SSObject there is no natural partition key but we have
                # to partition it because there are too many of them. I'm
                # going to partition on its primary key (and drop separate
                # primary key index).
                part_columns = ["ssObjectId"]
                primary_key = []
            elif table is ApdbTables.metadata:
                # Metadata is in one partition because we want to read all of
                # it in one query, add an extra column for partition.
                part_columns = ["meta_part"]
                add_columns = part_columns
            else:
                # TODO: Do not know what to do with the other tables
                continue

            column_defs = []
            if add_columns:
                column_defs = [
                    schema_model.Column(
                        id=f"#{name}", name=name, datatype=felis.datamodel.DataType.long, nullable=False
                    )
                    for name in add_columns
                ]

            annotations = dict(apdb_table_def.annotations)
            annotations["cassandra:apdb_column_names"] = [column.name for column in apdb_table_def.columns]
            if part_columns:
                annotations["cassandra:partitioning_columns"] = part_columns

            apdb_tables[table] = schema_model.Table(
                id=apdb_table_def.id,
                name=apdb_table_def.name,
                columns=column_defs + apdb_table_def.columns,
                primary_key=primary_key,
                indexes=[],
                constraints=[],
                annotations=annotations,
            )

        return apdb_tables

    def _extra_tables_schema(self) -> Mapping[ExtraTables, schema_model.Table]:
        """Generate schema for extra tables."""
        extra_tables: dict[ExtraTables, schema_model.Table] = {}

        if self._has_visit_detector_table:
            columns = [
                schema_model.Column(
                    id="#visit",
                    name="visit",
                    datatype=felis.datamodel.DataType.long,
                    nullable=False,
                ),
                schema_model.Column(
                    id="#detector",
                    name="detector",
                    datatype=felis.datamodel.DataType.short,
                    nullable=False,
                ),
            ]
            extra_tables[ExtraTables.ApdbVisitDetector] = schema_model.Table(
                id="#" + ExtraTables.ApdbVisitDetector.value,
                name=ExtraTables.ApdbVisitDetector.table_name(self._prefix),
                columns=columns,
                primary_key=[],
                indexes=[],
                constraints=[],
                annotations={"cassandra:partitioning_columns": ["visit", "detector"]},
            )

        # This table maps DiaSource ID to its partitions in DiaSource table and
        # DiaSourceChunks tables.
        extra_tables[ExtraTables.DiaSourceToPartition] = schema_model.Table(
            id="#" + ExtraTables.DiaSourceToPartition.value,
            name=ExtraTables.DiaSourceToPartition.table_name(self._prefix),
            columns=[
                schema_model.Column(
                    id="#diaSourceId",
                    name="diaSourceId",
                    datatype=felis.datamodel.DataType.long,
                    nullable=False,
                ),
                schema_model.Column(
                    id="#apdb_part", name="apdb_part", datatype=felis.datamodel.DataType.long, nullable=False
                ),
                schema_model.Column(
                    id="#apdb_time_part",
                    name="apdb_time_part",
                    datatype=felis.datamodel.DataType.int,
                    nullable=False,
                ),
                schema_model.Column(
                    id="#apdb_replica_chunk",
                    name="apdb_replica_chunk",
                    datatype=felis.datamodel.DataType.long,
                    nullable=True,
                ),
                schema_model.Column(
                    id="#apdb_replica_subchunk",
                    name="apdb_replica_subchunk",
                    datatype=felis.datamodel.DataType.int,
                    nullable=True,
                ),
            ],
            primary_key=[],
            indexes=[],
            constraints=[],
            annotations={"cassandra:partitioning_columns": ["diaSourceId"]},
        )

        # This table maps diaObjectId to its partition in DiaObjectLast table.
        extra_tables[ExtraTables.DiaObjectLastToPartition] = schema_model.Table(
            id="#" + ExtraTables.DiaObjectLastToPartition.value,
            name=ExtraTables.DiaObjectLastToPartition.table_name(self._prefix),
            columns=[
                schema_model.Column(
                    id="#diaObjectId",
                    name="diaObjectId",
                    datatype=felis.datamodel.DataType.long,
                    nullable=False,
                ),
                schema_model.Column(
                    id="#apdb_part", name="apdb_part", datatype=felis.datamodel.DataType.long, nullable=False
                ),
            ],
            primary_key=[],
            indexes=[],
            constraints=[],
            annotations={"cassandra:partitioning_columns": ["diaObjectId"]},
        )

        if not self._enable_replica:
            return extra_tables

        replica_chunk_column = schema_model.Column(
            id="#apdb_replica_chunk",
            name="apdb_replica_chunk",
            datatype=felis.datamodel.DataType.long,
            nullable=False,
        )

        replica_chunk_columns = [replica_chunk_column]
        if self._has_chunk_sub_partitions:
            replica_chunk_columns.append(
                schema_model.Column(
                    id="#apdb_replica_subchunk",
                    name="apdb_replica_subchunk",
                    datatype=felis.datamodel.DataType.int,
                    nullable=False,
                )
            )

        # Table containing replica chunks, this one is not partitioned, but
        # partition key must be defined.
        extra_tables[ExtraTables.ApdbReplicaChunks] = schema_model.Table(
            id="#" + ExtraTables.ApdbReplicaChunks.value,
            name=ExtraTables.ApdbReplicaChunks.table_name(self._prefix),
            columns=[
                schema_model.Column(
                    id="#partition", name="partition", datatype=felis.datamodel.DataType.int, nullable=False
                ),
                replica_chunk_column,
                schema_model.Column(
                    id="#last_update_time",
                    name="last_update_time",
                    datatype=felis.datamodel.DataType.timestamp,
                    nullable=False,
                ),
                schema_model.Column(
                    id="#unique_id",
                    name="unique_id",
                    datatype=schema_model.ExtraDataTypes.UUID,
                    nullable=False,
                ),
                schema_model.Column(
                    id="#has_subchunks",
                    name="has_subchunks",
                    datatype=felis.datamodel.DataType.boolean,
                    nullable=True,
                ),
            ],
            primary_key=[replica_chunk_column],
            indexes=[],
            constraints=[],
            annotations={"cassandra:partitioning_columns": ["partition"]},
        )

        replica_chunk_tables = ExtraTables.replica_chunk_tables(self._has_chunk_sub_partitions)
        for apdb_table_enum, chunk_table_enum in replica_chunk_tables.items():
            apdb_table_def = self._table_schemas[apdb_table_enum]

            extra_tables[chunk_table_enum] = schema_model.Table(
                id="#" + chunk_table_enum.value,
                name=chunk_table_enum.table_name(self._prefix),
                columns=replica_chunk_columns + apdb_table_def.columns,
                primary_key=apdb_table_def.primary_key[:],
                indexes=[],
                constraints=[],
                annotations={
                    "cassandra:partitioning_columns": [column.name for column in replica_chunk_columns],
                    "cassandra:apdb_column_names": [column.name for column in apdb_table_def.columns],
                },
            )

        # Table with replica chunk data for ApdbUpdateRecord.
        columns = [
            schema_model.Column(
                id=f"#{ExtraTables.ApdbUpdateRecordChunks.value}.update_time_ns",
                name="update_time_ns",
                datatype=felis.datamodel.DataType.long,
                nullable=False,
            ),
            schema_model.Column(
                id=f"#{ExtraTables.ApdbUpdateRecordChunks.value}.update_order",
                name="update_order",
                datatype=felis.datamodel.DataType.int,
                nullable=False,
            ),
            schema_model.Column(
                id=f"#{ExtraTables.ApdbUpdateRecordChunks.value}.update_unique_id",
                name="update_unique_id",
                datatype=schema_model.ExtraDataTypes.UUID,
                nullable=False,
            ),
            schema_model.Column(
                id=f"#{ExtraTables.ApdbUpdateRecordChunks.value}.update_payload",
                name="update_payload",
                datatype=felis.datamodel.DataType.string,
                nullable=False,
            ),
        ]
        extra_tables[ExtraTables.ApdbUpdateRecordChunks] = schema_model.Table(
            id=f"#{ExtraTables.ApdbUpdateRecordChunks.value}",
            name=ExtraTables.ApdbUpdateRecordChunks.table_name(self._prefix),
            columns=replica_chunk_columns + columns,
            primary_key=columns[:3],
            indexes=[],
            constraints=[],
            annotations={
                "cassandra:partitioning_columns": [column.name for column in replica_chunk_columns],
            },
        )

        return extra_tables

    @property
    def replication_enabled(self) -> bool:
        """True when replication is enabled (`bool`)."""
        return self._enable_replica

    def empty(self) -> bool:
        """Return True if database schema is empty.

        Returns
        -------
        empty : `bool`
            `True` if none of the required APDB tables exist in the database,
            `False` if all required tables exist.

        Raises
        ------
        InconsistentSchemaError
            Raised when some of the required tables exist but not all.
        """
        query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s"
        result = self._session.execute(query, (self._keyspace,))
        table_names = {row[0] for row in result.all()}

        existing_tables = []
        missing_tables = []
        for table_enum in self._apdb_tables:
            table_name = table_enum.table_name(self._prefix)
            if self._time_partition_tables and table_enum in self._time_partitioned_tables:
                # Check prefix for time-partitioned tables.
                exists = any(table.startswith(f"{table_name}_") for table in table_names)
            else:
                exists = table_name in table_names
            if exists:
                existing_tables.append(table_name)
            else:
                missing_tables.append(table_name)

        if not missing_tables:
            return False
        elif not existing_tables:
            return True
        else:
            raise InconsistentSchemaError(
                f"Only some required APDB tables exist: {existing_tables}, missing tables: {missing_tables}"
            )

    def existing_tables(self, *args: ApdbTables) -> dict[ApdbTables, list[str]]:
        """Return the list of existing table names for given table.

        Parameters
        ----------
        *args : `ApdbTables`
            Tables for which to return their existing table names.

        Returns
        -------
        tables : `dict` [`ApdbTables`, `list`[`str`]]
            Mapping of the APDB table to the list of the existing table names.
            More than one name can be present in the list if configuration
            specifies per-partition tables.
        """
        if self._time_partition_tables and not set(args).isdisjoint(self._time_partitioned_tables):
            # Some of the tables should have per-partition tables.
            query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s"
            result = self._session.execute(query, (self._keyspace,))
            table_names = {row[0] for row in result.all()}

            tables = {}
            for table_enum in args:
                base_name = table_enum.table_name(self._prefix)
                if table_enum in self._time_partitioned_tables:
                    tables[table_enum] = [table for table in table_names if table.startswith(f"{base_name}_")]
                else:
                    tables[table_enum] = [base_name]
            return tables
        else:
            # Do not check that they exist, we know that they should.
            return {table_enum: [table_enum.table_name(self._prefix)] for table_enum in args}

    def check_column(self, table_enum: ApdbTables | ExtraTables, column: str) -> bool:
        """Check for the existence of the column in a given table.

        Parameters
        ----------
        table_enum : `ApdbTables` or `ExtraTables`
            Table to check for a column.
        column : `str`
            Name of the column to check.

        Returns
        -------
        exists : `bool`
            True if column exists, False otherwise.
        """
        if self._time_partition_tables and table_enum in self._time_partitioned_tables:
            query = (
                "SELECT table_name FROM system_schema.columns WHERE keyspace_name = %s AND column_name = %s "
                "ALLOW FILTERING"
            )
            result = self._session.execute(query, (self._keyspace, column))
            base_name = table_enum.table_name(self._prefix)
            for row in result.all():
                table_name = row[0]
                if table_name.startswith(f"{base_name}_"):
                    return True
            return False
        else:
            table_name = table_enum.table_name(self._prefix)
            query = (
                "SELECT column_name FROM system_schema.columns "
                "WHERE keyspace_name = %s AND table_name = %s AND column_name = %s"
            )
            result = self._session.execute(query, (self._keyspace, table_name, column))
            row = result.one()
            return row is not None

    def tableName(self, table_name: ApdbTables | ExtraTables, time_partition: int | None = None) -> str:
        """Return Cassandra table name for APDB table.

        Parameters
        ----------
        table_name : `ApdbTables` or `ExtraTables`
            Table enum for which to generate table name.
        time_partition : `int`, optional
            Optional time partition, only used for tables that support time
            patitioning.
        """
        return table_name.table_name(self._prefix, time_partition)

    def keyspace(self) -> str:
        """Return Cassandra keyspace for APDB tables."""
        return self._keyspace

    def getColumnMap(self, table_name: ApdbTables | ExtraTables) -> Mapping[str, schema_model.Column]:
        """Return mapping of column names to Column definitions.

        Parameters
        ----------
        table_name : `ApdbTables`
            One of known APDB table names.

        Returns
        -------
        column_map : `dict`
            Mapping of column names to `ColumnDef` instances.
        """
        table_schema = self._table_schema(table_name)
        cmap = {column.name: column for column in table_schema.columns}
        return cmap

    def apdbColumnNames(self, table_name: ApdbTables | ExtraTables) -> list[str]:
        """Return a list of columns names for a table as defined in APDB
        schema.

        Parameters
        ----------
        table_name : `ApdbTables` or `ExtraTables`
            Enum for a table in APDB schema.

        Returns
        -------
        columns : `list` of `str`
            Names of regular columns in the table.
        """
        table_schema = self._table_schema(table_name)
        return table_schema.annotations["cassandra:apdb_column_names"]

    def partitionColumns(self, table_name: ApdbTables | ExtraTables) -> list[str]:
        """Return a list of columns used for table partitioning.

        Parameters
        ----------
        table_name : `ApdbTables`
            Table name in APDB schema

        Returns
        -------
        columns : `list` of `str`
            Names of columns used for partitioning.
        """
        table_schema = self._table_schema(table_name)
        return table_schema.annotations.get("cassandra:partitioning_columns", [])

    def clusteringColumns(self, table_name: ApdbTables | ExtraTables) -> list[str]:
        """Return a list of columns used for clustering.

        Parameters
        ----------
        table_name : `ApdbTables`
            Table name in APDB schema

        Returns
        -------
        columns : `list` of `str`
            Names of columns for used for clustering.
        """
        table_schema = self._table_schema(table_name)
        return [column.name for column in table_schema.primary_key]

    def makeSchema(
        self,
        *,
        drop: bool = False,
        part_range: ApdbCassandraTimePartitionRange | None = None,
        replication_factor: int | None = None,
        table_options: CreateTableOptions | None = None,
    ) -> None:
        """Create or re-create all tables.

        Parameters
        ----------
        drop : `bool`
            If True then drop tables before creating new ones. Note that
            only tables are dropped and not the whole keyspace.
        part_range : `ApdbCassandraTimePartitionRange` or `None`
            Start and end partition number for time partitions. Used to create
            per-partition DiaObject, DiaSource, and DiaForcedSource tables. If
            `None` then per-partition tables are not created.
        replication_factor : `int`, optional
            Replication factor used when creating new keyspace, if keyspace
            already exists its replication factor is not changed.
        """
        # Try to create keyspace if it does not exist
        if replication_factor is None:
            replication_factor = 1

        # If keyspace exists check its replication factor.
        query = "SELECT replication FROM system_schema.keyspaces WHERE keyspace_name = %s"
        result = self._session.execute(query, (self._keyspace,))
        if row := result.one():
            # Check replication factor, ignore strategy class.
            repl_config = cast(Mapping[str, str], row[0])
            current_repl = int(repl_config["replication_factor"])
            if replication_factor != current_repl:
                raise ValueError(
                    f"New replication factor {replication_factor} differs from the replication factor "
                    f"for already existing keyspace: {current_repl}"
                )
        else:
            # Need a new keyspace.
            query = (
                f'CREATE KEYSPACE "{self._keyspace}"'
                " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': "
                f"{replication_factor}"
                "}"
            )
            self._session.execute(query)

        table_options = self._update_table_options(table_options)
        for table in self._apdb_tables:
            if table is ApdbTables.DiaObject and self._enable_replica and self._replica_skips_diaobjects:
                continue
            if table is ApdbTables.SSSource:
                # We do not support SSSource table yet.
                continue
            self._makeTableSchema(table, drop, part_range, table_options)
        for extra_table in self._extra_tables:
            self._makeTableSchema(extra_table, drop, part_range, table_options)

    def _update_table_options(self, options: CreateTableOptions | None) -> CreateTableOptions | None:
        """Extend table options with options for internal tables."""
        # We want to add TTL option to ApdbVisitDetector table.
        if not self._has_visit_detector_table:
            return options

        if not options:
            options = CreateTableOptions()

        # set both TTL and gc_grace_seconds to 24h.
        options.table_options.append(
            TableOptions(
                tables=[ExtraTables.ApdbVisitDetector.table_name(self._prefix)],
                options="default_time_to_live=86400 AND gc_grace_seconds=86400",
            )
        )

        return options

    def _makeTableSchema(
        self,
        table: ApdbTables | ExtraTables,
        drop: bool = False,
        part_range: ApdbCassandraTimePartitionRange | None = None,
        table_options: CreateTableOptions | None = None,
    ) -> None:
        _LOG.debug("Making table %s", table)

        fullTable = table.table_name(self._prefix)

        table_list = [fullTable]
        if part_range is not None:
            if table in self._time_partitioned_tables:
                table_list = [table.table_name(self._prefix, part) for part in part_range.range()]

        if drop:
            queries = [f'DROP TABLE IF EXISTS "{self._keyspace}"."{table_name}"' for table_name in table_list]
            futures = [self._session.execute_async(query, timeout=None) for query in queries]
            for future in futures:
                _LOG.debug("wait for query: %s", future.query)
                future.result()
                _LOG.debug("query finished: %s", future.query)

        queries = []
        options = table_options.get_options(fullTable).strip() if table_options else None
        for table_name in table_list:
            if_not_exists = "" if drop else "IF NOT EXISTS"
            columns = ", ".join(self._tableColumns(table))
            query = f'CREATE TABLE {if_not_exists} "{self._keyspace}"."{table_name}" ({columns})'
            if options:
                query = f"{query} WITH {options}"
            _LOG.debug("query: %s", query)
            queries.append(query)
        futures = [self._session.execute_async(query, timeout=None) for query in queries]
        for future in futures:
            _LOG.debug("wait for query: %s", future.query)
            future.result()
            _LOG.debug("query finished: %s", future.query)

    def _tableColumns(self, table_name: ApdbTables | ExtraTables) -> list[str]:
        """Return set of columns in a table

        Parameters
        ----------
        table_name : `ApdbTables`
            Name of the table.

        Returns
        -------
        column_defs : `list`
            List of strings in the format "column_name type".
        """
        table_schema = self._table_schema(table_name)

        # must have partition columns and clustering columns
        part_columns = table_schema.annotations.get("cassandra:partitioning_columns", [])
        clust_columns = [column.name for column in table_schema.primary_key]
        _LOG.debug("part_columns: %s", part_columns)
        _LOG.debug("clust_columns: %s", clust_columns)
        if not part_columns:
            raise ValueError(f"Table {table_name} configuration is missing partition index")

        # all columns
        column_defs = []
        for column in table_schema.columns:
            ctype = self._type_map[column.datatype]
            column_defs.append(f'"{column.name}" {ctype}')

        # primary key definition
        part_columns = [f'"{col}"' for col in part_columns]
        clust_columns = [f'"{col}"' for col in clust_columns]
        if len(part_columns) > 1:
            columns = ", ".join(part_columns)
            part_columns = [f"({columns})"]
        pkey = ", ".join(part_columns + clust_columns)
        _LOG.debug("pkey: %s", pkey)
        column_defs.append(f"PRIMARY KEY ({pkey})")

        return column_defs

    def _table_schema(self, table: ApdbTables | ExtraTables) -> schema_model.Table:
        """Return schema definition for a table."""
        if isinstance(table, ApdbTables):
            table_schema = self._apdb_tables[table]
        else:
            table_schema = self._extra_tables[table]
        return table_schema

    def table_row_size(self, table: ApdbTables | ExtraTables) -> int:
        """Return an estimate of the row size of a given table.

        Parameters
        ----------
        table : `ApdbTables` or `ExtraTables`

        Returns
        -------
        size : `int`
            An estimate of a table row size.

        Notes
        -----
        Returned size is not exact. When table has variable-size columns (e.g.
        strings) may be incorrect. Stored data size or wire-level protocol size
        can be smaller if some columns are not set or set to NULL.
        """
        table_schema = self._table_schema(table)
        size = sum(column.size() for column in table_schema.columns)
        return size

    def time_partitioned_tables(self) -> list[ApdbTables]:
        """Make the list of time-partitioned tables.

        Returns
        -------
        tables : `list` [`ApdbTables`]
            Tables the are time-partitioned.
        """
        if not self._time_partition_tables:
            return []
        has_dia_object_table = not (self._enable_replica and self._replica_skips_diaobjects)
        tables = [ApdbTables.DiaSource, ApdbTables.DiaForcedSource]
        if has_dia_object_table:
            tables.append(ApdbTables.DiaObject)
        return tables
