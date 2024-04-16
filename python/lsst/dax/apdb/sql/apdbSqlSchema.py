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

"""Module responsible for APDB schema operations.
"""

from __future__ import annotations

__all__ = ["ApdbSqlSchema", "ExtraTables"]

import enum
import itertools
import logging
from collections.abc import Mapping

import felis.datamodel
import sqlalchemy

from .. import schema_model
from ..apdbSchema import ApdbSchema, ApdbTables
from .modelToSql import ModelToSql

_LOG = logging.getLogger(__name__)


class InconsistentSchemaError(RuntimeError):
    """Exception raised when schema state is inconsistent."""


@enum.unique
class ExtraTables(enum.Enum):
    """Names of the tables used for tracking insert IDs."""

    ApdbReplicaChunks = "ApdbReplicaChunks"
    """Name of the table for replica chunks records."""

    DiaObjectChunks = "DiaObjectChunks"
    """Name of the table for DIAObject chunk data."""

    DiaSourceChunks = "DiaSourceChunks"
    """Name of the table for DIASource chunk data."""

    DiaForcedSourceChunks = "DiaForcedSourceChunks"
    """Name of the table for DIAForcedSource chunk data."""

    def table_name(self, prefix: str = "") -> str:
        """Return full table name."""
        return prefix + self.value

    @classmethod
    def replica_chunk_tables(cls) -> Mapping[ExtraTables, ApdbTables]:
        """Return mapping of tables used for replica chunk storage to their
        corresponding regular tables.
        """
        return {
            cls.DiaObjectChunks: ApdbTables.DiaObject,
            cls.DiaSourceChunks: ApdbTables.DiaSource,
            cls.DiaForcedSourceChunks: ApdbTables.DiaForcedSource,
        }


class ApdbSqlSchema(ApdbSchema):
    """Class for management of APDB schema.

    Attributes
    ----------
    objects : `sqlalchemy.Table`
        DiaObject table instance
    objects_last : `sqlalchemy.Table`
        DiaObjectLast table instance, may be None
    sources : `sqlalchemy.Table`
        DiaSource table instance
    forcedSources : `sqlalchemy.Table`
        DiaForcedSource table instance
    has_replica_chunks : `bool`
        If true then schema has tables for replication chunks.

    Parameters
    ----------
    engine : `sqlalchemy.engine.Engine`
        SQLAlchemy engine instance
    dia_object_index : `str`
        Indexing mode for DiaObject table, see `ApdbSqlConfig.dia_object_index`
        for details.
    htm_index_column : `str`
        Name of a HTM index column for DiaObject and DiaSource tables.
    schema_file : `str`
        Name of the YAML schema file.
    schema_name : `str`, optional
        Name of the schema in YAML files.
    prefix : `str`, optional
        Prefix to add to all schema elements.
    namespace : `str`, optional
        Namespace (or schema name) to use for all APDB tables.
    enable_replica : `bool`, optional
        If `True` then use additional tables for replica chunks.
    """

    pixel_id_tables = (ApdbTables.DiaObject, ApdbTables.DiaObjectLast, ApdbTables.DiaSource)
    """Tables that need pixelId column for spatial indexing."""

    def __init__(
        self,
        engine: sqlalchemy.engine.Engine,
        dia_object_index: str,
        htm_index_column: str,
        schema_file: str,
        schema_name: str = "ApdbSchema",
        prefix: str = "",
        namespace: str | None = None,
        enable_replica: bool = False,
    ):
        super().__init__(schema_file, schema_name)

        self._engine = engine
        self._dia_object_index = dia_object_index
        self._htm_index_column = htm_index_column
        self._prefix = prefix
        self._enable_replica = enable_replica

        self._metadata = sqlalchemy.schema.MetaData(schema=namespace)

        # Add pixelId column and index to tables that need it
        for table in self.pixel_id_tables:
            tableDef = self.tableSchemas.get(table)
            if not tableDef:
                continue
            column = schema_model.Column(
                id=f"#{htm_index_column}",
                name=htm_index_column,
                datatype=felis.datamodel.DataType.LONG,
                nullable=False,
                value=None,
                description="Pixelization index column.",
                table=tableDef,
            )
            tableDef.columns.append(column)

            # Adjust index if needed
            if table == ApdbTables.DiaObject and self._dia_object_index == "pix_id_iov":
                tableDef.primary_key.insert(0, column)

            if table is ApdbTables.DiaObjectLast:
                # use it as a leading PK column
                tableDef.primary_key.insert(0, column)
            else:
                # make a regular index
                name = f"IDX_{tableDef.name}_{htm_index_column}"
                index = schema_model.Index(id=f"#{name}", name=name, columns=[column])
                tableDef.indexes.append(index)

        # generate schema for all tables, must be called last
        apdb_tables = self._make_apdb_tables()
        extra_tables = self._make_extra_tables(apdb_tables)

        converter = ModelToSql(metadata=self._metadata, prefix=self._prefix)
        id_to_table = converter.make_tables(itertools.chain(apdb_tables.values(), extra_tables.values()))

        self._apdb_tables = {
            apdb_enum: id_to_table[table_model.id] for apdb_enum, table_model in apdb_tables.items()
        }
        self._extra_tables = {
            extra_enum: id_to_table[table_model.id] for extra_enum, table_model in extra_tables.items()
        }

        self._has_replica_chunks: bool | None = None
        self._metadata_check: bool | None = None

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
        inspector = sqlalchemy.inspect(self._engine)
        table_names = set(inspector.get_table_names(self._metadata.schema))

        existing_tables = []
        missing_tables = []
        for table_enum in self._apdb_tables:
            table_name = table_enum.table_name(self._prefix)
            if table_name in table_names:
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

    def makeSchema(self, drop: bool = False) -> None:
        """Create or re-create all tables.

        Parameters
        ----------
        drop : `bool`, optional
            If True then drop tables before creating new ones.
        """
        # Create namespace if it does not exist yet, for now this only makes
        # sense for postgres.
        if self._metadata.schema:
            dialect = self._engine.dialect
            quoted_schema = dialect.preparer(dialect).quote_schema(self._metadata.schema)
            create_schema = sqlalchemy.schema.DDL(
                "CREATE SCHEMA IF NOT EXISTS %(schema)s", context={"schema": quoted_schema}
            ).execute_if(dialect="postgresql")
            sqlalchemy.event.listen(self._metadata, "before_create", create_schema)

        # create all tables (optionally drop first)
        if drop:
            _LOG.info("dropping all tables")
            self._metadata.drop_all(self._engine)
        _LOG.info("creating all tables")
        self._metadata.create_all(self._engine)

        # Reset possibly cached value.
        self._has_replica_chunks = None
        self._metadata_check = None

    def get_table(self, table_enum: ApdbTables | ExtraTables) -> sqlalchemy.schema.Table:
        """Return SQLAlchemy table instance for a specified table type/enum.

        Parameters
        ----------
        table_enum : `ApdbTables` or `ExtraTables`
            Type of table to return.

        Returns
        -------
        table : `sqlalchemy.schema.Table`
            Table instance.

        Raises
        ------
        ValueError
            Raised if ``table_enum`` is not valid for this database.
        """
        try:
            if isinstance(table_enum, ApdbTables):
                if table_enum is ApdbTables.metadata:
                    # There may be cases when schema is configured with the
                    # metadata table but database is still missing it. Check
                    # that table actually exists in the database. Note that
                    # this may interact with `makeSchema`.
                    if self._metadata_check is None:
                        inspector = sqlalchemy.inspect(self._engine)
                        table_name = table_enum.table_name(self._prefix)
                        self._metadata_check = inspector.has_table(table_name, schema=self._metadata.schema)
                    if not self._metadata_check:
                        # this will be caught below
                        raise LookupError("metadata table is missing")
                return self._apdb_tables[table_enum]
            else:
                return self._extra_tables[table_enum]
        except LookupError:
            raise ValueError(f"Table type {table_enum} does not exist in the schema") from None

    def get_apdb_columns(self, table_enum: ApdbTables | ExtraTables) -> list[sqlalchemy.schema.Column]:
        """Return list of columns defined for a table in APDB schema.

        Returned list excludes columns that are implementation-specific, e.g.
        ``pixelId`` column is not include in the returned list.

        Parameters
        ----------
        table_enum : `ApdbTables` or `ExtraTables`
            Type of table.

        Returns
        -------
        table : `list` [`sqlalchemy.schema.Column`]
            Table instance.

        Raises
        ------
        ValueError
            Raised if ``table_enum`` is not valid for this database.
        """
        table = self.get_table(table_enum)
        exclude_columns = set()
        if table_enum in self.pixel_id_tables:
            exclude_columns.add(self._htm_index_column)
        return [column for column in table.columns if column.name not in exclude_columns]

    @property
    def has_replica_chunks(self) -> bool:
        """Whether insert ID tables are to be used (`bool`)."""
        if self._has_replica_chunks is None:
            self._has_replica_chunks = self._enable_replica and self._check_replica_chunks()
        return self._has_replica_chunks

    def _check_replica_chunks(self) -> bool:
        """Check whether database has tables for tracking insert IDs."""
        inspector = sqlalchemy.inspect(self._engine)
        db_tables = set(inspector.get_table_names(schema=self._metadata.schema))
        return ExtraTables.ApdbReplicaChunks.table_name(self._prefix) in db_tables

    def _make_apdb_tables(self, mysql_engine: str = "InnoDB") -> Mapping[ApdbTables, schema_model.Table]:
        """Generate schema for regular tables.

        Parameters
        ----------
        mysql_engine : `str`, optional
            MySQL engine type to use for new tables.
        """
        tables = {}
        for table_enum in ApdbTables:
            if table_enum is ApdbTables.DiaObjectLast and self._dia_object_index != "last_object_table":
                continue
            if table_enum is ApdbTables.metadata and table_enum not in self.tableSchemas:
                # Schema does not define metadata.
                continue
            table = self.tableSchemas[table_enum]
            tables[table_enum] = table

        return tables

    def _make_extra_tables(
        self, apdb_tables: Mapping[ApdbTables, schema_model.Table]
    ) -> Mapping[ExtraTables, schema_model.Table]:
        """Generate schema for insert ID tables."""
        if not self._enable_replica:
            return {}

        tables = {}
        column_defs: list[schema_model.Column] = [
            schema_model.Column(
                name="apdb_replica_chunk",
                id="#ApdbReplicaChunks.apdb_replica_chunk",
                datatype=felis.datamodel.DataType.LONG,
            ),
            schema_model.Column(
                name="last_update_time",
                id="#ApdbReplicaChunks.last_update_time",
                datatype=felis.datamodel.DataType.TIMESTAMP,
                nullable=False,
            ),
            schema_model.Column(
                name="unique_id",
                id="#ApdbReplicaChunks.unique_id",
                datatype=schema_model.ExtraDataTypes.UUID,
                nullable=False,
            ),
        ]
        parent_table = schema_model.Table(
            name=ExtraTables.ApdbReplicaChunks.table_name(self._prefix),
            id="#ApdbReplicaChunks",
            columns=column_defs,
            primary_key=[column_defs[0]],
            constraints=[],
            indexes=[],
        )
        tables[ExtraTables.ApdbReplicaChunks] = parent_table

        for table_enum, apdb_enum in ExtraTables.replica_chunk_tables().items():
            apdb_table = apdb_tables[apdb_enum]
            table_name = table_enum.table_name(self._prefix)

            columns = self._replicaChunkColumns(table_enum, apdb_enum)
            column_map = {column.name: column for column in columns}
            # PK is the same as for original table
            pk_columns = [column_map[column.name] for column in apdb_table.primary_key]

            indices = self._replicaChunkIndices(table_enum, column_map)
            constraints = self._replicaChunkConstraints(table_enum, apdb_table, parent_table, column_map)
            table = schema_model.Table(
                name=table_name,
                id=f"#{table_name}",
                columns=columns,
                primary_key=pk_columns,
                indexes=indices,
                constraints=constraints,
            )
            tables[table_enum] = table

        return tables

    def _replicaChunkColumns(
        self, table_enum: ExtraTables, apdb_enum: ApdbTables
    ) -> list[schema_model.Column]:
        """Return list of columns for replica chunks tables."""
        table_name = table_enum.table_name()
        column_defs: list[schema_model.Column] = [
            schema_model.Column(
                name="apdb_replica_chunk",
                id=f"#{table_name}.apdb_replica_chunk",
                datatype=felis.datamodel.DataType.LONG,
                nullable=False,
            )
        ]
        if table_enum in ExtraTables.replica_chunk_tables():
            table_model = self.tableSchemas[apdb_enum]
            column_defs += [column.clone() for column in table_model.primary_key]
        else:
            assert False, "Above branches have to cover all enum values"
        return column_defs

    def _replicaChunkIndices(
        self,
        table_enum: ExtraTables,
        column_map: Mapping[str, schema_model.Column],
    ) -> list[schema_model.Index]:
        """Return set of indices for replica chunk table."""
        index_defs: list[schema_model.Index] = []
        if table_enum in ExtraTables.replica_chunk_tables():
            # Non-unique index on replica chunk column.
            name = self._prefix + table_enum.name + "_apdb_replica_chunk_idx"
            column = column_map["apdb_replica_chunk"]
            index_defs.append(schema_model.Index(name=name, id=f"#{name}", columns=[column]))
        return index_defs

    def _replicaChunkConstraints(
        self,
        table_enum: ExtraTables,
        apdb_table: schema_model.Table,
        parent_table: schema_model.Table,
        column_map: Mapping[str, schema_model.Column],
    ) -> list[schema_model.Constraint]:
        """Return set of constraints for replica chunk table."""
        constraints: list[schema_model.Constraint] = []
        replica_chunk_tables = ExtraTables.replica_chunk_tables()
        if table_enum in replica_chunk_tables:
            # Foreign key to original table
            name = f"{table_enum.table_name()}_fk_{apdb_table.name}"
            other_columns = apdb_table.primary_key
            this_columns = [column_map[column.name] for column in apdb_table.primary_key]
            constraints.append(
                schema_model.ForeignKeyConstraint(
                    name=name,
                    id=f"#{name}",
                    columns=this_columns,
                    referenced_columns=other_columns,
                    onupdate="CASCADE",
                    ondelete="CASCADE",
                )
            )

            # Foreign key to parent chunk ID table
            name = f"{table_enum.table_name()}_fk_{parent_table.name}"
            other_columns = parent_table.primary_key
            this_columns = [column_map[column.name] for column in parent_table.primary_key]
            constraints.append(
                schema_model.ForeignKeyConstraint(
                    name=name,
                    id=f"#{name}",
                    columns=this_columns,
                    referenced_columns=other_columns,
                    onupdate="CASCADE",
                    ondelete="CASCADE",
                )
            )
        return constraints
