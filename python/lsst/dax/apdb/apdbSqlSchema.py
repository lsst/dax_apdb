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
import logging
import uuid
from collections.abc import Mapping
from typing import Any

import felis.types
import sqlalchemy
from felis import simple
from sqlalchemy import (
    DDL,
    Column,
    ForeignKeyConstraint,
    Index,
    MetaData,
    PrimaryKeyConstraint,
    Table,
    UniqueConstraint,
    event,
    inspect,
)
from sqlalchemy.dialects.postgresql import UUID

from .apdbSchema import ApdbSchema, ApdbTables

_LOG = logging.getLogger(__name__)


#
# Copied from daf_butler.
#
class GUID(sqlalchemy.TypeDecorator):
    """Platform-independent GUID type.

    Uses PostgreSQL's UUID type, otherwise uses CHAR(32), storing as
    stringified hex values.
    """

    impl = sqlalchemy.CHAR

    cache_ok = True

    def load_dialect_impl(self, dialect: sqlalchemy.engine.Dialect) -> sqlalchemy.types.TypeEngine:
        if dialect.name == "postgresql":
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(sqlalchemy.CHAR(32))

    def process_bind_param(self, value: Any, dialect: sqlalchemy.engine.Dialect) -> str | None:
        if value is None:
            return value

        # Coerce input to UUID type, in general having UUID on input is the
        # only thing that we want but there is code right now that uses ints.
        if isinstance(value, int):
            value = uuid.UUID(int=value)
        elif isinstance(value, bytes):
            value = uuid.UUID(bytes=value)
        elif isinstance(value, str):
            # hexstring
            value = uuid.UUID(hex=value)
        elif not isinstance(value, uuid.UUID):
            raise TypeError(f"Unexpected type of a bind value: {type(value)}")

        if dialect.name == "postgresql":
            return str(value)
        else:
            return "%.32x" % value.int

    def process_result_value(
        self, value: str | uuid.UUID | None, dialect: sqlalchemy.engine.Dialect
    ) -> uuid.UUID | None:
        if value is None:
            return value
        elif isinstance(value, uuid.UUID):
            # sqlalchemy 2 converts to UUID internally
            return value
        else:
            return uuid.UUID(hex=value)


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

        self._metadata = MetaData(schema=namespace)

        # map YAML column types to SQLAlchemy
        self._type_map = {
            felis.types.Double: self._getDoubleType(engine),
            felis.types.Float: sqlalchemy.types.Float,
            felis.types.Timestamp: sqlalchemy.types.TIMESTAMP,
            felis.types.Long: sqlalchemy.types.BigInteger,
            felis.types.Int: sqlalchemy.types.Integer,
            felis.types.Short: sqlalchemy.types.Integer,
            felis.types.Byte: sqlalchemy.types.Integer,
            felis.types.Binary: sqlalchemy.types.LargeBinary,
            felis.types.Text: sqlalchemy.types.Text,
            felis.types.String: sqlalchemy.types.CHAR,
            felis.types.Char: sqlalchemy.types.CHAR,
            felis.types.Unicode: sqlalchemy.types.CHAR,
            felis.types.Boolean: sqlalchemy.types.Boolean,
        }

        # Add pixelId column and index to tables that need it
        for table in self.pixel_id_tables:
            tableDef = self.tableSchemas.get(table)
            if not tableDef:
                continue
            column = simple.Column(
                id=f"#{htm_index_column}",
                name=htm_index_column,
                datatype=felis.types.Long,
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
                index = simple.Index(id=f"#{name}", name=name, columns=[column])
                tableDef.indexes.append(index)

        # generate schema for all tables, must be called last
        self._apdb_tables = self._make_apdb_tables()
        self._extra_tables = self._make_extra_tables(self._apdb_tables)

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
        inspector = inspect(self._engine)
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
            create_schema = DDL(
                "CREATE SCHEMA IF NOT EXISTS %(schema)s", context={"schema": quoted_schema}
            ).execute_if(dialect="postgresql")
            event.listen(self._metadata, "before_create", create_schema)

        # create all tables (optionally drop first)
        if drop:
            _LOG.info("dropping all tables")
            self._metadata.drop_all(self._engine)
        _LOG.info("creating all tables")
        self._metadata.create_all(self._engine)

        # Reset possibly cached value.
        self._has_replica_chunks = None
        self._metadata_check = None

    def get_table(self, table_enum: ApdbTables | ExtraTables) -> Table:
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
                        inspector = inspect(self._engine)
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

    def get_apdb_columns(self, table_enum: ApdbTables | ExtraTables) -> list[Column]:
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
        inspector = inspect(self._engine)
        db_tables = set(inspector.get_table_names(schema=self._metadata.schema))
        return ExtraTables.ApdbReplicaChunks.table_name(self._prefix) in db_tables

    def _make_apdb_tables(self, mysql_engine: str = "InnoDB") -> Mapping[ApdbTables, Table]:
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

            columns = self._tableColumns(table_enum)
            constraints = self._tableIndices(table_enum)
            table = Table(
                table_enum.table_name(self._prefix),
                self._metadata,
                *columns,
                *constraints,
                mysql_engine=mysql_engine,
            )
            tables[table_enum] = table

        return tables

    def _make_extra_tables(
        self, apdb_tables: Mapping[ApdbTables, Table], mysql_engine: str = "InnoDB"
    ) -> Mapping[ExtraTables, Table]:
        """Generate schema for insert ID tables."""
        tables: dict[ExtraTables, Table] = {}
        if not self._enable_replica:
            return tables

        # Parent table needs to be defined first
        column_defs: list[Column] = [
            Column("apdb_replica_chunk", sqlalchemy.types.BigInteger, primary_key=True),
            Column("last_update_time", sqlalchemy.types.TIMESTAMP, nullable=False),
            Column("unique_id", GUID, nullable=False),
        ]
        parent_table = Table(
            ExtraTables.ApdbReplicaChunks.table_name(self._prefix),
            self._metadata,
            *column_defs,
            mysql_engine=mysql_engine,
        )
        tables[ExtraTables.ApdbReplicaChunks] = parent_table

        for table_enum, apdb_enum in ExtraTables.replica_chunk_tables().items():
            apdb_table = apdb_tables[apdb_enum]
            columns = self._replicaChunkColumns(table_enum)
            constraints = self._replicaChunkIndices(table_enum, apdb_table, parent_table)
            table = Table(
                table_enum.table_name(self._prefix),
                self._metadata,
                *columns,
                *constraints,
                mysql_engine=mysql_engine,
            )
            tables[table_enum] = table

        return tables

    def _tableColumns(self, table_name: ApdbTables) -> list[Column]:
        """Return set of columns in a table

        Parameters
        ----------
        table_name : `ApdbTables`
            Name of the table.

        Returns
        -------
        column_defs : `list`
            List of `Column` objects.
        """
        # get the list of columns in primary key, they are treated somewhat
        # specially below
        table_schema = self.tableSchemas[table_name]

        # convert all column dicts into alchemy Columns
        column_defs: list[Column] = []
        for column in table_schema.columns:
            kwargs: dict[str, Any] = dict(nullable=column.nullable)
            if column.value is not None:
                kwargs.update(server_default=str(column.value))
            if column in table_schema.primary_key:
                kwargs.update(autoincrement=False)
            ctype = self._type_map[column.datatype]
            column_defs.append(Column(column.name, ctype, **kwargs))

        return column_defs

    def _tableIndices(self, table_name: ApdbTables) -> list[sqlalchemy.schema.SchemaItem]:
        """Return set of constraints/indices in a table

        Parameters
        ----------
        table_name : `ApdbTables`
            Name of the table.
        info : `dict`
            Additional options passed to SQLAlchemy index constructor.

        Returns
        -------
        index_defs : `list`
            List of SQLAlchemy index/constraint objects.
        """
        table_schema = self.tableSchemas[table_name]

        # convert all index dicts into alchemy Columns
        index_defs: list[sqlalchemy.schema.SchemaItem] = []
        if table_schema.primary_key:
            index_defs.append(PrimaryKeyConstraint(*[column.name for column in table_schema.primary_key]))
        for index in table_schema.indexes:
            name = self._prefix + index.name if index.name else ""
            index_defs.append(Index(name, *[column.name for column in index.columns]))
        for constraint in table_schema.constraints:
            constr_name: str | None = None
            if constraint.name:
                constr_name = self._prefix + constraint.name
            if isinstance(constraint, simple.UniqueConstraint):
                index_defs.append(
                    UniqueConstraint(*[column.name for column in constraint.columns], name=constr_name)
                )

        return index_defs

    def _replicaChunkColumns(self, table_enum: ExtraTables) -> list[Column]:
        """Return list of columns for replica chunks tables."""
        column_defs: list[Column] = [
            Column("apdb_replica_chunk", sqlalchemy.types.BigInteger, nullable=False)
        ]
        replica_chunk_tables = ExtraTables.replica_chunk_tables()
        if table_enum in replica_chunk_tables:
            column_defs += self._tablePkColumns(replica_chunk_tables[table_enum])
        else:
            assert False, "Above branches have to cover all enum values"
        return column_defs

    def _tablePkColumns(self, table_enum: ApdbTables) -> list[Column]:
        """Return a list of columns for table PK."""
        table_schema = self.tableSchemas[table_enum]
        column_defs: list[Column] = []
        for column in table_schema.primary_key:
            ctype = self._type_map[column.datatype]
            column_defs.append(Column(column.name, ctype, nullable=False, autoincrement=False))
        return column_defs

    def _replicaChunkIndices(
        self,
        table_enum: ExtraTables,
        apdb_table: sqlalchemy.schema.Table,
        parent_table: sqlalchemy.schema.Table,
    ) -> list[sqlalchemy.schema.SchemaItem]:
        """Return set of constraints/indices for replica chunk tables."""
        index_defs: list[sqlalchemy.schema.SchemaItem] = []

        # Special case for insert ID tables that are not in felis schema.
        replica_chunk_tables = ExtraTables.replica_chunk_tables()
        if table_enum in replica_chunk_tables:
            # PK is the same as for original table
            pk_names = [column.name for column in self._tablePkColumns(replica_chunk_tables[table_enum])]
            index_defs.append(PrimaryKeyConstraint(*pk_names))
            # Non-unique index on replica chunk column.
            name = self._prefix + table_enum.name + "_idx"
            index_defs.append(Index(name, "apdb_replica_chunk"))
            # Foreign key to original table
            pk_columns = [apdb_table.columns[column] for column in pk_names]
            index_defs.append(
                ForeignKeyConstraint(pk_names, pk_columns, onupdate="CASCADE", ondelete="CASCADE")
            )
            # Foreign key to parent table
            index_defs.append(
                ForeignKeyConstraint(
                    ["apdb_replica_chunk"],
                    [parent_table.columns["apdb_replica_chunk"]],
                    onupdate="CASCADE",
                    ondelete="CASCADE",
                )
            )
        else:
            assert False, "Above branches have to cover all enum values"
        return index_defs

    @classmethod
    def _getDoubleType(cls, engine: sqlalchemy.engine.Engine) -> type | sqlalchemy.types.TypeEngine:
        """DOUBLE type is database-specific, select one based on dialect.

        Parameters
        ----------
        engine : `sqlalchemy.engine.Engine`
            Database engine.

        Returns
        -------
        type_object : `object`
            Database-specific type definition.
        """
        if engine.name == "mysql":
            from sqlalchemy.dialects.mysql import DOUBLE

            return DOUBLE(asdecimal=False)
        elif engine.name == "postgresql":
            from sqlalchemy.dialects.postgresql import DOUBLE_PRECISION

            return DOUBLE_PRECISION
        elif engine.name == "oracle":
            from sqlalchemy.dialects.oracle import DOUBLE_PRECISION

            return DOUBLE_PRECISION
        elif engine.name == "sqlite":
            # all floats in sqlite are 8-byte
            from sqlalchemy.dialects.sqlite import REAL

            return REAL
        else:
            raise TypeError("cannot determine DOUBLE type, unexpected dialect: " + engine.name)
