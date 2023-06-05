import lsst.dax.apdb.apdbSql
assert type(config)==lsst.dax.apdb.apdbSql.ApdbSqlConfig, 'config is of type %s.%s instead of lsst.dax.apdb.apdbSql.ApdbSqlConfig' % (type(config).__module__, type(config).__name__)

# SQLAlchemy database connection URI
config.db_url="sqlite:///apdbproto.db"

# Transaction isolation level
# Allowed values:
# 	READ_COMMITTED	Read committed
# 	READ_UNCOMMITTED	Read uncommitted
# 	REPEATABLE_READ	Repeatable read
# 	SERIALIZABLE	Serializable
# 	None	Field is optional
#
config.isolation_level='READ_UNCOMMITTED'

# If False then disable SQLAlchemy connection pool. Do not use connection pool when forking.
config.connection_pool=True

# If True then pass SQLAlchemy echo option.
config.sql_echo=False

# Indexing mode for DiaObject table
# Allowed values:
# 	baseline	Index defined in baseline schema
# 	pix_id_iov	(pixelId, objectId, iovStart) PK
# 	last_object_table	Separate DiaObjectLast table
# 	None	Field is optional
#
config.dia_object_index='last_object_table'

# Number of months of history to read from DiaSource
config.read_sources_months=12

# Number of months of history to read from DiaForcedSource
config.read_forced_sources_months=12

# List of columns to read from DiaObject, by default read all columns
config.dia_object_columns = [
    "diaObjectId", "lastNonForcedSource", "ra", "dec",
    "raSigma", "decSigma", "ra_dec_Cov", "pixelId"
    ]

# If True (default) then use "upsert" for DiaObjectsLast table
config.object_last_replace=True

# Location of (YAML) configuration file with standard schema
# config.schema_file = '${SDM_SCHEMAS_DIR}/yml/apdb.yaml'

# Name of the schema in YAML configuration file.
# config.schema_name = "ApdbSchema"

# Prefix to add to table names and index names
config.prefix=''

# If True then run EXPLAIN SQL command on each executed query
config.explain=False

# If True then print/log timing information
# config.timer=False
