import lsst.dax.apdb.apdbCassandra
assert type(config)==lsst.dax.apdb.apdbCassandra.ApdbCassandraConfig, 'config is of type %s.%s instead of lsst.dax.apdb.apdbCassandra.ApdbCassandraConfig' % (type(config).__module__, type(config).__name__)
# Number of months of history to read from DiaSource
config.read_sources_months=12

# Number of months of history to read from DiaForcedSource
config.read_forced_sources_months=12

# Location of (YAML) configuration file with standard schema
config.schema_file='${DAX_APDB_DIR}/data/apdb-schema.yaml'

# Location of (YAML) configuration file with extra schema
config.extra_schema_file='${DAX_APDB_DIR}/data/apdb-schema-extra.yaml'

# The list of contact points to try connecting for cluster discovery.
config.contact_points=['127.0.0.1']

# List of internal IP addresses for contact_points.
config.private_ips=[]

# Default keyspace for operations.
config.keyspace='apdb'

# Name for consistency level of read operations, default: QUORUM, can be ONE.
config.read_consistency='QUORUM'

# Name for consistency level of write operations, default: QUORUM, can be ONE.
config.write_consistency='QUORUM'

# Timeout in seconds for read operations.
config.read_timeout=120.0

# Timeout in seconds for write operations.
config.write_timeout=10.0

# Concurrency level for read operations.
config.read_concurrency=500

# Cassandra protocol version to use, default is V4
config.protocol_version=4

# List of columns to read from DiaObject, by default read all columns
config.dia_object_columns=[]

# Prefix to add to table names
config.prefix=''

# Pixelization used for partitioning index.
config.part_pixelization='mq3c'

# Pixelization level used for partitioning index.
config.part_pix_level=10

# Names ra/dec columns in DiaObject table
config.ra_dec_columns=['ra', 'decl']

# If True then print/log timing information
config.timer=False

# Use per-partition tables for sources instead of partitioning by time
config.time_partition_tables=True

# Time partitoning granularity in days, this value must not be changed after database is initialized
config.time_partition_days=30

# Starting time for per-partion tables, in yyyy-mm-ddThh:mm:ss format, in TAI. This is used only when time_partition_tables is True.
config.time_partition_start='2018-12-01T00:00:00'

# Ending time for per-partion tables, in yyyy-mm-ddThh:mm:ss format, in TAI This is used only when time_partition_tables is True.
config.time_partition_end='2030-01-01T00:00:00'

# If True then build separate query for each time partition, otherwise build one single query. This is only used when time_partition_tables is False in schema config.
config.query_per_time_part=False

# If True then build one query per spacial partition, otherwise build single query.
config.query_per_spatial_part=False

# If True then combine result rows before converting to pandas.
config.pandas_delay_conv=True

# Packing method for table records.
config.packing='none'

# If True use Cassandra prepared statements.
config.prepared_statements=True

