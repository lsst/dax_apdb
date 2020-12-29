import os
import lsst.dax.apdb.apdbCassandra
assert type(config)==lsst.dax.apdb.apdbCassandra.ApdbCassandraConfig, 'config is of type %s.%s instead of lsst.dax.apdb.apdbCassandra.ApdbCassandraConfig' % (type(config).__module__, type(config).__name__)

per_month_tables = True
data_dir = os.path.join(os.environ['DAX_APDB_DIR'], 'data')

# Location of (YAML) configuration file with standard schema
config.schema_file=os.path.join(data_dir, 'apdb-schema-cassandra-per-month.yaml' if per_month_tables else 'apdb-schema-cassandra.yaml')

# Location of (YAML) configuration file with extra schema
config.extra_schema_file=os.path.join(data_dir, 'apdb-schema-extra-cassandra.yaml')

# Location of (YAML) configuration file with column mapping
config.column_map=os.path.join(data_dir, 'apdb-afw-map.yaml')

# Prefix to add to table names
config.prefix=''

# Use per-month tables for sources instead of paritioning by month
config.per_month_tables=per_month_tables

# The list of contact points to try connecting for cluster discovery.
config.contact_points=['127.0.0.1']

# List of internal IP addresses for contact_points.
config.private_ips=[]

# Default keyspace for operations.
config.keyspace='apdb'

# Name for consistency level of read operations, defalut: QUORUM, can be ONE.
config.read_consistency='QUORUM'

# Name for consistency level of write operations, defalut: QUORUM, can be ONE.
config.write_consistency='QUORUM'

# Cassandra protocol version to use, default is V4
config.protocol_version=4

# Number of months of history to read from DiaSource
config.read_sources_months=12

# Number of months of history to read from DiaForcedSource
config.read_forced_sources_months=12

# List of columns to read from DiaObject, by default read all columns
config.dia_object_columns=[]

# Pixelization used for patitioning index.
config.part_pixelization='mq3c'

# Pixelization level used for patitioning index.
config.part_pix_level=10

# If True then print/log timing information
config.timer=True

# If True then store random values for fields not explicitly filled, for testing only
config.fillEmptyFields=True
