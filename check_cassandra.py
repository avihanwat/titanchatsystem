"""Quick Cassandra health check."""
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from config.settings import CASSANDRA_HOSTS, CASSANDRA_PORT

print(f"Connecting to Cassandra at {CASSANDRA_HOSTS}:{CASSANDRA_PORT}...")
c = Cluster(
    contact_points=CASSANDRA_HOSTS,
    port=CASSANDRA_PORT,
    load_balancing_policy=DCAwareRoundRobinPolicy(),
    protocol_version=4,
    connection_class=AsyncioConnection,
)
s = c.connect()
rows = list(s.execute("SELECT release_version FROM system.local"))
print(f"Connected! Cassandra version: {rows[0].release_version}")

# Create keyspace if needed
s.execute("""
    CREATE KEYSPACE IF NOT EXISTS titanchat
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")
print("Keyspace 'titanchat' exists/created.")

# Connect to keyspace
s2 = c.connect("titanchat")
print("Connected to 'titanchat' keyspace successfully!")

# List existing tables
tables = list(s2.execute(
    "SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'titanchat'"
))
print(f"\nTables in titanchat: {len(tables)}")
for t in sorted(tables, key=lambda x: x.table_name):
    print(f"  - {t.table_name}")

c.shutdown()
print("\nCassandra is healthy and working fine!")
