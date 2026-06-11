"""List all keyspaces in Cassandra."""
from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from config.settings import CASSANDRA_HOSTS, CASSANDRA_PORT

c = Cluster(
    contact_points=CASSANDRA_HOSTS,
    port=CASSANDRA_PORT,
    load_balancing_policy=DCAwareRoundRobinPolicy(),
    protocol_version=4,
    connection_class=AsyncioConnection,
)
s = c.connect()
rows = list(s.execute("SELECT keyspace_name FROM system_schema.keyspaces"))
print("All keyspaces in the cluster:")
for r in sorted(rows, key=lambda x: x.keyspace_name):
    print(f"  - {r.keyspace_name}")

# Check for chat_archive specifically
names = [r.keyspace_name for r in rows]
for kw in ["chat_archive", "chat_arhive", "chatarchive"]:
    if kw in names:
        print(f"\nFound: {kw}")
        s2 = c.connect(kw)
        tables = list(s2.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s", [kw]))
        print(f"Tables in {kw}: {len(tables)}")
        for t in tables:
            print(f"  - {t.table_name}")

c.shutdown()
