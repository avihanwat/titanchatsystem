"""Check all TitanChat service connections: Cassandra, Kafka, Redis, PostgreSQL."""
import asyncio
import sys

from config.settings import (
    CASSANDRA_HOSTS, CASSANDRA_PORT, CASSANDRA_KEYSPACE,
    KAFKA_BOOTSTRAP_SERVERS,
    REDIS_HOST, REDIS_PORT, REDIS_PASSWORD,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD,
)

results = {}


def check_cassandra():
    print(f"\n[1/4] Cassandra → {CASSANDRA_HOSTS}:{CASSANDRA_PORT}")
    try:
        from cassandra.cluster import Cluster
        from cassandra.io.asyncioreactor import AsyncioConnection
        from cassandra.policies import DCAwareRoundRobinPolicy

        cluster = Cluster(
            contact_points=CASSANDRA_HOSTS,
            port=CASSANDRA_PORT,
            load_balancing_policy=DCAwareRoundRobinPolicy(),
            protocol_version=4,
            connection_class=AsyncioConnection,
        )
        session = cluster.connect()
        rows = list(session.execute("SELECT release_version FROM system.local"))
        version = rows[0].release_version
        keyspace = CASSANDRA_KEYSPACE or "titanchat"
        tables = list(session.execute(
            f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}'"
        ))
        cluster.shutdown()
        print(f"       ✓ Connected! Version: {version}, Tables: {len(tables)}")
        results["cassandra"] = True
    except Exception as e:
        print(f"       ✗ FAILED: {e}")
        results["cassandra"] = False


async def check_kafka():
    print(f"\n[2/4] Kafka → {KAFKA_BOOTSTRAP_SERVERS}")
    try:
        from aiokafka import AIOKafkaProducer

        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
        await producer.stop()
        print("       ✓ Connected! Producer start/stop OK")
        results["kafka"] = True
    except Exception as e:
        print(f"       ✗ FAILED: {e}")
        results["kafka"] = False


async def check_redis():
    print(f"\n[3/4] Redis → {REDIS_HOST}:{REDIS_PORT}")
    try:
        import redis.asyncio as aioredis

        client = aioredis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True,
            socket_connect_timeout=5,
        )
        pong = await client.ping()
        info = await client.info("server")
        await client.aclose()
        print(f"       ✓ Connected! PING={pong}, Redis v{info['redis_version']}")
        results["redis"] = True
    except Exception as e:
        print(f"       ✗ FAILED: {e}")
        results["redis"] = False


async def check_postgres():
    print(f"\n[4/4] PostgreSQL → {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    try:
        import asyncpg

        conn = await asyncpg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            timeout=5,
        )
        version = await conn.fetchval("SELECT version()")
        await conn.close()
        print(f"       ✓ Connected! {version[:60]}...")
        results["postgres"] = True
    except Exception as e:
        print(f"       ✗ FAILED: {e}")
        results["postgres"] = False


async def main():
    print("=" * 60)
    print("  TitanChat — Connection Health Check")
    print("=" * 60)

    check_cassandra()
    await check_kafka()
    await check_redis()
    await check_postgres()

    print("\n" + "=" * 60)
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"  Results: {passed}/{total} connections OK")
    for name, ok in results.items():
        status = "✓" if ok else "✗"
        print(f"    {status} {name}")
    print("=" * 60)

    if passed < total:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
