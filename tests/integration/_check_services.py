"""Quick health check for all services: Redis, PostgreSQL, Gateway, API, Consumer."""
import asyncio
import sys
import urllib.request
import json

import psycopg2
import redis.asyncio as aioredis

from config.settings import (
    REDIS_HOST, REDIS_PORT, REDIS_PASSWORD,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD,
)

PASS = "[PASS]"
FAIL = "[FAIL]"

# ── Sync checks ───────────────────────────────────────────────────────────────

def check_http(name: str, url: str) -> None:
    try:
        res = urllib.request.urlopen(url, timeout=5)
        body = json.loads(res.read()) if res.headers.get_content_type() == "application/json" else "(HTML)"
        print(f"{PASS} {name}: HTTP {res.status} {body}")
    except Exception as e:
        print(f"{FAIL} {name}: {e}")


def check_postgres() -> None:
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST, port=POSTGRES_PORT,
            dbname=POSTGRES_DB, user=POSTGRES_USER,
            password=POSTGRES_PASSWORD, connect_timeout=5,
        )
        cur = conn.cursor()
        cur.execute("SELECT version();")
        version = cur.fetchone()[0].split(",")[0]
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public';")
        tables = [r[0] for r in cur.fetchall()]
        conn.close()
        print(f"{PASS} PostgreSQL: {version}")
        print(f"       Tables: {tables if tables else '(none — DB may not be initialised)'}")
    except Exception as e:
        print(f"{FAIL} PostgreSQL: {e}")


# ── Async checks ──────────────────────────────────────────────────────────────

async def check_redis() -> None:
    try:
        client = aioredis.Redis(
            host=REDIS_HOST, port=REDIS_PORT,
            password=REDIS_PASSWORD, decode_responses=True,
        )
        pong = await client.ping()
        total = len(await client.keys("*"))
        await client.aclose()
        print(f"{PASS} Redis: PONG={pong}  keys_in_db={total}")
    except Exception as e:
        print(f"{FAIL} Redis: {e}")


async def main() -> None:
    print("=" * 55)
    print("  TitanChat Service Health Check")
    print("=" * 55)

    print("\n--- Redis ---")
    await check_redis()

    print("\n--- PostgreSQL ---")
    check_postgres()

    print("\n--- Gateway (port 8000) ---")
    check_http("Gateway /docs",         "http://localhost:8000/docs")
    check_http("Gateway /admin/stats",  "http://localhost:8000/admin/stats")

    print("\n--- API Server (port 8001) ---")
    check_http("API /health",           "http://localhost:8001/health")
    check_http("API /docs",             "http://localhost:8001/docs")

    print("\n--- Consumer ---")
    print("  (Consumer is a background process — check its terminal for status)")

    print("\n" + "=" * 55)


if __name__ == "__main__":
    asyncio.run(main())
