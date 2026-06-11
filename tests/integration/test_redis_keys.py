"""Get all Redis keys with their values."""
import asyncio
import redis.asyncio as aioredis
from config.settings import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD


async def main():
    client = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
    print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    print()

    keys = await client.keys("*")
    print(f"Total keys: {len(keys)}")
    print()

    if not keys:
        print("  (no keys found)")
    else:
        for k in keys:
            val = await client.get(k)
            ttl = await client.ttl(k)
            ttl_str = f"{ttl}s" if ttl > 0 else "no expiry"
            print(f"  Key: {k}")
            print(f"  Value: {val}")
            print(f"  TTL: {ttl_str}")
            print()

    await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
