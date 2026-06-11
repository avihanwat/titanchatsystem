"""Quick Redis connectivity check."""
import asyncio
import redis.asyncio as aioredis


async def check():
    host = "34.69.100.253"
    port = 6379

    print(f"Connecting to Redis at {host}:{port} ...")

    # Try without password
    print("\n[1] Trying without password...")
    client = aioredis.Redis(
        host=host, port=port, password=None,
        decode_responses=True, socket_connect_timeout=5, socket_timeout=5,
    )
    try:
        pong = await client.ping()
        info = await client.info("server")
        print(f"    ✓ PING={pong}, Redis v{info['redis_version']}")
        await client.aclose()
        return
    except Exception as e:
        print(f"    ✗ {e}")
        await client.aclose()

    # Try with common passwords
    passwords_to_try = ["titanchatsystem", "titanchat", "redis", "password"]
    for pw in passwords_to_try:
        print(f"\n[2] Trying password='{pw}'...")
        client = aioredis.Redis(
            host=host, port=port, password=pw,
            decode_responses=True, socket_connect_timeout=5, socket_timeout=5,
        )
        try:
            pong = await client.ping()
            info = await client.info("server")
            ver = info["redis_version"]
            print(f"    ✓ PING={pong}, Redis v{ver}")
            print(f"\n    SUCCESS! Set REDIS_PASSWORD={pw} in your .env file.")
            await client.aclose()
            return
        except Exception as e:
            print(f"    ✗ {e}")
            await client.aclose()

    print("\n✗ Could not connect to Redis. The server requires a password not tested here.")
    print("  Set the correct REDIS_PASSWORD in .env")


if __name__ == "__main__":
    asyncio.run(check())
