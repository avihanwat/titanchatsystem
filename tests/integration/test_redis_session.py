"""
Test Redis connection registry (session management).
Verifies: connect, read, refresh TTL, disconnect operations.
"""
import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv()

import redis.asyncio as aioredis
from config.settings import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from shared.connection_registry import (
    register_connection,
    get_server_for_connection,
    refresh_connection_ttl,
    unregister_connection,
)


async def main():
    print("--- Redis Session (Connection Registry) Test ---\n")

    # Step 0: Basic Redis connectivity
    print("[TEST 0] Redis PING")
    try:
        client = aioredis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
        pong = await client.ping()
        print(f"  Redis @ {REDIS_HOST}:{REDIS_PORT} -> PING response: {pong}")
        assert pong is True, "PING failed"
        print("  PASS\n")
    except Exception as e:
        print(f"  FAIL - Cannot connect to Redis: {e}")
        print(f"  Check that Redis is running at {REDIS_HOST}:{REDIS_PORT}")
        return

    # Step 1: Register a connection
    print("[TEST 1] register_connection('conv-abc-123', 'gateway-1')")
    await register_connection("conv-abc-123", "gateway-1")
    # Verify key exists in Redis
    val = await client.get("ws:conn:conv-abc-123")
    print(f"  Redis key 'ws:conn:conv-abc-123' = '{val}'")
    assert val == "gateway-1", f"Expected 'gateway-1', got '{val}'"
    print("  PASS\n")

    # Step 2: Get server for connection
    print("[TEST 2] get_server_for_connection('conv-abc-123')")
    server = await get_server_for_connection("conv-abc-123")
    print(f"  Returned: '{server}'")
    assert server == "gateway-1", f"Expected 'gateway-1', got '{server}'"
    print("  PASS\n")

    # Step 3: Check TTL is set
    print("[TEST 3] Check TTL on key")
    ttl = await client.ttl("ws:conn:conv-abc-123")
    print(f"  TTL = {ttl} seconds")
    assert 0 < ttl <= 300, f"Expected TTL between 1-300, got {ttl}"
    print("  PASS\n")

    # Step 4: Refresh TTL
    print("[TEST 4] refresh_connection_ttl('conv-abc-123')")
    # Wait 1 second so TTL decreases
    await asyncio.sleep(1)
    ttl_before = await client.ttl("ws:conn:conv-abc-123")
    await refresh_connection_ttl("conv-abc-123")
    ttl_after = await client.ttl("ws:conn:conv-abc-123")
    print(f"  TTL before refresh: {ttl_before}s, after refresh: {ttl_after}s")
    assert ttl_after >= ttl_before, "TTL should be refreshed (reset to 300)"
    print("  PASS\n")

    # Step 5: Register a second connection
    print("[TEST 5] register_connection('conv-xyz-789', 'gateway-2')")
    await register_connection("conv-xyz-789", "gateway-2")
    server2 = await get_server_for_connection("conv-xyz-789")
    print(f"  Returned: '{server2}'")
    assert server2 == "gateway-2"
    print("  PASS\n")

    # Step 6: Unregister connection
    print("[TEST 6] unregister_connection('conv-abc-123')")
    await unregister_connection("conv-abc-123")
    val_after = await get_server_for_connection("conv-abc-123")
    print(f"  After unregister, get_server_for_connection returns: {val_after}")
    assert val_after is None, f"Expected None, got '{val_after}'"
    print("  PASS\n")

    # Step 7: Non-existent key returns None
    print("[TEST 7] get_server_for_connection('non-existent-conv')")
    val_none = await get_server_for_connection("non-existent-conv")
    print(f"  Returned: {val_none}")
    assert val_none is None
    print("  PASS\n")

    # Cleanup test keys
    await client.delete("ws:conn:conv-xyz-789")
    await client.aclose()

    print("=" * 50)
    print("RESULTS:")
    print("=" * 50)
    print("  [OK] Redis PING                : PASS")
    print("  [OK] register_connection        : PASS")
    print("  [OK] get_server_for_connection  : PASS")
    print("  [OK] TTL set correctly          : PASS")
    print("  [OK] refresh_connection_ttl     : PASS")
    print("  [OK] Multiple connections       : PASS")
    print("  [OK] unregister_connection      : PASS")
    print("  [OK] Non-existent key -> None   : PASS")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
