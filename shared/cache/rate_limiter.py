"""
Redis-based sliding window rate limiter.

Uses a simple counter with 60s TTL per user.
"""
import logging

import redis.asyncio as aioredis

from config.settings import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, RATE_LIMIT_PER_MINUTE

logger = logging.getLogger(__name__)

_client: aioredis.Redis | None = None


def _get_client() -> aioredis.Redis:
    global _client
    if _client is None:
        _client = aioredis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True,
            retry_on_timeout=True,
            health_check_interval=10,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
    return _client


async def check_rate_limit(user_id: str) -> bool:
    """
    Returns True if the user is within rate limit, False if exceeded.
    Each call increments the counter. Window = 60 seconds.
    Fails OPEN: if Redis is unreachable, allow the request through.
    """
    try:
        client = _get_client()
        key = f"rate:{user_id}"

        pipe = client.pipeline()
        pipe.incr(key)
        pipe.expire(key, 60, nx=True)  # Set TTL only if not already set
        results = await pipe.execute()

        count = results[0]
        if count > RATE_LIMIT_PER_MINUTE:
            logger.warning("rate_limit_exceeded user_id=%s count=%d", user_id, count)
            return False
        return True
    except Exception as exc:
        # Fail open — don't block users if Redis is temporarily down
        logger.error("Rate limiter Redis error (failing open): %s", exc)
        return True
