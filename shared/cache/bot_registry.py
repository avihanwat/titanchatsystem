"""
Bot registry — caches bot_id → admin_id in Redis for O(1) lookups.

Consumer handlers need to resolve which admin owns a bot to publish
events to the correct per-admin PubSub channel. This avoids hitting
PostgreSQL on every message.
"""
import logging

import redis.asyncio as aioredis

from config.settings import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

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
            socket_connect_timeout=5,
            socket_timeout=5,
        )
    return _client


def _key(bot_id: str) -> str:
    return f"bot:{bot_id}:admin_id"


async def cache_bot_mapping(bot_id: str, admin_id: str) -> None:
    """Store bot→admin mapping in Redis. Call on bot creation."""
    client = _get_client()
    await client.set(_key(bot_id), admin_id)
    logger.debug("cached bot_mapping bot_id=%s admin_id=%s", bot_id, admin_id)


async def invalidate_bot_mapping(bot_id: str) -> None:
    """Remove bot→admin mapping from Redis. Call on bot deletion."""
    client = _get_client()
    await client.delete(_key(bot_id))


async def get_admin_id_for_bot(bot_id: str) -> str | None:
    """
    Resolve bot_id → admin_id.

    Checks Redis cache first. On miss, returns None — caller should
    fall back to PostgreSQL and then call cache_bot_mapping().
    """
    if not bot_id:
        return None
    client = _get_client()
    admin_id = await client.get(_key(bot_id))
    return admin_id
