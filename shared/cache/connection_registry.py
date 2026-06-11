import logging
import redis.asyncio as aioredis
from config.settings import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

logger = logging.getLogger(__name__)

_client: aioredis.Redis | None = None
_TTL_SECONDS = 300  # 5 minutes; refreshed on each heartbeat
_KEY_PREFIX = "ws:conn"


def _make_key(conversation_id: str) -> str:
    """Build the Redis key for a given conversation."""
    return f"{_KEY_PREFIX}:{conversation_id}"


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


async def register_connection(
    conversation_id: str,
    server_id: str,
) -> None:
    """Store conversation_id -> gateway server_id mapping in Redis with TTL."""
    client = _get_client()
    key = _make_key(conversation_id)
    await client.set(key, server_id, ex=_TTL_SECONDS)
    logger.debug("registry: registered %s -> %s", conversation_id, server_id)


async def unregister_connection(conversation_id: str) -> None:
    client = _get_client()
    key = _make_key(conversation_id)
    await client.delete(key)
    logger.debug("registry: unregistered %s", conversation_id)


async def get_server_for_connection(
    conversation_id: str,
) -> str | None:
    client = _get_client()
    key = _make_key(conversation_id)
    return await client.get(key)


async def refresh_connection_ttl(conversation_id: str) -> None:
    """Call on heartbeat to keep the key alive."""
    client = _get_client()
    key = _make_key(conversation_id)
    await client.expire(key, _TTL_SECONDS)
