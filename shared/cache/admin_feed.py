"""
Admin feed publisher — pushes real-time events to per-admin Redis PubSub channels.

Consumer handlers call `publish_admin_event()` after processing each event.
The API server's WebSocket subscriber listens on `admin:{admin_id}:feed`
and forwards messages to the connected admin dashboard.
"""
import json
import time
import logging

import redis.asyncio as aioredis

from config.settings import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD
from shared.cache.bot_registry import get_admin_id_for_bot

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


async def publish_admin_event(
    bot_id: str,
    event_type: str,
    payload: dict,
) -> None:
    """
    Publish an event to the admin who owns the given bot.

    Fire-and-forget: errors are logged but never propagate to caller.
    If bot_id is empty or admin_id cannot be resolved, the event is dropped.
    """
    try:
        if not bot_id:
            return

        admin_id = await get_admin_id_for_bot(bot_id)
        if not admin_id:
            logger.debug("admin_feed: no admin_id for bot_id=%s, skipping", bot_id)
            return

        channel = f"admin:{admin_id}:feed"
        message = json.dumps({
            "event": event_type,
            "ts": int(time.time()),
            "bot_id": bot_id,
            **payload,
        })

        client = _get_client()
        await client.publish(channel, message)
        logger.debug("admin_feed: published %s to %s", event_type, channel)

    except Exception as exc:
        logger.warning("admin_feed: failed to publish %s: %s", event_type, exc)
