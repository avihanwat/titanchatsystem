"""
Redis-based online chat tracker for real-time admin visibility.

Provides O(1) add/remove and O(limit) paginated listing of all
currently active conversations — no Cassandra needed in the hot path.
"""
import time
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
            retry_on_timeout=True,
            health_check_interval=10,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
    return _client


# ── Lifecycle Events ──────────────────────────────────────────────────────────


async def chat_online(
    conversation_id: str,
    user_id: str,
    server_id: str,
    bot_id: str = "",
) -> None:
    """Mark a chat as online. Call on chat_start."""
    client = _get_client()
    now = time.time()
    pipe = client.pipeline()
    pipe.sadd("online:chats", conversation_id)
    pipe.zadd("online:chats:by_time", {conversation_id: now})
    pipe.sadd(f"online:chats:user:{user_id}", conversation_id)
    pipe.hset(f"online:chat:{conversation_id}", mapping={
        "user_id": user_id,
        "bot_id": bot_id,
        "agent_id": "",
        "server_id": server_id,
        "status": "queued",
        "started_at": str(int(now)),
        "last_msg_at": str(int(now)),
    })
    await pipe.execute()
    logger.debug("chat_online conversation_id=%s user_id=%s bot_id=%s", conversation_id, user_id, bot_id)


async def chat_assigned(conversation_id: str, agent_id: str) -> None:
    """Mark chat as assigned to an agent. Call on transfer_to_agent success."""
    client = _get_client()
    pipe = client.pipeline()
    pipe.hset(f"online:chat:{conversation_id}", mapping={
        "agent_id": agent_id,
        "status": "active",
    })
    pipe.sadd(f"online:chats:agent:{agent_id}", conversation_id)
    await pipe.execute()
    logger.debug("chat_assigned conversation_id=%s agent_id=%s", conversation_id, agent_id)


async def chat_message_received(conversation_id: str) -> None:
    """Update last message timestamp. Call on every incoming/outgoing message."""
    client = _get_client()
    await client.hset(
        f"online:chat:{conversation_id}",
        "last_msg_at",
        str(int(time.time())),
    )


async def chat_offline(conversation_id: str) -> None:
    """Remove chat from all online tracking. Call on chat_end or disconnect."""
    client = _get_client()
    meta = await client.hgetall(f"online:chat:{conversation_id}")
    if not meta:
        return

    user_id = meta.get("user_id", "")
    agent_id = meta.get("agent_id", "")

    pipe = client.pipeline()
    pipe.srem("online:chats", conversation_id)
    pipe.zrem("online:chats:by_time", conversation_id)
    pipe.delete(f"online:chat:{conversation_id}")
    if user_id:
        pipe.srem(f"online:chats:user:{user_id}", conversation_id)
    if agent_id:
        pipe.srem(f"online:chats:agent:{agent_id}", conversation_id)
    await pipe.execute()
    logger.debug("chat_offline conversation_id=%s", conversation_id)


# ── Admin Queries ─────────────────────────────────────────────────────────────


async def get_online_count() -> int:
    """Total number of online chats right now."""
    client = _get_client()
    return await client.scard("online:chats")


async def get_all_online_chats(
    offset: int = 0,
    limit: int = 50,
    newest_first: bool = True,
) -> list[dict]:
    """Paginated list of all online chats with metadata."""
    client = _get_client()

    if newest_first:
        conv_ids = await client.zrevrange(
            "online:chats:by_time", offset, offset + limit - 1
        )
    else:
        conv_ids = await client.zrange(
            "online:chats:by_time", offset, offset + limit - 1
        )

    if not conv_ids:
        return []

    pipe = client.pipeline()
    for cid in conv_ids:
        pipe.hgetall(f"online:chat:{cid}")
    results = await pipe.execute()

    chats = []
    for cid, meta in zip(conv_ids, results):
        if meta:
            chats.append({"conversation_id": cid, **meta})
    return chats


async def get_online_chats_by_status(status: str) -> list[dict]:
    """Filter online chats by status: 'queued', 'active', 'transferring'."""
    all_chats = await get_all_online_chats(limit=10000)
    return [c for c in all_chats if c.get("status") == status]


async def get_online_chats_for_agent(agent_id: str) -> list[str]:
    """All active conversation IDs for a specific agent."""
    client = _get_client()
    return list(await client.smembers(f"online:chats:agent:{agent_id}"))


async def get_online_chats_for_user(user_id: str) -> list[str]:
    """All active conversation IDs for a specific user."""
    client = _get_client()
    return list(await client.smembers(f"online:chats:user:{user_id}"))
