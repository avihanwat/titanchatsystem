"""
Agent real-time availability and routing.

Uses Redis sorted sets for O(1) least-loaded agent lookup.
Agents must heartbeat every 10s; TTL=30s auto-removes stale agents.

Routing uses a Lua script for atomic check-and-assign to prevent
race conditions under concurrent routing requests.
"""
import json
import logging

import redis.asyncio as aioredis

from config.settings import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

logger = logging.getLogger(__name__)

_client: aioredis.Redis | None = None

# Lua script: atomically find the least-loaded agent that is online and under capacity,
# then increment its score and assign the conversation — all in one round-trip.
_ROUTE_LUA = """
local candidates = redis.call('ZRANGEBYSCORE', 'agent:available', '0', '+inf', 'WITHSCORES')
local conversation_id = ARGV[1]
local required_skill = ARGV[2]

for i = 1, #candidates, 2 do
    local agent_id = candidates[i]
    local active_count = tonumber(candidates[i+1])

    -- Check still online (TTL key exists)
    local status = redis.call('GET', 'agent:status:' .. agent_id)
    if not status or status ~= 'online' then
        redis.call('ZREM', 'agent:available', agent_id)
    else
        -- Check capacity
        local max_chats = tonumber(redis.call('HGET', 'agent:meta:' .. agent_id, 'max_chats') or '5')
        if active_count < max_chats then
            -- Check skill match
            local ok = true
            if required_skill ~= '' then
                local skills = redis.call('HGET', 'agent:meta:' .. agent_id, 'skills') or ''
                if not string.find(',' .. skills .. ',', ',' .. required_skill .. ',') then
                    ok = false
                end
            end

            if ok then
                -- Atomic assign
                redis.call('ZINCRBY', 'agent:available', 1, agent_id)
                redis.call('SADD', 'agent:chats:' .. agent_id, conversation_id)
                redis.call('HSET', 'chat:active:' .. conversation_id, 'agent_id', agent_id, 'status', 'active')
                return agent_id
            end
        end
    end
end

return nil
"""


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


# ── Agent Lifecycle ───────────────────────────────────────────────────────────


async def agent_go_online(
    agent_id: str,
    max_chats: int = 5,
    skills: list[str] | None = None,
) -> None:
    """Register agent as available."""
    client = _get_client()
    pipe = client.pipeline()
    pipe.set(f"agent:status:{agent_id}", "online", ex=30)
    pipe.zadd("agent:available", {agent_id: 0})
    pipe.hset(f"agent:meta:{agent_id}", mapping={
        "max_chats": str(max_chats),
        "skills": ",".join(skills or []),
    })
    await pipe.execute()
    logger.info("agent_online agent_id=%s max_chats=%d", agent_id, max_chats)


async def agent_heartbeat(agent_id: str) -> None:
    """Refresh agent TTL. Must be called every 10s."""
    client = _get_client()
    await client.set(f"agent:status:{agent_id}", "online", ex=30)


async def agent_go_offline(agent_id: str) -> None:
    """Remove agent from the available pool."""
    client = _get_client()
    pipe = client.pipeline()
    pipe.delete(f"agent:status:{agent_id}")
    pipe.zrem("agent:available", agent_id)
    await pipe.execute()
    logger.info("agent_offline agent_id=%s", agent_id)


# ── Routing ───────────────────────────────────────────────────────────────────


async def route_to_agent(
    conversation_id: str,
    required_skill: str | None = None,
) -> str | None:
    """
    Atomically pick the least-loaded available agent using a Lua script.
    Eliminates race conditions — two concurrent calls cannot double-assign.
    Returns agent_id or None if nobody is available.
    """
    client = _get_client()

    agent_id = await client.eval(  # type: ignore[misc]
        _ROUTE_LUA,
        0,  # no KEYS args — all keys are constructed inside the script
        conversation_id,
        required_skill or "",
    )

    if agent_id:
        logger.info(
            "agent_assigned agent_id=%s conversation_id=%s",
            agent_id, conversation_id,
        )
        # Publish to admin feed (non-blocking, outside the atomic path)
        await client.publish("admin:feed", json.dumps({
            "event": "agent_assigned",
            "conversation_id": conversation_id,
            "agent_id": agent_id,
        }))
        return agent_id

    logger.warning("no_agent_available conversation_id=%s", conversation_id)
    return None


async def release_agent_slot(conversation_id: str) -> None:
    """Free up an agent slot when a chat ends."""
    client = _get_client()

    # Find which agent holds this conversation
    agent_id = await client.hget(f"chat:active:{conversation_id}", "agent_id")  # type: ignore[misc]
    if not agent_id:
        return

    pipe = client.pipeline()
    pipe.zincrby("agent:available", -1, agent_id)
    pipe.srem(f"agent:chats:{agent_id}", conversation_id)
    pipe.delete(f"chat:active:{conversation_id}")
    await pipe.execute()

    logger.info(
        "agent_slot_released agent_id=%s conversation_id=%s", agent_id, conversation_id
    )


# ── Admin Queries ─────────────────────────────────────────────────────────────


async def get_all_agents_status() -> list[dict]:
    """Return status of all agents (for admin dashboard)."""
    client = _get_client()
    agents = await client.zrangebyscore(
        "agent:available", "-inf", "+inf", withscores=True
    )
    result = []
    for agent_id, active_count in agents:
        meta = await client.hgetall(f"agent:meta:{agent_id}")  # type: ignore[misc]
        status = await client.get(f"agent:status:{agent_id}")
        chats = await client.smembers(f"agent:chats:{agent_id}")  # type: ignore[misc]
        result.append({
            "agent_id": agent_id,
            "status": status or "offline",
            "active_chats": int(active_count),
            "max_chats": int(meta.get("max_chats", "5")),
            "skills": meta.get("skills", "").split(","),
            "conversations": list(chats),
        })
    return result


async def get_active_chats() -> list[dict]:
    """Return all currently active chats (for admin dashboard)."""
    client = _get_client()
    keys = []
    async for key in client.scan_iter("chat:active:*"):
        keys.append(key)

    chats = []
    for key in keys:
        data = await client.hgetall(key)  # type: ignore[misc]
        if data:
            conv_id = key.replace("chat:active:", "")
            chats.append({"conversation_id": conv_id, **data})
    return chats
