"""
HTTP client that pushes payloads to the correct gateway instance.

Extracted from kafka_consumer so handlers can import it without
creating a circular dependency.

Uses a persistent httpx.AsyncClient with connection pooling for performance.
Resolves gateway URLs from Redis service registry (supports multi-gateway).
"""
import logging

import httpx
import redis.asyncio as aioredis

from config.settings import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD, SERVER_ID, GATEWAY_INTERNAL_URL
from shared.cache.connection_registry import get_server_for_connection

logger = logging.getLogger(__name__)

# ── Persistent HTTP client with connection pooling ────────────────────────────

_http_client: httpx.AsyncClient | None = None


def _get_http_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            timeout=httpx.Timeout(5.0, connect=2.0),
            limits=httpx.Limits(
                max_connections=100,
                max_keepalive_connections=20,
                keepalive_expiry=30,
            ),
        )
    return _http_client


async def shutdown_http_client() -> None:
    """Call on process shutdown to close the connection pool."""
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None


# ── Multi-gateway service discovery via Redis ─────────────────────────────────

_GATEWAY_REGISTRY_KEY = "gateway:registry"
_redis_client: aioredis.Redis | None = None


def _get_redis() -> aioredis.Redis:
    global _redis_client
    if _redis_client is None:
        _redis_client = aioredis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True,
            retry_on_timeout=True,
        )
    return _redis_client


async def register_gateway(server_id: str, internal_url: str) -> None:
    """Register this gateway's URL in Redis. Call on gateway startup."""
    client = _get_redis()
    await client.hset(_GATEWAY_REGISTRY_KEY, server_id, internal_url)
    logger.info("Registered gateway %s → %s", server_id, internal_url)


async def unregister_gateway(server_id: str) -> None:
    """Remove gateway from registry. Call on shutdown."""
    client = _get_redis()
    await client.hdel(_GATEWAY_REGISTRY_KEY, server_id)


async def _resolve_gateway_url(server_id: str) -> str | None:
    """Look up gateway internal URL from Redis registry, fallback to env."""
    # Fast path: if it's us, use the env var directly
    if server_id == SERVER_ID:
        return GATEWAY_INTERNAL_URL

    client = _get_redis()
    url = await client.hget(_GATEWAY_REGISTRY_KEY, server_id)
    if url:
        return url

    logger.warning("Gateway %s not found in registry.", server_id)
    return None


# ── Push to client ────────────────────────────────────────────────────────────


async def push_to_client(conversation_id: str, payload: dict) -> bool:
    """
    Look up which gateway owns this conversation via Redis,
    then POST the payload to its /internal/push endpoint.
    Returns True if delivered, False otherwise.
    """
    server_id = await get_server_for_connection(conversation_id)
    if not server_id:
        logger.debug(
            "No gateway registered for conversation_id=%s; payload dropped.",
            conversation_id,
        )
        return False

    base_url = await _resolve_gateway_url(server_id)
    if not base_url:
        logger.warning(
            "Cannot resolve URL for server_id=%s conversation_id=%s; payload dropped.",
            server_id,
            conversation_id,
        )
        return False

    url = f"{base_url}/internal/push"
    body = {"conversation_id": conversation_id, "payload": payload}

    try:
        client = _get_http_client()
        resp = await client.post(url, json=body)
        if resp.status_code == 200:
            return True
        logger.warning(
            "Gateway push returned %d for conversation_id=%s",
            resp.status_code,
            conversation_id,
        )
        return False
    except httpx.ConnectError:
        logger.error(
            "Cannot connect to gateway %s for conversation_id=%s",
            server_id,
            conversation_id,
        )
        return False
    except Exception as exc:
        logger.exception(
            "HTTP push failed for conversation_id=%s: %s", conversation_id, exc
        )
        return False
