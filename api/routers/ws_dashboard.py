"""
Admin dashboard WebSocket — real-time event stream via Redis PubSub.

Admin connects with JWT token, server subscribes to admin:{admin_id}:feed
and forwards all events to the WebSocket as JSON frames.
"""
import asyncio
import json
import logging

import redis.asyncio as aioredis
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from api.utils.security import decode_access_token
from config.settings import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

logger = logging.getLogger(__name__)

router = APIRouter(tags=["ws-dashboard"])


def _create_redis() -> aioredis.Redis:
    """Create a dedicated Redis connection for PubSub (not shared with pool)."""
    return aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        decode_responses=True,
        socket_connect_timeout=5,
    )


@router.websocket("/ws/dashboard")
async def ws_dashboard(websocket: WebSocket):
    """
    Real-time admin dashboard feed.

    Connect: ws://host:8001/ws/dashboard?token=<JWT>

    Events pushed:
      - new_chat: a customer started a conversation
      - new_message: a new message arrived in a conversation
      - agent_assigned: an agent was assigned to a conversation
      - chat_ended: a conversation was closed
    """
    # ── Auth ──────────────────────────────────────────────────────────────────
    token = websocket.query_params.get("token")
    if not token:
        await websocket.close(code=4001, reason="Missing token")
        return

    try:
        payload = decode_access_token(token)
    except Exception:
        await websocket.close(code=4001, reason="Invalid or expired token")
        return

    role = payload.get("role")
    if role == "admin":
        admin_id = payload.get("user_id")
    elif role == "agent":
        admin_id = payload.get("admin_id")
    else:
        await websocket.close(code=4003, reason="Unauthorized role")
        return

    if not admin_id:
        await websocket.close(code=4001, reason="Cannot resolve admin_id from token")
        return

    # ── Accept & Subscribe ────────────────────────────────────────────────────
    await websocket.accept()
    channel = f"admin:{admin_id}:feed"
    logger.info("ws_dashboard: connected admin_id=%s channel=%s", admin_id, channel)

    redis_client = _create_redis()
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)

    # ── Event Loop ────────────────────────────────────────────────────────────
    reader_task: asyncio.Task | None = None

    async def _reader():
        """Read from Redis PubSub and forward to WebSocket."""
        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                data = message["data"]
                await websocket.send_text(data)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.debug("ws_dashboard: reader stopped: %s", exc)

    async def _keepalive():
        """Send ping every 30s to keep connection alive."""
        try:
            while True:
                await asyncio.sleep(30)
                await websocket.send_json({"event": "ping"})
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    try:
        reader_task = asyncio.create_task(_reader())
        keepalive_task = asyncio.create_task(_keepalive())

        # Wait for client messages (pong or disconnect)
        while True:
            data = await websocket.receive_text()
            # Client can send pong or any keepalive — we just consume it
            if data:
                try:
                    msg = json.loads(data)
                    if msg.get("event") == "ping":
                        await websocket.send_json({"event": "pong"})
                except (json.JSONDecodeError, TypeError):
                    pass

    except WebSocketDisconnect:
        logger.info("ws_dashboard: disconnected admin_id=%s", admin_id)
    except Exception as exc:
        logger.warning("ws_dashboard: error admin_id=%s: %s", admin_id, exc)
    finally:
        # Cleanup
        if reader_task:
            reader_task.cancel()
        keepalive_task.cancel()
        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await redis_client.aclose()
