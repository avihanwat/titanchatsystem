

import asyncio
import logging
import uuid
import time
from contextlib import asynccontextmanager

import certifi
import ssl
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, Query
from pydantic import BaseModel

from config.settings import SERVER_ID
from gateway.websocket_manager import manager
from gateway.kafka_producer import start_producer, stop_producer, publish_message
from consumer.gateway_client import register_gateway, unregister_gateway
from shared.observability.logging import setup_logging
from shared.observability.trace import new_trace_id, set_trace
from shared.observability.telemetry import setup_tracer
from shared.auth import authenticate_ws, require_auth, require_role
from shared.cache.rate_limiter import check_rate_limit
from shared.cache.online_chat_tracker import (
    get_all_online_chats,
    get_online_count,
    get_online_chats_by_status,
)
from shared.cache.agent_router import get_all_agents_status
from config.topics import CHAT_INBOUND
from api.utils.bot_auth import validate_bot_api_key
from api.database import async_session

ssl._create_default_https_context = ssl.create_default_context(cafile=certifi.where())

# ── Startup ───────────────────────────────────────────────────────────────────
setup_logging()
logger  = logging.getLogger(__name__)
tracer  = setup_tracer("titan-gateway")

_VALID_EVENT_TYPES = frozenset(
    {"chat_start", "incoming_message", "transfer_to_agent", "chat_end", "ack"}
)


async def cleanup_stale_connections() -> None:
    while True:
        # Send pings to all connections — marks dead ones for cleanup
        await manager.ping_all()
        await asyncio.sleep(15)

        # Disconnect anything that hasn't responded within timeout
        stale = manager.get_stale_connections()
        for cid in stale:
            logger.info("Evicting stale connection: %s", cid)
            await manager.disconnect(cid)
        await asyncio.sleep(15)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await start_producer()
    # Register this gateway in Redis for multi-gateway discovery
    from config.settings import GATEWAY_INTERNAL_URL
    await register_gateway(SERVER_ID, GATEWAY_INTERNAL_URL)
    asyncio.create_task(cleanup_stale_connections())
    yield
    await unregister_gateway(SERVER_ID)
    await stop_producer()


app = FastAPI(lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "titanchat-gateway"}


# ── Admin endpoints ───────────────────────────────────────────────────────────


@app.get("/admin/chats/online")
async def admin_online_chats(
    offset: int = 0,
    limit: int = 50,
    status: str | None = None,
    user: dict = Depends(require_auth),
):
    """All currently online chats for admin dashboard."""
    if status:
        chats = await get_online_chats_by_status(status)
    else:
        chats = await get_all_online_chats(offset=offset, limit=limit)
    count = await get_online_count()
    return {"total_online": count, "chats": chats}


@app.get("/admin/agents")
async def admin_agents(user: dict = Depends(require_auth)):
    """Agent statuses and workloads."""
    agents = await get_all_agents_status()
    return {"agents": agents}


@app.get("/admin/stats")
async def admin_stats(user: dict = Depends(require_auth)):
    """High-level system stats."""
    online_count = await get_online_count()
    agents = await get_all_agents_status()
    online_agents = sum(1 for a in agents if a.get("status") == "online")
    return {
        "online_chats": online_count,
        "online_agents": online_agents,
        "total_agents": len(agents),
    }


# ── Internal endpoint called by the consumer VM ──────────────────────────────

class PushPayload(BaseModel):
    conversation_id: str
    payload: dict


@app.post("/internal/push")
async def internal_push(body: PushPayload) -> dict:
    delivered = await manager.send_message(body.conversation_id, body.payload)
    if not delivered:
        raise HTTPException(
            status_code=404,
            detail=f"No active WebSocket for conversation_id={body.conversation_id}",
        )
    return {"status": "delivered"}


# ── Public WebSocket endpoint ─────────────────────────────────────────────────

@app.websocket("/ws/{conversation_id}")
async def websocket_endpoint(
    websocket: WebSocket,
    conversation_id: str,
) -> None:
    # Authenticate: supports both ?token=JWT (admin/agent) and ?bot_token=API_KEY (bot)
    bot_token = websocket.query_params.get("bot_token")
    bot_id: str = ""

    if bot_token:
        # Bot authentication — validate api_key against PostgreSQL
        async with async_session() as db:
            bot_info = await validate_bot_api_key(bot_token, db)
        if not bot_info:
            await websocket.close(code=4001, reason="Invalid or inactive bot_token")
            return
        user_id = f"customer:{conversation_id}"
        bot_id = bot_info["bot_id"]
    else:
        # Standard JWT authentication (admin/agent)
        user = await authenticate_ws(websocket)
        if not user:
            return
        user_id = user.get("user_id", "")

    try:
        await manager.connect(conversation_id, websocket)
    except Exception as exc:
        logger.exception("Failed to register WebSocket connection: %s", exc)
        await websocket.close(code=1011, reason=f"Server error: {exc}")
        return

    try:
        while True:
            data = await websocket.receive_json()
            await manager.heartbeat(conversation_id)

            # Handle pong response to our ping — no further processing needed
            if data.get("event_type") == "pong":
                continue

            # ── Rate limiting ─────────────────────────────────────────────
            if not await check_rate_limit(user_id):
                await websocket.send_json(
                    {"status": "error", "detail": "Rate limit exceeded"}
                )
                continue

            # ── Build event ───────────────────────────────────────────────
            event_type = data.get("event_type", "incoming_message")
            if event_type not in _VALID_EVENT_TYPES:
                await websocket.send_json(
                    {"status": "error", "detail": f"Unknown event_type: {event_type}"}
                )
                continue

            trace_id   = new_trace_id()
            message_id = data.get("message_id", str(uuid.uuid4()))
            timestamp  = int(time.time())

            set_trace(trace_id, conversation_id, event_type)

            payload: dict = {
                "event_type":      event_type,
                "conversation_id": conversation_id,
                "message_id":      message_id,
                "user_id":         user_id,
                "bot_id":          bot_id,
                "timestamp":       timestamp,
                **{k: v for k, v in data.items()
                   if k not in ("event_type", "message_id", "user_id", "bot_id")},
            }

            logger.info(
                "event_received",
                extra={"stage": "ws_received", "message_id": message_id},
            )

            # ── Publish to Kafka ──────────────────────────────────────────
            try:
                with tracer.start_as_current_span(f"ws.publish.{event_type}") as span:
                        span.set_attribute("conversation_id", conversation_id)
                        span.set_attribute("trace_id", trace_id)
                        await publish_message(
                            topic=CHAT_INBOUND,
                            key=conversation_id,
                            payload=payload,
                            headers={
                                "trace_id":    trace_id,
                                "event_type":  event_type,
                            },
                        )

            except Exception as exc:
                logger.exception(
                    "Failed to publish event",
                    extra={"stage": "kafka_error", "message_id": message_id},
                )
                await websocket.send_json(
                    {"status": "error", "detail": f"Failed to publish: {exc}"}
                )
                continue

            await websocket.send_json(
                {"status": "accepted", "message_id": message_id}
            )
            logger.info(
                "event_accepted",
                extra={"stage": "ws_acked", "message_id": message_id},
            )

    except WebSocketDisconnect:
        logger.info("ws_disconnected", extra={"stage": "ws_disconnect"})
        await manager.disconnect(conversation_id)
    except Exception as exc:
        logger.exception("Unexpected WebSocket error: %s", exc)
    finally:
        await manager.disconnect(conversation_id)
