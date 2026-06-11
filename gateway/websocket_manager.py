import asyncio
import logging
import time
from fastapi import WebSocket
from shared.cache.connection_registry import (
    register_connection,
    unregister_connection,
    refresh_connection_ttl,
)
from shared.cache.online_chat_tracker import chat_offline
from config.settings import SERVER_ID

logger = logging.getLogger(__name__)

# Stale connection timeout — only evict if no ping/pong response
_STALE_TIMEOUT_SECONDS = 120


class ConnectionManager:

    def __init__(self) -> None:
        self.connections: dict[str, WebSocket] = {}
        self.last_seen: dict[str, float] = {}
        self.send_locks: dict[str, asyncio.Lock] = {}

    async def connect(
        self,
        conversation_id: str,
        websocket: WebSocket,
    ) -> None:
        # Guard: reject if conversation already has an active socket
        existing = self.connections.get(conversation_id)
        if existing:
            logger.warning(
                "Duplicate connection for conversation_id=%s — closing old socket.",
                conversation_id,
            )
            await self._force_close(conversation_id, existing)

        await websocket.accept()
        self.connections[conversation_id] = websocket
        self.last_seen[conversation_id] = time.time()
        self.send_locks[conversation_id] = asyncio.Lock()
        await register_connection(conversation_id, SERVER_ID)

    async def heartbeat(self, conversation_id: str) -> None:
        self.last_seen[conversation_id] = time.time()
        await refresh_connection_ttl(conversation_id)

    async def ping_all(self) -> None:
        """Send WebSocket ping to all connections; mark responsive ones as alive."""
        stale = []
        for cid, ws in list(self.connections.items()):
            try:
                await ws.send_json({"event_type": "ping"})
            except Exception:
                stale.append(cid)
        for cid in stale:
            await self.disconnect(cid)

    def get_stale_connections(self) -> list[str]:
        """Return conversation_ids that have not been seen within timeout."""
        now = time.time()
        return [
            cid for cid, ts in self.last_seen.items()
            if now - ts > _STALE_TIMEOUT_SECONDS
        ]

    async def disconnect(self, conversation_id: str) -> None:
        websocket = self.connections.pop(conversation_id, None)
        self.last_seen.pop(conversation_id, None)
        self.send_locks.pop(conversation_id, None)
        await unregister_connection(conversation_id)
        await chat_offline(conversation_id)

        if websocket:
            try:
                await websocket.close()
            except Exception:
                pass

    async def _force_close(self, conversation_id: str, websocket: WebSocket) -> None:
        """Force-close an existing socket without full disconnect cleanup."""
        self.connections.pop(conversation_id, None)
        self.last_seen.pop(conversation_id, None)
        self.send_locks.pop(conversation_id, None)
        try:
            await websocket.close(code=4000, reason="Superseded by new connection")
        except Exception:
            pass

    async def send_message(
        self,
        conversation_id: str,
        payload: dict,
    ) -> bool:
        websocket = self.connections.get(conversation_id)
        if not websocket:
            return False

        lock = self.send_locks.get(conversation_id)
        if not lock:
            return False

        async with lock:
            try:
                await websocket.send_json(payload)
                return True
            except Exception:
                await self.disconnect(conversation_id)
                return False


manager = ConnectionManager()
