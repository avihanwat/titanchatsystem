"""
Persistence worker — consumes from 'chat-persist' topic and writes to Cassandra.

Runs as a separate consumer group so it can lag behind the real-time path
without affecting message delivery latency.

Includes retry with exponential backoff and dead-letter topic for failed writes.

    Startup: called from consumer.main alongside the real-time consumer.
"""
import asyncio
import logging
from datetime import datetime, timezone

import orjson
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config.settings import KAFKA_BOOTSTRAP_SERVERS
from shared.db.cassandra import db_execute
from config.topics import CHAT_PERSIST

logger = logging.getLogger(__name__)

_consumer: AIOKafkaConsumer | None = None
_producer: AIOKafkaProducer | None = None
_task: asyncio.Task | None = None

# Retry config
_MAX_RETRIES = 3
_RETRY_BACKOFF_BASE = 0.5  # seconds: 0.5, 1.0, 2.0
_DLQ_TOPIC = "chat-persist-dlq"


# ── Persist handlers by _persist_type ─────────────────────────────────────────


async def _persist_message(payload: dict) -> None:
    """Write incoming/outgoing message to Cassandra."""
    now = datetime.fromtimestamp(payload["timestamp"], tz=timezone.utc)
    bucket = now.strftime("%Y%m%d")

    await db_execute(
        """INSERT INTO messages
           (conversation_id, bucket, created_at, message_id, sender_id,
            sender_type, content_type, content, seq, status, edited, deleted)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        [
            payload["conversation_id"],
            bucket,
            now,
            payload["message_id"],
            payload.get("sender_id", ""),
            payload.get("sender_type", "user"),
            payload.get("content_type", "text"),
            payload.get("message", ""),
            payload.get("seq", 0),
            "sent",
            False,
            False,
        ],
    )

    # Update conversation last_message preview
    preview = payload.get("message", "")[:100]
    await db_execute(
        """UPDATE conversations SET last_message_at=%s, last_message_preview=%s
           WHERE conversation_id=%s""",
        [now, preview, payload["conversation_id"]],
    )

    # Increment unread counter for the recipient
    recipient = payload.get("unread_target", "agent")
    await db_execute(
        "UPDATE unread_counters SET count = count + 1 WHERE user_id=%s AND conversation_id=%s",
        [recipient, payload["conversation_id"]],
    )

    logger.debug("persisted message_id=%s", payload["message_id"])


async def _persist_chat_start(payload: dict) -> None:
    """Write conversation + inbox + queue records."""
    now = datetime.fromtimestamp(payload["timestamp"], tz=timezone.utc)

    # conversations_by_user denormalized view
    await db_execute(
        """INSERT INTO conversations_by_user
           (user_id, last_message_at, conversation_id, status, unread_count)
           VALUES (%s, %s, %s, %s, %s)""",
        [payload["user_id"], now, payload["conversation_id"], "active", 0],
    )

    # Agent queue
    await db_execute(
        """INSERT INTO agent_queue
           (queue_id, queued_at, conversation_id, user_id, priority)
           VALUES (%s, %s, %s, %s, %s)""",
        ["default", now, payload["conversation_id"], payload["user_id"], 3],
    )


async def _persist_chat_end(payload: dict) -> None:
    """Close conversation in DB."""
    now = datetime.fromtimestamp(payload["timestamp"], tz=timezone.utc)

    await db_execute(
        "UPDATE conversations SET status=%s, ended_at=%s WHERE conversation_id=%s",
        ["ended", now, payload["conversation_id"]],
    )


async def _persist_ack(payload: dict) -> None:
    """Write ack receipt to Cassandra."""
    now = datetime.fromtimestamp(payload["timestamp"], tz=timezone.utc)

    await db_execute(
        """INSERT INTO message_acks
           (conversation_id, message_id, ack_type, acked_by, acked_at)
           VALUES (%s, %s, %s, %s, %s)""",
        [
            payload["conversation_id"],
            payload["message_id"],
            payload["ack_type"],
            payload.get("from_user_id", ""),
            now,
        ],
    )

    # Decrement unread counter on read
    if payload.get("ack_type") == "read":
        user_id = payload.get("from_user_id") or "agent"
        await db_execute(
            "UPDATE unread_counters SET count = count - 1 WHERE user_id=%s AND conversation_id=%s",
            [user_id, payload["conversation_id"]],
        )


async def _persist_assignment(payload: dict) -> None:
    """Record agent assignment in history table."""
    now = datetime.fromtimestamp(payload["timestamp"], tz=timezone.utc)

    await db_execute(
        """INSERT INTO agent_assignments
           (conversation_id, assigned_at, agent_id, assigned_by)
           VALUES (%s, %s, %s, %s)""",
        [payload["conversation_id"], now, payload["agent_id"], "system"],
    )


# ── Dispatch ──────────────────────────────────────────────────────────────────

_PERSIST_HANDLERS = {
    "message": _persist_message,
    "chat_start": _persist_chat_start,
    "chat_end": _persist_chat_end,
    "ack": _persist_ack,
    "assignment": _persist_assignment,
}


async def _execute_with_retry(handler, payload: dict) -> None:
    """Execute a persistence handler with exponential backoff retry."""
    for attempt in range(_MAX_RETRIES):
        try:
            await handler(payload)
            return
        except Exception as exc:
            if attempt < _MAX_RETRIES - 1:
                delay = _RETRY_BACKOFF_BASE * (2 ** attempt)
                logger.warning(
                    "Persist retry attempt=%d/%d delay=%.1fs error=%s",
                    attempt + 1, _MAX_RETRIES, delay, exc,
                )
                await asyncio.sleep(delay)
            else:
                # All retries exhausted — send to DLQ
                logger.error(
                    "Persist failed after %d attempts, sending to DLQ: %s",
                    _MAX_RETRIES, exc,
                )
                await _send_to_dlq(payload, str(exc))


async def _send_to_dlq(payload: dict, error: str) -> None:
    """Publish failed message to dead-letter topic for manual inspection."""
    if _producer is None:
        logger.error("DLQ producer not available — message lost: %s", payload)
        return

    dlq_payload = {
        "original_payload": payload,
        "error": error,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "retries_exhausted": _MAX_RETRIES,
    }
    try:
        await _producer.send_and_wait(
            _DLQ_TOPIC,
            value=orjson.dumps(dlq_payload),
            key=(payload.get("conversation_id", "unknown")).encode("utf-8"),
        )
    except Exception as exc:
        logger.exception("Failed to send to DLQ: %s", exc)


async def _consume_loop() -> None:
    assert _consumer is not None

    async for msg in _consumer:
        try:
            if msg.value is None:
                continue

            payload: dict = orjson.loads(msg.value)
            persist_type = payload.get("_persist_type", "")

            handler = _PERSIST_HANDLERS.get(persist_type)
            if handler is None:
                logger.warning(
                    "Unknown _persist_type=%s — dropping.", persist_type
                )
                continue

            await _execute_with_retry(handler, payload)

        except Exception as exc:
            logger.exception("Persistence worker fatal error: %s", exc)


# ── Lifecycle ─────────────────────────────────────────────────────────────────


async def start_persistence_worker() -> None:
    global _consumer, _producer, _task

    _producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )
    await _producer.start()

    _consumer = AIOKafkaConsumer(
        CHAT_PERSIST,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="titanchat-persist-group",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v,
        key_deserializer=lambda k: k,
    )
    await _consumer.start()
    _task = asyncio.create_task(_consume_loop())
    logger.info("Persistence worker started (topic=%s, dlq=%s)", CHAT_PERSIST, _DLQ_TOPIC)


async def stop_persistence_worker() -> None:
    global _consumer, _producer, _task

    if _task is not None:
        _task.cancel()
        try:
            await _task
        except asyncio.CancelledError:
            pass
        _task = None

    if _consumer is not None:
        await _consumer.stop()
        _consumer = None

    if _producer is not None:
        await _producer.stop()
        _producer = None
        logger.info("Persistence worker stopped")
