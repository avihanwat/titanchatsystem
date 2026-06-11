import asyncio
import logging

import orjson
from aiokafka import AIOKafkaConsumer

from config.settings import KAFKA_BOOTSTRAP_SERVERS
from shared.observability.trace import set_trace
from consumer.gateway_client import push_to_client  # noqa: F401 — re-exported for handlers
from consumer import handlers
from config.topics import CHAT_INBOUND, CHAT_ACKS, OUTGOING_MESSAGES

logger = logging.getLogger(__name__)

_consumer: AIOKafkaConsumer | None = None
_consumer_task: asyncio.Task | None = None

# Per-conversation queues and their worker tasks.
# One queue + one worker per active conversation → strict FIFO ordering.
_conv_queues:  dict[str, asyncio.Queue] = {}
_conv_workers: dict[str, asyncio.Task]  = {}

# Backpressure controls
_MAX_QUEUE_SIZE = 100       # per-conversation queue depth before blocking
_MAX_CONCURRENT_CONVS = 10_000  # max simultaneous conversation workers
_worker_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_CONVS)

# ── Event type → handler dispatch table ──────────────────────────────────────

_HANDLERS = {
    "chat_start":        handlers.chat_start.handle,
    "incoming_message":  handlers.incoming_message.handle,
    "outgoing_message":  handlers.outgoing_message.handle,
    "transfer_to_agent": handlers.transfer_to_agent.handle,
    "chat_end":          handlers.chat_end.handle,
    "ack":               handlers.acks.handle,
}

# ── Per-conversation worker ───────────────────────────────────────────────────

async def _conversation_worker(conversation_id: str, queue: asyncio.Queue) -> None:
    """
    Processes every message for ONE conversation strictly in FIFO order.
    Different conversations run in parallel; this conversation never will.
    Shuts down when it receives a sentinel (None).
    """
    while True:
        payload = await queue.get()
        if payload is None:          # sentinel — chat_end issued, shut down
            queue.task_done()
            break

        event_type = payload.get("event_type", "unknown")
        try:
            await _dispatch(conversation_id, payload)
        except Exception as exc:
            logger.exception(
                "Handler error | event_type=%s | error=%s", event_type, exc
            )
        finally:
            queue.task_done()

    # Clean up after sentinel
    _conv_queues.pop(conversation_id, None)
    _conv_workers.pop(conversation_id, None)
    logger.info(
        "worker_shutdown",
        extra={"stage": "worker_stopped"},
    )


async def _dispatch(conversation_id: str, payload: dict) -> None:
    """Route payload to the correct handler; unknown types are logged and dropped."""
    event_type: str = payload.get("event_type") or ""
    handler = _HANDLERS.get(event_type)

    if handler is None:
        logger.warning("Unknown event_type=%s — dropping message.", event_type)
        return

    await handler(conversation_id, payload)

    # After chat_end completes, send the sentinel to this conversation's queue
    # so the worker exits cleanly.
    if event_type == "chat_end":
        q = _conv_queues.get(conversation_id)
        if q:
            await q.put(None)


# ── Main consume loop ─────────────────────────────────────────────────────────


async def _guarded_worker(conversation_id: str, queue: asyncio.Queue) -> None:
    """Wraps _conversation_worker with semaphore to cap max concurrent workers."""
    async with _worker_semaphore:
        await _conversation_worker(conversation_id, queue)


async def _consume_loop() -> None:
    assert _consumer is not None

    async for msg in _consumer:
        try:
            conversation_id: str = msg.key.decode("utf-8") if msg.key else ""
            if not conversation_id:
                logger.warning("Received message with no key, skipping.")
                continue
            if msg.value is None:
                logger.warning("Received message with no value, skipping.")
                continue

            # Extract trace context from Kafka headers
            headers     = {k: v.decode("utf-8") for k, v in (msg.headers or [])}
            trace_id    = headers.get("trace_id", "-")
            event_type  = headers.get("event_type", "-")
            set_trace(trace_id, conversation_id, event_type)

            payload: dict = orjson.loads(msg.value)

            logger.info(
                "event_dequeued",
                extra={"stage": "consumer_received"},
            )

            # Get or create a queue + worker for this conversation
            if conversation_id not in _conv_queues:
                q: asyncio.Queue = asyncio.Queue(maxsize=_MAX_QUEUE_SIZE)
                _conv_queues[conversation_id] = q
                task = asyncio.create_task(
                    _guarded_worker(conversation_id, q)
                )
                _conv_workers[conversation_id] = task
                logger.info(
                    "worker_created",
                    extra={"stage": "worker_init"},
                )

            await _conv_queues[conversation_id].put(payload)

        except Exception as exc:
            logger.exception("Error enqueuing Kafka message: %s", exc)


# ── Lifecycle ─────────────────────────────────────────────────────────────────

async def start_consumer() -> None:
    global _consumer, _consumer_task

    _consumer = AIOKafkaConsumer(
        CHAT_INBOUND,
        CHAT_ACKS,
        OUTGOING_MESSAGES,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="titanchat-consumer-group",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v,
        key_deserializer=lambda k: k,
    )
    await _consumer.start()
    _consumer_task = asyncio.create_task(_consume_loop())
    logger.info(
        "consumer_started",
        extra={"stage": "consumer_ready"},
    )


async def stop_consumer() -> None:
    global _consumer, _consumer_task

    if _consumer_task is not None:
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass
        _consumer_task = None

    # Send sentinel to every active conversation worker
    for q in list(_conv_queues.values()):
        await q.put(None)

    if _conv_workers:
        await asyncio.gather(*list(_conv_workers.values()), return_exceptions=True)

    if _consumer is not None:
        await _consumer.stop()
        _consumer = None
        logger.info("consumer_stopped", extra={"stage": "consumer_stopped"})

