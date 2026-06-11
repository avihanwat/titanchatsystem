import asyncio
import logging
from datetime import datetime, timezone

from models.events import IncomingMessageEvent
from consumer.gateway_client import push_to_client
from gateway.kafka_producer import publish_message
from config.topics import CHAT_ACKS, CHAT_PERSIST
from shared.cache.online_chat_tracker import chat_message_received
from shared.cache.admin_feed import publish_admin_event

logger = logging.getLogger(__name__)


async def handle(conversation_id: str, raw: dict) -> None:
    event = IncomingMessageEvent(**raw)

    logger.info(
        "incoming_message",
        extra={"stage": "handler_start", "message_id": event.message_id},
    )

    # HOT PATH: push to client + ack + tracker — no Cassandra writes here
    await asyncio.gather(
        push_to_client(
            conversation_id,
            {
                "event_type": "message_received",
                "message_id": event.message_id,
                "conversation_id": event.conversation_id,
                "message": event.message,
                "timestamp": event.timestamp,
            },
        ),
        publish_message(
            topic=CHAT_ACKS,
            key=conversation_id,
            payload={
                "event_type": "ack",
                "ack_type": "delivered",
                "message_id": event.message_id,
                "conversation_id": event.conversation_id,
                "timestamp": event.timestamp,
            },
        ),
        chat_message_received(conversation_id),
    )

    # ASYNC PERSISTENCE: enqueue for background Cassandra writes
    await publish_message(
        topic=CHAT_PERSIST,
        key=conversation_id,
        payload={
            "_persist_type": "message",
            "conversation_id": event.conversation_id,
            "bot_id": event.bot_id,
            "message_id": event.message_id,
            "sender_id": event.user_id,
            "sender_type": "user",
            "content_type": "text",
            "message": event.message,
            "seq": event.seq,
            "timestamp": event.timestamp,
            "unread_target": "agent",
        },
    )

    # Real-time admin feed
    await publish_admin_event(event.bot_id, "new_message", {
        "conversation_id": event.conversation_id,
        "message_id": event.message_id,
        "message_preview": event.message[:100],
        "sender_id": event.user_id,
        "sender_type": "user",
    })

    logger.info(
        "incoming_message",
        extra={"stage": "handler_done", "message_id": event.message_id},
    )
