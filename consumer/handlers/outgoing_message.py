import asyncio
import logging

from models.events import OutgoingMessageEvent
from consumer.gateway_client import push_to_client
from gateway.kafka_producer import publish_message
from config.topics import CHAT_PERSIST
from shared.cache.online_chat_tracker import chat_message_received

logger = logging.getLogger(__name__)


async def handle(conversation_id: str, raw: dict) -> None:
    """Handle agent/AI → user messages."""
    event = OutgoingMessageEvent(**raw)

    logger.info(
        "outgoing_message",
        extra={"stage": "handler_start", "message_id": event.message_id},
    )

    # HOT PATH: push to client + update tracker
    await asyncio.gather(
        push_to_client(
            conversation_id,
            {
                "event_type": "message_received",
                "message_id": event.message_id,
                "conversation_id": event.conversation_id,
                "message": event.message,
                "sender_type": "agent",
                "agent_id": event.agent_id,
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
            "message_id": event.message_id,
            "sender_id": event.agent_id,
            "sender_type": "agent",
            "content_type": "text",
            "message": event.message,
            "seq": event.seq,
            "timestamp": event.timestamp,
            "unread_target": "",  # user is online, they receive it live
        },
    )

    logger.info(
        "outgoing_message",
        extra={"stage": "handler_done", "message_id": event.message_id},
    )
