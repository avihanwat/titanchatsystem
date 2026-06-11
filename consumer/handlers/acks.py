import logging
from datetime import datetime, timezone

from models.events import AckEvent
from consumer.gateway_client import push_to_client
from gateway.kafka_producer import publish_message
from config.topics import CHAT_PERSIST

logger = logging.getLogger(__name__)


async def handle(conversation_id: str, raw: dict) -> None:
    event = AckEvent(**raw)

    logger.info(
        "ack",
        extra={
            "stage":      "handler_start",
            "message_id": event.message_id,
        },
    )

    # HOT PATH: push the ack to the original sender's WebSocket
    await push_to_client(
        conversation_id,
        {
            "event_type":  "ack",
            "ack_type":    event.ack_type,
            "message_id":  event.message_id,
            "timestamp":   event.timestamp,
        },
    )

    # ASYNC PERSISTENCE: write ack to Cassandra + update counters
    await publish_message(
        topic=CHAT_PERSIST,
        key=conversation_id,
        payload={
            "_persist_type": "ack",
            "conversation_id": event.conversation_id,
            "message_id": event.message_id,
            "ack_type": event.ack_type,
            "from_user_id": event.from_user_id,
            "timestamp": event.timestamp,
        },
    )

    logger.info(
        "ack",
        extra={
            "stage":      "handler_done",
            "message_id": event.message_id,
        },
    )
