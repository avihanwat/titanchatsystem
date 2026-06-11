import logging
from datetime import datetime, timezone

from models.events import ChatEndEvent
from consumer.gateway_client import push_to_client
from shared.cache.agent_router import release_agent_slot
from shared.cache.online_chat_tracker import chat_offline
from shared.cache.admin_feed import publish_admin_event
from gateway.kafka_producer import publish_message
from config.topics import CHAT_PERSIST

logger = logging.getLogger(__name__)


async def handle(conversation_id: str, raw: dict) -> None:
    event = ChatEndEvent(**raw)

    logger.info("chat_end", extra={"stage": "handler_start"})

    # HOT PATH: release agent slot + remove from online tracker
    await release_agent_slot(event.conversation_id)
    await chat_offline(event.conversation_id)

    # Notify the client that the session has ended
    await push_to_client(
        conversation_id,
        {
            "event_type": "chat_ended",
            "conversation_id": event.conversation_id,
            "timestamp": event.timestamp,
        },
    )

    # ASYNC PERSISTENCE: update conversation status in Cassandra
    await publish_message(
        topic=CHAT_PERSIST,
        key=conversation_id,
        payload={
            "_persist_type": "chat_end",
            "conversation_id": event.conversation_id,
            "timestamp": event.timestamp,
        },
    )

    # Real-time admin feed
    await publish_admin_event(event.bot_id, "chat_ended", {
        "conversation_id": event.conversation_id,
    })

    logger.info("chat_end", extra={"stage": "handler_done"})
    # The dispatcher sends the sentinel None to the queue after this returns,
    # shutting down this conversation's worker cleanly.
