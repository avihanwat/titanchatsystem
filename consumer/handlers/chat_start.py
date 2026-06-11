import logging
from datetime import datetime, timezone

from models.events import ChatStartEvent
from consumer.gateway_client import push_to_client
from shared.db.cassandra import db_execute
from shared.cache.online_chat_tracker import chat_online
from shared.cache.admin_feed import publish_admin_event
from gateway.kafka_producer import publish_message
from config.topics import CHAT_PERSIST
from config.settings import SERVER_ID

logger = logging.getLogger(__name__)


async def handle(conversation_id: str, raw: dict) -> None:
    event = ChatStartEvent(**raw)
    now = datetime.now(timezone.utc)

    logger.info("chat_start", extra={"stage": "handler_start", "message_id": "-"})

    # HOT PATH: Create conversation record (needed for correctness)
    # + register in Redis online tracker
    await db_execute(
        """INSERT INTO conversations
           (conversation_id, bot_id, user_id, server_id, status, channel, started_at, last_message_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
        [event.conversation_id, event.bot_id, event.user_id, SERVER_ID, "active", "web", now, now],
    )

    # Write to conversations_by_bot for admin dashboard lookups
    if event.bot_id:
        await db_execute(
            """INSERT INTO conversations_by_bot
               (bot_id, started_at, conversation_id, user_id, status, last_message_at)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            [event.bot_id, now, event.conversation_id, event.user_id, "active", now],
        )

    await chat_online(event.conversation_id, event.user_id, SERVER_ID, event.bot_id)

    # Notify the client that the chat session is open
    await push_to_client(
        conversation_id,
        {
            "event_type": "chat_started",
            "conversation_id": event.conversation_id,
            "timestamp": event.timestamp,
        },
    )

    # ASYNC PERSISTENCE: inbox view + agent queue (non-blocking)
    await publish_message(
        topic=CHAT_PERSIST,
        key=conversation_id,
        payload={
            "_persist_type": "chat_start",
            "conversation_id": event.conversation_id,
            "bot_id": event.bot_id,
            "user_id": event.user_id,
            "timestamp": event.timestamp,
        },
    )

    # Real-time admin feed
    await publish_admin_event(event.bot_id, "new_chat", {
        "conversation_id": event.conversation_id,
        "user_id": event.user_id,
    })

    logger.info("chat_start", extra={"stage": "handler_done"})
