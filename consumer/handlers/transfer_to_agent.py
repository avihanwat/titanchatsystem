import logging
from datetime import datetime, timezone

from models.events import TransferToAgentEvent
from consumer.gateway_client import push_to_client
from shared.cache.agent_router import route_to_agent
from shared.cache.online_chat_tracker import chat_assigned
from shared.cache.admin_feed import publish_admin_event
from shared.db.cassandra import db_execute
from gateway.kafka_producer import publish_message
from config.topics import CHAT_PERSIST

logger = logging.getLogger(__name__)


async def handle(conversation_id: str, raw: dict) -> None:
    event = TransferToAgentEvent(**raw)
    now = datetime.now(timezone.utc)

    logger.info("transfer_to_agent", extra={"stage": "handler_start"})

    # Route to best available agent
    agent_id = await route_to_agent(event.conversation_id)

    if agent_id:
        # Update conversation with assigned agent (hot path — needed for reads)
        await db_execute(
            "UPDATE conversations SET agent_id=%s, status=%s WHERE conversation_id=%s",
            [agent_id, "active", event.conversation_id],
        )

        # Update online tracker
        await chat_assigned(event.conversation_id, agent_id)

        # ASYNC PERSISTENCE: record assignment history
        await publish_message(
            topic=CHAT_PERSIST,
            key=conversation_id,
            payload={
                "_persist_type": "assignment",
                "conversation_id": event.conversation_id,
                "agent_id": agent_id,
                "timestamp": event.timestamp,
            },
        )

    # Notify the client
    await push_to_client(
        conversation_id,
        {
            "event_type": "transfer_initiated",
            "conversation_id": event.conversation_id,
            "agent_id": agent_id or "waiting",
            "reason": event.reason,
            "timestamp": event.timestamp,
        },
    )

    # Real-time admin feed
    await publish_admin_event(event.bot_id, "agent_assigned", {
        "conversation_id": event.conversation_id,
        "agent_id": agent_id or "waiting",
    })

    logger.info("transfer_to_agent", extra={"stage": "handler_done"})
