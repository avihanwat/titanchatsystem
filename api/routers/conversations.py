"""
Conversations & chat history routes.
Reads from Cassandra, scoped by admin's bots.
"""
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_admin_id_from_user, get_current_user
from api.database import get_db
from api.models import Bot
from api.schemas import ConversationResponse, MessageResponse
from shared.db.cassandra import db_execute
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/conversations", tags=["conversations"])


async def _get_admin_bot_ids(admin_id: uuid.UUID, db: AsyncSession) -> list[str]:
    """Get all bot IDs belonging to this admin."""
    result = await db.execute(
        select(Bot.id).where(Bot.admin_id == admin_id)
    )
    return [str(row[0]) for row in result.fetchall()]


@router.get("", response_model=list[ConversationResponse])
async def list_conversations(
    status: str | None = Query(None, description="Filter by status: active, ended"),
    bot_id: str | None = Query(None, description="Filter by specific bot_id"),
    limit: int = Query(50, ge=1, le=200),
    admin_id: uuid.UUID = Depends(get_admin_id_from_user),
    db: AsyncSession = Depends(get_db),
):
    """
    List conversations scoped to admin's bots.
    Reads from Cassandra conversations_by_bot table.
    """
    bot_ids = await _get_admin_bot_ids(admin_id, db)
    if not bot_ids:
        return []

    # If specific bot_id requested, verify it belongs to this admin
    if bot_id:
        if bot_id not in bot_ids:
            raise HTTPException(status_code=403, detail="Bot does not belong to your account")
        bot_ids = [bot_id]

    conversations = []
    for bid in bot_ids:
        rows = await db_execute(
            "SELECT conversation_id, bot_id, user_id, agent_id, status, "
            "started_at, last_message_at, last_message_preview "
            "FROM conversations_by_bot WHERE bot_id = %s LIMIT %s",
            [bid, limit],
        )
        for row in rows:
            conversations.append(ConversationResponse(
                conversation_id=row.conversation_id,
                bot_id=row.bot_id,
                user_id=row.user_id,
                agent_id=row.agent_id,
                status=row.status,
                started_at=row.started_at,
                last_message_at=row.last_message_at,
                last_message_preview=row.last_message_preview,
            ))

    # Sort by last_message_at descending
    conversations.sort(key=lambda c: c.last_message_at or datetime.min, reverse=True)
    return conversations[:limit]


@router.get("/{conversation_id}", response_model=ConversationResponse)
async def get_conversation(
    conversation_id: str,
    admin_id: uuid.UUID = Depends(get_admin_id_from_user),
    db: AsyncSession = Depends(get_db),
):
    """Get conversation details."""
    rows = await db_execute(
        "SELECT conversation_id, bot_id, user_id, agent_id, status, "
        "started_at, ended_at, last_message_at, last_message_preview "
        "FROM conversations WHERE conversation_id = %s",
        [conversation_id],
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Conversation not found")

    row = rows[0]

    # Verify this conversation belongs to admin's bot
    bot_ids = await _get_admin_bot_ids(admin_id, db)
    if row.bot_id and row.bot_id not in bot_ids:
        raise HTTPException(status_code=403, detail="Not authorized to view this conversation")

    return ConversationResponse(
        conversation_id=row.conversation_id,
        bot_id=row.bot_id,
        user_id=row.user_id,
        agent_id=row.agent_id,
        status=row.status,
        started_at=row.started_at,
        last_message_at=row.last_message_at,
        last_message_preview=row.last_message_preview,
    )


@router.get("/{conversation_id}/messages", response_model=list[MessageResponse])
async def get_messages(
    conversation_id: str,
    bucket: str | None = Query(None, description="Day bucket YYYYMMDD. Defaults to today."),
    limit: int = Query(50, ge=1, le=200),
    admin_id: uuid.UUID = Depends(get_admin_id_from_user),
    db: AsyncSession = Depends(get_db),
):
    """
    Get messages for a conversation, paginated by day bucket.
    If no bucket is given, scans back up to 7 days to find messages.
    """
    # Verify access
    rows = await db_execute(
        "SELECT bot_id FROM conversations WHERE conversation_id = %s",
        [conversation_id],
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Conversation not found")

    bot_ids = await _get_admin_bot_ids(admin_id, db)
    if rows[0].bot_id and rows[0].bot_id not in bot_ids:
        raise HTTPException(status_code=403, detail="Not authorized")

    # Query messages
    if bucket:
        buckets_to_query = [bucket]
    else:
        # Scan back up to 7 days
        today = datetime.now(timezone.utc)
        buckets_to_query = [
            (today - timedelta(days=i)).strftime("%Y%m%d") for i in range(7)
        ]

    messages = []
    for b in buckets_to_query:
        msg_rows = await db_execute(
            "SELECT message_id, conversation_id, sender_id, sender_type, "
            "content, content_type, created_at, seq, status "
            "FROM messages WHERE conversation_id = %s AND bucket = %s "
            "ORDER BY created_at ASC LIMIT %s",
            [conversation_id, b, limit - len(messages)],
        )
        for row in msg_rows:
            messages.append(MessageResponse(
                message_id=row.message_id,
                conversation_id=row.conversation_id,
                sender_id=row.sender_id,
                sender_type=row.sender_type,
                content=row.content,
                content_type=row.content_type,
                created_at=row.created_at,
                seq=row.seq,
                status=row.status,
            ))
        if len(messages) >= limit:
            break

    return messages[:limit]
