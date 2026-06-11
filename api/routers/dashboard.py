"""
Admin dashboard routes — real-time stats from Redis, scoped by admin's bots.
"""
import uuid

from fastapi import APIRouter, Depends
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.database import get_db
from api.deps import get_current_admin
from api.models import Admin, Bot
from api.schemas import DashboardStats
from shared.cache.agent_router import get_all_agents_status
from shared.cache.online_chat_tracker import get_all_online_chats, get_online_count

router = APIRouter(prefix="/dashboard", tags=["dashboard"])


async def _get_admin_bot_ids(admin_id: uuid.UUID, db: AsyncSession) -> set[str]:
    result = await db.execute(select(Bot.id).where(Bot.admin_id == admin_id))
    return {str(row[0]) for row in result.fetchall()}


@router.get("/stats", response_model=DashboardStats)
async def dashboard_stats(
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """High-level dashboard stats scoped to admin's bots."""
    bot_ids = await _get_admin_bot_ids(admin.id, db)

    # Get all online chats and filter by admin's bots
    all_chats = await get_all_online_chats(offset=0, limit=10000)
    admin_chats = [c for c in all_chats if c.get("bot_id") in bot_ids]

    # Get agents that belong to this admin (from Redis agent status)
    all_agents = await get_all_agents_status()
    # Filter agents - for now show all (in production, scope by admin_id in agent meta)
    online_agents = sum(1 for a in all_agents if a.get("status") == "online")

    return DashboardStats(
        online_chats=len(admin_chats),
        online_agents=online_agents,
        total_agents=len(all_agents),
        queued_chats=0,  # TODO: read from agent_queue
    )


@router.get("/agents")
async def dashboard_agents(
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Real-time agent status from Redis."""
    agents = await get_all_agents_status()
    return {"agents": agents}


@router.get("/active-chats")
async def dashboard_active_chats(
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
    offset: int = 0,
    limit: int = 50,
):
    """All live chats scoped to admin's bots."""
    bot_ids = await _get_admin_bot_ids(admin.id, db)

    all_chats = await get_all_online_chats(offset=0, limit=10000)
    admin_chats = [c for c in all_chats if c.get("bot_id") in bot_ids]

    # Paginate
    paginated = admin_chats[offset: offset + limit]
    return {
        "total": len(admin_chats),
        "chats": paginated,
    }
