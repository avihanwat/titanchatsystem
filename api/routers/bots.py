"""
Bot management routes (admin only).
Admin creates bots (message sources/widgets), manages them.
"""
import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.database import get_db
from api.deps import get_current_admin
from api.models import Admin, Bot
from api.schemas import BotCreate, BotListResponse, BotResponse, BotUpdate
from api.utils.bot_auth import generate_api_key
from shared.cache.bot_registry import cache_bot_mapping, invalidate_bot_mapping

router = APIRouter(prefix="/bots", tags=["bots"])


@router.post("", response_model=BotResponse, status_code=201)
async def create_bot(
    body: BotCreate,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Create a new bot. Returns the bot with its api_key."""
    bot = Bot(
        admin_id=admin.id,
        name=body.name,
        api_key=generate_api_key(),
        config=body.config,
    )
    db.add(bot)
    await db.flush()
    await db.refresh(bot)

    # Cache bot→admin mapping in Redis for real-time feed routing
    await cache_bot_mapping(str(bot.id), str(admin.id))

    return bot


@router.get("", response_model=list[BotListResponse])
async def list_bots(
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """List all bots owned by this admin."""
    result = await db.execute(
        select(Bot).where(Bot.admin_id == admin.id).order_by(Bot.created_at.desc())
    )
    return result.scalars().all()


@router.get("/{bot_id}", response_model=BotResponse)
async def get_bot(
    bot_id: uuid.UUID,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get bot details (including api_key)."""
    result = await db.execute(
        select(Bot).where(Bot.id == bot_id, Bot.admin_id == admin.id)
    )
    bot = result.scalar_one_or_none()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")
    return bot


@router.patch("/{bot_id}", response_model=BotResponse)
async def update_bot(
    bot_id: uuid.UUID,
    body: BotUpdate,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update bot name, config, or active status."""
    result = await db.execute(
        select(Bot).where(Bot.id == bot_id, Bot.admin_id == admin.id)
    )
    bot = result.scalar_one_or_none()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")

    if body.name is not None:
        bot.name = body.name
    if body.config is not None:
        bot.config = body.config
    if body.is_active is not None:
        bot.is_active = body.is_active

    await db.flush()
    await db.refresh(bot)
    return bot


@router.delete("/{bot_id}", status_code=204)
async def delete_bot(
    bot_id: uuid.UUID,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Deactivate a bot (soft delete)."""
    result = await db.execute(
        select(Bot).where(Bot.id == bot_id, Bot.admin_id == admin.id)
    )
    bot = result.scalar_one_or_none()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")

    bot.is_active = False
    await db.flush()

    # Invalidate cache so events for this bot are no longer published
    await invalidate_bot_mapping(str(bot.id))


@router.post("/{bot_id}/regenerate-key", response_model=BotResponse)
async def regenerate_bot_key(
    bot_id: uuid.UUID,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Generate a new api_key for the bot. Old key becomes invalid immediately."""
    result = await db.execute(
        select(Bot).where(Bot.id == bot_id, Bot.admin_id == admin.id)
    )
    bot = result.scalar_one_or_none()
    if not bot:
        raise HTTPException(status_code=404, detail="Bot not found")

    bot.api_key = generate_api_key()
    await db.flush()
    await db.refresh(bot)
    return bot
