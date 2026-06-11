"""
Bot API key authentication.

Validates the api_key from a WebSocket query param or HTTP header,
returns the bot_id and admin_id for scoping.
"""
import logging
import secrets

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.models import Bot

logger = logging.getLogger(__name__)


def generate_api_key() -> str:
    """Generate a secure random 48-char hex API key."""
    return secrets.token_hex(24)


async def validate_bot_api_key(api_key: str, db: AsyncSession) -> dict | None:
    """
    Look up bot by api_key. Returns dict with bot_id, admin_id if valid.
    Returns None if invalid or inactive.
    """
    stmt = select(Bot).where(Bot.api_key == api_key, Bot.is_active == True)
    result = await db.execute(stmt)
    bot = result.scalar_one_or_none()

    if not bot:
        logger.warning("Invalid or inactive bot api_key attempted")
        return None

    return {
        "bot_id": str(bot.id),
        "admin_id": str(bot.admin_id),
        "bot_name": bot.name,
    }
