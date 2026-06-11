"""
FastAPI dependencies for the API server.
"""
import uuid

import jwt
from fastapi import Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.database import get_db
from api.models import Admin, Agent, Bot
from api.utils.security import decode_access_token


async def get_current_user(request: Request) -> dict:
    """
    Extract and validate JWT from Authorization header.
    Returns payload: {user_id, role, admin_id?}
    """
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")

    token = auth_header[7:]
    try:
        payload = decode_access_token(token)
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Invalid token")

    return payload


async def get_current_admin(
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> Admin:
    """
    Verify the current user is an admin and return the Admin ORM object.
    """
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")

    admin_id = uuid.UUID(user["user_id"])
    result = await db.execute(select(Admin).where(Admin.id == admin_id))
    admin = result.scalar_one_or_none()
    if not admin or not admin.is_active:
        raise HTTPException(status_code=403, detail="Admin account not found or inactive")
    return admin


async def get_current_agent(
    user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> Agent:
    """
    Verify the current user is an agent and return the Agent ORM object.
    """
    if user.get("role") != "agent":
        raise HTTPException(status_code=403, detail="Agent access required")

    agent_id = uuid.UUID(user["user_id"])
    result = await db.execute(select(Agent).where(Agent.id == agent_id))
    agent = result.scalar_one_or_none()
    if not agent or not agent.is_active:
        raise HTTPException(status_code=403, detail="Agent account not found or inactive")
    return agent


async def get_admin_id_from_user(user: dict = Depends(get_current_user)) -> uuid.UUID:
    """
    Return the admin_id for scoping queries.
    - Admin: user_id IS the admin_id
    - Agent: admin_id is in the token payload
    """
    role = user.get("role")
    if role == "admin":
        return uuid.UUID(user["user_id"])
    elif role == "agent":
        admin_id = user.get("admin_id")
        if not admin_id:
            raise HTTPException(status_code=403, detail="Agent token missing admin_id")
        return uuid.UUID(admin_id)
    else:
        raise HTTPException(status_code=403, detail="Invalid role")
