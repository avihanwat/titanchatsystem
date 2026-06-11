"""
Agent management routes (admin only).
Admin creates agents under their account.
"""
import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.database import get_db
from api.deps import get_current_admin
from api.models import Admin, Agent
from api.schemas import AgentCreate, AgentResponse, AgentUpdate
from api.utils.security import hash_password

router = APIRouter(prefix="/agents", tags=["agents"])


@router.post("", response_model=AgentResponse, status_code=201)
async def create_agent(
    body: AgentCreate,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Create a new agent under this admin."""
    # Check if email already taken
    existing = await db.execute(select(Agent).where(Agent.email == body.email))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Email already registered")

    agent = Agent(
        admin_id=admin.id,
        email=body.email,
        password_hash=hash_password(body.password),
        name=body.name,
        skills=body.skills,
        max_chats=body.max_chats,
    )
    db.add(agent)
    await db.flush()
    await db.refresh(agent)
    return agent


@router.get("", response_model=list[AgentResponse])
async def list_agents(
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """List all agents owned by this admin."""
    result = await db.execute(
        select(Agent).where(Agent.admin_id == admin.id).order_by(Agent.created_at.desc())
    )
    return result.scalars().all()


@router.get("/{agent_id}", response_model=AgentResponse)
async def get_agent(
    agent_id: uuid.UUID,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get agent details."""
    result = await db.execute(
        select(Agent).where(Agent.id == agent_id, Agent.admin_id == admin.id)
    )
    agent = result.scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent


@router.patch("/{agent_id}", response_model=AgentResponse)
async def update_agent(
    agent_id: uuid.UUID,
    body: AgentUpdate,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update agent skills, max_chats, name, or active status."""
    result = await db.execute(
        select(Agent).where(Agent.id == agent_id, Agent.admin_id == admin.id)
    )
    agent = result.scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    if body.name is not None:
        agent.name = body.name
    if body.skills is not None:
        agent.skills = body.skills
    if body.max_chats is not None:
        agent.max_chats = body.max_chats
    if body.is_active is not None:
        agent.is_active = body.is_active

    await db.flush()
    await db.refresh(agent)
    return agent


@router.delete("/{agent_id}", status_code=204)
async def delete_agent(
    agent_id: uuid.UUID,
    admin: Admin = Depends(get_current_admin),
    db: AsyncSession = Depends(get_db),
):
    """Deactivate an agent (soft delete)."""
    result = await db.execute(
        select(Agent).where(Agent.id == agent_id, Agent.admin_id == admin.id)
    )
    agent = result.scalar_one_or_none()
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")

    agent.is_active = False
    await db.flush()
