"""
Auth routes: admin registration, admin login, agent login.
"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.database import get_db
from api.models import Admin, Agent
from api.schemas import AdminLogin, AdminRegister, AgentLogin, TokenResponse
from api.utils.security import create_access_token, hash_password, verify_password

router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/admin/register", response_model=TokenResponse, status_code=201)
async def admin_register(body: AdminRegister, db: AsyncSession = Depends(get_db)):
    """Create a new admin account."""
    # Check if email already exists
    existing = await db.execute(select(Admin).where(Admin.email == body.email))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Email already registered")

    admin = Admin(
        email=body.email,
        password_hash=hash_password(body.password),
        name=body.name,
    )
    db.add(admin)
    await db.flush()

    token = create_access_token(user_id=str(admin.id), role="admin")
    return TokenResponse(
        access_token=token, role="admin", user_id=str(admin.id)
    )


@router.post("/admin/login", response_model=TokenResponse)
async def admin_login(body: AdminLogin, db: AsyncSession = Depends(get_db)):
    """Admin login with email + password."""
    result = await db.execute(select(Admin).where(Admin.email == body.email))
    admin = result.scalar_one_or_none()

    if not admin or not verify_password(body.password, admin.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    if not admin.is_active:
        raise HTTPException(status_code=403, detail="Account deactivated")

    token = create_access_token(user_id=str(admin.id), role="admin")
    return TokenResponse(
        access_token=token, role="admin", user_id=str(admin.id)
    )


@router.post("/agent/login", response_model=TokenResponse)
async def agent_login(body: AgentLogin, db: AsyncSession = Depends(get_db)):
    """Agent login with email + password."""
    result = await db.execute(select(Agent).where(Agent.email == body.email))
    agent = result.scalar_one_or_none()

    if not agent or not verify_password(body.password, agent.password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    if not agent.is_active:
        raise HTTPException(status_code=403, detail="Account deactivated")

    token = create_access_token(
        user_id=str(agent.id),
        role="agent",
        admin_id=str(agent.admin_id),
    )
    return TokenResponse(
        access_token=token, role="agent", user_id=str(agent.id)
    )
