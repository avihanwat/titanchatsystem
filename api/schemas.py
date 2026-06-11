"""
Pydantic schemas for API request/response validation.
"""
import uuid
from datetime import datetime

from pydantic import BaseModel, EmailStr, Field


# ── Auth ──────────────────────────────────────────────────────────────────────


class AdminRegister(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    name: str = Field(min_length=1, max_length=255)


class AdminLogin(BaseModel):
    email: EmailStr
    password: str


class AgentLogin(BaseModel):
    email: EmailStr
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    user_id: str


# ── Bot ───────────────────────────────────────────────────────────────────────


class BotCreate(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    config: dict = Field(default_factory=dict)


class BotUpdate(BaseModel):
    name: str | None = None
    config: dict | None = None
    is_active: bool | None = None


class BotResponse(BaseModel):
    id: uuid.UUID
    admin_id: uuid.UUID
    name: str
    api_key: str
    config: dict
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}


class BotListResponse(BaseModel):
    id: uuid.UUID
    name: str
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Agent ─────────────────────────────────────────────────────────────────────


class AgentCreate(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    name: str = Field(min_length=1, max_length=255)
    skills: list[str] = Field(default_factory=list)
    max_chats: int = Field(default=5, ge=1, le=20)


class AgentUpdate(BaseModel):
    name: str | None = None
    skills: list[str] | None = None
    max_chats: int | None = Field(default=None, ge=1, le=20)
    is_active: bool | None = None


class AgentResponse(BaseModel):
    id: uuid.UUID
    admin_id: uuid.UUID
    email: str
    name: str
    skills: list[str]
    max_chats: int
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Conversations ─────────────────────────────────────────────────────────────


class ConversationResponse(BaseModel):
    conversation_id: str
    bot_id: str | None = None
    user_id: str | None = None
    agent_id: str | None = None
    status: str | None = None
    started_at: datetime | None = None
    last_message_at: datetime | None = None
    last_message_preview: str | None = None


class MessageResponse(BaseModel):
    message_id: str
    conversation_id: str
    sender_id: str | None = None
    sender_type: str | None = None
    content: str | None = None
    content_type: str | None = None
    created_at: datetime | None = None
    seq: int | None = None
    status: str | None = None


# ── Dashboard ─────────────────────────────────────────────────────────────────


class DashboardStats(BaseModel):
    online_chats: int
    online_agents: int
    total_agents: int
    queued_chats: int
