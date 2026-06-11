from __future__ import annotations

from typing import Literal
from pydantic import BaseModel


class ChatStartEvent(BaseModel):
    event_type: Literal["chat_start"]
    conversation_id: str
    user_id: str
    bot_id: str = ""
    timestamp: int


class IncomingMessageEvent(BaseModel):
    event_type: Literal["incoming_message"]
    conversation_id: str
    message_id: str
    user_id: str = ""
    bot_id: str = ""
    message: str
    timestamp: int
    seq: int = 0  # monotonically increasing per conversation; 0 = unset


class TransferToAgentEvent(BaseModel):
    event_type: Literal["transfer_to_agent"]
    conversation_id: str
    bot_id: str = ""
    reason: str = ""
    timestamp: int


class ChatEndEvent(BaseModel):
    event_type: Literal["chat_end"]
    conversation_id: str
    bot_id: str = ""
    timestamp: int


class AckEvent(BaseModel):
    event_type: Literal["ack"]
    conversation_id: str
    message_id: str
    ack_type: Literal["delivered", "read"]
    from_user_id: str = ""
    timestamp: int


class OutgoingMessageEvent(BaseModel):
    event_type: Literal["outgoing_message"]
    conversation_id: str
    message_id: str
    agent_id: str
    message: str
    timestamp: int
    seq: int = 0
