"""
Coroutine-safe trace context using Python ContextVar.

Each asyncio Task (i.e. each per-conversation worker) has its own
isolated copy of these variables — no cross-conversation leakage.

Usage:
    from shared.trace_context import set_trace, get_trace_id, new_trace_id
"""
import uuid
from contextvars import ContextVar

_trace_id:        ContextVar[str] = ContextVar("trace_id",        default="-")
_conversation_id: ContextVar[str] = ContextVar("conversation_id", default="-")
_event_type:      ContextVar[str] = ContextVar("event_type",      default="-")


def set_trace(
    trace_id: str,
    conversation_id: str = "-",
    event_type: str = "-",
) -> None:
    _trace_id.set(trace_id)
    _conversation_id.set(conversation_id)
    _event_type.set(event_type)


def get_trace_id() -> str:
    return _trace_id.get()


def new_trace_id() -> str:
    return str(uuid.uuid4())
