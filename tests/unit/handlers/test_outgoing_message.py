"""Unit tests for consumer.handlers.outgoing_message"""
import pytest
from unittest.mock import AsyncMock, patch


@pytest.fixture
def _mock_deps():
    with (
        patch("consumer.handlers.outgoing_message.push_to_client", new_callable=AsyncMock) as mock_push,
        patch("consumer.handlers.outgoing_message.publish_message", new_callable=AsyncMock) as mock_kafka,
        patch("consumer.handlers.outgoing_message.chat_message_received", new_callable=AsyncMock) as mock_tracker,
    ):
        yield {
            "push_to_client": mock_push,
            "publish_message": mock_kafka,
            "chat_message_received": mock_tracker,
        }


@pytest.mark.asyncio
async def test_outgoing_message_pushes_to_client(_mock_deps, sample_outgoing_message_event):
    from consumer.handlers.outgoing_message import handle

    await handle("conv-123", sample_outgoing_message_event)

    _mock_deps["push_to_client"].assert_called_once()
    payload = _mock_deps["push_to_client"].call_args[0][1]
    assert payload["event_type"] == "message_received"
    assert payload["sender_type"] == "agent"
    assert payload["agent_id"] == "agent-1"


@pytest.mark.asyncio
async def test_outgoing_message_enqueues_persistence(_mock_deps, sample_outgoing_message_event):
    from consumer.handlers.outgoing_message import handle

    await handle("conv-123", sample_outgoing_message_event)

    _mock_deps["publish_message"].assert_called_once()
    call_kwargs = _mock_deps["publish_message"].call_args[1]
    assert call_kwargs["payload"]["_persist_type"] == "message"
    assert call_kwargs["payload"]["sender_id"] == "agent-1"
    assert call_kwargs["payload"]["sender_type"] == "agent"


@pytest.mark.asyncio
async def test_outgoing_message_updates_tracker(_mock_deps, sample_outgoing_message_event):
    from consumer.handlers.outgoing_message import handle

    await handle("conv-123", sample_outgoing_message_event)

    _mock_deps["chat_message_received"].assert_called_once_with("conv-123")
