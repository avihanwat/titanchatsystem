"""Unit tests for consumer.handlers.incoming_message"""
import pytest
from unittest.mock import AsyncMock, patch


@pytest.fixture
def _mock_deps():
    with (
        patch("consumer.handlers.incoming_message.push_to_client", new_callable=AsyncMock) as mock_push,
        patch("consumer.handlers.incoming_message.publish_message", new_callable=AsyncMock) as mock_kafka,
        patch("consumer.handlers.incoming_message.chat_message_received", new_callable=AsyncMock) as mock_tracker,
    ):
        yield {
            "push_to_client": mock_push,
            "publish_message": mock_kafka,
            "chat_message_received": mock_tracker,
        }


@pytest.mark.asyncio
async def test_incoming_message_pushes_to_client(_mock_deps, sample_incoming_message_event):
    from consumer.handlers.incoming_message import handle

    await handle("conv-123", sample_incoming_message_event)

    _mock_deps["push_to_client"].assert_called_once()
    payload = _mock_deps["push_to_client"].call_args[0][1]
    assert payload["event_type"] == "message_received"
    assert payload["message"] == "Hello, I need help with billing"


@pytest.mark.asyncio
async def test_incoming_message_publishes_ack(_mock_deps, sample_incoming_message_event):
    from consumer.handlers.incoming_message import handle

    await handle("conv-123", sample_incoming_message_event)

    # First publish_message call should be in the gather (ack)
    # Second call should be the persist enqueue
    calls = _mock_deps["publish_message"].call_args_list
    assert len(calls) == 2

    # Check the persist call
    persist_call = calls[1]
    assert persist_call[1]["payload"]["_persist_type"] == "message"
    assert persist_call[1]["payload"]["sender_id"] == "user-456"


@pytest.mark.asyncio
async def test_incoming_message_updates_tracker(_mock_deps, sample_incoming_message_event):
    from consumer.handlers.incoming_message import handle

    await handle("conv-123", sample_incoming_message_event)

    _mock_deps["chat_message_received"].assert_called_once_with("conv-123")


@pytest.mark.asyncio
async def test_incoming_message_no_db_in_hot_path(_mock_deps, sample_incoming_message_event):
    """Verify that no Cassandra calls happen in the hot path."""
    from consumer.handlers.incoming_message import handle

    with patch("shared.db.cassandra.db_execute", new_callable=AsyncMock) as mock_db:
        await handle("conv-123", sample_incoming_message_event)
        mock_db.assert_not_called()
