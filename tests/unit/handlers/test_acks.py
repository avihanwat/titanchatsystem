"""Unit tests for consumer.handlers.acks"""
import pytest
from unittest.mock import AsyncMock, patch


@pytest.fixture
def _mock_deps():
    with (
        patch("consumer.handlers.acks.push_to_client", new_callable=AsyncMock) as mock_push,
        patch("consumer.handlers.acks.publish_message", new_callable=AsyncMock) as mock_kafka,
    ):
        yield {
            "push_to_client": mock_push,
            "publish_message": mock_kafka,
        }


@pytest.mark.asyncio
async def test_ack_pushes_to_client(_mock_deps, sample_ack_event):
    from consumer.handlers.acks import handle

    await handle("conv-123", sample_ack_event)

    _mock_deps["push_to_client"].assert_called_once()
    payload = _mock_deps["push_to_client"].call_args[0][1]
    assert payload["event_type"] == "ack"
    assert payload["ack_type"] == "delivered"
    assert payload["message_id"] == "msg-789"


@pytest.mark.asyncio
async def test_ack_enqueues_persistence(_mock_deps, sample_ack_event):
    from consumer.handlers.acks import handle

    await handle("conv-123", sample_ack_event)

    _mock_deps["publish_message"].assert_called_once()
    call_kwargs = _mock_deps["publish_message"].call_args[1]
    assert call_kwargs["payload"]["_persist_type"] == "ack"
    assert call_kwargs["payload"]["ack_type"] == "delivered"


@pytest.mark.asyncio
async def test_ack_read_event(_mock_deps):
    from consumer.handlers.acks import handle

    event = {
        "event_type": "ack",
        "conversation_id": "conv-123",
        "message_id": "msg-789",
        "ack_type": "read",
        "from_user_id": "user-456",
        "timestamp": 1748275200,
    }

    await handle("conv-123", event)

    call_kwargs = _mock_deps["publish_message"].call_args[1]
    assert call_kwargs["payload"]["ack_type"] == "read"
    assert call_kwargs["payload"]["from_user_id"] == "user-456"
