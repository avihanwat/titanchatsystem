"""Unit tests for consumer.handlers.chat_start"""
import pytest
from unittest.mock import AsyncMock, patch


@pytest.fixture
def _mock_deps():
    """Patch all external dependencies for chat_start handler."""
    with (
        patch("consumer.handlers.chat_start.db_execute", new_callable=AsyncMock) as mock_db,
        patch("consumer.handlers.chat_start.chat_online", new_callable=AsyncMock) as mock_tracker,
        patch("consumer.handlers.chat_start.push_to_client", new_callable=AsyncMock) as mock_push,
        patch("consumer.handlers.chat_start.publish_message", new_callable=AsyncMock) as mock_kafka,
        patch("consumer.handlers.chat_start.SERVER_ID", "gateway-test"),
    ):
        yield {
            "db_execute": mock_db,
            "chat_online": mock_tracker,
            "push_to_client": mock_push,
            "publish_message": mock_kafka,
        }


@pytest.mark.asyncio
async def test_chat_start_creates_conversation(_mock_deps, sample_chat_start_event):
    from consumer.handlers.chat_start import handle

    await handle("conv-123", sample_chat_start_event)

    # Should insert into conversations table
    _mock_deps["db_execute"].assert_called_once()
    call_args = _mock_deps["db_execute"].call_args
    assert "INSERT INTO conversations" in call_args[0][0]
    assert "conv-123" in call_args[0][1]


@pytest.mark.asyncio
async def test_chat_start_registers_online(_mock_deps, sample_chat_start_event):
    from consumer.handlers.chat_start import handle

    await handle("conv-123", sample_chat_start_event)

    _mock_deps["chat_online"].assert_called_once_with(
        "conv-123", "user-456", "gateway-test", ""
    )


@pytest.mark.asyncio
async def test_chat_start_notifies_client(_mock_deps, sample_chat_start_event):
    from consumer.handlers.chat_start import handle

    await handle("conv-123", sample_chat_start_event)

    _mock_deps["push_to_client"].assert_called_once()
    payload = _mock_deps["push_to_client"].call_args[0][1]
    assert payload["event_type"] == "chat_started"
    assert payload["conversation_id"] == "conv-123"


@pytest.mark.asyncio
async def test_chat_start_enqueues_persistence(_mock_deps, sample_chat_start_event):
    from consumer.handlers.chat_start import handle

    await handle("conv-123", sample_chat_start_event)

    _mock_deps["publish_message"].assert_called_once()
    call_kwargs = _mock_deps["publish_message"].call_args[1]
    assert call_kwargs["payload"]["_persist_type"] == "chat_start"
    assert call_kwargs["payload"]["user_id"] == "user-456"
