"""Unit tests for consumer.handlers.chat_end"""
import pytest
from unittest.mock import AsyncMock, patch


@pytest.fixture
def _mock_deps():
    with (
        patch("consumer.handlers.chat_end.release_agent_slot", new_callable=AsyncMock) as mock_release,
        patch("consumer.handlers.chat_end.chat_offline", new_callable=AsyncMock) as mock_tracker,
        patch("consumer.handlers.chat_end.push_to_client", new_callable=AsyncMock) as mock_push,
        patch("consumer.handlers.chat_end.publish_message", new_callable=AsyncMock) as mock_kafka,
    ):
        yield {
            "release_agent_slot": mock_release,
            "chat_offline": mock_tracker,
            "push_to_client": mock_push,
            "publish_message": mock_kafka,
        }


@pytest.mark.asyncio
async def test_chat_end_releases_agent_slot(_mock_deps, sample_chat_end_event):
    from consumer.handlers.chat_end import handle

    await handle("conv-123", sample_chat_end_event)

    _mock_deps["release_agent_slot"].assert_called_once_with("conv-123")


@pytest.mark.asyncio
async def test_chat_end_removes_from_tracker(_mock_deps, sample_chat_end_event):
    from consumer.handlers.chat_end import handle

    await handle("conv-123", sample_chat_end_event)

    _mock_deps["chat_offline"].assert_called_once_with("conv-123")


@pytest.mark.asyncio
async def test_chat_end_notifies_client(_mock_deps, sample_chat_end_event):
    from consumer.handlers.chat_end import handle

    await handle("conv-123", sample_chat_end_event)

    _mock_deps["push_to_client"].assert_called_once()
    payload = _mock_deps["push_to_client"].call_args[0][1]
    assert payload["event_type"] == "chat_ended"


@pytest.mark.asyncio
async def test_chat_end_enqueues_persistence(_mock_deps, sample_chat_end_event):
    from consumer.handlers.chat_end import handle

    await handle("conv-123", sample_chat_end_event)

    _mock_deps["publish_message"].assert_called_once()
    call_kwargs = _mock_deps["publish_message"].call_args[1]
    assert call_kwargs["payload"]["_persist_type"] == "chat_end"
    assert call_kwargs["payload"]["conversation_id"] == "conv-123"
