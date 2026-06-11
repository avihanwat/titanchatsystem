"""Unit tests for consumer.handlers.transfer_to_agent"""
import pytest
from unittest.mock import AsyncMock, patch


@pytest.fixture
def _mock_deps():
    with (
        patch("consumer.handlers.transfer_to_agent.route_to_agent", new_callable=AsyncMock) as mock_route,
        patch("consumer.handlers.transfer_to_agent.chat_assigned", new_callable=AsyncMock) as mock_tracker,
        patch("consumer.handlers.transfer_to_agent.db_execute", new_callable=AsyncMock) as mock_db,
        patch("consumer.handlers.transfer_to_agent.push_to_client", new_callable=AsyncMock) as mock_push,
        patch("consumer.handlers.transfer_to_agent.publish_message", new_callable=AsyncMock) as mock_kafka,
    ):
        yield {
            "route_to_agent": mock_route,
            "chat_assigned": mock_tracker,
            "db_execute": mock_db,
            "push_to_client": mock_push,
            "publish_message": mock_kafka,
        }


@pytest.mark.asyncio
async def test_transfer_assigns_agent(_mock_deps, sample_transfer_event):
    from consumer.handlers.transfer_to_agent import handle

    _mock_deps["route_to_agent"].return_value = "agent-5"

    await handle("conv-123", sample_transfer_event)

    _mock_deps["route_to_agent"].assert_called_once_with("conv-123")
    _mock_deps["db_execute"].assert_called_once()
    _mock_deps["chat_assigned"].assert_called_once_with("conv-123", "agent-5")


@pytest.mark.asyncio
async def test_transfer_no_agent_available(_mock_deps, sample_transfer_event):
    from consumer.handlers.transfer_to_agent import handle

    _mock_deps["route_to_agent"].return_value = None

    await handle("conv-123", sample_transfer_event)

    # Should NOT update DB or tracker when no agent available
    _mock_deps["db_execute"].assert_not_called()
    _mock_deps["chat_assigned"].assert_not_called()

    # Should still notify client
    _mock_deps["push_to_client"].assert_called_once()
    payload = _mock_deps["push_to_client"].call_args[0][1]
    assert payload["agent_id"] == "waiting"


@pytest.mark.asyncio
async def test_transfer_notifies_client_with_agent(_mock_deps, sample_transfer_event):
    from consumer.handlers.transfer_to_agent import handle

    _mock_deps["route_to_agent"].return_value = "agent-5"

    await handle("conv-123", sample_transfer_event)

    payload = _mock_deps["push_to_client"].call_args[0][1]
    assert payload["event_type"] == "transfer_initiated"
    assert payload["agent_id"] == "agent-5"
    assert payload["reason"] == "Customer wants to talk to human"


@pytest.mark.asyncio
async def test_transfer_enqueues_assignment_persistence(_mock_deps, sample_transfer_event):
    from consumer.handlers.transfer_to_agent import handle

    _mock_deps["route_to_agent"].return_value = "agent-5"

    await handle("conv-123", sample_transfer_event)

    _mock_deps["publish_message"].assert_called_once()
    call_kwargs = _mock_deps["publish_message"].call_args[1]
    assert call_kwargs["payload"]["_persist_type"] == "assignment"
    assert call_kwargs["payload"]["agent_id"] == "agent-5"
