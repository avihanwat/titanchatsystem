"""Unit tests for shared.cache.agent_router"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.fixture
def mock_redis():
    client = AsyncMock()
    pipeline = MagicMock()
    pipeline.execute = AsyncMock(return_value=[])
    client.pipeline = MagicMock(return_value=pipeline)
    return client, pipeline


class TestRouteToAgent:
    @pytest.mark.asyncio
    async def test_assigns_least_loaded_agent(self, mock_redis):
        client, pipeline = mock_redis
        # Lua script returns the agent_id string on success
        client.eval.return_value = "agent-2"

        with patch("shared.cache.agent_router._get_client", return_value=client):
            from shared.cache.agent_router import route_to_agent

            result = await route_to_agent("conv-1")

            assert result == "agent-2"
            client.eval.assert_called_once()
            client.publish.assert_called_once()  # admin feed notification

    @pytest.mark.asyncio
    async def test_returns_none_when_no_agents(self, mock_redis):
        client, _ = mock_redis
        # Lua script returns None when no agent available
        client.eval.return_value = None

        with patch("shared.cache.agent_router._get_client", return_value=client):
            from shared.cache.agent_router import route_to_agent

            result = await route_to_agent("conv-1")
            assert result is None
            client.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_offline_agents(self, mock_redis):
        client, pipeline = mock_redis
        # Lua script internally skips offline agents and returns next available
        client.eval.return_value = "agent-2"

        with patch("shared.cache.agent_router._get_client", return_value=client):
            from shared.cache.agent_router import route_to_agent

            result = await route_to_agent("conv-1")
            assert result == "agent-2"

    @pytest.mark.asyncio
    async def test_skips_full_capacity_agents(self, mock_redis):
        client, pipeline = mock_redis
        # All agents full — Lua returns None
        client.eval.return_value = None

        with patch("shared.cache.agent_router._get_client", return_value=client):
            from shared.cache.agent_router import route_to_agent

            result = await route_to_agent("conv-1")
            assert result is None


class TestReleaseAgentSlot:
    @pytest.mark.asyncio
    async def test_decrements_and_cleans_up(self, mock_redis):
        client, pipeline = mock_redis
        client.hget.return_value = "agent-5"

        with patch("shared.cache.agent_router._get_client", return_value=client):
            from shared.cache.agent_router import release_agent_slot

            await release_agent_slot("conv-1")

            pipeline.zincrby.assert_called_once_with("agent:available", -1, "agent-5")
            pipeline.srem.assert_called_once()
            pipeline.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_op_when_no_agent_assigned(self, mock_redis):
        client, pipeline = mock_redis
        client.hget.return_value = None

        with patch("shared.cache.agent_router._get_client", return_value=client):
            from shared.cache.agent_router import release_agent_slot

            await release_agent_slot("conv-1")

            pipeline.execute.assert_not_called()


class TestAgentGoOnline:
    @pytest.mark.asyncio
    async def test_registers_agent(self, mock_redis):
        client, pipeline = mock_redis

        with patch("shared.cache.agent_router._get_client", return_value=client):
            from shared.cache.agent_router import agent_go_online

            await agent_go_online("agent-1", max_chats=3, skills=["billing", "tech"])

            pipeline.set.assert_called_once()
            pipeline.zadd.assert_called_once()
            pipeline.hset.assert_called_once()
