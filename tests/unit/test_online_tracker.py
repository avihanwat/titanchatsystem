"""Unit tests for shared.cache.online_chat_tracker"""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


@pytest.fixture
def mock_redis():
    client = AsyncMock()
    pipeline = MagicMock()
    pipeline.execute = AsyncMock(return_value=[])
    client.pipeline = MagicMock(return_value=pipeline)
    return client, pipeline


class TestChatOnline:
    @pytest.mark.asyncio
    async def test_registers_all_keys(self, mock_redis):
        client, pipeline = mock_redis

        with patch("shared.cache.online_chat_tracker._get_client", return_value=client):
            from shared.cache.online_chat_tracker import chat_online

            await chat_online("conv-1", "user-1", "gw-1")

            # Should add to set, sorted set, user set, and hash
            assert pipeline.sadd.call_count == 2  # online:chats + online:chats:user:*
            pipeline.zadd.assert_called_once()
            pipeline.hset.assert_called_once()
            pipeline.execute.assert_called_once()


class TestChatOffline:
    @pytest.mark.asyncio
    async def test_removes_all_keys(self, mock_redis):
        client, pipeline = mock_redis
        client.hgetall.return_value = {
            "user_id": "user-1",
            "agent_id": "agent-1",
            "status": "active",
        }

        with patch("shared.cache.online_chat_tracker._get_client", return_value=client):
            from shared.cache.online_chat_tracker import chat_offline

            await chat_offline("conv-1")

            # Should remove from set, zset, hash, user set, agent set
            assert pipeline.srem.call_count == 3
            pipeline.zrem.assert_called_once()
            pipeline.delete.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_op_if_not_tracked(self, mock_redis):
        client, pipeline = mock_redis
        client.hgetall.return_value = {}

        with patch("shared.cache.online_chat_tracker._get_client", return_value=client):
            from shared.cache.online_chat_tracker import chat_offline

            await chat_offline("conv-nonexistent")

            pipeline.execute.assert_not_called()


class TestChatAssigned:
    @pytest.mark.asyncio
    async def test_updates_hash_and_agent_set(self, mock_redis):
        client, pipeline = mock_redis

        with patch("shared.cache.online_chat_tracker._get_client", return_value=client):
            from shared.cache.online_chat_tracker import chat_assigned

            await chat_assigned("conv-1", "agent-5")

            pipeline.hset.assert_called_once()
            pipeline.sadd.assert_called_once()


class TestGetOnlineCount:
    @pytest.mark.asyncio
    async def test_returns_scard(self, mock_redis):
        client, _ = mock_redis
        client.scard.return_value = 42

        with patch("shared.cache.online_chat_tracker._get_client", return_value=client):
            from shared.cache.online_chat_tracker import get_online_count

            count = await get_online_count()
            assert count == 42
            client.scard.assert_called_once_with("online:chats")


class TestGetAllOnlineChats:
    @pytest.mark.asyncio
    async def test_returns_paginated_results(self, mock_redis):
        client, pipeline = mock_redis
        client.zrevrange.return_value = ["conv-1", "conv-2"]
        pipeline.execute.return_value = [
            {"user_id": "u1", "status": "active"},
            {"user_id": "u2", "status": "queued"},
        ]

        with patch("shared.cache.online_chat_tracker._get_client", return_value=client):
            from shared.cache.online_chat_tracker import get_all_online_chats

            chats = await get_all_online_chats(offset=0, limit=10)

            assert len(chats) == 2
            assert chats[0]["conversation_id"] == "conv-1"
            assert chats[1]["status"] == "queued"

    @pytest.mark.asyncio
    async def test_empty_when_no_chats(self, mock_redis):
        client, _ = mock_redis
        client.zrevrange.return_value = []

        with patch("shared.cache.online_chat_tracker._get_client", return_value=client):
            from shared.cache.online_chat_tracker import get_all_online_chats

            chats = await get_all_online_chats()
            assert chats == []
