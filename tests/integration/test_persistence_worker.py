"""
Integration tests for the persistence worker.

These test the full flow: event payload → persistence_worker dispatch → DB writes.
Requires mocked Cassandra (no live DB needed).
"""
import pytest
from unittest.mock import AsyncMock, patch


@pytest.fixture
def mock_db():
    with patch("consumer.persistence_worker.db_execute", new_callable=AsyncMock) as mock:
        yield mock


class TestPersistMessage:
    @pytest.mark.asyncio
    async def test_inserts_message_and_updates_conversation(self, mock_db):
        from consumer.persistence_worker import _persist_message

        payload = {
            "conversation_id": "conv-1",
            "message_id": "msg-1",
            "sender_id": "user-1",
            "sender_type": "user",
            "content_type": "text",
            "message": "Hello world",
            "seq": 1,
            "timestamp": 1748275200,
            "unread_target": "agent",
        }

        await _persist_message(payload)

        # 3 DB calls: insert message, update conversation, increment counter
        assert mock_db.call_count == 3

        # Verify message insert
        first_call = mock_db.call_args_list[0]
        assert "INSERT INTO messages" in first_call[0][0]

        # Verify conversation update
        second_call = mock_db.call_args_list[1]
        assert "UPDATE conversations" in second_call[0][0]

        # Verify counter increment
        third_call = mock_db.call_args_list[2]
        assert "unread_counters" in third_call[0][0]


class TestPersistChatStart:
    @pytest.mark.asyncio
    async def test_creates_inbox_and_queue(self, mock_db):
        from consumer.persistence_worker import _persist_chat_start

        payload = {
            "conversation_id": "conv-1",
            "user_id": "user-1",
            "timestamp": 1748275200,
        }

        await _persist_chat_start(payload)

        assert mock_db.call_count == 2
        calls = [c[0][0] for c in mock_db.call_args_list]
        assert any("conversations_by_user" in c for c in calls)
        assert any("agent_queue" in c for c in calls)


class TestPersistChatEnd:
    @pytest.mark.asyncio
    async def test_closes_conversation(self, mock_db):
        from consumer.persistence_worker import _persist_chat_end

        payload = {
            "conversation_id": "conv-1",
            "timestamp": 1748275300,
        }

        await _persist_chat_end(payload)

        mock_db.assert_called_once()
        assert "UPDATE conversations" in mock_db.call_args[0][0]
        assert "ended" in mock_db.call_args[0][1]


class TestPersistAck:
    @pytest.mark.asyncio
    async def test_writes_ack_receipt(self, mock_db):
        from consumer.persistence_worker import _persist_ack

        payload = {
            "conversation_id": "conv-1",
            "message_id": "msg-1",
            "ack_type": "delivered",
            "from_user_id": "user-1",
            "timestamp": 1748275200,
        }

        await _persist_ack(payload)

        mock_db.assert_called_once()
        assert "message_acks" in mock_db.call_args[0][0]

    @pytest.mark.asyncio
    async def test_read_ack_decrements_counter(self, mock_db):
        from consumer.persistence_worker import _persist_ack

        payload = {
            "conversation_id": "conv-1",
            "message_id": "msg-1",
            "ack_type": "read",
            "from_user_id": "user-1",
            "timestamp": 1748275200,
        }

        await _persist_ack(payload)

        assert mock_db.call_count == 2
        second_call = mock_db.call_args_list[1]
        assert "count - 1" in second_call[0][0]


class TestPersistAssignment:
    @pytest.mark.asyncio
    async def test_records_assignment(self, mock_db):
        from consumer.persistence_worker import _persist_assignment

        payload = {
            "conversation_id": "conv-1",
            "agent_id": "agent-5",
            "timestamp": 1748275200,
        }

        await _persist_assignment(payload)

        mock_db.assert_called_once()
        assert "agent_assignments" in mock_db.call_args[0][0]
        assert "agent-5" in mock_db.call_args[0][1]
