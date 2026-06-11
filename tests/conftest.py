"""
Test configuration and shared fixtures for all TitanChat tests.
"""
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def mock_redis():
    """Mock Redis client with pipeline support."""
    client = AsyncMock()
    pipeline = AsyncMock()
    pipeline.execute = AsyncMock(return_value=[])
    client.pipeline.return_value = pipeline
    return client


@pytest.fixture
def mock_db_execute():
    """Mock Cassandra db_execute."""
    with patch("shared.db.cassandra.db_execute", new_callable=AsyncMock) as mock:
        yield mock


@pytest.fixture
def mock_publish_message():
    """Mock Kafka publish_message."""
    with patch("gateway.kafka_producer.publish_message", new_callable=AsyncMock) as mock:
        yield mock


@pytest.fixture
def mock_push_to_client():
    """Mock gateway push."""
    with patch("consumer.gateway_client.push_to_client", new_callable=AsyncMock) as mock:
        mock.return_value = True
        yield mock


@pytest.fixture
def sample_chat_start_event():
    """Sample chat_start event payload."""
    return {
        "event_type": "chat_start",
        "conversation_id": "conv-123",
        "user_id": "user-456",
        "timestamp": 1748275200,
    }


@pytest.fixture
def sample_incoming_message_event():
    """Sample incoming_message event payload."""
    return {
        "event_type": "incoming_message",
        "conversation_id": "conv-123",
        "message_id": "msg-789",
        "user_id": "user-456",
        "message": "Hello, I need help with billing",
        "timestamp": 1748275200,
        "seq": 1,
    }


@pytest.fixture
def sample_outgoing_message_event():
    """Sample outgoing_message event payload."""
    return {
        "event_type": "outgoing_message",
        "conversation_id": "conv-123",
        "message_id": "msg-out-001",
        "agent_id": "agent-1",
        "message": "Hi! How can I help you?",
        "timestamp": 1748275200,
        "seq": 2,
    }


@pytest.fixture
def sample_chat_end_event():
    """Sample chat_end event payload."""
    return {
        "event_type": "chat_end",
        "conversation_id": "conv-123",
        "timestamp": 1748275300,
    }


@pytest.fixture
def sample_ack_event():
    """Sample ack event payload."""
    return {
        "event_type": "ack",
        "conversation_id": "conv-123",
        "message_id": "msg-789",
        "ack_type": "delivered",
        "from_user_id": "user-456",
        "timestamp": 1748275200,
    }


@pytest.fixture
def sample_transfer_event():
    """Sample transfer_to_agent event payload."""
    return {
        "event_type": "transfer_to_agent",
        "conversation_id": "conv-123",
        "reason": "Customer wants to talk to human",
        "timestamp": 1748275200,
    }
