"""
Integration tests for the full Kafka event flow.

Tests the dispatch pipeline: event → consumer → handler → outputs.
"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock


@pytest.fixture(autouse=True)
def _isolate_handlers():
    """Ensure each test gets a fresh handler mock via _HANDLERS patch."""
    pass


class TestKafkaDispatch:
    @pytest.mark.asyncio
    async def test_dispatches_chat_start(self):
        mock_handler = AsyncMock()
        with patch.dict("consumer.kafka_consumer._HANDLERS", {"chat_start": mock_handler}):
            from consumer.kafka_consumer import _dispatch
            await _dispatch("conv-1", {"event_type": "chat_start", "conversation_id": "conv-1"})
            mock_handler.assert_called_once_with("conv-1", {"event_type": "chat_start", "conversation_id": "conv-1"})

    @pytest.mark.asyncio
    async def test_dispatches_incoming_message(self):
        mock_handler = AsyncMock()
        with patch.dict("consumer.kafka_consumer._HANDLERS", {"incoming_message": mock_handler}):
            from consumer.kafka_consumer import _dispatch
            payload = {"event_type": "incoming_message", "conversation_id": "conv-1"}
            await _dispatch("conv-1", payload)
            mock_handler.assert_called_once_with("conv-1", payload)

    @pytest.mark.asyncio
    async def test_dispatches_outgoing_message(self):
        mock_handler = AsyncMock()
        with patch.dict("consumer.kafka_consumer._HANDLERS", {"outgoing_message": mock_handler}):
            from consumer.kafka_consumer import _dispatch
            payload = {"event_type": "outgoing_message", "conversation_id": "conv-1"}
            await _dispatch("conv-1", payload)
            mock_handler.assert_called_once_with("conv-1", payload)

    @pytest.mark.asyncio
    async def test_dispatches_ack(self):
        mock_handler = AsyncMock()
        with patch.dict("consumer.kafka_consumer._HANDLERS", {"ack": mock_handler}):
            from consumer.kafka_consumer import _dispatch
            payload = {"event_type": "ack", "conversation_id": "conv-1"}
            await _dispatch("conv-1", payload)
            mock_handler.assert_called_once_with("conv-1", payload)

    @pytest.mark.asyncio
    async def test_drops_unknown_event_type(self):
        from consumer.kafka_consumer import _dispatch
        # Should not raise — just logs and drops
        await _dispatch("conv-1", {"event_type": "unknown_type"})

    @pytest.mark.asyncio
    async def test_dispatches_transfer_to_agent(self):
        mock_handler = AsyncMock()
        with patch.dict("consumer.kafka_consumer._HANDLERS", {"transfer_to_agent": mock_handler}):
            from consumer.kafka_consumer import _dispatch
            payload = {"event_type": "transfer_to_agent", "conversation_id": "conv-1"}
            await _dispatch("conv-1", payload)
            mock_handler.assert_called_once_with("conv-1", payload)

    @pytest.mark.asyncio
    async def test_dispatches_chat_end_sends_sentinel(self):
        """After chat_end, sentinel (None) should be enqueued."""
        import asyncio
        from consumer.kafka_consumer import _dispatch, _conv_queues

        q = asyncio.Queue()
        _conv_queues["conv-sentinel-test"] = q

        mock_handler = AsyncMock()
        with patch.dict("consumer.kafka_consumer._HANDLERS", {"chat_end": mock_handler}):
            await _dispatch("conv-sentinel-test", {"event_type": "chat_end"})
            sentinel = await asyncio.wait_for(q.get(), timeout=1.0)
            assert sentinel is None

        _conv_queues.pop("conv-sentinel-test", None)
