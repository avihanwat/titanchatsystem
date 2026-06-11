"""Unit tests for gateway.websocket_manager"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock


@pytest.fixture
def ws_manager():
    """Fresh ConnectionManager instance."""
    from gateway.websocket_manager import ConnectionManager
    return ConnectionManager()


class TestConnect:
    @pytest.mark.asyncio
    async def test_accepts_and_registers(self, ws_manager):
        ws = AsyncMock()

        with (
            patch("gateway.websocket_manager.register_connection", new_callable=AsyncMock) as mock_reg,
            patch("gateway.websocket_manager.SERVER_ID", "gw-1"),
        ):
            await ws_manager.connect("conv-1", ws)

            ws.accept.assert_called_once()
            mock_reg.assert_called_once_with("conv-1", "gw-1")
            assert "conv-1" in ws_manager.connections
            assert "conv-1" in ws_manager.last_seen


class TestDisconnect:
    @pytest.mark.asyncio
    async def test_removes_and_unregisters(self, ws_manager):
        ws = AsyncMock()
        ws_manager.connections["conv-1"] = ws
        ws_manager.last_seen["conv-1"] = 1000.0
        ws_manager.send_locks["conv-1"] = AsyncMock()

        with (
            patch("gateway.websocket_manager.unregister_connection", new_callable=AsyncMock) as mock_unreg,
            patch("gateway.websocket_manager.chat_offline", new_callable=AsyncMock) as mock_offline,
        ):
            await ws_manager.disconnect("conv-1")

            mock_unreg.assert_called_once_with("conv-1")
            mock_offline.assert_called_once_with("conv-1")
            ws.close.assert_called_once()
            assert "conv-1" not in ws_manager.connections

    @pytest.mark.asyncio
    async def test_handles_nonexistent_conversation(self, ws_manager):
        with (
            patch("gateway.websocket_manager.unregister_connection", new_callable=AsyncMock),
            patch("gateway.websocket_manager.chat_offline", new_callable=AsyncMock),
        ):
            await ws_manager.disconnect("conv-nonexistent")  # Should not raise


class TestSendMessage:
    @pytest.mark.asyncio
    async def test_sends_json(self, ws_manager):
        ws = AsyncMock()
        ws_manager.connections["conv-1"] = ws
        ws_manager.send_locks["conv-1"] = AsyncMock()
        ws_manager.send_locks["conv-1"].__aenter__ = AsyncMock()
        ws_manager.send_locks["conv-1"].__aexit__ = AsyncMock(return_value=False)

        import asyncio
        ws_manager.send_locks["conv-1"] = asyncio.Lock()

        result = await ws_manager.send_message("conv-1", {"msg": "hello"})

        assert result is True
        ws.send_json.assert_called_once_with({"msg": "hello"})

    @pytest.mark.asyncio
    async def test_returns_false_for_unknown_conversation(self, ws_manager):
        result = await ws_manager.send_message("conv-unknown", {"msg": "hello"})
        assert result is False
