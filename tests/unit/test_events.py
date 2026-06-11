"""Unit tests for models.events — Pydantic event validation."""
import pytest
from pydantic import ValidationError

from models.events import (
    ChatStartEvent,
    IncomingMessageEvent,
    OutgoingMessageEvent,
    TransferToAgentEvent,
    ChatEndEvent,
    AckEvent,
)


class TestChatStartEvent:
    def test_valid_event(self):
        event = ChatStartEvent(
            event_type="chat_start",
            conversation_id="conv-1",
            user_id="user-1",
            timestamp=1000,
        )
        assert event.conversation_id == "conv-1"

    def test_missing_user_id(self):
        with pytest.raises(ValidationError):
            ChatStartEvent(
                event_type="chat_start",
                conversation_id="conv-1",
                timestamp=1000,
            )

    def test_wrong_event_type(self):
        with pytest.raises(ValidationError):
            ChatStartEvent(
                event_type="wrong_type",
                conversation_id="conv-1",
                user_id="user-1",
                timestamp=1000,
            )


class TestIncomingMessageEvent:
    def test_valid_event(self):
        event = IncomingMessageEvent(
            event_type="incoming_message",
            conversation_id="conv-1",
            message_id="msg-1",
            user_id="user-1",
            message="Hello",
            timestamp=1000,
        )
        assert event.seq == 0  # default

    def test_user_id_defaults_empty(self):
        event = IncomingMessageEvent(
            event_type="incoming_message",
            conversation_id="conv-1",
            message_id="msg-1",
            message="Hello",
            timestamp=1000,
        )
        assert event.user_id == ""

    def test_missing_message(self):
        with pytest.raises(ValidationError):
            IncomingMessageEvent(
                event_type="incoming_message",
                conversation_id="conv-1",
                message_id="msg-1",
                timestamp=1000,
            )


class TestOutgoingMessageEvent:
    def test_valid_event(self):
        event = OutgoingMessageEvent(
            event_type="outgoing_message",
            conversation_id="conv-1",
            message_id="msg-1",
            agent_id="agent-1",
            message="Hi there!",
            timestamp=1000,
        )
        assert event.agent_id == "agent-1"

    def test_missing_agent_id(self):
        with pytest.raises(ValidationError):
            OutgoingMessageEvent(
                event_type="outgoing_message",
                conversation_id="conv-1",
                message_id="msg-1",
                message="Hi",
                timestamp=1000,
            )


class TestAckEvent:
    def test_delivered_ack(self):
        event = AckEvent(
            event_type="ack",
            conversation_id="conv-1",
            message_id="msg-1",
            ack_type="delivered",
            timestamp=1000,
        )
        assert event.ack_type == "delivered"

    def test_read_ack(self):
        event = AckEvent(
            event_type="ack",
            conversation_id="conv-1",
            message_id="msg-1",
            ack_type="read",
            from_user_id="user-1",
            timestamp=1000,
        )
        assert event.from_user_id == "user-1"

    def test_invalid_ack_type(self):
        with pytest.raises(ValidationError):
            AckEvent(
                event_type="ack",
                conversation_id="conv-1",
                message_id="msg-1",
                ack_type="invalid",
                timestamp=1000,
            )


class TestTransferToAgentEvent:
    def test_valid_event(self):
        event = TransferToAgentEvent(
            event_type="transfer_to_agent",
            conversation_id="conv-1",
            timestamp=1000,
        )
        assert event.reason == ""  # default

    def test_with_reason(self):
        event = TransferToAgentEvent(
            event_type="transfer_to_agent",
            conversation_id="conv-1",
            reason="Need human help",
            timestamp=1000,
        )
        assert event.reason == "Need human help"


class TestChatEndEvent:
    def test_valid_event(self):
        event = ChatEndEvent(
            event_type="chat_end",
            conversation_id="conv-1",
            timestamp=1000,
        )
        assert event.conversation_id == "conv-1"
