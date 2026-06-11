CHAT_INBOUND      = "chat-inbound"       # all inbound events from WebSocket connector
OUTGOING_MESSAGES = "outgoing-messages"  # agent/AI → user messages
CHAT_ACKS         = "chat-acks"          # delivered / read acknowledgements
CHAT_PERSIST      = "chat-persist"       # async DB writes (decoupled from hot path)
