"""
WebSocket test script -- persistent connection, type messages interactively.
Usage:
    python test_websocket.py
Type a message and press Enter to send. Ctrl+C to quit.
"""

import asyncio
import json
import websockets


WS_URL = "ws://34.69.100.253:8000/ws/conv-123"


async def test():
    print("Connecting to %s ..." % WS_URL)
    async with websockets.connect(WS_URL) as ws:
        print("Connected. Type a message and press Enter. Ctrl+C to quit.\n")

        while True:
            # Read input without blocking the event loop
            text = await asyncio.get_event_loop().run_in_executor(
                None, input, "You: "
            )
            text = text.strip()
            if not text:
                continue

            await ws.send(json.dumps({"message": text}))

            response = await ws.recv()
            data = json.loads(response)

            if data.get("status") == "accepted":
                print("[OK] message_id=%s\n" % data.get("message_id"))
            else:
                print("[RESPONSE] %s\n" % data)


if __name__ == "__main__":
    try:
        asyncio.run(test())
    except KeyboardInterrupt:
        print("\nDisconnected.")
