"""Quick test: send a message to 'incoming-message' topic."""
import asyncio
import uuid
import time
import orjson
from aiokafka import AIOKafkaProducer
from config.settings import KAFKA_BOOTSTRAP_SERVERS


async def send():
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: orjson.dumps(v),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer.start()

    payload = {
        "message_id": str(uuid.uuid4()),
        "conversation_id": "test-conv-001",
        "message": "Hello from test producer!",
        "timestamp": int(time.time()),
    }

    record = await producer.send_and_wait(
        "incoming-message", key="test-conv-001", value=payload
    )
    print(f"Sent to incoming-message: partition={record.partition} offset={record.offset}")
    print(f"Payload: {payload}")

    await producer.stop()


if __name__ == "__main__":
    asyncio.run(send())
