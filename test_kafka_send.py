"""Test: send 'foosball hara' from user 'yask' to Kafka and read it back."""
import asyncio
import orjson
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

BROKER = "34.69.100.253:9092"
TOPIC = "incoming-message"


async def main():
    # Produce
    producer = AIOKafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: orjson.dumps(v),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer.start()
    print(f"Producer connected to {BROKER}")

    payload = {
        "event_type": "incoming_message",
        "conversation_id": "conv-test-001",
        "message_id": "msg-test-001",
        "user_id": "yask",
        "message": "foosball hara",
        "timestamp": 1748275200,
        "seq": 1,
    }

    record = await producer.send_and_wait(TOPIC, key="conv-test-001", value=payload)
    print(f"Sent to partition={record.partition} offset={record.offset}")
    print(f"Payload: {payload}")
    await producer.stop()

    # Consume
    consumer = AIOKafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        group_id="test-verify-group",
        auto_offset_reset="latest",
        value_deserializer=lambda v: orjson.loads(v),
    )
    await consumer.start()

    # Send again so consumer picks it up (subscribed after first send)
    producer2 = AIOKafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: orjson.dumps(v),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer2.start()
    await producer2.send_and_wait(TOPIC, key="conv-test-001", value=payload)
    await producer2.stop()

    try:
        msg = await asyncio.wait_for(consumer.getone(), timeout=10.0)
        key = msg.key.decode() if msg.key else ""
        print(f"Received: key={key} value={msg.value}")
        print("Kafka is working!")
    except asyncio.TimeoutError:
        print("Timeout - check broker connectivity")
    await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
