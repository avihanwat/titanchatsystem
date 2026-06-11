"""
End-to-end test: Verify Kafka Producer and Consumer work correctly.

This test:
1. Produces a message to 'incoming-message' topic (simulating gateway producer)
2. Consumes it back to verify delivery
3. Produces a message to 'outgoing-messages' topic (simulating AI backend)
4. Verifies the consumer can read it

Usage:
    python test_producer_consumer.py
"""

import asyncio
import time
import uuid
import orjson
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer, TopicPartition
from config.settings import KAFKA_BOOTSTRAP_SERVERS

BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS


async def test_producer():
    """Test: Produce a message to 'incoming-message' topic."""
    print("=" * 60, flush=True)
    print("[TEST 1] Kafka Producer — publish to 'incoming-message'", flush=True)
    print("=" * 60, flush=True)

    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: orjson.dumps(v),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer.start()
    print("  Producer connected.", flush=True)

    test_message_id = str(uuid.uuid4())
    payload = {
        "message_id": test_message_id,
        "conversation_id": "test-conv-001",
        "message": "Hello from producer test!",
        "timestamp": int(time.time()),
    }

    record = await producer.send_and_wait(
        "incoming-message",
        key="test-conv-001",
        value=payload,
    )
    print(f"  [OK] Message produced!", flush=True)
    print(f"     topic={record.topic} partition={record.partition} offset={record.offset}", flush=True)
    print(f"     message_id={test_message_id}", flush=True)

    await producer.stop()
    return test_message_id


async def test_consumer_reads_incoming(expected_message_id: str):
    """Test: Consume from 'incoming-message' and find our test message."""
    print("\n" + "=" * 60, flush=True)
    print("[TEST 2] Kafka Consumer — read from 'incoming-message'", flush=True)
    print("=" * 60, flush=True)

    consumer = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
    )
    await consumer.start()

    partitions = consumer.partitions_for_topic("incoming-message")
    tps = [TopicPartition("incoming-message", p) for p in partitions]
    consumer.assign(tps)

    # Seek to end minus 5 (read last 5 messages)
    for tp in tps:
        end_offset = await consumer.end_offsets([tp])
        offset = max(0, end_offset[tp] - 5)
        consumer.seek(tp, offset)

    print("  Reading last 5 messages...", flush=True)
    found = False
    data = await consumer.getmany(*tps, timeout_ms=5000)
    for tp, messages in data.items():
        for msg in messages:
            try:
                value = orjson.loads(msg.value) if msg.value else {}
            except Exception:
                # Skip non-JSON messages (e.g. raw text "hi")
                continue
            if value.get("message_id") == expected_message_id:
                found = True
                print(f"  [OK] Found our test message!", flush=True)
                print(f"     key={msg.key.decode() if msg.key else '(null)'}", flush=True)
                print(f"     payload={value}", flush=True)

    if not found:
        print(f"  [FAIL] Test message not found in last 5 messages.", flush=True)

    await consumer.stop()
    return found


async def test_outgoing_messages():
    """Test: Produce to 'outgoing-messages' and consume it (simulating AI backend → consumer)."""
    print("\n" + "=" * 60, flush=True)
    print("[TEST 3] Produce to 'outgoing-messages' + consume it", flush=True)
    print("         (simulates AI backend sending response)", flush=True)
    print("=" * 60, flush=True)

    # Produce
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: orjson.dumps(v),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await producer.start()

    test_id = str(uuid.uuid4())
    response_payload = {
        "message_id": test_id,
        "conversation_id": "test-conv-001",
        "response": "This is the AI response!",
        "timestamp": int(time.time()),
    }

    record = await producer.send_and_wait(
        "outgoing-messages",
        key="test-conv-001",
        value=response_payload,
    )
    print(f"  Produced to outgoing-messages: offset={record.offset}", flush=True)
    await producer.stop()

    # Consume
    consumer = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",
    )
    await consumer.start()

    partitions = consumer.partitions_for_topic("outgoing-messages")
    if not partitions:
        print("  [FAIL] No partitions for 'outgoing-messages'", flush=True)
        await consumer.stop()
        return False

    tps = [TopicPartition("outgoing-messages", p) for p in partitions]
    consumer.assign(tps)

    # Seek to end minus 3
    for tp in tps:
        end_offset = await consumer.end_offsets([tp])
        offset = max(0, end_offset[tp] - 3)
        consumer.seek(tp, offset)

    found = False
    data = await consumer.getmany(*tps, timeout_ms=5000)
    for tp, messages in data.items():
        for msg in messages:
            value = orjson.loads(msg.value) if msg.value else {}
            if value.get("message_id") == test_id:
                found = True
                print(f"  [OK] Consumer read the outgoing message!", flush=True)
                print(f"     key={msg.key.decode() if msg.key else '(null)'}", flush=True)
                print(f"     payload={value}", flush=True)

    if not found:
        print(f"  [FAIL] Outgoing message not found.", flush=True)

    await consumer.stop()
    return found


async def main():
    print("\n--- TitanChat Producer/Consumer Integration Test ---\n", flush=True)

    # Test 1: Producer
    message_id = await test_producer()

    # Test 2: Consumer reads what producer wrote
    incoming_ok = await test_consumer_reads_incoming(message_id)

    # Test 3: Outgoing messages (AI backend → consumer flow)
    outgoing_ok = await test_outgoing_messages()

    # Summary
    print("\n" + "=" * 60, flush=True)
    print("RESULTS:", flush=True)
    print("=" * 60, flush=True)
    print(f"  [OK] Producer -> Kafka 'incoming-message'    : PASS", flush=True)
    print(f"  [{'OK' if incoming_ok else 'FAIL'}] Consumer <- Kafka 'incoming-message'    : {'PASS' if incoming_ok else 'FAIL'}", flush=True)
    print(f"  [{'OK' if outgoing_ok else 'FAIL'}] Producer -> Consumer 'outgoing-messages': {'PASS' if outgoing_ok else 'FAIL'}", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
