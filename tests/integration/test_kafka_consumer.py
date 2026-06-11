"""
Read messages from Kafka topic to confirm delivery.
Uses no consumer group (assign mode) to avoid rebalancing delays.
Usage:
    python test_kafka_consumer.py
"""

import asyncio
from aiokafka import AIOKafkaConsumer, TopicPartition
from config.settings import KAFKA_BOOTSTRAP_SERVERS

BOOTSTRAP_SERVERS = KAFKA_BOOTSTRAP_SERVERS
TOPIC = "incoming-message"


async def consume():
    consumer = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
    )
    await consumer.start()
    print("Connected to Kafka broker.", flush=True)

    # Get all partitions for the topic
    partitions = consumer.partitions_for_topic(TOPIC)
    if not partitions:
        print("No partitions found for topic: %s" % TOPIC, flush=True)
        await consumer.stop()
        return

    print("Partitions for %s: %s" % (TOPIC, partitions), flush=True)

    # Manually assign partitions (no group coordinator needed)
    tps = [TopicPartition(TOPIC, p) for p in partitions]
    consumer.assign(tps)

    # Seek to beginning
    await consumer.seek_to_beginning(*tps)
    print("Reading all messages from beginning...\n", flush=True)

    count = 0
    try:
        # Use getmany with timeout instead of infinite async for loop
        while True:
            data = await consumer.getmany(*tps, timeout_ms=5000)
            if not data:
                # No messages received within timeout — done
                print("(No more messages within 5s timeout)", flush=True)
                break
            for tp, messages in data.items():
                for msg in messages:
                    count += 1
                    value = msg.value.decode("utf-8") if msg.value else "(null)"
                    key = msg.key.decode("utf-8") if msg.key else "(null)"
                    print(
                        "[msg #%d] partition=%d  offset=%d  key=%s"
                        % (count, msg.partition, msg.offset, key),
                        flush=True,
                    )
                    print("  value=%s\n" % value, flush=True)
    except Exception as e:
        print("Stopped: %s" % e, flush=True)
    finally:
        await consumer.stop()
        print("Done. Total messages read: %d" % count, flush=True)


if __name__ == "__main__":
    asyncio.run(consume())
