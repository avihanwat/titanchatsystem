"""Diagnose Kafka broker advertised listeners."""
import asyncio
import sys
from aiokafka import AIOKafkaConsumer
from config.settings import KAFKA_BOOTSTRAP_SERVERS


async def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...", flush=True)
    consumer = AIOKafkaConsumer(
        "incoming-message",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        group_id="test-debug-group",
        request_timeout_ms=10000,
        metadata_max_age_ms=5000,
    )
    try:
        await consumer.start()
        print("Connected!", flush=True)

        # Print broker metadata
        cluster = consumer._client.cluster
        print(f"Controller: {cluster.controller}", flush=True)
        for node_id, broker in cluster._brokers.items():
            print(f"  Broker node_id={node_id}: host={broker.host} port={broker.port}", flush=True)

        await consumer.stop()
        print("Done.", flush=True)
    except Exception as e:
        print(f"ERROR: {e}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
