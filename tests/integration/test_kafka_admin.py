"""Check Kafka broker advertised listeners and list topics."""
import asyncio
from aiokafka.admin import AIOKafkaAdminClient
from config.settings import KAFKA_BOOTSTRAP_SERVERS


async def main():
    admin = AIOKafkaAdminClient(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        request_timeout_ms=5000,
    )
    await admin.start()
    topics = await admin.list_topics()
    print("Topics on broker:", topics)
    await admin.close()


asyncio.run(main())
