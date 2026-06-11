import logging

import orjson
from aiokafka import AIOKafkaProducer

from config.settings import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)

_producer: AIOKafkaProducer | None = None


async def start_producer() -> None:
    global _producer
    servers: str = KAFKA_BOOTSTRAP_SERVERS
    logger.info("Starting Kafka producer", extra={"stage": "producer_start"})
    _producer = AIOKafkaProducer(
        bootstrap_servers=servers,
        value_serializer=lambda v: orjson.dumps(v),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await _producer.start()
    logger.info("Kafka producer started", extra={"stage": "producer_ready"})


async def stop_producer() -> None:
    global _producer
    if _producer is not None:
        await _producer.stop()
        _producer = None
        logger.info("Kafka producer stopped", extra={"stage": "producer_stopped"})


async def publish_message(
    topic: str,
    key: str,
    payload: dict,
    headers: dict[str, str] | None = None,
) -> None:
    """
    Publish payload to a Kafka topic.

    headers — optional key/value pairs carried as Kafka message headers
              (e.g. trace_id, event_type). These travel alongside the
              message without polluting the payload schema.
    """
    if _producer is None:
        raise RuntimeError("Kafka producer is not running.")

    kafka_headers = [(k, v.encode("utf-8")) for k, v in (headers or {}).items()]

    record_metadata = await _producer.send_and_wait(
        topic,
        key=key,
        value=payload,
        headers=kafka_headers,
    )

    logger.info(
        "kafka_delivered",
        extra={
            "stage":     "kafka_ack",
            "topic":     record_metadata.topic,
            "partition": record_metadata.partition,
            "offset":    record_metadata.offset,
        },
    )
