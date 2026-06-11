"""
Standalone entrypoint for the Consumer service.
Run this on the Consumer VM — no FastAPI/uvicorn needed.

    python -m consumer.main
"""
import asyncio
import logging
import sys

from consumer.kafka_consumer import start_consumer, stop_consumer
from consumer.persistence_worker import start_persistence_worker, stop_persistence_worker
from consumer.gateway_client import shutdown_http_client
from gateway.kafka_producer import start_producer, stop_producer
from shared.db.cassandra import shutdown as cassandra_shutdown
from shared.observability.logging import setup_logging
from shared.observability.telemetry import setup_tracer

setup_logging()
setup_tracer("titan-consumer")
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("Starting TitanChat Consumer service...")
    await start_producer()
    await start_consumer()
    await start_persistence_worker()

    stop_event = asyncio.Event()

    if sys.platform != "win32":
        import signal

        loop = asyncio.get_running_loop()

        def _handle_signal():
            logger.info("Shutdown signal received.")
            stop_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, _handle_signal)

        await stop_event.wait()
    else:
        # On Windows, signal handlers don't work with asyncio.
        # Just run forever; Ctrl+C will raise KeyboardInterrupt.
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            pass

    logger.info("Stopping consumer...")
    await stop_consumer()
    await stop_persistence_worker()
    await stop_producer()
    await shutdown_http_client()
    cassandra_shutdown()
    logger.info("Consumer service stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
