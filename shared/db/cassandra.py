"""
Cassandra session manager + asyncio-compatible execute helper.

Bridges cassandra-driver's synchronous ResponseFuture into asyncio.Future
so handlers can `await db_execute(query, params)`.

Uses prepared statement caching for performance at scale.
"""
import asyncio
import logging

from cassandra.io.asyncioreactor import AsyncioConnection
from cassandra.cluster import Cluster, Session
from cassandra.query import PreparedStatement
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider

from config.settings import (
    CASSANDRA_HOSTS,
    CASSANDRA_PORT,
    CASSANDRA_KEYSPACE,
    CASSANDRA_USERNAME,
    CASSANDRA_PASSWORD,
)

logger = logging.getLogger(__name__)

_cluster: Cluster | None = None
_session: Session | None = None
_prepared: dict[str, PreparedStatement] = {}


def get_session() -> Session:
    global _cluster, _session
    if _session is None:
        auth = (
            PlainTextAuthProvider(CASSANDRA_USERNAME, CASSANDRA_PASSWORD)
            if CASSANDRA_USERNAME
            else None
        )
        _cluster = Cluster(
            contact_points=CASSANDRA_HOSTS,
            port=CASSANDRA_PORT,
            load_balancing_policy=DCAwareRoundRobinPolicy(),
            auth_provider=auth,
            protocol_version=4,
            connection_class=AsyncioConnection,
        )
        _session = _cluster.connect(CASSANDRA_KEYSPACE)
        logger.info(
            "Cassandra connected to %s keyspace=%s",
            CASSANDRA_HOSTS,
            CASSANDRA_KEYSPACE,
        )
    return _session


def _get_prepared(query: str) -> PreparedStatement:
    """Return a cached PreparedStatement, creating it on first use."""
    if query not in _prepared:
        session = get_session()
        _prepared[query] = session.prepare(query)
    return _prepared[query]


async def db_execute(query: str, params: list | tuple | None = None):
    """Run a Cassandra query asynchronously inside asyncio with prepared statements."""
    loop = asyncio.get_event_loop()
    fut: asyncio.Future = loop.create_future()

    def on_success(result):
        loop.call_soon_threadsafe(fut.set_result, result)

    def on_error(exc):
        loop.call_soon_threadsafe(fut.set_exception, exc)

    session = get_session()
    prepared = _get_prepared(query)
    response = session.execute_async(prepared, params or [])
    response.add_callbacks(on_success, on_error)
    return await fut


def shutdown():
    """Close Cassandra cluster connection."""
    global _cluster, _session, _prepared
    if _cluster:
        _cluster.shutdown()
        _cluster = None
        _session = None
        _prepared = {}
        logger.info("Cassandra connection closed")
