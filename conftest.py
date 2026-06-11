"""Ensure the project root is on sys.path for all test files."""
import sys
import os
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(__file__))

# Pre-mock the cassandra driver which fails to import on Python 3.12
# due to removed asyncore module. Tests never need a real Cassandra connection.
_cassandra_mock = MagicMock()
_cassandra_mock.cluster.Cluster = MagicMock
_cassandra_mock.cluster.Session = MagicMock
_cassandra_mock.query.PreparedStatement = MagicMock
_cassandra_mock.policies.DCAwareRoundRobinPolicy = MagicMock
_cassandra_mock.auth.PlainTextAuthProvider = MagicMock

sys.modules.setdefault("cassandra", _cassandra_mock)
sys.modules.setdefault("cassandra.cluster", _cassandra_mock.cluster)
sys.modules.setdefault("cassandra.query", _cassandra_mock.query)
sys.modules.setdefault("cassandra.policies", _cassandra_mock.policies)
sys.modules.setdefault("cassandra.auth", _cassandra_mock.auth)
sys.modules.setdefault("cassandra.io", MagicMock())
sys.modules.setdefault("cassandra.io.asyncioreactor", MagicMock())

# Pre-import subpackages so that mock.patch can resolve dotted paths
import shared.db  # noqa: E402, F401
import shared.cache  # noqa: E402, F401
import shared.auth  # noqa: E402, F401
import consumer.handlers  # noqa: E402, F401
