"""Unit tests for shared.db.cassandra — Cassandra client."""
import pytest
from unittest.mock import MagicMock, patch, AsyncMock


class TestGetSession:
    def test_creates_session_on_first_call(self):
        mock_cluster_class = MagicMock()
        mock_cluster = MagicMock()
        mock_session = MagicMock()
        mock_cluster.connect.return_value = mock_session
        mock_cluster_class.return_value = mock_cluster

        with (
            patch("shared.db.cassandra.Cluster", mock_cluster_class),
            patch("shared.db.cassandra._cluster", None),
            patch("shared.db.cassandra._session", None),
        ):
            from shared.db.cassandra import get_session

            # Reset module state
            import shared.db.cassandra as mod
            mod._cluster = None
            mod._session = None

            session = get_session()
            assert session == mock_session
            mock_cluster_class.assert_called_once()

    def test_reuses_existing_session(self):
        mock_session = MagicMock()

        import shared.db.cassandra as mod
        original_session = mod._session
        mod._session = mock_session

        try:
            session = mod.get_session()
            assert session == mock_session
        finally:
            mod._session = original_session


class TestShutdown:
    def test_closes_cluster(self):
        mock_cluster = MagicMock()

        import shared.db.cassandra as mod
        original = (mod._cluster, mod._session, mod._prepared)
        mod._cluster = mock_cluster
        mod._session = MagicMock()
        mod._prepared = {"query": MagicMock()}

        try:
            mod.shutdown()
            mock_cluster.shutdown.assert_called_once()
            assert mod._cluster is None
            assert mod._session is None
            assert mod._prepared == {}
        finally:
            mod._cluster, mod._session, mod._prepared = original

    def test_no_op_when_not_connected(self):
        import shared.db.cassandra as mod
        original = mod._cluster
        mod._cluster = None

        try:
            mod.shutdown()  # Should not raise
        finally:
            mod._cluster = original


class TestPreparedStatementCaching:
    def test_caches_prepared_statements(self):
        mock_session = MagicMock()
        mock_prepared = MagicMock()
        mock_session.prepare.return_value = mock_prepared

        import shared.db.cassandra as mod
        original = (mod._session, mod._prepared)
        mod._session = mock_session
        mod._prepared = {}

        try:
            result = mod._get_prepared("SELECT * FROM users")
            assert result == mock_prepared
            mock_session.prepare.assert_called_once_with("SELECT * FROM users")

            # Second call should use cache
            result2 = mod._get_prepared("SELECT * FROM users")
            assert result2 == mock_prepared
            assert mock_session.prepare.call_count == 1  # not called again
        finally:
            mod._session, mod._prepared = original
