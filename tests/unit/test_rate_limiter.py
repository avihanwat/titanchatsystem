"""Unit tests for shared.cache.rate_limiter"""
import pytest
from unittest.mock import AsyncMock, patch, MagicMock


@pytest.fixture
def mock_redis_client():
    client = AsyncMock()
    pipeline = MagicMock()
    pipeline.execute = AsyncMock(return_value=[5, True])
    client.pipeline = MagicMock(return_value=pipeline)
    return client, pipeline


@pytest.mark.asyncio
async def test_rate_limit_allows_under_limit(mock_redis_client):
    client, pipeline = mock_redis_client
    pipeline.execute.return_value = [5, True]  # count=5, expire set

    with patch("shared.cache.rate_limiter._get_client", return_value=client):
        from shared.cache.rate_limiter import check_rate_limit

        result = await check_rate_limit("user-1")
        assert result is True


@pytest.mark.asyncio
async def test_rate_limit_blocks_over_limit(mock_redis_client):
    client, pipeline = mock_redis_client
    pipeline.execute.return_value = [61, False]  # count=61, over default 60

    with patch("shared.cache.rate_limiter._get_client", return_value=client):
        with patch("shared.cache.rate_limiter.RATE_LIMIT_PER_MINUTE", 60):
            from shared.cache.rate_limiter import check_rate_limit

            result = await check_rate_limit("user-1")
            assert result is False


@pytest.mark.asyncio
async def test_rate_limit_at_exact_limit(mock_redis_client):
    client, pipeline = mock_redis_client
    pipeline.execute.return_value = [60, False]  # count=60, at limit

    with patch("shared.cache.rate_limiter._get_client", return_value=client):
        with patch("shared.cache.rate_limiter.RATE_LIMIT_PER_MINUTE", 60):
            from shared.cache.rate_limiter import check_rate_limit

            result = await check_rate_limit("user-1")
            assert result is True  # at limit, not over
