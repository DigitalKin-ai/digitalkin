"""Tests for RedisClient — init, verify, close."""

from unittest.mock import AsyncMock, patch

import pytest

from digitalkin.core.task_manager.redis.redis_client import RedisClient

pytestmark = [pytest.mark.timeout(10)]


class TestRedisClientLifecycle:
    """Init / close lifecycle."""

    async def test_init_creates_two_pools(self) -> None:
        """Init creates both default and blocking pools."""
        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_from_url.return_value = AsyncMock()
            client = RedisClient("redis://localhost/0", pool_size=100)
            assert mock_from_url.call_count == 2
            await client.close()

    async def test_init_uses_env_fallback(self) -> None:
        """Empty URL falls back to DIGITALKIN_REDIS_URL env."""
        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_from_url.return_value = AsyncMock()
            client = RedisClient("")
            assert client.url
            await client.close()

    async def test_close_closes_both_pools(self) -> None:
        """Close calls aclose on both pools."""
        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_client = AsyncMock()
            mock_from_url.return_value = mock_client
            client = RedisClient("redis://localhost/0")
            await client.close()
            assert mock_client.aclose.call_count == 2


class TestRedisClientVerify:
    """Health check."""

    async def test_verify_success(self) -> None:
        """Verify returns True when ping succeeds."""
        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_client = AsyncMock()
            mock_client.ping = AsyncMock(return_value=True)
            mock_from_url.return_value = mock_client
            client = RedisClient("redis://localhost/0")
            assert await client.verify() is True
            await client.close()

    async def test_verify_failure(self) -> None:
        """Verify returns False when ping fails."""
        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_client = AsyncMock()
            mock_client.ping = AsyncMock(side_effect=ConnectionError("down"))
            mock_from_url.return_value = mock_client
            client = RedisClient("redis://localhost/0")
            assert await client.verify() is False
            await client.close()
