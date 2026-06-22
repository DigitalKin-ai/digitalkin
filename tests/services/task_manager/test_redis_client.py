"""Tests for RedisClient — init, verify, close."""

from unittest.mock import AsyncMock, patch

import pytest

from digitalkin.core.task_manager.redis.redis_client import RedisClient

pytestmark = [pytest.mark.timeout(10)]


class TestRedisClientLifecycle:
    """Init / close lifecycle."""

    async def test_init_creates_two_pools(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Init creates both default and blocking pools."""
        monkeypatch.setenv("DIGITALKIN_REDIS_POOL_SIZE", "100")
        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_from_url.return_value = AsyncMock()
            client = RedisClient("redis://localhost/0")
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

    async def test_verify_pings_both_pools(self) -> None:
        """Verify pings ``_client`` AND ``_blocking_client`` so both are warm at boot."""
        default_pool = AsyncMock()
        default_pool.ping = AsyncMock(return_value=True)
        blocking_pool = AsyncMock()
        blocking_pool.ping = AsyncMock(return_value=True)
        with patch("redis.asyncio.Redis.from_url", side_effect=[default_pool, blocking_pool]):
            client = RedisClient("redis://localhost/0")
            assert await client.verify() is True
            assert default_pool.ping.await_count == 1
            assert blocking_pool.ping.await_count == 1
            await client.close()

    async def test_verify_failure_on_blocking_pool_only(self) -> None:
        """Verify returns False if only the blocking pool ping fails."""
        default_pool = AsyncMock()
        default_pool.ping = AsyncMock(return_value=True)
        blocking_pool = AsyncMock()
        blocking_pool.ping = AsyncMock(side_effect=ConnectionError("blocking pool down"))
        with patch("redis.asyncio.Redis.from_url", side_effect=[default_pool, blocking_pool]):
            client = RedisClient("redis://localhost/0")
            assert await client.verify() is False
            await client.close()
