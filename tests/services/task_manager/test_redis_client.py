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


class TestRedisClientResilience:
    """Timeout + retry policy on the two pools.

    redis-py defaults to zero retries and an implicit 5s client-side ``socket_timeout``, so a
    blocked event loop killed in-flight calls with ``REDIS_UNAVAILABLE`` while Redis was healthy.
    """

    @staticmethod
    def _connections(url: str = "redis://localhost/0") -> tuple:
        client = RedisClient(url)
        return (
            client._client.connection_pool.make_connection(),
            client._blocking_client.connection_pool.make_connection(),
        )

    def test_socket_timeout_is_explicit_on_both_pools(self) -> None:
        """Neither pool may inherit redis-py's implicit 5s default."""
        from digitalkin.models.settings.redis import get_redis_settings

        expected = get_redis_settings().pool.socket_timeout
        default_conn, blocking_conn = self._connections()
        assert default_conn.socket_timeout == pytest.approx(expected)
        assert blocking_conn.socket_timeout == pytest.approx(expected)

    def test_blocking_pool_retries_transient_errors(self) -> None:
        """XREAD is a cursor read with no ack, so re-issuing it is idempotent."""
        _, blocking_conn = self._connections()
        assert blocking_conn.retry._retries == 3
        assert {e.__name__ for e in blocking_conn.retry._supported_errors} == {
            "ConnectionError",
            "TimeoutError",
        }

    def test_non_blocking_pool_does_not_retry(self) -> None:
        """XADD is not idempotent: a retry after an ambiguous failure would duplicate a frame."""
        default_conn, _ = self._connections()
        assert default_conn.retry._retries == 0

    def test_socket_timeout_exceeds_the_xread_block_window(self) -> None:
        """A socket timeout at or below the XREAD block time would fire on every idle stream."""
        from digitalkin.models.settings.gateway import get_gateway_settings
        from digitalkin.models.settings.redis import get_redis_settings

        block_s = get_gateway_settings().stream.stream_read_block_ms / 1000
        assert get_redis_settings().pool.socket_timeout > block_s
