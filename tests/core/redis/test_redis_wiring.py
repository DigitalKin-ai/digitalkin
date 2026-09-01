"""Tests for Redis wiring into core components.

Covers:
- TaskSession.status property → RedisStateManager fire-and-forget write
- RedisClient.verify() health check
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

pytestmark = [pytest.mark.timeout(15)]

SKIP_NO_FAKEREDIS = pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed")


# ===========================================================================
# TaskSession.status → RedisStateManager
# ===========================================================================


class TestTaskSessionStatusWiring:
    """TaskSession.set_status awaits the RedisStateManager write."""

    async def test_set_status_awaits_state_manager(self) -> None:
        """Calling set_status() triggers a Redis write inline (no task spawn)."""
        from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy

        state_mgr = MagicMock()
        state_mgr.set_status = AsyncMock()

        module = Mock()
        module.context = Mock()
        module.context.task_manager = Mock(spec=TaskManagerStrategy)
        module.context.session = Mock()
        module.context.session.setup_id = "s:1"
        module.context.session.setup_version_id = "sv:1"
        module.context.session.current_ids = Mock(return_value={})
        module.context.cleanup = AsyncMock()
        module.stop = AsyncMock()

        from digitalkin.core.task_manager.task_session import TaskSession

        session = TaskSession("t1", "missions:m1", module, state_manager=state_mgr)

        await session.set_status("running")

        state_mgr.set_status.assert_awaited_with("t1", "running")
        assert session.status == "running"

    async def test_set_status_without_state_manager(self) -> None:
        """Calling set_status() without state_manager works (in-memory only)."""
        from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy

        module = Mock()
        module.context = Mock()
        module.context.task_manager = Mock(spec=TaskManagerStrategy)
        module.context.session = Mock()
        module.context.session.setup_id = "s:1"
        module.context.session.setup_version_id = "sv:1"
        module.context.session.current_ids = Mock(return_value={})

        from digitalkin.core.task_manager.task_session import TaskSession

        session = TaskSession("t2", "missions:m1", module)

        await session.set_status("running")
        assert session.status == "running"


# ===========================================================================
# RedisClient.verify
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestRedisClientVerify:
    """RedisClient.verify() health check."""

    async def test_verify_succeeds_on_healthy_redis(self) -> None:
        from digitalkin.core.task_manager.redis.redis_client import RedisClient

        client = RedisClient("redis://localhost:6379/15")
        client._client = fakeredis_aio.FakeRedis()
        client._blocking_client = fakeredis_aio.FakeRedis()
        result = await client.verify()
        assert result is True
        await client.close()

    async def test_verify_fails_on_unreachable(self) -> None:
        from unittest.mock import AsyncMock, patch

        from digitalkin.core.task_manager.redis.redis_client import RedisClient

        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_client = AsyncMock()
            mock_client.ping = AsyncMock(side_effect=ConnectionError("down"))
            mock_from_url.return_value = mock_client
            client = RedisClient("redis://nonexistent:9999/0")
            result = await client.verify()
            assert result is False
            await client.close()


class TestRedisClientHealthCheckInterval:
    """RedisClient must pass ``health_check_interval`` to both pools."""

    async def test_default_health_check_interval_15(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from unittest.mock import patch

        from digitalkin.core.task_manager.redis.redis_client import RedisClient
        from digitalkin.models.settings.redis import get_redis_settings

        monkeypatch.delenv("DIGITALKIN_REDIS_HEALTH_CHECK_INTERVAL", raising=False)
        get_redis_settings.cache_clear()

        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_from_url.return_value = AsyncMock()
            RedisClient("redis://localhost:6379/15")
            kwargs_calls = [call.kwargs for call in mock_from_url.call_args_list]
            assert len(kwargs_calls) == 2
            for kwargs in kwargs_calls:
                assert kwargs.get("health_check_interval") == 15

    async def test_env_override_flows_to_both_pools(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from unittest.mock import patch

        from digitalkin.core.task_manager.redis.redis_client import RedisClient
        from digitalkin.models.settings.redis import get_redis_settings

        monkeypatch.setenv("DIGITALKIN_REDIS_HEALTH_CHECK_INTERVAL", "30")
        get_redis_settings.cache_clear()

        with patch("redis.asyncio.Redis.from_url") as mock_from_url:
            mock_from_url.return_value = AsyncMock()
            RedisClient("redis://localhost:6379/15")
            kwargs_calls = [call.kwargs for call in mock_from_url.call_args_list]
            assert len(kwargs_calls) == 2
            for kwargs in kwargs_calls:
                assert kwargs.get("health_check_interval") == 30
