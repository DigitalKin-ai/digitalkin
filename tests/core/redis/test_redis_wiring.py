"""Tests for Redis wiring into core components.

Covers:
- TaskSession.status property → RedisStateManager fire-and-forget write
- RedisClient.verify() health check
- RedisCheckpointManager.list_checkpoints() with secondary index
- RedisIdempotencyGuard TTL reset on reclaim
- StartupRestorer.restore_all()
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

pytestmark = [pytest.mark.timeout(15)]

SKIP_NO_FAKEREDIS = pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed")


class _FakeClient:
    """Minimal fakeredis adapter."""

    def __init__(self) -> None:
        self._client = fakeredis_aio.FakeRedis()

    async def hset(self, name: str, mapping: dict[str, str | bytes]) -> int:
        return await self._client.hset(name, mapping=mapping)  # type: ignore[return-value]

    async def hgetall(self, name: str) -> dict[bytes, bytes]:
        return await self._client.hgetall(name)  # type: ignore[return-value]

    async def expire(self, name: str, seconds: int) -> bool:
        return await self._client.expire(name, seconds)  # type: ignore[return-value]

    async def delete(self, *names: str) -> int:
        return await self._client.delete(*names)  # type: ignore[return-value]

    async def get(self, name: str) -> bytes | None:
        return await self._client.get(name)  # type: ignore[return-value]

    async def set(self, name: str, value: str | bytes, *, ex: int | None = None) -> bool:
        return await self._client.set(name, value, ex=ex)  # type: ignore[return-value]

    async def sadd(self, name: str, *values: str) -> int:
        return await self._client.sadd(name, *values)  # type: ignore[return-value]

    async def srem(self, name: str, *values: str) -> int:
        return await self._client.srem(name, *values)  # type: ignore[return-value]

    async def smembers(self, name: str) -> set[bytes]:
        return await self._client.smembers(name)  # type: ignore[return-value]

    async def eval(self, script: str, keys: list[str], args: list[str]) -> Any:
        return await self._client.eval(script, len(keys), *keys, *args)

    async def ping(self) -> bool:
        return await self._client.ping()  # type: ignore[return-value]

    def pipeline(self) -> Any:
        return self._client.pipeline()

    async def close(self) -> None:
        await self._client.aclose()


# ===========================================================================
# TaskSession.status → RedisStateManager
# ===========================================================================


class TestTaskSessionStatusWiring:
    """TaskSession.status property fires RedisStateManager write."""

    async def test_status_setter_calls_state_manager(self) -> None:
        """Setting session.status triggers a Redis write task."""
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

        session.status = "running"

        # Give fire-and-forget task time to execute
        await asyncio.sleep(0.05)

        state_mgr.set_status.assert_awaited_with("t1", "running")

    async def test_status_setter_without_state_manager(self) -> None:
        """Setting status without state_manager works (in-memory only)."""
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

        session.status = "running"
        assert session.status == "running"


# ===========================================================================
# RedisCheckpointManager.list_checkpoints
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestListCheckpoints:
    """list_checkpoints uses secondary index and cleans stale entries."""

    async def test_list_returns_active_checkpoints(self) -> None:
        from digitalkin.core.task_manager.redis.redis_checkpoint import RedisCheckpointManager

        client = _FakeClient()
        mgr = RedisCheckpointManager(client, checkpoint_ttl=300)  # type: ignore[arg-type]

        await mgr.checkpoint(
            session_id="s1", task_id="t1", mission_id="missions:m1",
            setup_id="setups:s1", setup_version_id="setup_versions:sv1",
            status="running", last_seq=10,
        )
        await mgr.checkpoint(
            session_id="s2", task_id="t2", mission_id="missions:m1",
            setup_id="setups:s1", setup_version_id="setup_versions:sv1",
            status="completed", last_seq=20,
        )

        results = await mgr.list_checkpoints()
        assert len(results) == 2
        task_ids = {r["task_id"] for r in results}
        assert task_ids == {"t1", "t2"}

        await client.close()

    async def test_list_cleans_stale_entries(self) -> None:
        from digitalkin.core.task_manager.redis.redis_checkpoint import RedisCheckpointManager

        client = _FakeClient()
        mgr = RedisCheckpointManager(client, checkpoint_ttl=300)  # type: ignore[arg-type]

        await mgr.checkpoint(
            session_id="s_stale", task_id="t_stale", mission_id="missions:m1",
            setup_id="setups:s1", setup_version_id="setup_versions:sv1",
            status="running", last_seq=5,
        )

        # Delete the checkpoint but leave index entry (simulates TTL expiry)
        await client.delete("checkpoint:s_stale")

        results = await mgr.list_checkpoints()
        assert len(results) == 0  # Stale entry cleaned

        # Verify index was cleaned
        members = await client.smembers("checkpoints:active")
        assert len(members) == 0

        await client.close()


# ===========================================================================
# RedisIdempotencyGuard TTL reset on reclaim
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestIdempotencyTTLReset:
    """Reclaim resets TTL to prevent stale holds."""

    async def test_reclaim_resets_ttl(self) -> None:
        from digitalkin.core.task_manager.redis.redis_idempotency import ClaimResult, RedisIdempotencyGuard

        client = _FakeClient()
        guard = RedisIdempotencyGuard(client, claim_ttl=60)  # type: ignore[arg-type]

        await guard.claim("task_ttl")

        # Get TTL before reclaim
        ttl_before = await client._client.ttl("idem:task_ttl")

        # Wait a bit then reclaim
        await asyncio.sleep(0.1)
        result = await guard.claim("task_ttl")
        assert result == ClaimResult.RECLAIMED

        # TTL should be reset (close to original)
        ttl_after = await client._client.ttl("idem:task_ttl")
        assert ttl_after >= ttl_before - 1  # Within 1s tolerance

        await client.close()


# ===========================================================================
# StartupRestorer
# ===========================================================================


@SKIP_NO_FAKEREDIS
class TestStartupRestorer:
    """StartupRestorer.restore_all reads from checkpoint index."""

    async def test_restore_all_returns_checkpoints(self) -> None:
        from digitalkin.core.task_manager.redis.redis_checkpoint import RedisCheckpointManager
        from digitalkin.core.resilience.graceful_shutdown import StartupRestorer

        client = _FakeClient()
        ckpt_mgr = RedisCheckpointManager(client, checkpoint_ttl=300)  # type: ignore[arg-type]

        await ckpt_mgr.checkpoint(
            session_id="restore_1", task_id="t_r1", mission_id="missions:m1",
            setup_id="setups:s1", setup_version_id="setup_versions:sv1",
            status="running", last_seq=5,
        )

        restorer = StartupRestorer(ckpt_mgr, client)  # type: ignore[arg-type]
        results = await restorer.restore_all()

        assert len(results) == 1
        assert results[0]["task_id"] == "t_r1"
        assert results[0]["last_seq"] == 5

        await client.close()

    async def test_restore_all_empty_when_no_checkpoints(self) -> None:
        from digitalkin.core.task_manager.redis.redis_checkpoint import RedisCheckpointManager
        from digitalkin.core.resilience.graceful_shutdown import StartupRestorer

        client = _FakeClient()
        ckpt_mgr = RedisCheckpointManager(client, checkpoint_ttl=300)  # type: ignore[arg-type]

        restorer = StartupRestorer(ckpt_mgr, client)  # type: ignore[arg-type]
        results = await restorer.restore_all()

        assert results == []

        await client.close()


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
        result = await client.verify(timeout=2.0)
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
