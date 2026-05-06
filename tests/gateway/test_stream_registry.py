"""Unit tests for StreamRegistry.

Covers: capacity enforcement, register/unregister, heartbeat touch,
zombie reaper, shutdown cleanup. Uses a mock RedisClient for fast unit tests.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from digitalkin.grpc_servers.stream_registry import StreamRegistry
from digitalkin.grpc_servers.stream_session import StreamSession

pytestmark = [pytest.mark.timeout(15)]


def _mock_redis() -> MagicMock:
    """Build a MagicMock RedisClient with the methods StreamRegistry uses."""
    mock = MagicMock()
    mock.eval = AsyncMock(return_value=1)
    mock.zadd = AsyncMock()
    mock.zrangebyscore = AsyncMock(return_value=[])
    pipe = MagicMock()
    pipe.decr = MagicMock(return_value=pipe)
    pipe.zrem = MagicMock(return_value=pipe)
    pipe.delete = MagicMock(return_value=pipe)
    pipe.execute = AsyncMock(return_value=[])
    mock.pipeline = MagicMock(return_value=pipe)
    return mock


class TestRegistryCapacity:
    """Capacity enforcement via max_streams."""

    async def test_register_within_capacity(self) -> None:
        reg = StreamRegistry(_mock_redis(), max_streams=5)
        for i in range(5):
            await reg.register(StreamSession(task_id=f"t_{i}"))
        assert reg.active_count == 5

    async def test_register_over_capacity_returns_false(self) -> None:
        redis = _mock_redis()
        redis.eval = AsyncMock(side_effect=[1, 1, 0])
        reg = StreamRegistry(redis, max_streams=2)
        assert await reg.register(StreamSession(task_id="t_0")) is True
        assert await reg.register(StreamSession(task_id="t_1")) is True
        assert await reg.register(StreamSession(task_id="t_overflow")) is False

    async def test_unregister_frees_slot(self) -> None:
        reg = StreamRegistry(_mock_redis(), max_streams=1)
        await reg.register(StreamSession(task_id="t_a"))
        await reg.unregister("t_a")
        await reg.register(StreamSession(task_id="t_b"))
        assert reg.active_count == 1


class TestRegistryLookup:
    """Get and unregister operations."""

    async def test_get_returns_session(self) -> None:
        reg = StreamRegistry(_mock_redis(), max_streams=10)
        s = StreamSession(task_id="t_get")
        await reg.register(s)
        assert reg.get("t_get") is s

    def test_get_unknown_returns_none(self) -> None:
        reg = StreamRegistry(_mock_redis(), max_streams=10)
        assert reg.get("nonexistent") is None

    async def test_unregister_returns_session(self) -> None:
        reg = StreamRegistry(_mock_redis(), max_streams=10)
        s = StreamSession(task_id="t_unreg")
        await reg.register(s)
        removed = await reg.unregister("t_unreg")
        assert removed is s
        assert reg.active_count == 0

    async def test_unregister_unknown_returns_none(self) -> None:
        reg = StreamRegistry(_mock_redis(), max_streams=10)
        result = await reg.unregister("nonexistent")
        assert result is None


class TestRegistryLruEviction:
    """LRU cache eviction when local cache is full."""

    async def test_lru_evicts_oldest(self) -> None:
        reg = StreamRegistry(_mock_redis(), max_streams=100, max_local=3)
        for i in range(4):
            await reg.register(StreamSession(task_id=f"t_{i}"))
        # t_0 should be evicted (oldest)
        assert reg.get("t_0") is None
        assert reg.get("t_3") is not None
        assert reg.active_count == 3


class TestRegistryShutdown:
    """Clean shutdown."""

    async def test_shutdown_tears_down_all_sessions(self) -> None:
        reg = StreamRegistry(_mock_redis(), max_streams=10)
        for i in range(5):
            await reg.register(StreamSession(task_id=f"t_sd_{i}"))

        await reg.shutdown()
        assert reg.active_count == 0

    async def test_shutdown_cancels_reaper(self) -> None:
        """Reaper starts with mock Redis — shutdown cancels it cleanly."""
        reg = StreamRegistry(_mock_redis(), max_streams=10, reaper_interval=0.05)
        await reg.start_reaper()
        assert reg._reaper_task is not None

        await reg.shutdown()
        assert reg._reaper_task.done()


class TestRegistryTaskMonitoring:
    """Reaper supervises fire-and-forget asyncio tasks: refs + exception logging."""

    async def test_monitor_holds_strong_reference(self) -> None:
        """The reaper keeps a strong ref so a fire-and-forget task can't be GC'd."""
        reg = StreamRegistry(_mock_redis(), max_streams=10)
        started = asyncio.Event()
        finish = asyncio.Event()

        async def _worker() -> None:
            started.set()
            await finish.wait()

        task = asyncio.create_task(_worker(), name="worker_holdref")
        reg.monitor_task(task)
        await started.wait()
        assert task in reg._monitored_tasks

        finish.set()
        await task
        # done-callback discards the task on completion
        assert task not in reg._monitored_tasks

    async def test_monitor_logs_unhandled_exception(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A monitored task that raises must produce a logged error, not a silent drop."""
        from digitalkin.grpc_servers import stream_registry as sr_mod

        calls: list[tuple[str, tuple, dict]] = []

        def _capture(msg: str, *args: object, **kwargs: object) -> None:
            calls.append((msg % args if args else msg, args, kwargs))

        monkeypatch.setattr(sr_mod.logger, "error", _capture)

        reg = StreamRegistry(_mock_redis(), max_streams=10)

        async def _boom() -> None:
            raise RuntimeError("kaboom")

        task = asyncio.create_task(_boom(), name="worker_boom")
        reg.monitor_task(task)
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)

        assert any(
            "worker_boom" in msg and "kaboom" in msg for msg, _args, _kw in calls
        ), f"expected error log mentioning task name + exception, got: {[m for m, _, _ in calls]}"
        # done-callback already retrieved the exception → no asyncio warning
        assert task.exception() is not None

    async def test_monitor_silent_on_cancellation(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Cancelled tasks are routine — no error log."""
        from digitalkin.grpc_servers import stream_registry as sr_mod

        calls: list[str] = []
        monkeypatch.setattr(
            sr_mod.logger,
            "error",
            lambda msg, *args, **_kw: calls.append(msg % args if args else msg),
        )

        reg = StreamRegistry(_mock_redis(), max_streams=10)

        async def _wait_forever() -> None:
            await asyncio.Event().wait()

        task = asyncio.create_task(_wait_forever(), name="worker_cancel")
        reg.monitor_task(task)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(0)

        assert not any("worker_cancel" in m for m in calls), (
            f"cancellation should be silent, got: {calls}"
        )

    async def test_shutdown_cancels_monitored_tasks(self) -> None:
        """shutdown() cancels every still-running monitored task."""
        reg = StreamRegistry(_mock_redis(), max_streams=10)
        started = asyncio.Event()

        async def _wait_forever() -> None:
            started.set()
            await asyncio.Event().wait()

        task = asyncio.create_task(_wait_forever(), name="worker_shutdown")
        reg.monitor_task(task)
        await started.wait()

        await reg.shutdown()

        assert task.done()
        assert task.cancelled()
        assert task not in reg._monitored_tasks
