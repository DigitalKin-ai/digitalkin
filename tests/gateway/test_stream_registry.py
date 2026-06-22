"""Unit tests for StreamRegistry.

Covers: capacity enforcement, register/unregister, heartbeat touch,
zombie reaper, shutdown cleanup. Uses a mock RedisClient for fast unit tests.
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from digitalkin.grpc_servers.stream_registry import StreamRegistry
from digitalkin.grpc_servers.stream_session import StreamSession
from digitalkin.models.settings.gateway import get_gateway_settings

pytestmark = [pytest.mark.timeout(15)]


def _mock_redis() -> MagicMock:
    """Placeholder RedisClient — StreamRegistry no longer touches Redis."""
    return MagicMock()


class TestRegistryCapacity:
    """Capacity enforcement via max_streams."""

    async def test_register_within_capacity(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "5")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())
        for i in range(5):
            await reg.register(StreamSession(task_id=f"t_{i}"))
        assert reg.active_count == 5

    async def test_register_over_capacity_returns_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Capacity is now enforced process-locally from len(_local_cache)
        # against max_streams — no Redis Lua call.
        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "2")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())
        assert await reg.register(StreamSession(task_id="t_0")) is True
        assert await reg.register(StreamSession(task_id="t_1")) is True
        assert await reg.register(StreamSession(task_id="t_overflow")) is False

    async def test_unregister_frees_slot(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "1")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())
        await reg.register(StreamSession(task_id="t_a"))
        await reg.unregister("t_a")
        await reg.register(StreamSession(task_id="t_b"))
        assert reg.active_count == 1


class TestRegistryLookup:
    """Get and unregister operations."""

    async def test_get_returns_session(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "10")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())
        s = StreamSession(task_id="t_get")
        await reg.register(s)
        assert reg.get("t_get") is s

    def test_get_unknown_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "10")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())
        assert reg.get("nonexistent") is None

    async def test_unregister_returns_session(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "10")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())
        s = StreamSession(task_id="t_unreg")
        await reg.register(s)
        removed = await reg.unregister("t_unreg")
        assert removed is s
        assert reg.active_count == 0

    async def test_unregister_unknown_returns_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "10")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())
        result = await reg.unregister("nonexistent")
        assert result is None


class TestRegistryCapacity:
    """H3: registry rejects at max_streams instead of evicting a live session."""

    async def test_rejects_at_capacity_keeps_live_sessions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "3")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())
        for i in range(3):
            assert await reg.register(StreamSession(task_id=f"t_{i}")) is True
        # 4th registration is rejected; no live session is evicted.
        assert await reg.register(StreamSession(task_id="t_3")) is False
        assert reg.get("t_0") is not None
        assert reg.get("t_3") is None
        assert reg.active_count == 3


class TestRegistryShutdown:
    """Clean shutdown."""

    async def test_shutdown_tears_down_all_sessions(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "10")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())
        for i in range(5):
            await reg.register(StreamSession(task_id=f"t_sd_{i}"))

        await reg.shutdown()
        assert reg.active_count == 0


class TestRegistryTaskMonitoring:
    """Reaper supervises fire-and-forget asyncio tasks: refs + exception logging."""

    async def test_monitor_holds_strong_reference(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The reaper keeps a strong ref so a fire-and-forget task can't be GC'd."""
        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "10")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())
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

        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "10")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())

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

        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "10")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())

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

    async def test_shutdown_cancels_monitored_tasks(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """shutdown() cancels every still-running monitored task."""
        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "10")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())
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

    async def test_dial_done_callback_reaps_local_zombie(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A dial_consumer task that finishes without unregistering is reaped."""
        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "10")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())
        task_id = "zombie_task"
        session = StreamSession(task_id=task_id)
        await reg.register(session)
        assert reg.get(task_id) is session

        async def _dial_finishes_without_unregister() -> None:
            return None

        dial_task = asyncio.create_task(
            _dial_finishes_without_unregister(),
            name=f"dial_consumer_{task_id}",
        )
        reg.monitor_task(dial_task)
        await dial_task

        # Done-callback schedules _reap_local. Yield to let it run.
        for _ in range(20):
            if reg.get(task_id) is None:
                break
            await asyncio.sleep(0.01)
        assert reg.get(task_id) is None, "local zombie was not reaped"

    async def test_dial_done_callback_skips_when_finally_unregistered(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """If the dial-back's finally already unregistered, the callback is a no-op."""
        monkeypatch.setenv("DIGITALKIN_GATEWAY_MAX_STREAMS", "10")
        get_gateway_settings.cache_clear()
        reg = StreamRegistry(_mock_redis())
        task_id = "clean_task"
        session = StreamSession(task_id=task_id)
        await reg.register(session)

        async def _dial_with_unregister() -> None:
            await reg.unregister(task_id)

        dial_task = asyncio.create_task(
            _dial_with_unregister(),
            name=f"dial_consumer_{task_id}",
        )
        reg.monitor_task(dial_task)
        await dial_task
        await asyncio.sleep(0.01)
        # _reap_local should have been a no-op (session already gone).
        assert reg.get(task_id) is None
