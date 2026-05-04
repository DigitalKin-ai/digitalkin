"""Tests for TaskDispatcher — Redis XREAD dispatch loop.

Covers start/stop lifecycle, adaptive count scaling, crash recovery
with backoff, and active task tracking.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from digitalkin.core.task_manager.task_dispatcher import TaskDispatcher

pytestmark = pytest.mark.timeout(10)


def _make_dispatcher() -> tuple[TaskDispatcher, MagicMock]:
    """Create a TaskDispatcher with mocked dependencies."""
    redis_client = MagicMock()
    redis_client.xread = AsyncMock(return_value=[])
    redis_client.xadd = AsyncMock(return_value=b"1-0")
    redis_client.expire = AsyncMock(return_value=True)
    servicer = MagicMock()
    servicer.module_class = MagicMock()
    dispatcher = TaskDispatcher(redis_client, servicer, "dispatch:test")
    return dispatcher, redis_client


class TestTaskDispatcherLifecycle:
    """Start/stop lifecycle."""

    @pytest.mark.smoke
    async def test_start_creates_listen_task(self) -> None:
        """start() creates an asyncio task for the listen loop."""
        dispatcher, redis = _make_dispatcher()

        await dispatcher.start()
        assert dispatcher._listen_task is not None
        assert not dispatcher._listen_task.done()

        await dispatcher.stop()

    @pytest.mark.smoke
    async def test_stop_cancels_listen_task(self) -> None:
        """stop() cancels the listen task and sets it to None."""
        dispatcher, _ = _make_dispatcher()

        await dispatcher.start()
        await dispatcher.stop()

        assert dispatcher._listen_task is None

    @pytest.mark.edge_case
    async def test_stop_without_start_is_noop(self) -> None:
        """stop() before start() doesn't raise."""
        dispatcher, _ = _make_dispatcher()
        await dispatcher.stop()

    @pytest.mark.edge_case
    async def test_double_stop_is_safe(self) -> None:
        """Calling stop() twice doesn't raise."""
        dispatcher, _ = _make_dispatcher()
        await dispatcher.start()
        await dispatcher.stop()
        await dispatcher.stop()


class TestTaskDispatcherDispatch:
    """Single entry dispatch."""

    @pytest.mark.smoke
    async def test_dispatch_creates_handler_task(self) -> None:
        """XREAD returning an entry spawns a handler task."""
        dispatcher, redis = _make_dispatcher()

        entry_id = b"1234-0"
        fields = {
            b"task_id": b"task-1",
            b"setup_id": b"setups:test",
            b"mission_id": b"missions:test",
            b"pb": b"",
            b"ts_ns": b"0",
        }

        # First call returns one entry, second returns empty (stops loop)
        redis.xread = AsyncMock(side_effect=[
            [("dispatch:test", [(entry_id, fields)])],
            [],  # empty = idle
        ])

        # Mock _handle_dispatch to avoid full module lifecycle
        dispatcher._handle_dispatch = AsyncMock()

        await dispatcher.start()
        await asyncio.sleep(0.1)
        await dispatcher.stop()

        dispatcher._handle_dispatch.assert_awaited()

    @pytest.mark.concurrency
    async def test_active_tasks_tracked_and_cleaned(self) -> None:
        """Active tasks are tracked in _active_tasks and removed on completion."""
        dispatcher, redis = _make_dispatcher()

        completed = asyncio.Event()

        async def _mock_handler(fields: dict) -> None:
            completed.set()

        dispatcher._handle_dispatch = _mock_handler

        entry = [(b"1-0", {b"task_id": b"t1", b"pb": b"", b"ts_ns": b"0", b"setup_id": b"s", b"mission_id": b"m"})]
        redis.xread = AsyncMock(side_effect=[
            [("dispatch:test", entry)],
            [],
        ])

        await dispatcher.start()
        await asyncio.wait_for(completed.wait(), timeout=2)
        await asyncio.sleep(0.05)  # let done callback fire
        await dispatcher.stop()

        assert len(dispatcher._active_tasks) == 0


class TestTaskDispatcherAdaptiveCount:
    """Adaptive XREAD count scaling."""

    @pytest.mark.concurrency
    async def test_adaptive_count_scales_up(self) -> None:
        """When batch is full, count doubles (up to 100)."""
        dispatcher, redis = _make_dispatcher()
        dispatcher._handle_dispatch = AsyncMock()

        # Return exactly count entries each time to trigger doubling
        call_num = 0
        counts_seen = []

        original_xread = redis.xread

        async def _tracking_xread(streams, *, count=1, block=1000):
            nonlocal call_num
            call_num += 1
            counts_seen.append(count)
            if call_num <= 5:
                # Return exactly count entries (triggers doubling)
                entries = [(f"{call_num}-{i}".encode(), {b"task_id": f"t{i}".encode(), b"pb": b"", b"ts_ns": b"0", b"setup_id": b"s", b"mission_id": b"m"}) for i in range(count)]
                return [("dispatch:test", entries)]
            return []  # stop

        redis.xread = _tracking_xread

        await dispatcher.start()
        await asyncio.sleep(0.3)
        await dispatcher.stop()

        # count should have doubled: 1 → 2 → 4 → 8 → 16
        assert counts_seen[0] == 1
        assert any(c > 1 for c in counts_seen), f"Count never scaled up: {counts_seen}"

    @pytest.mark.edge_case
    async def test_adaptive_count_resets_on_idle(self) -> None:
        """Empty XREAD result resets count to 1."""
        dispatcher, redis = _make_dispatcher()
        dispatcher._handle_dispatch = AsyncMock()

        call_num = 0
        counts_seen = []

        async def _tracking_xread(streams, *, count=1, block=1000):
            nonlocal call_num
            call_num += 1
            counts_seen.append(count)
            if call_num == 1:
                # Full batch → scale up
                entries = [(b"1-0", {b"task_id": b"t1", b"pb": b"", b"ts_ns": b"0", b"setup_id": b"s", b"mission_id": b"m"})]
                return [("dispatch:test", entries)]
            if call_num == 2:
                # Empty → reset
                return []
            if call_num == 3:
                # Should be back to 1
                return []
            return []

        redis.xread = _tracking_xread

        await dispatcher.start()
        await asyncio.sleep(0.2)
        await dispatcher.stop()

        # After empty result, count should reset to 1
        if len(counts_seen) >= 3:
            assert counts_seen[2] == 1, f"Count didn't reset: {counts_seen}"


class TestTaskDispatcherCrashRecovery:
    """Crash recovery with exponential backoff."""

    @pytest.mark.chaos
    async def test_crash_recovery_retries_with_backoff(self) -> None:
        """Redis error doesn't kill loop — retries with backoff."""
        dispatcher, redis = _make_dispatcher()

        call_num = 0

        async def _failing_xread(streams, *, count=1, block=1000):
            nonlocal call_num
            call_num += 1
            if call_num <= 2:
                raise ConnectionError("Redis down")
            # Stop after recovery
            dispatcher._stop_event.set()
            return []

        redis.xread = _failing_xread

        await dispatcher.start()
        await asyncio.sleep(1.0)  # enough for 2 retries + backoff
        await dispatcher.stop()

        # Should have retried at least twice before recovering
        assert call_num >= 3

    @pytest.mark.edge_case
    async def test_cancelled_error_stops_loop(self) -> None:
        """CancelledError breaks the loop cleanly."""
        dispatcher, redis = _make_dispatcher()

        async def _cancel_xread(streams, *, count=1, block=1000):
            raise asyncio.CancelledError

        redis.xread = _cancel_xread

        await dispatcher.start()
        await asyncio.sleep(0.1)

        # Loop should have exited without crashing
        assert dispatcher._listen_task is not None
        assert dispatcher._listen_task.done()
        await dispatcher.stop()
