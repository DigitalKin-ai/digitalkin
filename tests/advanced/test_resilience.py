"""Tests for resilience components.

Covers WatchdogThread, Bulkhead, SessionReaper, and GracefulShutdownHandler.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Generator
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

pytestmark = [pytest.mark.timeout(15)]


# ===========================================================================
# WatchdogThread
# ===========================================================================


class TestWatchdogThread:
    """Event loop stall detection."""

    async def test_starts_and_stops(self) -> None:
        from digitalkin.core.resilience.watchdog import WatchdogThread

        loop = asyncio.get_running_loop()
        wd = WatchdogThread(loop, stall_threshold=5.0, check_interval=0.1)
        wd.start()
        assert wd.is_alive
        wd.stop()
        assert not wd.is_alive

    async def test_healthy_loop_not_killed(self) -> None:
        """A healthy loop (counter increments) is not flagged as stalled."""
        from digitalkin.core.resilience.watchdog import WatchdogThread

        loop = asyncio.get_running_loop()
        wd = WatchdogThread(loop, stall_threshold=1.0, check_interval=0.1)
        wd.start()

        # Let watchdog run for a few ticks — loop is healthy
        await asyncio.sleep(0.5)

        # Counter should have incremented (loop is alive)
        assert wd._counter > 0
        wd.stop()

    async def test_double_start_is_noop(self) -> None:
        from digitalkin.core.resilience.watchdog import WatchdogThread

        loop = asyncio.get_running_loop()
        wd = WatchdogThread(loop, stall_threshold=5.0, check_interval=0.1)
        wd.start()
        thread_1 = wd._thread
        wd.start()
        assert wd._thread is thread_1  # Same thread
        wd.stop()

    async def test_stop_before_start_is_safe(self) -> None:
        from digitalkin.core.resilience.watchdog import WatchdogThread

        loop = asyncio.get_running_loop()
        wd = WatchdogThread(loop)
        wd.stop()  # Should not raise


# ===========================================================================
# Bulkhead
# ===========================================================================


class TestBulkhead:
    """Per-service concurrency limiting."""

    @pytest.fixture(autouse=True)
    def _clear(self) -> Generator[None]:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        Bulkhead._instances.clear()
        yield
        Bulkhead._instances.clear()

    async def test_allows_within_limit(self) -> None:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        bh = Bulkhead.for_service("test_svc", max_concurrent=3)
        async with bh:
            assert bh.active == 1
        assert bh.active == 0

    async def test_concurrent_within_limit(self) -> None:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        bh = Bulkhead.for_service("conc_svc", max_concurrent=5)
        results: list[int] = []

        async def work(i: int) -> None:
            async with bh:
                results.append(i)
                await asyncio.sleep(0.01)

        await asyncio.gather(*[work(i) for i in range(5)])
        assert len(results) == 5

    async def test_raises_when_full(self) -> None:
        from digitalkin.core.exceptions import BulkheadFullError
        from digitalkin.core.resilience.bulkhead import Bulkhead

        bh = Bulkhead.for_service("full_svc", max_concurrent=1, acquire_timeout=0.05)
        barrier = asyncio.Event()

        async def hold_slot() -> None:
            async with bh:
                barrier.set()
                await asyncio.sleep(1.0)

        task = asyncio.create_task(hold_slot())
        await barrier.wait()

        with pytest.raises(BulkheadFullError):
            async with bh:
                pass  # Should not reach here

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    async def test_singleton_per_service(self) -> None:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        a = Bulkhead.for_service("singleton_svc", max_concurrent=10)
        b = Bulkhead.for_service("singleton_svc")
        assert a is b

    async def test_different_services_independent(self) -> None:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        a = Bulkhead.for_service("svc_a", max_concurrent=1, acquire_timeout=0.05)
        b = Bulkhead.for_service("svc_b", max_concurrent=1, acquire_timeout=0.05)

        async with a:
            # a is full, but b should still be available
            async with b:
                assert a.active == 1
                assert b.active == 1

    async def test_available_property(self) -> None:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        bh = Bulkhead.for_service("avail_svc", max_concurrent=3)
        assert bh.available == 3
        async with bh:
            assert bh.available == 2


# ===========================================================================
# SessionReaper
# ===========================================================================


class TestSessionReaper:
    """Zombie session detection and cleanup."""

    async def test_reaps_orphaned_sessions(self) -> None:
        """Session with done supervisor and expired TTL gets reaped."""
        from digitalkin.core.resilience.session_reaper import SessionReaper

        # Mock task manager
        mgr = MagicMock()

        # Create a mock session that looks like a zombie
        session = MagicMock()
        session.mission_id = "missions:m1"
        session.completed_at = MagicMock()  # Has completed_at → zombie
        session.created_at = MagicMock()
        session.created_at.timestamp.return_value = 0  # Very old

        # Supervisor is done
        supervisor = MagicMock()
        supervisor.done.return_value = True

        mgr.tasks_sessions = {"zombie_task": session}
        mgr.tasks = {"zombie_task": supervisor}
        mgr._cleanup_task = AsyncMock()

        reaper = SessionReaper(mgr, ttl=0.01, interval=0.05)
        await reaper._scan_once()

        mgr._cleanup_task.assert_awaited_once_with("zombie_task", "missions:m1")

    async def test_does_not_reap_active_sessions(self) -> None:
        """Session with running supervisor is not reaped."""
        from digitalkin.core.resilience.session_reaper import SessionReaper

        mgr = MagicMock()
        session = MagicMock()
        session.mission_id = "missions:m1"

        supervisor = MagicMock()
        supervisor.done.return_value = False  # Still running

        mgr.tasks_sessions = {"active_task": session}
        mgr.tasks = {"active_task": supervisor}
        mgr._cleanup_task = AsyncMock()

        reaper = SessionReaper(mgr, ttl=0.01, interval=0.05)
        await reaper._scan_once()

        mgr._cleanup_task.assert_not_awaited()

    async def test_start_stop_lifecycle(self) -> None:
        from digitalkin.core.resilience.session_reaper import SessionReaper

        mgr = MagicMock()
        mgr.tasks_sessions = {}
        mgr.tasks = {}

        reaper = SessionReaper(mgr, ttl=1.0, interval=0.05)
        await reaper.start()
        assert reaper._task is not None

        await asyncio.sleep(0.1)
        await reaper.stop()
        assert reaper._task is None or reaper._task.done()


# ===========================================================================
# GracefulShutdownHandler
# ===========================================================================


class TestGracefulShutdown:
    """Sequenced shutdown with checkpoint."""

    async def test_shutdown_checkpoints_and_cancels(self) -> None:
        from digitalkin.core.resilience.graceful_shutdown import GracefulShutdownHandler

        # Mock task manager with one session
        mgr = MagicMock()
        session = MagicMock()
        session.task_id = "t1"
        session.mission_id = "missions:m1"
        session.status = "running"
        session.module = MagicMock()
        session.module.context.session.setup_id = "setups:s1"
        session.module.context.session.setup_version_id = "setup_versions:sv1"
        mgr.tasks_sessions = {"t1": session}
        mgr.shutdown = AsyncMock()

        # Mock checkpoint manager
        ckpt = MagicMock()
        ckpt.checkpoint = AsyncMock()

        handler = GracefulShutdownHandler(mgr, checkpoint_mgr=ckpt, shutdown_timeout=5.0)

        # Trigger shutdown directly (not via signal)
        await handler._do_shutdown()

        ckpt.checkpoint.assert_awaited_once()
        mgr.shutdown.assert_awaited()

    async def test_shutdown_without_redis(self) -> None:
        """Shutdown works when Redis is not configured."""
        from digitalkin.core.resilience.graceful_shutdown import GracefulShutdownHandler

        mgr = MagicMock()
        session = MagicMock()
        session.mission_id = "missions:m1"
        mgr.tasks_sessions = {"t1": session}
        mgr.shutdown = AsyncMock()

        handler = GracefulShutdownHandler(mgr, checkpoint_mgr=None, redis_client=None)
        await handler._do_shutdown()

        mgr.shutdown.assert_awaited()

    def test_is_shutting_down_flag(self) -> None:
        from digitalkin.core.resilience.graceful_shutdown import GracefulShutdownHandler

        mgr = MagicMock()
        handler = GracefulShutdownHandler(mgr)
        assert not handler.is_shutting_down
        handler._shutdown_event.set()
        assert handler.is_shutting_down
