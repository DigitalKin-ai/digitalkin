"""Comprehensive tests for LocalTaskManager.

Tests for local task execution including:
- Task creation and registration
- Integration with TaskExecutor
- Task limits and validation
- Cancellation (single, multiple, graceful shutdown)
- Signal operations
- Cleanup and shutdown
- Concurrency stress tests
- Edge cases
"""

import asyncio
import random
import time
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.task_monitor import CancellationReason
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy

# Set timeout for all tests in this file (30 seconds)
pytestmark = pytest.mark.timeout(30)


# ============================================================================
# Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def mock_signal_service() -> Mock:
    """Mock TaskManagerStrategy with all required async methods."""
    svc = Mock(spec=TaskManagerStrategy)
    svc.send_signal = AsyncMock(return_value={})

    _sub_counter = 0

    async def _make_subscription(*_args, **_kwargs):
        nonlocal _sub_counter
        _sub_counter += 1

        async def _empty_gen():
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return
            yield  # pragma: no cover

        return (f"sub_{_sub_counter}", _empty_gen())

    svc.subscribe_signals = AsyncMock(side_effect=_make_subscription)
    svc.unsubscribe_signals = AsyncMock()
    svc.close = AsyncMock()
    return svc


@pytest_asyncio.fixture
async def mock_base_module(mock_signal_service: Mock) -> Mock:
    """Mock BaseModule with async stop() method and signal service."""
    module = Mock(spec=BaseModule)
    module.stop = AsyncMock()
    module.context = Mock()
    module.context.session = Mock()
    module.context.session.setup_id = "setup:test"
    module.context.session.setup_version_id = "setup_version:test"
    module.context.session.current_ids = Mock(return_value={
        "mission_id": "missions:test",
        "task_id": "test",
        "setup_id": "setup:test",
        "setup_version_id": "setup_version:test",
    })
    module.context.task_manager = mock_signal_service
    module.context.cleanup = AsyncMock()
    return module


def _make_mock_task_session(mock_signal_service: Mock) -> Mock:
    """Create a fresh mock task session.

    Args:
        mock_signal_service: Signal service mock.

    Returns:
        Mock TaskSession with expected attributes.
    """
    session = Mock(spec=TaskSession)
    session.mission_id = "missions:mock"
    session.status = "pending"
    session.cancellation_reason = CancellationReason.UNKNOWN
    session.setup_id = "setup:test"
    session.setup_version_id = "setup_version:test"
    session.started_at = None
    session.completed_at = None
    session.signal_service = mock_signal_service
    session.cleanup = AsyncMock()
    session._last_exception = None
    session._last_traceback = None

    async def stay_alive():
        try:
            while True:
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            raise

    session.listen_signals = AsyncMock(side_effect=stay_alive)
    return session


@pytest_asyncio.fixture
async def mock_task_session(mock_signal_service: Mock) -> Mock:
    """Mock TaskSession with expected attributes and async methods."""
    return _make_mock_task_session(mock_signal_service)


@pytest_asyncio.fixture
async def task_manager() -> LocalTaskManager:
    """Standard LocalTaskManager with test-friendly settings."""
    mgr = LocalTaskManager(default_timeout=2.0)
    mgr.max_concurrent_tasks = 10
    return mgr


@pytest_asyncio.fixture
async def high_capacity_manager() -> LocalTaskManager:
    """High-capacity manager for stress tests."""
    mgr = LocalTaskManager(default_timeout=1.0)
    mgr.max_concurrent_tasks = 150
    return mgr


# ============================================================================
# Test: Task Creation & Initialization
# ============================================================================


class TestTaskCreation:
    """Comprehensive task creation tests."""

    @pytest.mark.asyncio
    async def test_create_task_success(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test successful task creation with all components initialized."""
        task_id = "test_create_success"
        mission_id = "missions:create"

        async def simple_coro() -> None:
            await asyncio.sleep(0.1)

        await task_manager.create_task(task_id, mission_id, mock_base_module, simple_coro())

        assert task_id in task_manager.tasks
        assert task_id in task_manager.tasks_sessions
        assert task_manager.task_count >= 1
        assert task_id in task_manager.running_tasks

    @pytest.mark.asyncio
    async def test_create_task_duplicate_raises(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Negative: Duplicate ID raises ValueError."""

        async def work() -> None:
            await asyncio.sleep(0.5)

        await task_manager.create_task("dup", "missions:id", mock_base_module, work())

        with pytest.raises(ValueError, match="already exists"):
            await task_manager.create_task("dup", "missions:id", mock_base_module, work())

    @pytest.mark.asyncio
    async def test_create_task_max_limit(
        self,
        mock_base_module: Mock,
    ) -> None:
        """Negative: Exceeding max tasks raises RuntimeError after wait timeout."""
        small_manager = LocalTaskManager(default_timeout=1.0)
        small_manager.max_concurrent_tasks = 2
        small_manager._max_queued_tasks = 0
        small_manager._task_wait_timeout = 0.1

        async def work() -> None:
            await asyncio.sleep(0.5)

        await small_manager.create_task("t1", "missions:id", mock_base_module, work())
        await small_manager.create_task("t2", "missions:id", mock_base_module, work())

        with pytest.raises(RuntimeError, match="Maximum concurrent tasks"):
            await small_manager.create_task("t3", "missions:id", mock_base_module, work())


# ============================================================================
# Test: Integration with TaskExecutor
# ============================================================================


class TestExecutorIntegration:
    """Tests for LocalTaskManager integration with TaskExecutor."""

    @pytest.mark.asyncio
    async def test_executor_instance_created(self, task_manager: LocalTaskManager) -> None:
        """Test that LocalTaskManager creates a TaskExecutor instance."""
        from digitalkin.core.task_manager.task_executor import TaskExecutor

        assert isinstance(task_manager._executor, TaskExecutor)

    @pytest.mark.asyncio
    async def test_task_stored_in_registry(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test that supervisor task is stored in tasks registry."""
        task_id = "registry_test"
        mission_id = "missions:registry"

        async def simple_coro() -> None:
            await asyncio.sleep(0.05)

        await task_manager.create_task(task_id, mission_id, mock_base_module, simple_coro())

        assert task_id in task_manager.tasks
        assert isinstance(task_manager.tasks[task_id], asyncio.Task)

    @pytest.mark.asyncio
    async def test_task_executes_successfully(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test that task execution completes successfully."""
        task_id = "exec_test"
        mission_id = "missions:exec"
        execution_log = []

        async def logging_coro() -> None:
            execution_log.append("started")
            await asyncio.sleep(0.05)
            execution_log.append("completed")

        await task_manager.create_task(task_id, mission_id, mock_base_module, logging_coro())

        # Capture the session reference BEFORE awaiting supervisor completion —
        # the supervisor's `finally` now folds cleanup (former _deferred_cleanup),
        # so by the time supervisor_task returns the session has been removed
        # from `tasks_sessions`.
        session = task_manager.tasks_sessions[task_id]
        supervisor_task = task_manager.tasks[task_id]
        await supervisor_task

        assert "started" in execution_log
        assert "completed" in execution_log
        assert session.status == "completed"
        # Cleanup ran inside supervisor's `finally` — session should be gone.
        assert task_id not in task_manager.tasks_sessions


# ============================================================================
# Test: Cancellation
# ============================================================================


class TestCancellation:
    """Task cancellation tests."""

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_task(self, task_manager: LocalTaskManager) -> None:
        """Test cancelling a task that doesn't exist."""
        result = await task_manager.cancel_task("nonexistent_task", "missions:cancel")
        assert result is True

    @pytest.mark.asyncio
    async def test_cancel_task_force_after_timeout(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test force cancellation when graceful shutdown times out."""
        task_id = "force_cancel"
        mission_id = "missions:force"

        async def stubborn_coro() -> None:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                await asyncio.sleep(0.5)
                raise

        await task_manager.create_task(task_id, mission_id, mock_base_module, stubborn_coro())
        await asyncio.sleep(0.05)

        start = time.time()
        result = await task_manager.cancel_task(task_id, mission_id, timeout=0.1)
        duration = time.time() - start

        assert result is True
        assert duration < 2.0

    @pytest.mark.asyncio
    async def test_cancel_already_completed_task(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test cancelling a task that has already completed."""
        task_id = "already_done"
        mission_id = "missions:done"

        async def quick_coro() -> None:
            await asyncio.sleep(0.05)

        await task_manager.create_task(task_id, mission_id, mock_base_module, quick_coro())
        await asyncio.sleep(0.3)

        result = await task_manager.cancel_task(task_id, mission_id)
        assert result is True

    @pytest.mark.asyncio
    async def test_cancel_all_tasks(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test cancelling all tasks at once."""
        task_ids = [f"cancel_all_{i}" for i in range(3)]
        mission_id = "missions:all"

        async def long_coro() -> None:
            await asyncio.sleep(10)

        for task_id in task_ids:
            await task_manager.create_task(task_id, mission_id, mock_base_module, long_coro())

        await asyncio.sleep(0.05)
        await task_manager.cancel_all_tasks(mission_id)

        assert len(task_manager.running_tasks) == 0


# ============================================================================
# Test: Signals
# ============================================================================


class TestSignals:
    """Signal handling tests."""

    @pytest.mark.asyncio
    async def test_send_signal_cancel(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_signal_service: Mock,
    ) -> None:
        """Test sending cancel signal."""
        task_id = "sig_cancel"

        async def work() -> None:
            await asyncio.sleep(1)

        await task_manager.create_task(task_id, "missions:signal", mock_base_module, work())

        result = await task_manager.send_signal(task_id, "missions:signal", "cancel", {})
        assert result is True
        mock_signal_service.send_signal.assert_called()

    @pytest.mark.asyncio
    async def test_signal_unknown_task(self, task_manager: LocalTaskManager) -> None:
        """Negative: Unknown task returns False."""
        result = await task_manager.send_signal("unknown", "missions:signal", "cancel", {})
        assert result is False


# ============================================================================
# Test: Cleanup and Shutdown
# ============================================================================


class TestCleanupShutdown:
    """Session cleanup and shutdown tests."""

    @pytest.mark.asyncio
    async def test_shutdown_sets_event(self, task_manager: LocalTaskManager) -> None:
        """Test shutdown sets the shutdown event."""
        assert not task_manager._shutdown_event.is_set()
        await task_manager.shutdown("missions:shutdown")
        assert task_manager._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(self, task_manager: LocalTaskManager) -> None:
        """Test shutdown can be called multiple times safely."""
        await task_manager.shutdown("missions:shutdown")
        await task_manager.shutdown("missions:shutdown")
        assert task_manager._shutdown_event.is_set()


# ============================================================================
# Test: Concurrency Stress Tests
# ============================================================================


class TestConcurrencyStress:
    """Stress tests for concurrent operations."""

    @pytest.mark.asyncio
    async def test_high_task_churn(
        self,
        high_capacity_manager: LocalTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Stress: Rapid create/complete cycles."""
        completed_count = 0

        async def quick_task() -> None:
            nonlocal completed_count
            await asyncio.sleep(random.uniform(0.01, 0.05))
            completed_count += 1

        for i in range(50):
            await high_capacity_manager.create_task(
                f"churn_{i}", "missions:churn", mock_base_module, quick_task(),
            )

        await asyncio.sleep(1.0)
        assert completed_count >= 40

    @pytest.mark.asyncio
    async def test_concurrent_create_cancel_race(
        self,
        high_capacity_manager: LocalTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Stress: Race between creation and cancellation."""

        async def medium_task() -> None:
            await asyncio.sleep(0.2)

        task_ids = []
        for i in range(20):
            task_id = f"race_{i}"
            task_ids.append(task_id)
            await high_capacity_manager.create_task(
                task_id, "missions:race", mock_base_module, medium_task(),
            )

        # Immediately start cancelling randomly
        cancel_tasks = [
            high_capacity_manager.cancel_task(task_id, "missions:race")
            for task_id in random.sample(task_ids, 10)
        ]

        # Should not raise
        await asyncio.gather(*cancel_tasks, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_toctou_lock_prevents_oversubscription(
        self,
        mock_base_module: Mock,
    ) -> None:
        """Test that semaphore prevents oversubscription of max_concurrent_tasks."""
        mgr = LocalTaskManager()
        mgr.max_concurrent_tasks = 5
        mgr._max_queued_tasks = 0
        mgr._task_wait_timeout = 0.1

        async def slow_task() -> None:
            await asyncio.sleep(1)

        # Try to create 10 tasks concurrently with max=5
        coros = [
            mgr.create_task(f"race_{i}", "missions:race", mock_base_module, slow_task())
            for i in range(10)
        ]

        results = await asyncio.gather(*coros, return_exceptions=True)

        successes = sum(1 for r in results if r is None)
        failures = sum(1 for r in results if isinstance(r, RuntimeError))

        assert successes == 5
        assert failures == 5


# ============================================================================
# Test: Properties and State
# ============================================================================


class TestPropertiesState:
    """Tests for task count and running tasks properties."""

    @pytest.mark.asyncio
    async def test_task_count_property(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test task_count property returns correct count."""
        assert task_manager.task_count == 0

        async def work() -> None:
            await asyncio.sleep(0.5)

        await task_manager.create_task("t1", "missions:prop", mock_base_module, work())
        assert task_manager.task_count >= 1

        await task_manager.create_task("t2", "missions:prop", mock_base_module, work())
        assert task_manager.task_count >= 2

    @pytest.mark.asyncio
    async def test_running_tasks_property(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test running_tasks property returns active task IDs."""
        assert len(task_manager.running_tasks) == 0

        async def work() -> None:
            await asyncio.sleep(0.5)

        await task_manager.create_task("t1", "missions:prop", mock_base_module, work())
        await task_manager.create_task("t2", "missions:prop", mock_base_module, work())

        running = task_manager.running_tasks
        assert "t1" in running
        assert "t2" in running


# ============================================================================
# Test: Edge Cases
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and corner scenarios."""

    @pytest.mark.asyncio
    async def test_immediate_task_completion(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test task that completes immediately."""
        task_id = "immediate"
        mission_id = "missions:immediate"

        async def instant() -> None:
            pass

        await task_manager.create_task(task_id, mission_id, mock_base_module, instant())
        assert task_id in task_manager.tasks
