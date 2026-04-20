"""Comprehensive tests for RemoteTaskManager.

Tests for remote task registration including:
- Task registration without execution
- Coroutine closure
- Session creation for signal handling
- Verify tasks dict remains empty (no local supervisor)
- Signal operations work with remote tasks
- Cleanup and shutdown for remote metadata
- Validation and error handling
"""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

from digitalkin.core.task_manager.base_task_manager import BaseTaskManager
from digitalkin.core.task_manager.remote_task_manager import RemoteTaskManager
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.settings.task.task import TaskSettings
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


@pytest_asyncio.fixture
async def task_manager(monkeypatch: pytest.MonkeyPatch) -> RemoteTaskManager:
    """Standard RemoteTaskManager with test-friendly settings."""
    monkeypatch.setenv("TASK_MAX_CONCURRENT_TASKS", "10")
    BaseTaskManager._task_settings = TaskSettings()
    mgr = RemoteTaskManager(default_timeout=2.0)
    return mgr


# ============================================================================
# Test: Task Registration (No Execution)
# ============================================================================


class TestTaskRegistration:
    """Tests for remote task registration without execution."""

    @pytest.mark.asyncio
    async def test_register_task_success(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test successful task registration without execution."""
        task_id = "remote_register"
        mission_id = "missions:register"

        async def simple_coro() -> None:
            await asyncio.sleep(0.1)

        await task_manager.create_task(task_id, mission_id, mock_base_module, simple_coro())

        # Session created but no task in registry
        assert task_id in task_manager.tasks_sessions
        assert task_id not in task_manager.tasks
        assert task_manager.task_count == 1
        assert len(task_manager.tasks) == 0

    @pytest.mark.asyncio
    async def test_register_task_duplicate_raises(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test duplicate task ID raises ValueError."""

        async def work() -> None:
            await asyncio.sleep(0.5)

        await task_manager.create_task("dup", "missions:dup", mock_base_module, work())

        with pytest.raises(ValueError, match="already exists"):
            await task_manager.create_task("dup", "missions:dup", mock_base_module, work())

    @pytest.mark.asyncio
    async def test_register_task_max_limit(
        self, mock_base_module: Mock,
            monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test exceeding max tasks raises RuntimeError after wait timeout."""
        monkeypatch.setenv("TASK_MAX_CONCURRENT_TASKS", "2")
        monkeypatch.setenv("TASK_MAX_QUEUED_TASKS", "0")
        monkeypatch.setenv("TASK_WAIT_TIMEOUT", "0.1")
        BaseTaskManager._task_settings = TaskSettings()
        small_manager = RemoteTaskManager(default_timeout=1.0)

        async def work() -> None:
            await asyncio.sleep(0.5)

        await small_manager.create_task("t1", "missions:limit", mock_base_module, work())
        await small_manager.create_task("t2", "missions:limit", mock_base_module, work())

        with pytest.raises(RuntimeError, match="Maximum concurrent tasks"):
            await small_manager.create_task("t3", "missions:limit", mock_base_module, work())


# ============================================================================
# Test: Coroutine Closure
# ============================================================================


class TestCoroutineClosure:
    """Tests for coroutine closure behavior."""

    @pytest.mark.asyncio
    async def test_coroutine_closed_immediately(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test that coroutine is closed immediately (not executed)."""
        task_id = "coro_close"
        mission_id = "missions:close"
        execution_log = []

        async def logging_coro() -> None:
            execution_log.append("executed")
            await asyncio.sleep(0.1)

        coro = logging_coro()
        await task_manager.create_task(task_id, mission_id, mock_base_module, coro)

        await asyncio.sleep(0.2)
        assert "executed" not in execution_log

    @pytest.mark.asyncio
    async def test_coroutine_closed_prevents_execution(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test that closed coroutine cannot be awaited."""
        task_id = "coro_prevent"
        mission_id = "missions:prevent"

        async def work() -> None:
            await asyncio.sleep(0.1)

        coro = work()
        await task_manager.create_task(task_id, mission_id, mock_base_module, coro)

        with pytest.raises((StopIteration, RuntimeError)):
            await coro


# ============================================================================
# Test: Session Creation
# ============================================================================


class TestSessionCreation:
    """Tests for session creation for signal handling."""

    @pytest.mark.asyncio
    async def test_session_created_for_signals(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test that session is created for signal handling."""
        task_id = "session_test"
        mission_id = "missions:session"

        async def simple_coro() -> None:
            await asyncio.sleep(0.1)

        await task_manager.create_task(task_id, mission_id, mock_base_module, simple_coro())

        assert task_id in task_manager.tasks_sessions
        session = task_manager.tasks_sessions[task_id]
        assert isinstance(session, TaskSession)


# ============================================================================
# Test: No Local Execution
# ============================================================================


class TestNoLocalExecution:
    """Tests verifying no local execution happens."""

    @pytest.mark.asyncio
    async def test_tasks_dict_remains_empty(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test that tasks dict remains empty (no local execution)."""
        task_ids = [f"no_exec_{i}" for i in range(5)]

        async def work() -> None:
            await asyncio.sleep(0.1)

        for task_id in task_ids:
            await task_manager.create_task(task_id, "missions:no_exec", mock_base_module, work())

        assert len(task_manager.tasks_sessions) == 5
        assert task_manager.task_count == 5
        assert len(task_manager.tasks) == 0

    @pytest.mark.asyncio
    async def test_running_tasks_empty(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test that running_tasks property returns empty (no local execution)."""
        async def work() -> None:
            await asyncio.sleep(0.1)

        await task_manager.create_task("t1", "missions:running", mock_base_module, work())
        assert len(task_manager.running_tasks) == 0


# ============================================================================
# Test: Signal Operations
# ============================================================================


class TestSignalOperations:
    """Tests for signal operations with remote tasks."""

    @pytest.mark.asyncio
    async def test_send_signal_to_remote_task(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
        mock_signal_service: Mock,
    ) -> None:
        """Test sending signal to remote task."""
        async def work() -> None:
            await asyncio.sleep(0.1)

        await task_manager.create_task("t1", "missions:signal", mock_base_module, work())

        result = await task_manager.send_signal("t1", "missions:signal", "cancel", {})
        assert result is True
        mock_signal_service.send_signal.assert_called()

    @pytest.mark.asyncio
    async def test_signal_unknown_task(self, task_manager: RemoteTaskManager) -> None:
        """Test signal to unknown task returns False."""
        result = await task_manager.send_signal("unknown", "missions:signal", "cancel", {})
        assert result is False


# ============================================================================
# Test: Cleanup and Shutdown
# ============================================================================


class TestCleanupShutdown:
    """Tests for cleanup and shutdown of remote task metadata."""

    @pytest.mark.asyncio
    async def test_shutdown_sets_event(self, task_manager: RemoteTaskManager) -> None:
        """Test shutdown sets the shutdown event."""
        assert not task_manager._shutdown_event.is_set()
        await task_manager.shutdown("missions:shutdown")
        assert task_manager._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(self, task_manager: RemoteTaskManager) -> None:
        """Test shutdown can be called multiple times safely."""
        await task_manager.shutdown("missions:shutdown")
        await task_manager.shutdown("missions:shutdown")
        assert task_manager._shutdown_event.is_set()


# ============================================================================
# Test: Cancellation (Metadata Only)
# ============================================================================


class TestCancellation:
    """Tests for cancellation of remote task metadata."""

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_task(self, task_manager: RemoteTaskManager) -> None:
        """Test cancelling a task that doesn't exist."""
        result = await task_manager.cancel_task("nonexistent", "missions:cancel")
        assert result is True


# ============================================================================
# Test: TOCTOU Lock
# ============================================================================


class TestTasksLock:
    """Tests for _tasks_lock preventing TOCTOU race conditions."""

    @pytest.mark.asyncio
    async def test_concurrent_register_respects_max(self, mock_base_module: Mock, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test concurrent registers don't exceed max_concurrent_tasks."""
        monkeypatch.setenv("TASK_MAX_CONCURRENT_TASKS", "3")
        monkeypatch.setenv("TASK_MAX_QUEUED_TASKS", "0")
        monkeypatch.setenv("TASK_WAIT_TIMEOUT", "0.1")
        BaseTaskManager._task_settings = TaskSettings()
        mgr = RemoteTaskManager()

        async def work():
            await asyncio.sleep(1)

        coros = [
            mgr.create_task(f"t{i}", "missions:test", mock_base_module, work())
            for i in range(5)
        ]

        results = await asyncio.gather(*coros, return_exceptions=True)

        successes = sum(1 for r in results if r is None)
        failures = sum(1 for r in results if isinstance(r, RuntimeError))

        assert successes == 3
        assert failures == 2
