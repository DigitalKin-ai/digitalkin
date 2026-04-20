"""Comprehensive tests for BaseTaskManager.

Tests for abstract base task manager functionality including:
- Abstract method enforcement
- Validation logic (_validate_task_creation)
- Session creation (_create_session)
- Cleanup logic (_cleanup_task)
- Signal sending (send_signal)
- Cancellation logic (cancel_task, cancel_all_tasks)
- Shutdown coordination
- Property accessors (task_count, running_tasks)
"""

import asyncio
import contextlib
from collections.abc import Coroutine
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

from digitalkin.core.task_manager.base_task_manager import BaseTaskManager
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.task_monitor import CancellationReason
from digitalkin.models.settings.task.task import TaskSettings
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy

# Set timeout for all tests in this file (60 seconds)
pytestmark = pytest.mark.timeout(60)


# ============================================================================
# Minimal Concrete Implementation for Testing
# ============================================================================


class ConcreteTaskManager(BaseTaskManager):
    """Minimal concrete implementation for testing BaseTaskManager."""

    async def create_task(
            self,
            task_id: str,
            mission_id: str,
            module: BaseModule,
            coro: Coroutine[Any, Any, None],
    ) -> None:
        """Create task using base validation and session creation.

        Args:
            task_id: Unique task identifier
            mission_id: Mission identifier
            module: Module instance
            coro: Coroutine to execute
        """
        await self._acquire_task_slot(coro)
        try:
            async with self._tasks_lock:
                await self._validate_task_creation(task_id, mission_id, coro)
                self._create_session(task_id, mission_id, module)
        except Exception:
            if task_id not in self.tasks_sessions:
                self._task_slot.release()
            raise


# ============================================================================
# Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def mock_signal_service() -> Mock:
    """Mock TaskManagerStrategy with all required async methods."""
    svc = Mock(spec=TaskManagerStrategy)
    svc.send_signal = AsyncMock(return_value={})
    svc.subscribe_signals = AsyncMock(return_value=("sub_123", _empty_gen()))
    svc.unsubscribe_signals = AsyncMock()
    svc.close = AsyncMock()
    return svc


async def _empty_gen():
    """Empty async generator."""
    return
    yield  # pragma: no cover


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
async def task_manager() -> ConcreteTaskManager:
    """Standard concrete task manager for testing."""
    BaseTaskManager._task_settings = TaskSettings()
    mgr = ConcreteTaskManager(default_timeout=2.0)
    return mgr


@pytest_asyncio.fixture
async def mock_task_session(mock_signal_service: Mock) -> Mock:
    """Mock TaskSession with expected attributes and async methods."""
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
    session._write_lock = asyncio.Lock()
    return session


# ============================================================================
# Test: Abstract Method Enforcement
# ============================================================================


class TestAbstractMethods:
    """Tests for abstract method enforcement."""

    def test_cannot_instantiate_base(self) -> None:
        """Test that BaseTaskManager cannot be instantiated directly."""
        with pytest.raises(TypeError):
            BaseTaskManager()

    def test_concrete_can_instantiate(self) -> None:
        """Test that concrete implementation can be instantiated."""
        mgr = ConcreteTaskManager()
        assert mgr is not None

    def test_default_params(self) -> None:
        """Test default parameter values."""
        mgr = ConcreteTaskManager()
        assert mgr.default_timeout == 300.0
        assert mgr.max_concurrent_tasks == 100

    def test_custom_params(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test custom parameter values."""
        monkeypatch.setenv("TASK_MAX_CONCURRENT_TASKS", "50")
        BaseTaskManager._task_settings = TaskSettings()
        mgr = ConcreteTaskManager(default_timeout=5.0)
        assert mgr.default_timeout == 5.0
        assert mgr.max_concurrent_tasks == 50


# ============================================================================
# Test: Validation (_validate_task_creation)
# ============================================================================


class TestValidation:
    """Tests for task creation validation."""

    @pytest.mark.asyncio
    async def test_duplicate_task_id_raises(
            self, task_manager: ConcreteTaskManager, mock_base_module: Mock,
    ) -> None:
        """Test that duplicate task_id raises ValueError."""
        async def work():
            await asyncio.sleep(1)

        await task_manager.create_task("dup", "missions:test", mock_base_module, work())

        with pytest.raises(ValueError, match="already exists"):
            await task_manager.create_task("dup", "missions:test", mock_base_module, work())

    @pytest.mark.asyncio
    async def test_max_concurrent_tasks_raises(self, mock_base_module: Mock, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that exceeding max tasks raises RuntimeError after wait timeout."""
        monkeypatch.setenv("TASK_MAX_CONCURRENT_TASKS", "2")
        monkeypatch.setenv("TASK_MAX_QUEUED_TASKS", "0")
        monkeypatch.setenv("TASK_WAIT_TIMEOUT", "1")
        BaseTaskManager._task_settings = TaskSettings()
        mgr = ConcreteTaskManager(default_timeout=1.0)

        async def work():
            await asyncio.sleep(1)

        await mgr.create_task("t1", "missions:test", mock_base_module, work())
        await mgr.create_task("t2", "missions:test", mock_base_module, work())

        with pytest.raises(RuntimeError, match="Maximum concurrent tasks"):
            await mgr.create_task("t3", "missions:test", mock_base_module, work())

    @pytest.mark.asyncio
    async def test_duplicate_closes_coroutine(
            self, task_manager: ConcreteTaskManager, mock_base_module: Mock,
    ) -> None:
        """Test that duplicate validation closes the rejected coroutine."""
        async def work():
            await asyncio.sleep(1)

        await task_manager.create_task("dup2", "missions:test", mock_base_module, work())

        coro = work()
        with pytest.raises(ValueError):
            await task_manager.create_task("dup2", "missions:test", mock_base_module, coro)

        # Coroutine should be closed
        with pytest.raises((StopIteration, RuntimeError)):
            await coro

    @pytest.mark.asyncio
    async def test_max_tasks_closes_coroutine(self, mock_base_module: Mock, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that max tasks validation closes the rejected coroutine."""
        monkeypatch.setenv("TASK_MAX_CONCURRENT_TASKS", "1")
        monkeypatch.setenv("TASK_MAX_QUEUED_TASKS", "0")
        monkeypatch.setenv("TASK_WAIT_TIMEOUT", "1")
        BaseTaskManager._task_settings = TaskSettings()
        mgr = ConcreteTaskManager()

        async def work():
            await asyncio.sleep(1)

        await mgr.create_task("t1", "missions:test", mock_base_module, work())

        coro = work()
        with pytest.raises(RuntimeError):
            await mgr.create_task("t2", "missions:test", mock_base_module, coro)

        with pytest.raises((StopIteration, RuntimeError)):
            await coro


# ============================================================================
# Test: Session Creation (_create_session)
# ============================================================================


class TestSessionCreation:
    """Tests for _create_session."""

    @pytest.mark.asyncio
    async def test_create_session_registers(
            self, task_manager: ConcreteTaskManager, mock_base_module: Mock,
    ) -> None:
        """Test _create_session registers session in tasks_sessions."""
        session = task_manager._create_session("t1", "missions:test", mock_base_module)

        assert "t1" in task_manager.tasks_sessions
        assert isinstance(session, TaskSession)
        assert session.task_id == "t1"
        assert session.mission_id == "missions:test"

    @pytest.mark.asyncio
    async def test_create_session_pending_status(
            self, task_manager: ConcreteTaskManager, mock_base_module: Mock,
    ) -> None:
        """Test _create_session creates session with pending status."""
        session = task_manager._create_session("t2", "missions:test", mock_base_module)
        assert session.status == "pending"


# ============================================================================
# Test: Cleanup (_cleanup_task)
# ============================================================================


class TestCleanup:
    """Tests for _cleanup_task."""

    @pytest.mark.asyncio
    async def test_cleanup_removes_session(
            self, task_manager: ConcreteTaskManager, mock_task_session: Mock,
    ) -> None:
        """Test cleanup removes session from tracking."""
        task_manager.tasks_sessions["t1"] = mock_task_session

        await task_manager._cleanup_task("t1", "missions:test")

        assert "t1" not in task_manager.tasks_sessions

    @pytest.mark.asyncio
    async def test_cleanup_calls_session_cleanup(
            self, task_manager: ConcreteTaskManager, mock_task_session: Mock,
    ) -> None:
        """Test cleanup calls session.cleanup()."""
        task_manager.tasks_sessions["t1"] = mock_task_session

        await task_manager._cleanup_task("t1", "missions:test")

        mock_task_session.cleanup.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_removes_task(
            self, task_manager: ConcreteTaskManager, mock_task_session: Mock,
    ) -> None:
        """Test cleanup removes task from tasks dict."""
        task_manager.tasks["t1"] = asyncio.create_task(asyncio.sleep(10))
        task_manager.tasks_sessions["t1"] = mock_task_session

        await task_manager._cleanup_task("t1", "missions:test")

        assert "t1" not in task_manager.tasks

    @pytest.mark.asyncio
    async def test_cleanup_handles_missing_session(
            self, task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cleanup handles non-existent session gracefully."""
        # Should not raise
        await task_manager._cleanup_task("nonexistent", "missions:test")

    @pytest.mark.asyncio
    async def test_cleanup_handles_session_cleanup_failure(
            self, task_manager: ConcreteTaskManager, mock_task_session: Mock,
    ) -> None:
        """Test cleanup continues even if session.cleanup() fails."""
        mock_task_session.cleanup = AsyncMock(side_effect=RuntimeError("cleanup failed"))
        task_manager.tasks_sessions["t1"] = mock_task_session

        # Should not raise
        await task_manager._cleanup_task("t1", "missions:test")

        # Session should still be removed
        assert "t1" not in task_manager.tasks_sessions


# ============================================================================
# Test: Signal Sending (send_signal)
# ============================================================================


class TestSignalSending:
    """Tests for send_signal."""

    @pytest.mark.asyncio
    async def test_send_signal_success(
            self,
            task_manager: ConcreteTaskManager,
            mock_task_session: Mock,
            mock_signal_service: Mock,
    ) -> None:
        """Test successful signal sending."""
        task_manager.tasks_sessions["t1"] = mock_task_session

        result = await task_manager.send_signal("t1", "missions:test", "cancel", {})

        assert result is True
        mock_signal_service.send_signal.assert_called_once()

    @pytest.mark.asyncio
    async def test_send_signal_unknown_task(
            self, task_manager: ConcreteTaskManager,
    ) -> None:
        """Test signal to non-existent task returns False."""
        result = await task_manager.send_signal("unknown", "missions:test", "cancel", {})
        assert result is False

    @pytest.mark.asyncio
    async def test_send_signal_includes_action(
            self,
            task_manager: ConcreteTaskManager,
            mock_task_session: Mock,
            mock_signal_service: Mock,
    ) -> None:
        """Test signal includes correct action type."""
        task_manager.tasks_sessions["t1"] = mock_task_session

        await task_manager.send_signal("t1", "missions:test", "cancel", {})

        call_data = mock_signal_service.send_signal.call_args[0][1]
        assert call_data["action"] == "cancel"

    @pytest.mark.asyncio
    async def test_send_signal_with_payload(
            self,
            task_manager: ConcreteTaskManager,
            mock_task_session: Mock,
            mock_signal_service: Mock,
    ) -> None:
        """Test signal includes payload."""
        task_manager.tasks_sessions["t1"] = mock_task_session

        await task_manager.send_signal("t1", "missions:test", "start", {"key": "value"})

        call_data = mock_signal_service.send_signal.call_args[0][1]
        assert call_data["payload"] == {"key": "value"}


# ============================================================================
# Test: Cancellation (cancel_task)
# ============================================================================


class TestCancelTask:
    """Tests for cancel_task."""

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_task(
            self, task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cancelling a task that doesn't exist returns True."""
        result = await task_manager.cancel_task("nonexistent", "missions:test")
        assert result is True

    @pytest.mark.asyncio
    async def test_cancel_completed_task(
            self,
            task_manager: ConcreteTaskManager,
            mock_task_session: Mock,
    ) -> None:
        """Test cancelling an already completed task."""
        done_task = asyncio.create_task(asyncio.sleep(0))
        await done_task  # Let it complete

        task_manager.tasks["t1"] = done_task
        task_manager.tasks_sessions["t1"] = mock_task_session

        result = await task_manager.cancel_task("t1", "missions:test")
        assert result is True


# ============================================================================
# Test: Cancel All Tasks
# ============================================================================


class TestCancelAllTasks:
    """Tests for cancel_all_tasks."""

    @pytest.mark.asyncio
    async def test_cancel_all_no_tasks(
            self, task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cancel_all_tasks with no tasks."""
        results = await task_manager.cancel_all_tasks("missions:test")
        assert results == {}

    @pytest.mark.asyncio
    async def test_cancel_all_cancels_all_running(
            self,
            task_manager: ConcreteTaskManager,
            mock_task_session: Mock,
    ) -> None:
        """Test cancel_all_tasks cancels all running tasks."""
        for i in range(3):
            task = asyncio.create_task(asyncio.sleep(10))
            task_manager.tasks[f"t{i}"] = task
            session = Mock(spec=TaskSession)
            session.status = "running"
            session.cancellation_reason = CancellationReason.UNKNOWN
            session.signal_service = mock_task_session.signal_service
            session.setup_id = "setup:test"
            session.setup_version_id = "setup_version:test"
            session.cleanup = AsyncMock()
            task_manager.tasks_sessions[f"t{i}"] = session

        results = await task_manager.cancel_all_tasks("missions:test", timeout=0.5)

        assert len(results) == 3


# ============================================================================
# Test: Shutdown
# ============================================================================


class TestShutdown:
    """Tests for shutdown."""

    @pytest.mark.asyncio
    async def test_shutdown_sets_event(
            self, task_manager: ConcreteTaskManager,
    ) -> None:
        """Test shutdown sets the shutdown event."""
        assert not task_manager._shutdown_event.is_set()
        await task_manager.shutdown("missions:test")
        assert task_manager._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(
            self, task_manager: ConcreteTaskManager,
    ) -> None:
        """Test shutdown can be called multiple times safely."""
        await task_manager.shutdown("missions:test")
        await task_manager.shutdown("missions:test")
        assert task_manager._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_shutdown_marks_sessions_with_reason(
            self,
            task_manager: ConcreteTaskManager,
            mock_task_session: Mock,
    ) -> None:
        """Test shutdown marks sessions with SHUTDOWN reason."""
        mock_task_session.cancellation_reason = CancellationReason.UNKNOWN
        task_manager.tasks_sessions["t1"] = mock_task_session

        await task_manager.shutdown("missions:test")

        assert mock_task_session.cancellation_reason == CancellationReason.SHUTDOWN


# ============================================================================
# Test: Properties
# ============================================================================


class TestProperties:
    """Tests for task_count and running_tasks properties."""

    @pytest.mark.asyncio
    async def test_task_count_empty(
            self, task_manager: ConcreteTaskManager,
    ) -> None:
        """Test task_count is 0 when empty."""
        assert task_manager.task_count == 0

    @pytest.mark.asyncio
    async def test_task_count_active(
            self,
            task_manager: ConcreteTaskManager,
            mock_base_module: Mock,
    ) -> None:
        """Test task_count counts pending and running sessions."""
        async def work():
            await asyncio.sleep(1)

        await task_manager.create_task("t1", "missions:test", mock_base_module, work())
        assert task_manager.task_count == 1

        await task_manager.create_task("t2", "missions:test", mock_base_module, work())
        assert task_manager.task_count == 2

    @pytest.mark.asyncio
    async def test_running_tasks_empty(
            self, task_manager: ConcreteTaskManager,
    ) -> None:
        """Test running_tasks is empty when no tasks."""
        assert len(task_manager.running_tasks) == 0

    @pytest.mark.asyncio
    async def test_running_tasks_tracks_active(
            self, task_manager: ConcreteTaskManager,
    ) -> None:
        """Test running_tasks returns IDs of active tasks."""
        task = asyncio.create_task(asyncio.sleep(10))
        task_manager.tasks["t1"] = task

        assert "t1" in task_manager.running_tasks

        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


# ============================================================================
# Test: Async Context Manager
# ============================================================================


class TestAsyncContextManager:
    """Tests for __aenter__ and __aexit__."""

    @pytest.mark.asyncio
    async def test_context_manager_returns_self(self) -> None:
        """Test __aenter__ returns self."""
        mgr = ConcreteTaskManager()
        async with mgr as ctx:
            assert ctx is mgr

    @pytest.mark.asyncio
    async def test_context_manager_calls_shutdown(self) -> None:
        """Test __aexit__ calls shutdown."""
        mgr = ConcreteTaskManager()
        async with mgr:
            pass
        assert mgr._shutdown_event.is_set()


# ============================================================================
# Test: TOCTOU Lock
# ============================================================================


class TestTasksLock:
    """Tests for _tasks_lock preventing TOCTOU race conditions."""

    @pytest.mark.asyncio
    async def test_concurrent_create_respects_max(self, mock_base_module: Mock, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that concurrent creates don't exceed max_concurrent_tasks."""
        monkeypatch.setenv("TASK_MAX_CONCURRENT_TASKS", "3")
        monkeypatch.setenv("TASK_MAX_QUEUED_TASKS", "0")
        monkeypatch.setenv("TASK_WAIT_TIMEOUT", "1")
        BaseTaskManager._task_settings = TaskSettings()
        mgr = ConcreteTaskManager()

        async def work():
            await asyncio.sleep(1)

        # Try to create 5 tasks concurrently with max=3
        coros = [
            mgr.create_task(f"t{i}", "missions:test", mock_base_module, work())
            for i in range(5)
        ]

        results = await asyncio.gather(*coros, return_exceptions=True)

        successes = [r for r in results if r is None]
        failures = [r for r in results if isinstance(r, RuntimeError)]

        assert len(successes) == 3
        assert len(failures) == 2
