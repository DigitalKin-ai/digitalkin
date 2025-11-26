"""Comprehensive tests for BaseTaskManager.

Tests for abstract base task manager functionality including:
- Abstract method enforcement
- Validation logic (_validate_task_creation)
- Session creation (_create_session)
- Cleanup logic (_cleanup_task)
- Signal sending (send_signal, get_task_status)
- Cancellation logic (cancel_task, cancel_all_tasks)
- Shutdown coordination
- Property accessors (task_count, running_tasks)
"""

import asyncio
import contextlib
import datetime
from collections.abc import Coroutine
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio

from digitalkin.core.task_manager.base_task_manager import BaseTaskManager
from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.task_monitor import CancellationReason, SignalType, TaskStatus
from digitalkin.modules._base_module import BaseModule

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
        heartbeat_interval: datetime.timedelta = datetime.timedelta(seconds=2),
        connection_timeout: datetime.timedelta = datetime.timedelta(seconds=5),
    ) -> None:
        """Minimal create_task implementation for testing."""
        await self._validate_task_creation(task_id, mission_id, coro)
        _channel, _session = await self._create_session(
            task_id, mission_id, module, heartbeat_interval, connection_timeout
        )

        # Create a simple task for testing
        async def simple_supervisor() -> None:
            await asyncio.sleep(0.1)

        self.tasks[task_id] = asyncio.create_task(simple_supervisor())


# ============================================================================
# Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def mock_base_module() -> Mock:
    """Mock BaseModule with async stop() method."""
    module = Mock(spec=BaseModule)
    module.stop = AsyncMock()
    return module


@pytest_asyncio.fixture
async def mock_surreal_connection() -> Mock:
    """Create a mock SurrealDB connection with async methods."""
    conn = Mock(spec=SurrealDBConnection)
    conn.init_surreal_instance = AsyncMock()
    conn.create = AsyncMock(return_value={"id": "signal_123"})
    conn.update = AsyncMock()
    conn.close = AsyncMock()
    return conn


@pytest_asyncio.fixture
async def mock_task_session() -> Mock:
    """Mock TaskSession with expected attributes and async methods."""
    session = Mock(spec=TaskSession)
    session.mission_id = "missions:mock"
    session.status = TaskStatus.PENDING
    session.cancellation_reason = CancellationReason.UNKNOWN
    session.started_at = None
    session.completed_at = None
    session.db = Mock()
    session.db.close = AsyncMock()
    session.db.update = AsyncMock()
    session.listen_signals = AsyncMock(side_effect=asyncio.CancelledError())
    session.generate_heartbeats = AsyncMock(side_effect=asyncio.CancelledError())

    # Realistic cleanup implementation that calls db.close()
    async def mock_cleanup() -> None:
        await session.db.close()

    session.cleanup = AsyncMock(side_effect=mock_cleanup)
    return session


@pytest_asyncio.fixture
async def task_manager() -> ConcreteTaskManager:
    """Standard ConcreteTaskManager for testing base functionality."""
    return ConcreteTaskManager(default_timeout=2.0, max_concurrent_tasks=10)


# ============================================================================
# Test: Abstract Method Enforcement
# ============================================================================


class TestAbstractMethods:
    """Tests for abstract method enforcement."""

    def test_cannot_instantiate_base_class(self) -> None:
        """Test that BaseTaskManager cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BaseTaskManager()  # type: ignore

    def test_initialization_custom_params(self) -> None:
        """Test initialization with custom parameters."""
        manager = ConcreteTaskManager(default_timeout=5.0, max_concurrent_tasks=50)

        assert manager.default_timeout == 5.0
        assert manager.max_concurrent_tasks == 50


# ============================================================================
# Test: Validation Logic
# ============================================================================


class TestValidation:
    """Tests for _validate_task_creation logic."""

    @pytest.mark.asyncio
    async def test_validate_duplicate_task_id(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test validation fails for duplicate task ID."""
        task_id = "duplicate"
        mission_id = "missions:dup"

        # Create a mock session
        task_manager.tasks_sessions[task_id] = Mock()

        async def work() -> None:
            await asyncio.sleep(0.1)

        coro = work()

        with pytest.raises(ValueError, match="already exists"):
            await task_manager._validate_task_creation(task_id, mission_id, coro)

        # Verify coroutine was closed
        with pytest.raises((StopIteration, RuntimeError)):
            await coro

    @pytest.mark.asyncio
    async def test_validate_max_concurrent_tasks(
        self,
    ) -> None:
        """Test validation fails when max tasks reached."""
        manager = ConcreteTaskManager(max_concurrent_tasks=2)
        mission_id = "missions:max"

        # Fill up to max
        manager.tasks_sessions["t1"] = Mock()
        manager.tasks_sessions["t2"] = Mock()

        async def work() -> None:
            await asyncio.sleep(0.1)

        coro = work()

        with pytest.raises(RuntimeError, match="Maximum concurrent tasks"):
            await manager._validate_task_creation("t3", mission_id, coro)

        # Verify coroutine was closed
        with pytest.raises((StopIteration, RuntimeError)):
            await coro

    @pytest.mark.asyncio
    async def test_validate_success(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test validation succeeds for valid task."""
        task_id = "valid"
        mission_id = "missions:valid"

        async def work() -> None:
            await asyncio.sleep(0.1)

        coro = work()

        # Should not raise
        await task_manager._validate_task_creation(task_id, mission_id, coro)

        # Coroutine should still be valid
        coro.close()  # Clean up


# ============================================================================
# Test: Session Creation
# ============================================================================


class TestSessionCreation:
    """Tests for _create_session logic."""

    @pytest.mark.asyncio
    async def test_create_session_success(
        self,
        task_manager: ConcreteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test successful session creation."""
        task_id = "session_create"
        mission_id = "missions:session"

        with (
            patch(
                "digitalkin.core.task_manager.base_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.base_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            channel, session = await task_manager._create_session(
                task_id,
                mission_id,
                mock_base_module,
                datetime.timedelta(seconds=2),
                datetime.timedelta(seconds=5),
            )

            # Verify return values
            assert channel == mock_surreal_connection
            assert session == mock_task_session

            # Verify session registered
            assert task_id in task_manager.tasks_sessions
            assert task_manager.tasks_sessions[task_id] == mock_task_session

            # Verify DB initialized
            mock_surreal_connection.init_surreal_instance.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_session_custom_intervals(
        self,
        task_manager: ConcreteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test session creation with custom intervals."""
        task_id = "session_custom"
        mission_id = "missions:custom"

        with (
            patch(
                "digitalkin.core.task_manager.base_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.base_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            await task_manager._create_session(
                task_id,
                mission_id,
                mock_base_module,
                datetime.timedelta(seconds=1),
                datetime.timedelta(seconds=10),
            )

            assert task_id in task_manager.tasks_sessions


# ============================================================================
# Test: Cleanup Logic
# ============================================================================


class TestCleanup:
    """Rigorous tests for BaseTaskManager._cleanup_task delegation.

    Tests the contract between BaseTaskManager and TaskSession:
    - BaseTaskManager delegates cleanup to TaskSession.cleanup()
    - Tracking dictionaries are properly maintained
    - Cleanup happens in all cancellation scenarios
    """

    @pytest.mark.asyncio
    async def test_cleanup_task_delegates_to_session(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test _cleanup_task properly delegates to session.cleanup()."""
        task_id = "delegate_test"
        mission_id = "missions:delegate"

        # Create mock session with cleanup
        mock_session = Mock(spec=TaskSession)
        mock_session.cleanup = AsyncMock()
        mock_session.status = TaskStatus.PENDING
        mock_session.cancellation_reason = CancellationReason.UNKNOWN

        task_manager.tasks_sessions[task_id] = mock_session
        task_manager.tasks[task_id] = Mock()  # Mock task object

        # Execute cleanup
        await task_manager._cleanup_task(task_id, mission_id)

        # Assert: cleanup() was called exactly once
        mock_session.cleanup.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cleanup_task_removes_from_tracking_dicts(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cleanup removes task from both tracking dictionaries."""
        task_id = "tracking_test"
        mission_id = "missions:tracking"

        # Setup: Add to both dictionaries
        mock_session = Mock(spec=TaskSession)
        mock_session.cleanup = AsyncMock()
        mock_session.status = TaskStatus.PENDING
        mock_session.cancellation_reason = CancellationReason.UNKNOWN
        task_manager.tasks_sessions[task_id] = mock_session
        task_manager.tasks[task_id] = Mock()

        # Verify present before cleanup
        assert task_id in task_manager.tasks_sessions
        assert task_id in task_manager.tasks

        # Execute cleanup
        await task_manager._cleanup_task(task_id, mission_id)

        # Assert: Removed from both dictionaries
        assert task_id not in task_manager.tasks_sessions
        assert task_id not in task_manager.tasks

    @pytest.mark.asyncio
    async def test_cleanup_task_with_no_session(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cleanup handles missing session gracefully."""
        task_id = "no_session_test"
        mission_id = "missions:no_session"

        # Only task exists, no session
        task_manager.tasks[task_id] = Mock()

        # Should not raise
        await task_manager._cleanup_task(task_id, mission_id)

        # Assert: Task still removed
        assert task_id not in task_manager.tasks

    @pytest.mark.asyncio
    async def test_cleanup_task_with_no_task(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cleanup handles missing task gracefully."""
        task_id = "no_task_test"
        mission_id = "missions:no_task"

        # Only session exists, no task
        mock_session = Mock(spec=TaskSession)
        mock_session.cleanup = AsyncMock()
        mock_session.status = TaskStatus.PENDING
        mock_session.cancellation_reason = CancellationReason.UNKNOWN
        task_manager.tasks_sessions[task_id] = mock_session

        # Should not raise
        await task_manager._cleanup_task(task_id, mission_id)

        # Assert: Session removed, cleanup called
        assert task_id not in task_manager.tasks_sessions
        mock_session.cleanup.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cleanup_task_idempotent(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cleanup can be called multiple times safely."""
        task_id = "idempotent_test"
        mission_id = "missions:idempotent"

        # First cleanup with session
        mock_session = Mock(spec=TaskSession)
        mock_session.cleanup = AsyncMock()
        mock_session.status = TaskStatus.PENDING
        mock_session.cancellation_reason = CancellationReason.UNKNOWN
        task_manager.tasks_sessions[task_id] = mock_session
        task_manager.tasks[task_id] = Mock()

        await task_manager._cleanup_task(task_id, mission_id)

        # Second cleanup - should not raise
        await task_manager._cleanup_task(task_id, mission_id)

        # Session was removed after first call
        assert task_id not in task_manager.tasks_sessions

    @pytest.mark.asyncio
    async def test_cancel_task_orphaned_session_gets_cleaned(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cancel_task cleans up orphaned sessions (session without task).

        This is a critical edge case - session exists but task was never created
        or was already removed. cancel_task must still cleanup the session.
        """
        task_id = "orphaned_test"
        mission_id = "missions:orphaned"

        # Session exists but NO task
        mock_session = Mock(spec=TaskSession)
        mock_session.cleanup = AsyncMock()
        mock_session.status = TaskStatus.PENDING
        mock_session.cancellation_reason = CancellationReason.UNKNOWN
        task_manager.tasks_sessions[task_id] = mock_session

        # Execute cancel - task not found, but cleanup should still happen
        result = await task_manager.cancel_task(task_id, mission_id)

        # Assert: Returns True (success)
        assert result is True

        # Assert: Session cleanup was called
        mock_session.cleanup.assert_awaited_once()

        # Assert: Session removed from tracking
        assert task_id not in task_manager.tasks_sessions

    @pytest.mark.asyncio
    async def test_cancel_task_cleanup_in_finally_block(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cleanup happens even if cancel_task encounters errors.

        Cleanup must happen in finally block to ensure resource release.
        """
        task_id = "finally_test"
        mission_id = "missions:finally"

        # Create a task that will timeout and be force-cancelled
        async def long_running_task() -> None:
            await asyncio.sleep(100)

        task = asyncio.create_task(long_running_task())
        task_manager.tasks[task_id] = task

        # Add session
        mock_session = Mock(spec=TaskSession)
        mock_session.cleanup = AsyncMock()
        mock_session.status = TaskStatus.RUNNING
        mock_session.cancellation_reason = CancellationReason.UNKNOWN
        task_manager.tasks_sessions[task_id] = mock_session

        # Cancel with short timeout - will timeout and force cancel
        result = await task_manager.cancel_task(task_id, mission_id, timeout=0.05)

        # Assert: Cancel succeeded
        assert result is True

        # Assert: Cleanup still happened despite timeout
        mock_session.cleanup.assert_awaited_once()

        # Assert: Resources cleaned up
        assert task_id not in task_manager.tasks_sessions
        assert task_id not in task_manager.tasks

    @pytest.mark.asyncio
    async def test_cleanup_task_execution_order_verified(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cleanup executes: session.cleanup() then dict removal."""
        task_id = "order_test"
        mission_id = "missions:order"

        execution_order = []

        mock_session = Mock(spec=TaskSession)
        mock_session.status = TaskStatus.PENDING
        mock_session.cancellation_reason = CancellationReason.UNKNOWN

        # Track when cleanup is called and when session is still in dict
        async def track_cleanup() -> None:
            # At this point, session should still be in dict
            if task_id in task_manager.tasks_sessions:
                execution_order.append("cleanup_while_in_dict")
            else:
                execution_order.append("cleanup_after_removed")

        mock_session.cleanup = AsyncMock(side_effect=track_cleanup)
        task_manager.tasks_sessions[task_id] = mock_session

        await task_manager._cleanup_task(task_id, mission_id)

        # Assert: cleanup was called while still in dict (proper order)
        assert execution_order == ["cleanup_while_in_dict"]

        # Assert: Now removed
        assert task_id not in task_manager.tasks_sessions

    @pytest.mark.asyncio
    async def test_cleanup_nonexistent_task(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cleanup of completely nonexistent task doesn't fail."""
        # Task not in any dictionary - should not raise
        await task_manager._cleanup_task("nonexistent", "missions:nonexistent")

        # Also test via cancel_task
        result = await task_manager.cancel_task("nonexistent2", "missions:cancel")
        assert result is True


# ============================================================================
# Test: Signal Operations
# ============================================================================


class TestSignalOperations:
    """Tests for signal sending operations."""

    @pytest.mark.asyncio
    async def test_send_signal_success(
        self,
        task_manager: ConcreteTaskManager,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test successful signal sending."""
        task_id = "signal_test"
        mission_id = "missions:signal"

        task_manager.channel = mock_surreal_connection
        task_manager.tasks_sessions[task_id] = mock_task_session

        result = await task_manager.send_signal(task_id, mission_id, SignalType.CANCEL, {"key": "value"})

        assert result is True
        mock_task_session.db.update.assert_awaited_once_with(
            "signals", task_id, {"type": SignalType.CANCEL, "payload": {"key": "value"}}
        )

    @pytest.mark.asyncio
    async def test_send_signal_unknown_task(
        self,
        task_manager: ConcreteTaskManager,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test signal sending to unknown task returns False."""
        task_manager.channel = mock_surreal_connection

        result = await task_manager.send_signal("unknown", "missions:signal", SignalType.CANCEL, {})

        assert result is False
        mock_surreal_connection.update.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_task_status(
        self,
        task_manager: ConcreteTaskManager,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test get_task_status sends status signal."""
        task_id = "status_test"
        mission_id = "missions:status"

        task_manager.channel = mock_surreal_connection
        task_manager.tasks_sessions[task_id] = mock_task_session

        result = await task_manager.get_task_status(task_id, mission_id)

        assert result is True
        mock_task_session.db.update.assert_awaited_once_with("signals", task_id, {"type": "status", "payload": {}})


# ============================================================================
# Test: Cancellation
# ============================================================================


class TestCancellation:
    """Tests for task cancellation logic."""

    @pytest.mark.asyncio
    async def test_cancel_task_graceful(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test graceful task cancellation."""
        task_id = "cancel_graceful"
        mission_id = "missions:cancel"

        async def quick_task() -> None:
            await asyncio.sleep(0.05)

        task = asyncio.create_task(quick_task())
        task_manager.tasks[task_id] = task

        result = await task_manager.cancel_task(task_id, mission_id, timeout=1.0)

        assert result is True
        assert task.done()

    @pytest.mark.asyncio
    async def test_cancel_task_force_after_timeout(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test force cancellation after timeout."""
        task_id = "cancel_force"
        mission_id = "missions:force"

        async def long_task() -> None:
            await asyncio.sleep(10)

        task = asyncio.create_task(long_task())
        task_manager.tasks[task_id] = task

        result = await task_manager.cancel_task(task_id, mission_id, timeout=0.05)

        assert result is True
        assert task.done()
        assert task.cancelled()

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_task(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cancelling nonexistent task returns True."""
        result = await task_manager.cancel_task("nonexistent", "missions:cancel")

        assert result is True

    @pytest.mark.asyncio
    async def test_cancel_all_tasks(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cancelling all tasks."""
        mission_id = "missions:cancel_all"

        async def task_work() -> None:
            await asyncio.sleep(10)

        # Create multiple tasks
        for i in range(3):
            task_id = f"task_{i}"
            task = asyncio.create_task(task_work())
            task_manager.tasks[task_id] = task

        results = await task_manager.cancel_all_tasks(mission_id, timeout=0.1)

        assert len(results) == 3
        assert all(results.values())

    @pytest.mark.asyncio
    async def test_cancel_all_tasks_empty(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cancel_all_tasks with no running tasks."""
        results = await task_manager.cancel_all_tasks("missions:empty")

        assert len(results) == 0


# ============================================================================
# Test: Shutdown
# ============================================================================


class TestShutdown:
    """Tests for shutdown coordination."""

    @pytest.mark.asyncio
    async def test_shutdown_sets_event(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test shutdown sets shutdown event."""
        assert not task_manager._shutdown_event.is_set()

        await task_manager.shutdown("missions:shutdown")

        assert task_manager._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_shutdown_cancels_tasks(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test shutdown cancels all running tasks."""
        mission_id = "missions:shutdown"

        async def task_work() -> None:
            await asyncio.sleep(10)

        # Create tasks
        for i in range(2):
            task_id = f"shutdown_task_{i}"
            task = asyncio.create_task(task_work())
            task_manager.tasks[task_id] = task

        await task_manager.shutdown(mission_id, timeout=0.1)

        # All tasks should be done
        for task in task_manager.tasks.values():
            assert task.done()

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test shutdown can be called multiple times."""
        await task_manager.shutdown("missions:shutdown")
        assert task_manager._shutdown_event.is_set()

        # Should not raise
        await task_manager.shutdown("missions:shutdown")
        assert task_manager._shutdown_event.is_set()


# ============================================================================
# Test: Properties
# ============================================================================


class TestProperties:
    """Tests for property accessors."""

    @pytest.mark.asyncio
    async def test_task_count_property(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test task_count property."""
        assert task_manager.task_count == 0

        # Add sessions
        task_manager.tasks_sessions["t1"] = Mock()
        task_manager.tasks_sessions["t2"] = Mock()

        assert task_manager.task_count == 2

    @pytest.mark.asyncio
    async def test_running_tasks_property(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test running_tasks property."""
        assert len(task_manager.running_tasks) == 0

        # Add running task
        async def task_work() -> None:
            await asyncio.sleep(10)

        task = asyncio.create_task(task_work())
        task_manager.tasks["running"] = task

        # Add completed task
        async def quick_work() -> None:
            pass

        completed_task = asyncio.create_task(quick_work())
        await completed_task
        task_manager.tasks["completed"] = completed_task

        running = task_manager.running_tasks

        assert "running" in running
        assert "completed" not in running
        assert len(running) == 1

        # Cleanup
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_shutdown_event_internal_access(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test _shutdown_event internal attribute access."""
        event = task_manager._shutdown_event

        assert isinstance(event, asyncio.Event)
        assert not event.is_set()

        event.set()
        assert event.is_set()


# ============================================================================
# Test: Async Context Manager
# ============================================================================


class TestAsyncContextManager:
    """Tests for async context manager functionality."""

    @pytest.mark.asyncio
    async def test_context_manager_basic_usage(
        self,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test basic async context manager usage."""
        with (
            patch(
                "digitalkin.core.task_manager.base_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.base_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            async with ConcreteTaskManager() as manager:
                # Manager should be usable within context
                assert isinstance(manager, ConcreteTaskManager)
                assert len(manager.tasks) == 0
                assert len(manager.tasks_sessions) == 0

                # Create a task within context
                await manager.create_task(
                    "context_task",
                    "missions:context",
                    mock_base_module,
                    asyncio.sleep(0.01),
                )

                assert "context_task" in manager.tasks_sessions

            # After exit, shutdown should have been called
            assert manager._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_context_manager_calls_shutdown_on_exit(
        self,
    ) -> None:
        """Test that __aexit__ calls shutdown."""
        shutdown_called = False
        shutdown_mission_id = None

        async def mock_shutdown(mission_id: str) -> None:
            nonlocal shutdown_called, shutdown_mission_id
            shutdown_called = True
            shutdown_mission_id = mission_id

        manager = ConcreteTaskManager()
        manager.shutdown = mock_shutdown  # type: ignore

        async with manager:
            pass

        # Verify shutdown was called with correct mission_id
        assert shutdown_called
        assert shutdown_mission_id == "context_manager_cleanup"

    @pytest.mark.asyncio
    async def test_context_manager_cleanup_on_exception(
        self,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test context manager cleans up even when exception occurs."""
        with (
            patch(
                "digitalkin.core.task_manager.base_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.base_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            manager = ConcreteTaskManager()

            try:
                async with manager:
                    # Create a task
                    await manager.create_task(
                        "exception_task",
                        "missions:exception",
                        mock_base_module,
                        asyncio.sleep(0.01),
                    )

                    # Raise an exception
                    msg = "Test exception"
                    raise ValueError(msg)
            except ValueError:
                pass

            # Shutdown should still have been called
            assert manager._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_context_manager_nested_usage(
        self,
    ) -> None:
        """Test nested context manager usage."""
        async with ConcreteTaskManager() as outer_manager:
            assert not outer_manager._shutdown_event.is_set()

            async with ConcreteTaskManager() as inner_manager:
                assert not inner_manager._shutdown_event.is_set()
                assert not outer_manager._shutdown_event.is_set()

            # Inner should be shut down, outer still active
            assert inner_manager._shutdown_event.is_set()
            assert not outer_manager._shutdown_event.is_set()

        # Both should be shut down
        assert outer_manager._shutdown_event.is_set()
        assert inner_manager._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_context_manager_returns_self(
        self,
    ) -> None:
        """Test that __aenter__ returns self."""
        manager = ConcreteTaskManager()

        async with manager as context_var:
            assert context_var is manager
            assert id(context_var) == id(manager)

    @pytest.mark.asyncio
    async def test_context_manager_with_running_tasks(
        self,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test context manager properly cancels running tasks on exit."""
        with (
            patch(
                "digitalkin.core.task_manager.base_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.base_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            tasks_created = []

            async with ConcreteTaskManager() as manager:
                # Create multiple long-running tasks
                for i in range(3):
                    task_id = f"long_task_{i}"

                    async def long_work() -> None:
                        await asyncio.sleep(10)

                    await manager.create_task(
                        task_id,
                        "missions:long",
                        mock_base_module,
                        long_work(),
                    )

                    if task_id in manager.tasks:
                        tasks_created.append(manager.tasks[task_id])

            # All tasks should be cancelled after exit
            for task in tasks_created:
                assert task.done() or task.cancelled()

    @pytest.mark.asyncio
    async def test_context_manager_exception_info_passed(
        self,
    ) -> None:
        """Test that exception info is passed to __aexit__."""
        exc_type_seen = None
        exc_val_seen = None
        exc_tb_seen = None

        class TrackingManager(ConcreteTaskManager):
            async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
                nonlocal exc_type_seen, exc_val_seen, exc_tb_seen
                exc_type_seen = exc_type
                exc_val_seen = exc_val
                exc_tb_seen = exc_tb
                await super().__aexit__(exc_type, exc_val, exc_tb)

        try:
            async with TrackingManager():
                msg = "Test error"
                raise RuntimeError(msg)
        except RuntimeError:
            pass

        # Verify exception info was passed
        assert exc_type_seen == RuntimeError
        assert str(exc_val_seen) == "Test error"
        assert exc_tb_seen is not None

    @pytest.mark.asyncio
    async def test_context_manager_multiple_sequential_uses(
        self,
    ) -> None:
        """Test that same manager instance can be used multiple times."""
        manager = ConcreteTaskManager()

        # First usage
        async with manager:
            assert not manager._shutdown_event.is_set()

        assert manager._shutdown_event.is_set()

        # Clear the event manually for test
        manager._shutdown_event.clear()

        # Second usage should work
        async with manager:
            assert not manager._shutdown_event.is_set()

        assert manager._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_context_manager_with_session_cleanup(
        self,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test that context manager properly cleans up sessions."""
        with patch(
            "digitalkin.core.task_manager.base_task_manager.SurrealDBConnection",
            return_value=mock_surreal_connection,
        ):
            session_db_connections = []

            async with ConcreteTaskManager() as manager:
                # Create tasks with mock sessions
                for i in range(3):
                    task_id = f"session_task_{i}"
                    mock_session = Mock(spec=TaskSession)
                    mock_db = AsyncMock()
                    mock_db.close = AsyncMock()
                    mock_session.db = mock_db
                    mock_session.queue = asyncio.Queue()
                    mock_session.status = TaskStatus.RUNNING
                    mock_session.cancellation_reason = CancellationReason.UNKNOWN

                    # Add cleanup method that calls db.close()
                    # Use default argument to capture current value
                    def make_cleanup(session):
                        async def cleanup_impl() -> None:
                            await session.db.close()

                        return cleanup_impl

                    mock_session.cleanup = AsyncMock(side_effect=make_cleanup(mock_session))

                    manager.tasks_sessions[task_id] = mock_session
                    session_db_connections.append(mock_db)

                    # Create a dummy task
                    manager.tasks[task_id] = asyncio.create_task(asyncio.sleep(0.01))

            # After context exit, all DB connections should be closed
            for mock_db in session_db_connections:
                mock_db.close.assert_awaited()

            # Sessions should be cleaned up
            assert len(manager.tasks_sessions) == 0


# ============================================================================
# Test: Edge Cases
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_empty_task_id(
        self,
        task_manager: ConcreteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test with empty task_id."""
        task_id = ""
        mission_id = "missions:empty"

        async def work() -> None:
            await asyncio.sleep(0.01)

        with (
            patch(
                "digitalkin.core.task_manager.base_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.base_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection
            await task_manager.create_task(task_id, mission_id, mock_base_module, work())

            assert task_id in task_manager.tasks_sessions

    @pytest.mark.asyncio
    async def test_very_long_task_id(
        self,
        task_manager: ConcreteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test with very long task_id."""
        task_id = "x" * 1000
        mission_id = "missions:long"

        async def work() -> None:
            await asyncio.sleep(0.01)

        with (
            patch(
                "digitalkin.core.task_manager.base_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.base_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection
            await task_manager.create_task(task_id, mission_id, mock_base_module, work())

            assert task_id in task_manager.tasks_sessions


# ============================================================================
# Test: Integration Tests
# ============================================================================


class TestCleanupIntegration:
    """Integration tests verifying cleanup through full task lifecycle.

    These tests verify the complete workflow from task creation through
    cancellation and cleanup, ensuring proper integration between components.
    """

    @pytest.mark.asyncio
    async def test_cleanup_full_integration_real_task(
        self,
        task_manager: ConcreteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Integration test: Create task, run it, cancel it, verify cleanup.

        This test exercises the complete task lifecycle to ensure cleanup
        happens correctly in a realistic scenario.
        """
        task_id = "integration_test"
        mission_id = "missions:integration"

        # Create a task that runs for a bit
        async def work_task() -> None:
            await asyncio.sleep(0.5)

        # Setup patches for task creation
        with (
            patch(
                "digitalkin.core.task_manager.base_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.base_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection

            # Create task
            await task_manager.create_task(task_id, mission_id, mock_base_module, work_task())

            # Verify task created
            assert task_id in task_manager.tasks
            assert task_id in task_manager.tasks_sessions

            # Cancel task
            result = await task_manager.cancel_task(task_id, mission_id, timeout=0.1)

            # Assert: Cancel succeeded
            assert result is True

            # Assert: cleanup() was called during cancel
            mock_task_session.cleanup.assert_awaited()

            # Assert: All resources released
            assert task_id not in task_manager.tasks
            assert task_id not in task_manager.tasks_sessions

    @pytest.mark.asyncio
    async def test_shutdown_calls_cleanup_for_all_tasks(
        self,
        task_manager: ConcreteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
    ) -> None:
        """Integration test: shutdown cleans up all running tasks.

        Verifies that shutdown properly calls cleanup() for each session.
        """
        mission_id = "missions:shutdown_cleanup"

        # Create multiple mock sessions
        sessions = {}
        for i in range(3):
            task_id = f"task_{i}"
            mock_session = Mock(spec=TaskSession)
            mock_session.cleanup = AsyncMock()
            mock_session.status = TaskStatus.RUNNING
            mock_session.cancellation_reason = CancellationReason.UNKNOWN
            sessions[task_id] = mock_session
            task_manager.tasks_sessions[task_id] = mock_session

            # Create long-running task
            async def long_task() -> None:
                await asyncio.sleep(100)

            task_manager.tasks[task_id] = asyncio.create_task(long_task())

        # Execute shutdown
        await task_manager.shutdown(mission_id, timeout=0.1)

        # Assert: cleanup called for all sessions
        for task_id, session in sessions.items():
            session.cleanup.assert_awaited_once()

        # Assert: All cleaned up
        assert len(task_manager.tasks) == 0
        assert len(task_manager.tasks_sessions) == 0

    @pytest.mark.asyncio
    async def test_multiple_concurrent_cancels_with_cleanup(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Integration test: concurrent cancellations all cleanup properly.

        Tests that cleanup works correctly under concurrent load.
        """
        mission_id = "missions:concurrent"

        # Create multiple tasks
        sessions = {}
        cancel_coros = []

        for i in range(5):
            task_id = f"concurrent_{i}"

            # Create mock session
            mock_session = Mock(spec=TaskSession)
            mock_session.cleanup = AsyncMock()
            mock_session.status = TaskStatus.RUNNING
            mock_session.cancellation_reason = CancellationReason.UNKNOWN
            sessions[task_id] = mock_session
            task_manager.tasks_sessions[task_id] = mock_session

            # Create task
            async def work() -> None:
                await asyncio.sleep(10)

            task_manager.tasks[task_id] = asyncio.create_task(work())

            # Prepare cancel coroutine
            cancel_coros.append(task_manager.cancel_task(task_id, mission_id, timeout=0.05))

        # Execute all cancels concurrently
        results = await asyncio.gather(*cancel_coros)

        # Assert: All cancels succeeded
        assert all(results)

        # Assert: All sessions cleaned up
        for session in sessions.values():
            session.cleanup.assert_awaited_once()

        # Assert: All resources released
        assert len(task_manager.tasks) == 0
        assert len(task_manager.tasks_sessions) == 0
