"""Comprehensive tests for BaseTaskManager.

Tests for abstract base task manager functionality including:
- Abstract method enforcement
- Validation logic (_validate_task_creation)
- Session creation (_create_session)
- Cleanup logic (_cleanup_task)
- Signal sending (send_signal, pause_task, resume_task, get_task_status)
- Cancellation logic (cancel_task, cancel_all_tasks)
- Shutdown coordination
- Property accessors (task_count, running_tasks)
"""

import asyncio
import datetime
from collections.abc import Coroutine
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio

from digitalkin.core.task_manager.base_task_manager import BaseTaskManager
from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.task_monitor import SignalType, TaskStatus
from digitalkin.modules._base_module import BaseModule

pytestmark = pytest.mark.asyncio


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
        channel, session = await self._create_session(
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
    session.started_at = None
    session.completed_at = None
    session.db = Mock()
    session.db.close = AsyncMock()
    session.listen_signals = AsyncMock(side_effect=asyncio.CancelledError())
    session.generate_heartbeats = AsyncMock(side_effect=asyncio.CancelledError())
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

    def test_must_implement_create_task(self) -> None:
        """Test that subclasses must implement create_task."""

        class IncompleteManager(BaseTaskManager):
            pass

        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteManager()  # type: ignore

    async def test_concrete_implementation_works(
        self,
        task_manager: ConcreteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test that concrete implementation with create_task works."""
        task_id = "concrete_test"
        mission_id = "missions:concrete"

        async def work() -> None:
            await asyncio.sleep(0.05)

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

            assert task_id in task_manager.tasks
            assert task_id in task_manager.tasks_sessions


# ============================================================================
# Test: Initialization
# ============================================================================


class TestInitialization:
    """Tests for BaseTaskManager initialization."""

    def test_initialization_defaults(self) -> None:
        """Test initialization with default parameters."""
        manager = ConcreteTaskManager()

        assert manager.default_timeout == 10.0
        assert manager.max_concurrent_tasks == 100
        assert len(manager.tasks) == 0
        assert len(manager.tasks_sessions) == 0
        assert isinstance(manager._shutdown_event, asyncio.Event)
        assert not manager._shutdown_event.is_set()

    def test_initialization_custom_params(self) -> None:
        """Test initialization with custom parameters."""
        manager = ConcreteTaskManager(default_timeout=5.0, max_concurrent_tasks=50)

        assert manager.default_timeout == 5.0
        assert manager.max_concurrent_tasks == 50

    def test_shutdown_event_internal_attribute(self) -> None:
        """Test that _shutdown_event internal attribute exists."""
        manager = ConcreteTaskManager()

        # Access internal attribute
        assert hasattr(manager, "_shutdown_event")
        event = manager._shutdown_event
        assert isinstance(event, asyncio.Event)
        assert not event.is_set()


# ============================================================================
# Test: Validation Logic
# ============================================================================


class TestValidation:
    """Tests for _validate_task_creation logic."""

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
    """Tests for _cleanup_task logic."""

    async def test_cleanup_closes_db(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cleanup closes database connection."""
        task_id = "cleanup_test"
        mission_id = "missions:cleanup"

        mock_db = AsyncMock()
        mock_db.close = AsyncMock()

        task_manager.tasks_sessions[task_id] = Mock()
        task_manager.tasks_sessions[task_id].db = mock_db

        await task_manager._cleanup_task(task_id, mission_id)

        mock_db.close.assert_awaited_once()

    async def test_cleanup_nonexistent_task(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cleanup of nonexistent task doesn't fail."""
        # Should not raise
        await task_manager._cleanup_task("nonexistent", "missions:cleanup")


# ============================================================================
# Test: Signal Operations
# ============================================================================


class TestSignalOperations:
    """Tests for signal sending operations."""

    async def test_send_signal_success(
        self,
        task_manager: ConcreteTaskManager,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test successful signal sending."""
        task_id = "signal_test"
        mission_id = "missions:signal"

        task_manager.channel = mock_surreal_connection
        task_manager.tasks_sessions[task_id] = Mock()

        result = await task_manager.send_signal(task_id, mission_id, SignalType.PAUSE, {"key": "value"})

        assert result is True
        mock_surreal_connection.update.assert_awaited_once_with("tasks", SignalType.PAUSE, {"key": "value"})

    async def test_send_signal_unknown_task(
        self,
        task_manager: ConcreteTaskManager,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test signal sending to unknown task returns False."""
        task_manager.channel = mock_surreal_connection

        result = await task_manager.send_signal("unknown", "missions:signal", SignalType.PAUSE, {})

        assert result is False
        mock_surreal_connection.update.assert_not_called()

    async def test_pause_task(
        self,
        task_manager: ConcreteTaskManager,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test pause_task sends pause signal."""
        task_id = "pause_test"
        mission_id = "missions:pause"

        task_manager.channel = mock_surreal_connection
        task_manager.tasks_sessions[task_id] = Mock()

        result = await task_manager.pause_task(task_id, mission_id)

        assert result is True
        mock_surreal_connection.update.assert_awaited_once_with("tasks", "pause", {})

    async def test_resume_task(
        self,
        task_manager: ConcreteTaskManager,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test resume_task sends resume signal."""
        task_id = "resume_test"
        mission_id = "missions:resume"

        task_manager.channel = mock_surreal_connection
        task_manager.tasks_sessions[task_id] = Mock()

        result = await task_manager.resume_task(task_id, mission_id)

        assert result is True
        mock_surreal_connection.update.assert_awaited_once_with("tasks", "resume", {})

    async def test_get_task_status(
        self,
        task_manager: ConcreteTaskManager,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test get_task_status sends status signal."""
        task_id = "status_test"
        mission_id = "missions:status"

        task_manager.channel = mock_surreal_connection
        task_manager.tasks_sessions[task_id] = Mock()

        result = await task_manager.get_task_status(task_id, mission_id)

        assert result is True
        mock_surreal_connection.update.assert_awaited_once_with("tasks", "status", {})


# ============================================================================
# Test: Cancellation
# ============================================================================


class TestCancellation:
    """Tests for task cancellation logic."""

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

    async def test_cancel_nonexistent_task(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test cancelling nonexistent task returns True."""
        result = await task_manager.cancel_task("nonexistent", "missions:cancel")

        assert result is True

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

    async def test_shutdown_sets_event(
        self,
        task_manager: ConcreteTaskManager,
    ) -> None:
        """Test shutdown sets shutdown event."""
        assert not task_manager._shutdown_event.is_set()

        await task_manager.shutdown("missions:shutdown")

        assert task_manager._shutdown_event.is_set()

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
        try:
            await task
        except asyncio.CancelledError:
            pass

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
# Test: Edge Cases
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

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
