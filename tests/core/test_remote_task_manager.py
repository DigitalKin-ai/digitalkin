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
import datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio

from digitalkin.core.task_manager.remote_task_manager import RemoteTaskManager
from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.task_monitor import SignalType, TaskStatus
from digitalkin.modules._base_module import BaseModule

# Set timeout for all tests in this file (30 seconds)
pytestmark = pytest.mark.timeout(30)


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

    # Add cleanup method that calls db.close()
    async def mock_cleanup():
        await session.db.close()

    session.cleanup = AsyncMock(side_effect=mock_cleanup)
    return session


@pytest_asyncio.fixture
async def task_manager() -> RemoteTaskManager:
    """Standard RemoteTaskManager with test-friendly settings."""
    return RemoteTaskManager(default_timeout=2.0, max_concurrent_tasks=10)


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
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test successful task registration without execution."""
        task_id = "remote_register"
        mission_id = "missions:register"

        async def simple_coro() -> None:
            await asyncio.sleep(0.1)

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

            await task_manager.create_task(task_id, mission_id, mock_base_module, simple_coro())

            # Session created but no task in registry
            assert task_id in task_manager.tasks_sessions
            assert task_id not in task_manager.tasks  # Key difference from LocalTaskManager
            assert task_manager.task_count == 1  # One session created (task_count = len(tasks_sessions))
            assert len(task_manager.tasks) == 0  # No local task execution

    @pytest.mark.asyncio
    async def test_register_task_duplicate_raises(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test duplicate task ID raises ValueError."""

        async def work() -> None:
            await asyncio.sleep(0.5)

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

            await task_manager.create_task("dup", "missions:dup", mock_base_module, work())

            with pytest.raises(ValueError, match="already exists"):
                await task_manager.create_task("dup", "missions:dup", mock_base_module, work())

    @pytest.mark.asyncio
    async def test_register_task_max_limit(
        self,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test exceeding max tasks raises RuntimeError."""
        small_manager = RemoteTaskManager(default_timeout=1.0, max_concurrent_tasks=2)

        async def work() -> None:
            await asyncio.sleep(0.5)

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
            small_manager.channel = mock_surreal_connection

            await small_manager.create_task("t1", "missions:limit", mock_base_module, work())
            await small_manager.create_task("t2", "missions:limit", mock_base_module, work())

            with pytest.raises(RuntimeError, match="Maximum concurrent tasks"):
                await small_manager.create_task("t3", "missions:limit", mock_base_module, work())

    @pytest.mark.asyncio
    async def test_register_task_custom_intervals(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test task registration with custom heartbeat and connection intervals."""
        task_id = "custom_intervals"
        mission_id = "missions:intervals"

        async def simple_coro() -> None:
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

            await task_manager.create_task(
                task_id,
                mission_id,
                mock_base_module,
                simple_coro(),
                heartbeat_interval=datetime.timedelta(seconds=1),
                connection_timeout=datetime.timedelta(seconds=3),
            )

            assert task_id in task_manager.tasks_sessions
            assert task_id not in task_manager.tasks


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
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test that coroutine is closed immediately (not executed)."""
        task_id = "coro_close"
        mission_id = "missions:close"
        execution_log = []

        async def logging_coro() -> None:
            execution_log.append("executed")  # Should never happen
            await asyncio.sleep(0.1)

        coro = logging_coro()

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

            await task_manager.create_task(task_id, mission_id, mock_base_module, coro)

            # Wait a bit to ensure it doesn't execute
            await asyncio.sleep(0.2)

            # Coroutine should not have executed
            assert "executed" not in execution_log
            assert task_id in task_manager.tasks_sessions

    @pytest.mark.asyncio
    async def test_coroutine_closed_prevents_execution(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test that closed coroutine cannot be awaited."""
        task_id = "coro_prevent"
        mission_id = "missions:prevent"

        async def work() -> None:
            await asyncio.sleep(0.1)

        coro = work()

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

            await task_manager.create_task(task_id, mission_id, mock_base_module, coro)

            # Try to await the closed coroutine - should fail
            with pytest.raises((StopIteration, RuntimeError)):
                # Coroutine is already closed, awaiting it will raise
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
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test that session is created for signal handling."""
        task_id = "session_test"
        mission_id = "missions:session"

        async def simple_coro() -> None:
            await asyncio.sleep(0.1)

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

            await task_manager.create_task(task_id, mission_id, mock_base_module, simple_coro())

            # Session should be created
            assert task_id in task_manager.tasks_sessions
            assert task_manager.tasks_sessions[task_id] == mock_task_session

    @pytest.mark.asyncio
    async def test_db_connection_initialized(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test that DB connection is initialized."""
        task_id = "db_init"
        mission_id = "missions:db"

        async def simple_coro() -> None:
            await asyncio.sleep(0.1)

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

            await task_manager.create_task(task_id, mission_id, mock_base_module, simple_coro())

            # DB should be initialized
            mock_surreal_connection.init_surreal_instance.assert_called_once()


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
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test that tasks dict remains empty (no local execution)."""
        task_ids = [f"no_exec_{i}" for i in range(5)]

        async def work() -> None:
            await asyncio.sleep(0.1)

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

            for task_id in task_ids:
                await task_manager.create_task(task_id, "missions:no_exec", mock_base_module, work())

            # All sessions created
            assert len(task_manager.tasks_sessions) == 5
            assert task_manager.task_count == 5  # task_count = len(tasks_sessions)

            # But NO tasks in execution dict
            assert len(task_manager.tasks) == 0

    @pytest.mark.asyncio
    async def test_running_tasks_empty(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test that running_tasks property returns empty (no local execution)."""
        task_id = "running_test"
        mission_id = "missions:running"

        async def work() -> None:
            await asyncio.sleep(0.1)

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

            # No running tasks locally
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
        mock_surreal_connection: Mock,
    ) -> None:
        """Test sending signal to remote task."""
        task_id = "signal_test"

        # Create mock session with db.update
        mock_session = Mock()
        mock_session.db = Mock()
        mock_session.db.update = AsyncMock()

        task_manager.channel = mock_surreal_connection
        task_manager.tasks_sessions[task_id] = mock_session

        result = await task_manager.send_signal(task_id, "missions:signal", SignalType.CANCEL, {})
        assert result is True
        mock_session.db.update.assert_awaited_once_with("signals", task_id, {"type": SignalType.CANCEL, "payload": {}})

    @pytest.mark.asyncio
    async def test_signal_unknown_task(
        self,
        task_manager: RemoteTaskManager,
    ) -> None:
        """Test signal to unknown task returns False."""
        task_manager.channel = Mock()
        result = await task_manager.send_signal("unknown", "missions:signal", SignalType.CANCEL, {})
        assert result is False


# ============================================================================
# Test: Cleanup and Shutdown
# ============================================================================


class TestCleanupShutdown:
    """Tests for cleanup and shutdown of remote task metadata."""

    @pytest.mark.asyncio
    async def test_cleanup_closes_db(
        self,
        task_manager: RemoteTaskManager,
    ) -> None:
        """Test cleanup closes database connection."""
        mock_db = AsyncMock()
        mock_db.close = AsyncMock()

        mock_session = Mock()
        mock_session.db = mock_db

        # Create cleanup that calls db.close
        async def mock_cleanup():
            await mock_session.db.close()

        mock_session.cleanup = AsyncMock(side_effect=mock_cleanup)
        task_manager.tasks_sessions["t1"] = mock_session

        await task_manager._cleanup_task("t1", mission_id="missions:cleanup")

        mock_db.close.assert_awaited_once()
        assert "t1" not in task_manager.tasks_sessions

    @pytest.mark.asyncio
    async def test_cleanup_stops_module(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test cleanup stops module."""
        mock_session = Mock()
        mock_session.db = AsyncMock()
        mock_session.module = mock_base_module

        # Create cleanup that calls module.stop and db.close
        async def mock_cleanup():
            await mock_session.module.stop()
            await mock_session.db.close()

        mock_session.cleanup = AsyncMock(side_effect=mock_cleanup)
        task_manager.tasks_sessions["t1"] = mock_session

        await task_manager._cleanup_task("t1", mission_id="missions:cleanup")

        mock_base_module.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cleanup_on_registration_failure(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test cleanup when task registration fails."""
        task_id = "init_fail"
        mission_id = "missions:fail"

        async def simple_coro() -> None:
            await asyncio.sleep(0.1)

        # Make SurrealDB initialization fail
        mock_surreal_connection.init_surreal_instance = AsyncMock(
            side_effect=ConnectionError("DB connection failed")
        )

        with patch(
            "digitalkin.core.task_manager.base_task_manager.SurrealDBConnection",
            return_value=mock_surreal_connection,
        ):
            task_manager.channel = mock_surreal_connection

            with pytest.raises(ConnectionError, match="DB connection failed"):
                await task_manager.create_task(task_id, mission_id, mock_base_module, simple_coro())

            # Task should not be registered
            assert task_id not in task_manager.tasks
            assert task_id not in task_manager.tasks_sessions

    @pytest.mark.asyncio
    async def test_shutdown_sets_event(
        self,
        task_manager: RemoteTaskManager,
    ) -> None:
        """Test shutdown sets the shutdown event."""
        assert not task_manager._shutdown_event.is_set()

        await task_manager.shutdown("missions:shutdown")

        assert task_manager._shutdown_event.is_set()

    @pytest.mark.asyncio
    async def test_shutdown_idempotent(
        self,
        task_manager: RemoteTaskManager,
    ) -> None:
        """Test shutdown can be called multiple times safely."""
        await task_manager.shutdown("missions:shutdown")
        assert task_manager._shutdown_event.is_set()

        # Should not raise
        await task_manager.shutdown("missions:shutdown")
        assert task_manager._shutdown_event.is_set()


# ============================================================================
# Test: Cancellation (Metadata Only)
# ============================================================================


class TestCancellation:
    """Tests for cancellation of remote task metadata."""

    @pytest.mark.asyncio
    async def test_cancel_remote_task_metadata(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test cancelling remote task (only metadata, worker does actual cancel)."""
        task_id = "cancel_remote"
        mission_id = "missions:cancel"

        async def work() -> None:
            await asyncio.sleep(0.5)

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

            # Cancel returns True (metadata cancelled, signal sent)
            result = await task_manager.cancel_task(task_id, mission_id)
            assert result is True

    @pytest.mark.asyncio
    async def test_cancel_nonexistent_task(
        self,
        task_manager: RemoteTaskManager,
    ) -> None:
        """Test cancelling a task that doesn't exist."""
        result = await task_manager.cancel_task("nonexistent", "missions:cancel")
        assert result is True

    @pytest.mark.asyncio
    async def test_cancel_all_remote_tasks(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test cancelling all remote task metadata."""
        task_ids = [f"cancel_all_{i}" for i in range(3)]

        async def work() -> None:
            await asyncio.sleep(0.5)

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

            for task_id in task_ids:
                await task_manager.create_task(task_id, "missions:all", mock_base_module, work())

            # Cancel all
            await task_manager.cancel_all_tasks("missions:all")

            # Sessions should be cleaned up (no local tasks to cancel)
            # Note: cancel_all_tasks behavior depends on BaseTaskManager implementation


# ============================================================================
# Test: Edge Cases
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases specific to remote task management."""

    @pytest.mark.asyncio
    async def test_empty_task_id(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test with empty task_id (valid but unusual)."""
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
            assert task_id not in task_manager.tasks

    @pytest.mark.asyncio
    async def test_very_long_task_name(
        self,
        task_manager: RemoteTaskManager,
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

    @pytest.mark.asyncio
    async def test_multiple_sessions_no_tasks(
        self,
        task_manager: RemoteTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test that multiple sessions can exist with zero tasks."""
        task_ids = [f"session_{i}" for i in range(10)]

        async def work() -> None:
            await asyncio.sleep(0.1)

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

            for task_id in task_ids:
                await task_manager.create_task(task_id, "missions:multi", mock_base_module, work())

            # Many sessions
            assert len(task_manager.tasks_sessions) == 10
            assert task_manager.task_count == 10  # task_count = len(tasks_sessions)

            # Zero local tasks
            assert len(task_manager.tasks) == 0
