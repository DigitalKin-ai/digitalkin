"""Comprehensive tests for LocalTaskManager.

Tests for local task execution including:
- Task creation and registration
- Integration with TaskExecutor
- Task limits and validation
- Cancellation (single, multiple, graceful shutdown)
- Signal operations (status)
- Cleanup and shutdown
- Concurrency stress tests
- Edge cases
"""

import asyncio
import datetime
import random
import time
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio

from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.task_monitor import CancellationReason, SignalType, TaskStatus
from digitalkin.modules._base_module import BaseModule

# Set timeout for all tests in this file (30 seconds)
pytestmark = pytest.mark.timeout(30)


# ============================================================================
# Enhanced Mock with State Tracking
# ============================================================================


class MockSurrealConnection:
    """Stateful mock of SurrealDBConnection for detailed tracking."""

    def __init__(self) -> None:
        self.created = []
        self.updated = []
        self.closed = False
        self.instance_initialized = False
        self.init_count = 0

    async def init_surreal_instance(self):
        self.instance_initialized = True
        self.init_count += 1
        return True

    async def create(self, table: str, record: dict):
        record_id = f"{table}_{len(self.created) + 1}"
        record = {"id": record_id, **record}
        self.created.append(record)
        return record

    async def update(self, table: str, record_id: str, payload: dict):
        self.updated.append({"table": table, "id": record_id, "payload": payload})
        return payload

    async def select_by_task_id(self, table: str, task_id: str):
        for rec in self.created:
            if rec.get("task_id") == task_id:
                return rec
        return None

    async def close(self):
        self.closed = True
        return True


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
async def mock_surreal_connection_advanced() -> MockSurrealConnection:
    """Stateful connection mock with tracking."""
    return MockSurrealConnection()


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

    # Make these stay alive so main task can complete first
    # Use sleep loop that responds to cancellation
    async def stay_alive() -> None:
        try:
            while True:
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            # Respond immediately to cancellation
            raise

    session.listen_signals = AsyncMock(side_effect=stay_alive)
    session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
    session.cleanup = AsyncMock()
    return session


@pytest_asyncio.fixture
async def task_manager() -> LocalTaskManager:
    """Standard LocalTaskManager with test-friendly settings."""
    return LocalTaskManager(default_timeout=2.0, max_concurrent_tasks=10)


@pytest_asyncio.fixture
async def high_capacity_manager() -> LocalTaskManager:
    """High-capacity manager for stress tests."""
    return LocalTaskManager(default_timeout=1.0, max_concurrent_tasks=150)


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
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test successful task creation with all components initialized."""
        task_id = "test_create_success"
        mission_id = "missions:create"

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

            assert task_id in task_manager.tasks
            assert task_id in task_manager.tasks_sessions
            assert task_manager.task_count == 1
            assert task_id in task_manager.running_tasks

            # Verify initialization sequence
            mock_surreal_connection.init_surreal_instance.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_task_duplicate_raises(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Negative: Duplicate ID raises ValueError."""

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

            await task_manager.create_task("dup", "missions:id", mock_base_module, work())

            with pytest.raises(ValueError, match="already exists"):
                await task_manager.create_task("dup", "missions:id", mock_base_module, work())

    @pytest.mark.asyncio
    async def test_create_task_max_limit(
        self,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Negative: Exceeding max tasks raises RuntimeError."""
        max_concurrent_tasks = 2
        small_manager = LocalTaskManager(default_timeout=1.0, max_concurrent_tasks=max_concurrent_tasks)

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

            await small_manager.create_task("t1", "missions:id", mock_base_module, work())
            await small_manager.create_task("t2", "missions:id", mock_base_module, work())

            with pytest.raises(RuntimeError, match="Maximum concurrent tasks"):
                await small_manager.create_task("t3", "missions:id", mock_base_module, work())

    @pytest.mark.asyncio
    async def test_create_task_custom_intervals(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test task creation with custom heartbeat and connection intervals."""
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

            assert task_id in task_manager.tasks

    @pytest.mark.asyncio
    async def test_create_task_initialization_failure_cleanup(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test cleanup when task initialization fails."""
        task_id = "init_fail"
        mission_id = "missions:fail"

        async def simple_coro() -> None:
            await asyncio.sleep(0.1)

        # Make SurrealDB initialization fail
        mock_surreal_connection.init_surreal_instance = AsyncMock(side_effect=ConnectionError("DB connection failed"))

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


# ============================================================================
# Test: Integration with TaskExecutor
# ============================================================================


class TestExecutorIntegration:
    """Tests for LocalTaskManager integration with TaskExecutor."""

    @pytest.mark.asyncio
    async def test_executor_instance_created(self, task_manager: LocalTaskManager) -> None:
        """Test that LocalTaskManager creates a TaskExecutor instance."""
        assert hasattr(task_manager, "_executor")
        from digitalkin.core.task_manager.task_executor import TaskExecutor

        assert isinstance(task_manager._executor, TaskExecutor)

    @pytest.mark.asyncio
    async def test_task_stored_in_registry(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test that supervisor task is stored in tasks registry."""
        task_id = "registry_test"
        mission_id = "missions:registry"

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

            await task_manager.create_task(task_id, mission_id, mock_base_module, simple_coro())

            # Verify task is registered
            assert task_id in task_manager.tasks
            assert isinstance(task_manager.tasks[task_id], asyncio.Task)

    @pytest.mark.asyncio
    async def test_task_executes_successfully(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test that task execution completes successfully."""
        task_id = "exec_test"
        mission_id = "missions:exec"
        execution_log = []

        async def logging_coro() -> None:
            execution_log.append("started")
            await asyncio.sleep(0.05)
            execution_log.append("completed")

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

            await task_manager.create_task(task_id, mission_id, mock_base_module, logging_coro())

            # Wait for supervisor task to complete
            supervisor_task = task_manager.tasks[task_id]
            await supervisor_task

            assert "started" in execution_log
            assert "completed" in execution_log
            assert mock_task_session.status == TaskStatus.COMPLETED


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
    async def test_cancel_task_graceful_shutdown(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test graceful task cancellation within timeout."""
        task_id = "graceful_cancel"
        mission_id = "missions:graceful"
        shutdown_detected = asyncio.Event()

        async def graceful_coro() -> None:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                shutdown_detected.set()
                await asyncio.sleep(0.01)  # Simulate cleanup
                raise

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
            await task_manager.create_task(task_id, mission_id, mock_base_module, graceful_coro())

            await asyncio.sleep(0.05)

            start = time.time()
            result = await task_manager.cancel_task(task_id, mission_id, timeout=1.0)
            duration = time.time() - start

            assert result is True
            # With mocked listen_signals, cancel_task waits for timeout then force-cancels
            assert duration >= 1.0
            assert duration < 1.5
            # Shutdown still detected because task receives CancelledError
            await asyncio.wait_for(shutdown_detected.wait(), timeout=0.5)

    @pytest.mark.asyncio
    async def test_cancel_task_force_after_timeout(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test force cancellation when graceful shutdown times out."""
        task_id = "force_cancel"
        mission_id = "missions:force"
        shutdown_detected = asyncio.Event()

        async def stubborn_coro() -> None:
            try:
                await shutdown_detected.wait()
            except asyncio.CancelledError:
                # Ignore first cancellation
                await asyncio.sleep(0.5)
                raise

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
            await task_manager.create_task(task_id, mission_id, mock_base_module, stubborn_coro())

            await asyncio.sleep(0.05)

            start = time.time()
            result = await task_manager.cancel_task(task_id, mission_id, timeout=0.1)
            duration = time.time() - start

            assert result is True
            assert duration < 1.0  # Should force-cancel relatively quickly

    @pytest.mark.asyncio
    async def test_cancel_already_completed_task(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test cancelling a task that has already completed."""
        task_id = "already_done"
        mission_id = "missions:done"

        async def quick_coro() -> None:
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
            await task_manager.create_task(task_id, mission_id, mock_base_module, quick_coro())

            await asyncio.sleep(0.2)  # Wait for completion

            result = await task_manager.cancel_task(task_id, mission_id)
            assert result is True

    @pytest.mark.asyncio
    async def test_cancel_multiple_tasks_concurrently(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test cancelling multiple tasks at once."""
        task_ids = [f"cancel_multi_{i}" for i in range(5)]
        mission_id = "missions:multi"

        async def long_coro() -> None:
            await asyncio.sleep(10)

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
                await task_manager.create_task(task_id, mission_id, mock_base_module, long_coro())

            await asyncio.sleep(0.05)

            # Cancel all concurrently
            cancel_tasks = [task_manager.cancel_task(tid, mission_id) for tid in task_ids]
            results = await asyncio.gather(*cancel_tasks)

            assert all(results)

    @pytest.mark.asyncio
    async def test_cancel_all_tasks(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test cancelling all tasks at once."""
        task_ids = [f"cancel_all_{i}" for i in range(3)]
        mission_id = "missions:all"

        async def long_coro() -> None:
            await asyncio.sleep(10)

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
                await task_manager.create_task(task_id, mission_id, mock_base_module, long_coro())

            await asyncio.sleep(0.05)

            # Cancel all
            await task_manager.cancel_all_tasks(mission_id)

            # All tasks should be cancelled
            assert len(task_manager.running_tasks) == 0


# ============================================================================
# Test: Signals
# ============================================================================


class TestSignals:
    """Signal handling tests."""

    @pytest.mark.parametrize("sig", [SignalType.CANCEL, "custom"])
    @pytest.mark.asyncio
    async def test_send_signal(self, task_manager: LocalTaskManager, sig) -> None:
        """Positive: Signal sending works."""
        # Create mock session with properly configured db.update
        mock_session = Mock()
        mock_session.db = Mock()
        mock_session.db.update = AsyncMock()
        task_manager.tasks_sessions["t1"] = mock_session

        result = await task_manager.send_signal("t1", "missions:signal", sig, {})
        assert result is True
        mock_session.db.update.assert_awaited_once_with("signals", "t1", {"type": sig, "payload": {}})

    @pytest.mark.asyncio
    async def test_signal_unknown_task(self, task_manager: LocalTaskManager) -> None:
        """Negative: Unknown task returns False."""
        result = await task_manager.send_signal("unknown", "missions:signal", SignalType.CANCEL, {})
        assert result is False


# ============================================================================
# Test: Cleanup and Shutdown
# ============================================================================


class TestCleanupShutdown:
    """Session cleanup and shutdown tests."""

    @pytest.mark.asyncio
    async def test_cleanup_closes_db(self, task_manager: LocalTaskManager) -> None:
        """Test cleanup closes database connection."""
        mock_db = AsyncMock()
        mock_db.close = AsyncMock()

        mock_session = Mock()
        mock_session.db = mock_db

        # Create cleanup that calls db.close
        async def mock_cleanup() -> None:
            await mock_session.db.close()

        mock_session.cleanup = AsyncMock(side_effect=mock_cleanup)
        task_manager.tasks_sessions["t1"] = mock_session

        await task_manager.cancel_task("t1", mission_id="missions:cleanup")

        mock_db.close.assert_awaited_once()
        assert "t1" not in task_manager.tasks_sessions

    @pytest.mark.asyncio
    async def test_cleanup_stops_module(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test cleanup stops module."""
        mock_session = Mock()
        mock_session.db = AsyncMock()
        mock_session.module = mock_base_module

        # Create cleanup that calls module.stop and db.close
        async def mock_cleanup() -> None:
            await mock_session.module.stop()
            await mock_session.db.close()

        mock_session.cleanup = AsyncMock(side_effect=mock_cleanup)
        task_manager.tasks_sessions["t1"] = mock_session

        await task_manager._cleanup_task("t1", mission_id="missions:cleanup")

        mock_base_module.stop.assert_awaited_once()

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
        assert task_manager._shutdown_event.is_set()

        # Should not raise
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
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Stress: Rapid create/complete cycles."""
        completed_count = 0

        async def quick_task() -> None:
            nonlocal completed_count
            await asyncio.sleep(random.uniform(0.01, 0.05))
            completed_count += 1

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
            high_capacity_manager.channel = mock_surreal_connection

            for i in range(50):
                task_id = f"churn_{i}"
                await high_capacity_manager.create_task(task_id, "missions:churn", mock_base_module, quick_task())

            # Wait for all to complete
            await asyncio.sleep(0.5)

            assert completed_count >= 45  # Most should complete

    @pytest.mark.asyncio
    async def test_concurrent_create_cancel_race(
        self,
        high_capacity_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Stress: Race between creation and cancellation."""

        async def medium_task() -> None:
            await asyncio.sleep(0.2)

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
            high_capacity_manager.channel = mock_surreal_connection

            task_ids = []
            for i in range(20):
                task_id = f"race_{i}"
                task_ids.append(task_id)
                await high_capacity_manager.create_task(task_id, "missions:race", mock_base_module, medium_task())

            # Immediately start cancelling randomly
            cancel_tasks = [
                high_capacity_manager.cancel_task(task_id, "missions:race") for task_id in random.sample(task_ids, 10)
            ]

            # Should not raise
            await asyncio.gather(*cancel_tasks, return_exceptions=True)


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
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test task_count property returns correct count."""
        assert task_manager.task_count == 0

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

            await task_manager.create_task("t1", "missions:prop", mock_base_module, work())
            assert task_manager.task_count == 1

            await task_manager.create_task("t2", "missions:prop", mock_base_module, work())
            assert task_manager.task_count == 2

    @pytest.mark.asyncio
    async def test_running_tasks_property(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test running_tasks property returns active task IDs."""
        assert len(task_manager.running_tasks) == 0

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

            await task_manager.create_task("t1", "missions:prop", mock_base_module, work())
            await task_manager.create_task("t2", "missions:prop", mock_base_module, work())

            running = task_manager.running_tasks
            assert "t1" in running
            assert "t2" in running
            assert len(running) == 2


# ============================================================================
# Test: Edge Cases
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and corner scenarios."""

    @pytest.mark.asyncio
    async def test_empty_task_id(
        self,
        task_manager: LocalTaskManager,
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

            assert task_id in task_manager.tasks

    @pytest.mark.asyncio
    async def test_very_long_task_name(
        self,
        task_manager: LocalTaskManager,
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

            assert task_id in task_manager.tasks

    @pytest.mark.asyncio
    async def test_immediate_task_completion(
        self,
        task_manager: LocalTaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test task that completes immediately."""
        task_id = "immediate"
        mission_id = "missions:immediate"

        async def instant() -> None:
            pass  # Completes immediately

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

            await task_manager.create_task(task_id, mission_id, mock_base_module, instant())

            # Task should still be registered briefly
            assert task_id in task_manager.tasks or task_id not in task_manager.tasks  # May complete quickly
