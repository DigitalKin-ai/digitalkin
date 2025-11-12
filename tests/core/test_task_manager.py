"""Comprehensive production-ready tests for TaskManager.

⚠️ DEPRECATION NOTICE ⚠️
This file is DEPRECATED and all tests are skipped.
This file is kept as an artifact for backward compatibility only.

New tests should use the following files:
- test_local_task_manager.py - for LocalTaskManager tests
- test_remote_task_manager.py - for RemoteTaskManager tests
- test_task_executor.py - for TaskExecutor tests
- test_base_task_manager.py - for BaseTaskManager tests

To run tests, use:
    docker compose run --rm tests

Or for specific tests:
    docker compose run --rm tests pytest tests/core/test_local_task_manager.py

Combines behavioral coverage, timing precision, concurrency stress tests,
and advanced edge cases. Each public function has positive/negative tests
plus regression coverage for race conditions and failure modes.
"""

import asyncio
import datetime
import random
import time
from typing import NoReturn
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio

from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.task_monitor import SignalType, TaskStatus
from digitalkin.modules._base_module import BaseModule

# Mark all tests in this file as skipped (deprecated)
pytestmark = [pytest.mark.asyncio, pytest.mark.skip(reason="Deprecated - use test_local_task_manager.py instead")]

# Alias for backward compatibility in tests
TaskManager = LocalTaskManager


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
    """Mock TaskSession with, "missions:hj" expected attributes and async methods."""
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
async def task_manager() -> TaskManager:
    """Standard TaskManager with test-friendly settings."""
    return TaskManager(default_timeout=2.0, max_concurrent_tasks=10)


@pytest_asyncio.fixture
async def high_capacity_manager() -> TaskManager:
    """High-capacity manager for stress tests."""
    return TaskManager(default_timeout=1.0, max_concurrent_tasks=150)


# ============================================================================
# Utility Functions
# ============================================================================


async def wait_event(event: asyncio.Event, timeout: float = 1.0) -> bool:
    """Wait for event with timeout, return success status."""
    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        return False


# ============================================================================
# Test: Task Creation & Initialization
# ============================================================================


class TestTaskCreation:
    """Comprehensive task creation tests."""

    async def test_create_task_success(
        self,
        task_manager: TaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test successful task creation with all components initialized."""
        task_id = "test_create_success"
        mission_id = "missions:o"

        async def simple_coro() -> None:
            await asyncio.sleep(0.1)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
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

    async def test_create_task_duplicate_raises(
        self,
        task_manager,
        mock_base_module,
        mock_surreal_connection,
        mock_task_session,
    ) -> None:
        """Negative: Duplicate ID raises ValueError."""

        async def work() -> None:
            await asyncio.sleep(0.5)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection
            await task_manager.create_task("dup", "missions:eh", mock_base_module, work())

            with pytest.raises(ValueError, match="already exists"):
                await task_manager.create_task("dup", "missions:raise", mock_base_module, work())

    async def test_create_task_max_limit(
        self,
        mock_base_module,
        mock_surreal_connection,
        mock_task_session,
    ) -> None:
        """Negative: Max concurrent limit enforced."""
        mgr = TaskManager(max_concurrent_tasks=3)

        async def long_work() -> None:
            await asyncio.sleep(10)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            mgr.channel = mock_surreal_connection
            for i in range(3):
                await mgr.create_task(f"task_{i}", f"missions:{i}", mock_base_module, long_work())

            with pytest.raises(RuntimeError, match="Maximum concurrent"):
                await mgr.create_task("overflow", "missions:overflow", mock_base_module, long_work())

    async def test_create_task_with_custom_intervals(
        self,
        task_manager: TaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test task creation with custom heartbeat and connection intervals."""
        task_id = "custom_intervals_task"
        heartbeat_interval = datetime.timedelta(seconds=5)
        connection_timeout = datetime.timedelta(seconds=10)

        async def simple_coro() -> None:
            await asyncio.sleep(0.1)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
            ) as mock_session_class,
        ):
            task_manager.channel = mock_surreal_connection

            await task_manager.create_task(
                task_id,
                "missions:mock",
                mock_base_module,
                simple_coro(),
                heartbeat_interval=heartbeat_interval,
                connection_timeout=connection_timeout,
            )

            # Verify TaskSession was, "missions:hj" created with correct parameters
            mock_session_class.assert_called_once()
            call_args = mock_session_class.call_args
            assert call_args[0][4] == heartbeat_interval

    async def test_create_task_initialization_failure(
        self,
        task_manager: TaskManager,
        mock_base_module: Mock,
    ) -> None:
        """Test cleanup on task initialization failure."""
        task_id = "init_failure_task"
        mid = "missions:ghjkl"
        cleanup_called = False

        async def dummy_coro() -> None:
            pass

        async def mock_cleanup(tid: str, mission_id: str) -> None:
            nonlocal cleanup_called
            cleanup_called = True
            assert tid == task_id
            assert mission_id == mid

        task_manager._cleanup_task = mock_cleanup  # type: ignore

        with patch(
            "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
            side_effect=ConnectionError("Database unavailable"),
        ):
            with pytest.raises(ConnectionError):
                await task_manager.create_task(task_id, mid, mock_base_module, dummy_coro())

            assert cleanup_called
            assert task_id not in task_manager.tasks
            assert task_id not in task_manager.tasks_sessions

    async def test_concurrent_cancellation_and_creation(
        self,
        task_manager: TaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test creating new tasks while cancelling others."""
        mgr = TaskManager(max_concurrent_tasks=3)

        async def medium_coro() -> None:
            await asyncio.sleep(1.0)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            mgr.channel = mock_surreal_connection

            # Create initial batch
            for i in range(3):
                await mgr.create_task(f"initial_{i}", f"missions:{i}", mock_base_module, medium_coro())

            await asyncio.sleep(0.05)

            # Concurrently cancel some and create new ones
            operations = []
            operations.extend([mgr.cancel_task(f"initial_{i}", "") for i in range(2)])
            operations.extend([
                mgr.create_task(f"new_{i}", "missions:k", mock_base_module, medium_coro()) for i in range(3)
            ])

            results = await asyncio.gather(*operations, return_exceptions=True)

            # Verify no exceptions and proper state
            assert all(not isinstance(r, Exception) for r in results[:2])

    async def test_signal_send_during_task_completion(
        self,
        task_manager: TaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test sending signal while task is completing."""
        task_id = "signal_race"

        async def completing_coro() -> None:
            await asyncio.sleep(0.1)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection
            await task_manager.create_task(task_id, "missions:id", mock_base_module, completing_coro())

            # Try to send signal right as task completes
            await asyncio.sleep(0.09)

            # This might succeed or fail depending on timing
            result = await task_manager.send_signal(task_id, "missions:dfr", "pause", {})
            assert isinstance(result, bool)


# ============================================================================
# Test: Task Wrapper
# ============================================================================


@pytest.mark.skip(reason="TestTaskWrapper tests use deprecated _task_wrapper method. See test_task_executor.py for current tests.")
class TestTaskWrapper:
    """Task wrapper and supervisor tests.

    ⚠️ DEPRECATED: These tests use _task_wrapper which no longer exists.
    TaskExecutor is now tested in tests/core/test_task_executor.py
    """

    async def test_wrapper_main_task_completes_normally(
        self,
        task_manager: TaskManager,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test wrapper when main task completes successfully."""
        task_id = "main_success"
        execution_log = []
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        module = Mock(BaseModule)
        session = TaskSession(task_id, "missions:hj", mock_surreal_connection, module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        async def main_coro() -> None:
            execution_log.append("main_start")
            await asyncio.sleep(0.1)
            execution_log.append("main_end")

        task_manager.channel = mock_surreal_connection
        supervisor = await task_manager._task_wrapper(task_id, "missions:ou", main_coro(), session)

        await supervisor

        assert session.status == TaskStatus.COMPLETED
        assert "main_start" in execution_log
        assert "main_end" in execution_log
        assert session.started_at is not None
        assert session.completed_at is not None

    async def test_wrapper_main_task_raises_exception(
        self,
        task_manager: TaskManager,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test wrapper when main task raises an exception."""
        task_id = "main_exception"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        module = Mock(BaseModule)
        session = TaskSession(task_id, "missions:hj", mock_surreal_connection, module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        async def failing_coro() -> None:
            await asyncio.sleep(0.05)
            msg = "Intentional failure"
            raise ValueError(msg)

        task_manager.channel = mock_surreal_connection
        supervisor = await task_manager._task_wrapper(task_id, "missions:ou", failing_coro(), session)

        with pytest.raises(ValueError, match="Intentional failure"):
            await supervisor

        assert session.status == TaskStatus.FAILED

    async def test_wrapper_heartbeat_fails(
        self,
        task_manager: TaskManager,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test wrapper when heartbeat task fails."""
        task_id = "heartbeat_failure"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        module = Mock(BaseModule)
        session = TaskSession(task_id, "missions:hj", mock_surreal_connection, module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def failing_heartbeat() -> NoReturn:
            await asyncio.sleep(0.05)
            msg = f"Heartbeat failure for task {task_id}"
            raise RuntimeError(msg)

        session.generate_heartbeats = failing_heartbeat

        async def long_main() -> None:
            await asyncio.sleep(10)

        task_manager.channel = mock_surreal_connection
        supervisor = await task_manager._task_wrapper(task_id, "missions:ou", long_main(), session)

        with pytest.raises(RuntimeError, match=f"Heartbeat failure for task {task_id}"):
            await supervisor

        assert session.status == TaskStatus.FAILED

    async def test_wrapper_signal_stops_task(
        self,
        task_manager: TaskManager,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test wrapper when signal listener receives stop signal."""
        task_id = "signal_stop"

        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        async def signal_that_stops() -> None:
            await asyncio.sleep(0.05)
            # Return normally to simulate stop signal received

        module = Mock(BaseModule)
        session = TaskSession(task_id, "missions:hj", mock_surreal_connection, module)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = signal_that_stops

        async def long_main() -> None:
            await asyncio.sleep(10)

        task_manager.channel = mock_surreal_connection
        supervisor = await task_manager._task_wrapper(task_id, "missions:ou", long_main(), session)

        await supervisor

        assert session.status == TaskStatus.CANCELLED

    async def test_wrapper_supervisor_cancellation(
        self,
        task_manager: TaskManager,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test wrapper handling external cancellation."""
        task_id = "external_cancel"

        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        module = Mock(BaseModule)
        session = TaskSession(task_id, "missions:hj", mock_surreal_connection, module)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def long_main() -> None:
            await asyncio.sleep(10)

        task_manager.channel = mock_surreal_connection
        supervisor = await task_manager._task_wrapper(task_id, "missions:ou", long_main(), session)

        # Cancel externally after brief delay
        await asyncio.sleep(0.05)
        supervisor.cancel()

        with pytest.raises(asyncio.CancelledError):
            await supervisor

        assert session.status == TaskStatus.CANCELLED

    async def test_wrapper_concurrent_execution(
        self,
        task_manager: TaskManager,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test that main, heartbeat, and listener run concurrently."""
        task_id = "concurrent_test"
        execution_timeline = []

        session = Mock(spec=TaskSession)
        session.status = TaskStatus.PENDING

        async def heartbeat_with_logs() -> None:
            try:
                for i in range(5):
                    execution_timeline.append(f"heartbeat_{i}")
                    await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                pass

        async def listener_with_logs() -> None:
            try:
                for i in range(5):
                    execution_timeline.append(f"listener_{i}")
                    await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                pass

        session.generate_heartbeats = heartbeat_with_logs
        session.listen_signals = listener_with_logs

        async def main_with_logs() -> None:
            for i in range(3):
                execution_timeline.append(f"main_{i}")
                await asyncio.sleep(0.05)

        task_manager.channel = mock_surreal_connection
        supervisor = await task_manager._task_wrapper(task_id, "missions:ou", main_with_logs(), session)

        await supervisor

        # Verify interleaved execution
        assert len(execution_timeline) >= 3
        # Check that different tasks are interleaved (not sequential)
        main_indices = [i for i, log in enumerate(execution_timeline) if "main" in log]
        hb_indices = [i for i, log in enumerate(execution_timeline) if "heartbeat" in log]

        # They should be interleaved, not all mains first then all heartbeats
        if len(main_indices) > 1 and len(hb_indices) > 1:
            assert not (max(main_indices) < min(hb_indices))
            assert not (max(hb_indices) < min(main_indices))

    async def test_wrapper_completion_timing(
        self,
        task_manager,
        mock_surreal_connection,
    ) -> None:
        """Positive: Accurate timing and COMPLETED status."""
        task_id = "timing"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        module = Mock(BaseModule)
        session = TaskSession(task_id, "missions:hj", mock_surreal_connection, module)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def job() -> None:
            await asyncio.sleep(0.08)

        task_manager.channel = mock_surreal_connection
        start = time.monotonic()
        supervisor = await task_manager._task_wrapper(task_id, "missions:ou", job(), session)
        await supervisor
        elapsed = time.monotonic() - start

        assert session.status == TaskStatus.COMPLETED
        assert session.started_at is not None
        assert session.completed_at is not None
        recorded = (session.completed_at - session.started_at).total_seconds()
        assert abs(recorded - elapsed) < 0.07

    async def test_wrapper_exception_failed(
        self,
        task_manager,
        mock_surreal_connection,
    ) -> None:
        """Negative: Exception sets FAILED status."""
        task_id = "fail"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        module = Mock(BaseModule)
        session = TaskSession(task_id, "missions:hj", mock_surreal_connection, module)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def failing() -> NoReturn:
            msg = "boom"
            raise ValueError(msg)

        task_manager.channel = mock_surreal_connection
        supervisor = await task_manager._task_wrapper(task_id, "missions:ou", failing(), session)

        with pytest.raises(ValueError):
            await supervisor
        assert session.status == TaskStatus.FAILED


# ============================================================================
# Test: Cancellation
# ============================================================================


class TestCancellation:
    """Task cancellation tests."""

    async def test_cancel_nonexistent_task(self, task_manager: TaskManager) -> None:
        """Test cancelling a task that doesn't exist."""
        result = await task_manager.cancel_task("nonexistent_task", "missions:os")
        assert result is True

    async def test_cancel_task_graceful_shutdown(
        self,
        task_manager: TaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test graceful task cancellation within timeout."""
        task_id = "graceful_cancel"
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
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection
            await task_manager.create_task(task_id, "missions:id", mock_base_module, graceful_coro())

            await asyncio.sleep(0.05)

            start = time.time()
            result = await task_manager.cancel_task(task_id, "missions:os", timeout=1.0)
            duration = time.time() - start

            assert result is True
            assert duration < 0.5  # Should complete quickly
            await asyncio.wait_for(shutdown_detected.wait(), timeout=1.0)

    async def test_cancel_task_force_after_timeout(
        self,
        task_manager: TaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test force cancellation when graceful shutdown times out."""
        task_id = "force_cancel"
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
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection
            await task_manager.create_task(task_id, "missions:id", mock_base_module, stubborn_coro())

            await asyncio.sleep(0.05)

            start = time.time()
            result = await task_manager.cancel_task(task_id, "missions:os", timeout=0.1)
            duration = time.time() - start

            assert result is True
            assert duration < 1.0  # Should force-cancel relatively quickly

    async def test_cancel_already_completed_task(
        self,
        task_manager: TaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test cancelling a task that has already completed."""
        task_id = "already_done"

        async def quick_coro() -> None:
            await asyncio.sleep(0.05)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection
            await task_manager.create_task(task_id, "missions:id", mock_base_module, quick_coro())

            await asyncio.sleep(0.2)  # Wait for completion

            result = await task_manager.cancel_task(task_id, "missions:os")
            assert result is True

    async def test_cancel_multiple_tasks_concurrently(
        self,
        task_manager: TaskManager,
        mock_base_module: Mock,
        mock_surreal_connection: Mock,
        mock_task_session: Mock,
    ) -> None:
        """Test cancelling multiple tasks at once."""
        task_ids = [f"cancel_multi_{i}" for i in range(5)]

        async def long_coro() -> None:
            await asyncio.sleep(10)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection

            for task_id in task_ids:
                await task_manager.create_task(task_id, "missions:id", mock_base_module, long_coro())

            await asyncio.sleep(0.05)

            # Cancel all concurrently
            cancel_tasks = [task_manager.cancel_task(tid, "missions:os") for tid in task_ids]
            results = await asyncio.gather(*cancel_tasks)

            assert all(results)


# ============================================================================
# Test: Signals
# ============================================================================


class TestSignals:
    """Signal handling tests."""

    @pytest.mark.parametrize("sig", [SignalType.PAUSE, SignalType.RESUME, "custom"])
    async def test_send_signal(self, task_manager, sig) -> None:
        """Positive: Signal sending works."""
        mock_chan = AsyncMock()
        mock_chan.update = AsyncMock()
        task_manager.channel = mock_chan
        task_manager.tasks_sessions["t1"] = Mock()

        result = await task_manager.send_signal("t1", "missions:os", sig, {})
        assert result is True
        mock_chan.update.assert_awaited_once_with("tasks", sig, {})

    async def test_signal_unknown_task(self, task_manager) -> None:
        """Negative: Unknown task returns False."""
        task_manager.channel = Mock()
        result = await task_manager.send_signal("unknown", "missions:os", SignalType.PAUSE, {})
        assert result is False


# ============================================================================
# Test: Cleanup
# ============================================================================


class TestCleanup:
    """Session cleanup tests."""

    async def test_cleanup_closes_db(self, task_manager) -> None:
        """Positive: DB connection closed."""
        sess = Mock()
        sess.db = Mock()
        sess.db.close = AsyncMock()
        task_manager.tasks_sessions["clean"] = sess

        await task_manager._cleanup_task("clean", "missions:os")
        sess.db.close.assert_awaited_once()

    async def test_clean_session_success(
        self,
        task_manager,
        mock_base_module,
        mock_surreal_connection,
        mock_task_session,
    ) -> None:
        """Positive: Session cleaned properly."""
        mission_id = "missions:sgh"

        async def job() -> None:
            await asyncio.sleep(0.03)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection
            mock_task_session.module = mock_base_module

            await task_manager.create_task("clean", mission_id, mock_base_module, job())
            await asyncio.sleep(0.1)

            result = await task_manager.clean_session("clean", mission_id)
            assert result is True


# ============================================================================
# Test: Shutdown
# ============================================================================


class TestShutdown:
    """Shutdown tests."""

    async def test_shutdown_sets_event(
        self,
        task_manager,
        mock_base_module,
        mock_surreal_connection,
        mock_task_session,
    ) -> None:
        """Positive: Shutdown sets event and cancels tasks."""

        async def persistent() -> None:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                raise

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection
            for i in range(3):
                await task_manager.create_task(f"sd_{i}", f"missions:{i}", mock_base_module, persistent())

            await asyncio.sleep(0.05)
            await task_manager.shutdown("missions:oeoe", timeout=0.3)

            assert task_manager._shutdown_event.is_set()

    async def test_shutdown_idempotent(self, task_manager) -> None:
        """Positive: Multiple shutdowns safe."""
        await task_manager.shutdown("missions:dfg", timeout=0.1)
        await task_manager.shutdown("missions:dfg", timeout=0.1)
        assert task_manager._shutdown_event.is_set()


# ============================================================================
# Test: Concurrency & Stress
# ============================================================================


class TestConcurrency:
    """Concurrency and stress tests."""

    async def test_high_task_churn(
        self,
        mock_base_module,
        mock_surreal_connection,
        mock_task_session,
    ) -> None:
        """Stress: Rapid creation/completion."""
        mgr = TaskManager(max_concurrent_tasks=150, default_timeout=1.0)

        async def tiny() -> None:
            await asyncio.sleep(0.01)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            mgr.channel = mock_surreal_connection

            start = time.monotonic()
            tasks = [mgr.create_task(f"c_{i}", f"missions:os{i}", mock_base_module, tiny()) for i in range(100)]
            await asyncio.gather(*tasks)
            await asyncio.sleep(0.2)
            elapsed = time.monotonic() - start

            assert elapsed < 5.0
            assert mgr.task_count <= 100

    async def test_concurrent_create_cancel(
        self,
        mock_base_module,
        mock_surreal_connection,
        mock_task_session,
    ) -> None:
        """Stress: Create/cancel race conditions."""
        mgr = TaskManager(max_concurrent_tasks=100, default_timeout=1.0)

        async def sleeper() -> None:
            try:
                await asyncio.sleep(0.25)
            except asyncio.CancelledError:
                raise

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            mgr.channel = mock_surreal_connection
            total = 40

            # Create tasks
            await asyncio.gather(
                *(mgr.create_task(f"r_{i}", f"missions:os{i}", mock_base_module, sleeper()) for i in range(total))
            )

            # Cancel half randomly
            to_cancel = random.sample([f"r_{i}" for i in range(total)], k=total // 2)
            cancel_ops = [mgr.cancel_task(tid, "missions:def", timeout=0.05) for tid in to_cancel]

            # Create more
            new_ops = [mgr.create_task(f"new_{i}", f"missions:{i}", mock_base_module, sleeper()) for i in range(10)]

            results = await asyncio.gather(*(cancel_ops + new_ops), return_exceptions=True)

            # No unexpected exceptions
            for r in results[: len(cancel_ops)]:
                assert not isinstance(r, Exception) or isinstance(r, RuntimeError)

    async def test_signal_storm(self, task_manager) -> None:
        """Stress: Many concurrent signals."""
        mock_chan = Mock()
        mock_chan.update = AsyncMock()
        task_manager.channel = mock_chan

        task_ids = [f"storm_{i}" for i in range(10)]
        for tid in task_ids:
            task_manager.tasks_sessions[tid] = Mock()

        # 50 concurrent signals
        signal_ops = [
            task_manager.send_signal(random.choice(task_ids), "missions:defrg", "ping", {}) for _ in range(50)
        ]

        start = time.monotonic()
        results = await asyncio.gather(*signal_ops)
        elapsed = time.monotonic() - start

        assert all(results)
        assert elapsed < 1.0
        assert mock_chan.update.await_count == 50


# ============================================================================
# Test: Error Handling & Recovery
# ============================================================================


class TestErrorHandling:
    """Error handling and recovery tests."""

    async def test_main_exception_no_crash(
        self,
        task_manager,
        mock_base_module,
        mock_surreal_connection,
        mock_task_session,
    ) -> None:
        """Regression: Exception doesn't crash manager."""

        async def failing() -> NoReturn:
            await asyncio.sleep(0.02)
            msg = "boom"
            raise ValueError(msg)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection
            await task_manager.create_task("failer", "missions:failer", mock_base_module, failing())
            await asyncio.sleep(0.1)

            assert "failer" in task_manager.tasks

    async def test_cleanup_on_init_failure(self, task_manager, mock_base_module) -> None:
        """Regression: Init failure triggers cleanup."""
        cleanup_called = asyncio.Event()

        async def mock_cleanup(tid, mission_id) -> None:
            cleanup_called.set()

        task_manager._cleanup_task = mock_cleanup

        async def job() -> None:
            pass

        with patch(
            "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
            side_effect=RuntimeError("init fail"),
        ):
            with pytest.raises(RuntimeError):
                await task_manager.create_task("fail", "missions:fail", mock_base_module, job())

            assert await wait_event(cleanup_called, 1.0)


# ============================================================================
# Test: Timing & Precision
# ============================================================================


class TestTiming:
    """Timing precision tests."""

    async def test_timing_accuracy(self, task_manager, mock_surreal_connection) -> None:
        """Positive: Recorded duration matches wall clock."""
        session = Mock(spec=TaskSession)
        session.status = TaskStatus.PENDING
        session.listen_signals = AsyncMock(side_effect=asyncio.CancelledError())
        session.generate_heartbeats = AsyncMock(side_effect=asyncio.CancelledError())

        async def job() -> None:
            await asyncio.sleep(0.06)

        task_manager.channel = mock_surreal_connection
        start = time.monotonic()
        sup = await task_manager._task_wrapper("timing", "missions:dfg", job(), session)
        await sup
        elapsed = time.monotonic() - start

        recorded = (session.completed_at - session.started_at).total_seconds()
        assert abs(recorded - elapsed) < 0.07


# ============================================================================
# Test: Edge Cases
# ============================================================================


class TestEdgeCases:
    """Edge case tests."""

    async def test_empty_task_id(
        self,
        task_manager,
        mock_base_module,
        mock_surreal_connection,
        mock_task_session,
    ) -> None:
        """Edge: Empty string task ID."""

        async def job() -> None:
            await asyncio.sleep(0.05)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection
            await task_manager.create_task("", "missions:void", mock_base_module, job())
            assert "" in task_manager.tasks

    async def test_immediate_completion(self, task_manager, mock_surreal_connection) -> None:
        """Edge: Task completes immediately."""
        task_id = "test_immediate_completion"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        module = Mock(BaseModule)
        session = TaskSession(task_id, "missions:hj", mock_surreal_connection, module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        async def instant() -> None:
            return

        task_manager.channel = mock_surreal_connection
        sup = await task_manager._task_wrapper("instant", "missions:dfih", instant(), session)
        await sup

        assert session.status == TaskStatus.COMPLETED

    async def test_very_long_task_name(
        self,
        task_manager,
        mock_base_module,
        mock_surreal_connection,
        mock_task_session,
    ) -> None:
        """Edge: Very long task ID."""
        long_id = "x" * 1000

        async def job() -> None:
            await asyncio.sleep(0.05)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection
            await task_manager.create_task(long_id, "missions:mock", mock_base_module, job())
            assert long_id in task_manager.tasks


# ============================================================================
# Test: Internal State Consistency
# ============================================================================


class TestInternalState:
    """Internal state consistency tests."""

    async def test_task_count_property(
        self,
        task_manager,
        mock_base_module,
        mock_surreal_connection,
        mock_task_session,
    ) -> None:
        """Regression: task_count accurate."""

        async def job() -> None:
            await asyncio.sleep(0.05)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            task_manager.channel = mock_surreal_connection

            assert task_manager.task_count == 0
            await task_manager.create_task("t1", "missions:mock1", mock_base_module, job())
            assert task_manager.task_count == 1
            await task_manager.create_task("t2", "missions:mock2", mock_base_module, job())
            assert task_manager.task_count == 2

    async def test_running_tasks_list(
        self,
        mock_base_module,
        mock_surreal_connection,
        mock_task_session,
    ) -> None:
        """Regression: running_tasks list accurate."""
        mgr = TaskManager(max_concurrent_tasks=50)

        async def job() -> None:
            await asyncio.sleep(0.02)

        with (
            patch(
                "digitalkin.core.task_manager.local_task_manager.SurrealDBConnection",
                return_value=mock_surreal_connection,
            ),
            patch(
                "digitalkin.core.task_manager.local_task_manager.TaskSession",
                return_value=mock_task_session,
            ),
        ):
            mgr.channel = mock_surreal_connection
            await asyncio.gather(
                *(mgr.create_task(f"is_{i}", f"missions:{i}", mock_base_module, job()) for i in range(6))
            )
            await asyncio.sleep(0.05)

            assert isinstance(mgr.running_tasks, set)
            assert mgr.task_count <= 6
