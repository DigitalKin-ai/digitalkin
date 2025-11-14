"""Comprehensive tests for TaskExecutor.

Tests the supervisor pattern implementation including:
- Three concurrent tasks (main, heartbeat, signal listener)
- Outcome determination (completed, failed, cancelled)
- Exception handling and propagation
- Cleanup on cancellation
- Timing precision
"""

import asyncio
import time
from typing import NoReturn
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.core.task_manager.task_executor import TaskExecutor
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.task_monitor import TaskStatus
from digitalkin.modules._base_module import BaseModule

# Set timeout for all tests in this file (30 seconds)
pytestmark = pytest.mark.timeout(30)


# ============================================================================
# Fixtures
# ============================================================================


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
async def task_executor() -> TaskExecutor:
    """Standard TaskExecutor instance."""
    return TaskExecutor()


@pytest_asyncio.fixture
async def mock_base_module() -> Mock:
    """Mock BaseModule with async stop() method."""
    module = Mock(spec=BaseModule)
    module.stop = AsyncMock()
    return module


# ============================================================================
# Test: Main Task Completion Scenarios
# ============================================================================


class TestMainTaskCompletion:
    """Tests for normal main task completion."""

    @pytest.mark.asyncio
    async def test_main_task_completes_successfully(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test executor when main task completes successfully."""
        task_id = "main_success"
        mission_id = "missions:test"
        execution_log = []
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        async def main_coro() -> None:
            execution_log.append("main_start")
            await asyncio.sleep(0.1)
            execution_log.append("main_end")

        supervisor = await task_executor.execute_task(
            task_id, mission_id, main_coro(), session, mock_surreal_connection
        )

        await supervisor

        assert session.status == TaskStatus.COMPLETED
        assert "main_start" in execution_log
        assert "main_end" in execution_log
        assert session.started_at is not None
        assert session.completed_at is not None

    @pytest.mark.asyncio
    async def test_main_task_completion_timing_accuracy(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test accurate timing measurement for completed tasks."""
        task_id = "timing_test"
        mission_id = "missions:timing"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def job() -> None:
            await asyncio.sleep(0.08)

        start = time.monotonic()
        supervisor = await task_executor.execute_task(
            task_id, mission_id, job(), session, mock_surreal_connection
        )
        await supervisor
        elapsed = time.monotonic() - start

        assert session.status == TaskStatus.COMPLETED
        assert session.started_at is not None
        assert session.completed_at is not None
        recorded = (session.completed_at - session.started_at).total_seconds()
        assert abs(recorded - elapsed) < 0.07

    @pytest.mark.asyncio
    async def test_main_task_with_result_value(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that main task can return values (though supervisor returns None)."""
        task_id = "result_test"
        mission_id = "missions:result"
        running_event = asyncio.Event()
        result_log = []

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        async def main_with_result() -> None:
            result_log.append(42)
            await asyncio.sleep(0.05)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, main_with_result(), session, mock_surreal_connection
        )

        await supervisor

        assert session.status == TaskStatus.COMPLETED
        assert result_log == [42]


# ============================================================================
# Test: Exception Handling
# ============================================================================


class TestExceptionHandling:
    """Tests for exception handling in various scenarios."""

    @pytest.mark.asyncio
    async def test_main_task_raises_exception(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test executor when main task raises an exception."""
        task_id = "main_exception"
        mission_id = "missions:error"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        async def failing_coro() -> None:
            await asyncio.sleep(0.05)
            msg = "Intentional failure"
            raise ValueError(msg)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, failing_coro(), session, mock_surreal_connection
        )

        with pytest.raises(ValueError, match="Intentional failure"):
            await supervisor

        assert session.status == TaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_exception_sets_failed_status(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that any exception sets status to FAILED."""
        task_id = "fail"
        mission_id = "missions:fail"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def failing() -> NoReturn:
            msg = "boom"
            raise ValueError(msg)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, failing(), session, mock_surreal_connection
        )

        with pytest.raises(ValueError):
            await supervisor
        assert session.status == TaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_exception_propagated_to_caller(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that exceptions are properly propagated to the caller."""
        task_id = "propagate"
        mission_id = "missions:propagate"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        class CustomError(Exception):
            pass

        async def custom_failure() -> NoReturn:
            await asyncio.sleep(0.01)
            raise CustomError("custom error message")

        supervisor = await task_executor.execute_task(
            task_id, mission_id, custom_failure(), session, mock_surreal_connection
        )

        with pytest.raises(CustomError, match="custom error message"):
            await supervisor


# ============================================================================
# Test: Heartbeat Failures
# ============================================================================


class TestHeartbeatFailures:
    """Tests for heartbeat task failure scenarios."""

    @pytest.mark.asyncio
    async def test_heartbeat_failure_stops_task(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test executor when heartbeat task fails."""
        task_id = "heartbeat_failure"
        mission_id = "missions:hb_fail"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def failing_heartbeat() -> NoReturn:
            await asyncio.sleep(0.05)
            msg = f"Heartbeat stopped for {task_id}"
            raise RuntimeError(msg)

        session.generate_heartbeats = failing_heartbeat

        async def long_main() -> None:
            await asyncio.sleep(10)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_main(), session, mock_surreal_connection
        )

        with pytest.raises(RuntimeError, match=f"Heartbeat stopped for {task_id}"):
            await supervisor

        assert session.status == TaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_heartbeat_failure_sets_status_correctly(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that heartbeat failure correctly sets FAILED status."""
        task_id = "hb_status"
        mission_id = "missions:hb_status"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def failing_heartbeat() -> NoReturn:
            await asyncio.sleep(0.02)
            msg = f"Heartbeat stopped for {task_id}"
            raise RuntimeError(msg)

        session.generate_heartbeats = failing_heartbeat

        async def long_main() -> None:
            await asyncio.sleep(10)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_main(), session, mock_surreal_connection
        )

        try:
            await supervisor
        except RuntimeError:
            pass

        assert session.status == TaskStatus.FAILED
        assert session.completed_at is not None


# ============================================================================
# Test: Signal Listener Scenarios
# ============================================================================


class TestSignalListener:
    """Tests for signal listener behavior."""

    @pytest.mark.asyncio
    async def test_signal_listener_stops_task(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test executor when signal listener receives stop signal."""
        task_id = "signal_stop"
        mission_id = "missions:signal"

        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        async def signal_that_stops() -> None:
            await asyncio.sleep(0.05)
            # Return normally to simulate stop signal received

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = signal_that_stops

        async def long_main() -> None:
            await asyncio.sleep(10)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_main(), session, mock_surreal_connection
        )

        await supervisor

        assert session.status == TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_signal_creates_start_message(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that signal wrapper creates START signal message."""
        task_id = "signal_start"
        mission_id = "missions:start_signal"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        async def quick_task() -> None:
            await asyncio.sleep(0.05)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, quick_task(), session, mock_surreal_connection
        )

        await supervisor

        # Verify START signal was created
        assert mock_surreal_connection.create.call_count >= 2  # START and STOP
        calls = mock_surreal_connection.create.call_args_list
        start_call = calls[0]
        assert start_call[0][0] == "tasks"
        assert start_call[0][1]["task_id"] == task_id
        assert start_call[0][1]["action"] == "start"

    @pytest.mark.asyncio
    async def test_signal_creates_stop_message(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that signal wrapper creates STOP signal message on completion."""
        task_id = "signal_stop_msg"
        mission_id = "missions:stop_signal"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        async def quick_task() -> None:
            await asyncio.sleep(0.05)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, quick_task(), session, mock_surreal_connection
        )

        await supervisor

        # Verify STOP signal was created
        calls = mock_surreal_connection.create.call_args_list
        stop_call = calls[-1]  # Last call should be STOP
        assert stop_call[0][0] == "tasks"
        assert stop_call[0][1]["task_id"] == task_id
        assert stop_call[0][1]["action"] == "stop"


# ============================================================================
# Test: Cancellation
# ============================================================================


class TestCancellation:
    """Tests for task cancellation scenarios."""

    @pytest.mark.asyncio
    async def test_supervisor_cancellation(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test executor handling external cancellation."""
        task_id = "external_cancel"
        mission_id = "missions:cancel"

        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def long_main() -> None:
            await asyncio.sleep(10)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_main(), session, mock_surreal_connection
        )

        # Cancel externally after brief delay
        await asyncio.sleep(0.05)
        supervisor.cancel()

        with pytest.raises(asyncio.CancelledError):
            await supervisor

        assert session.status == TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_cancellation_cleanup(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that cancellation properly cleans up all sub-tasks."""
        task_id = "cancel_cleanup"
        mission_id = "missions:cleanup"

        running_event = asyncio.Event()
        cleanup_log = []

        async def stay_alive_with_cleanup() -> None:
            try:
                await running_event.wait()
            except asyncio.CancelledError:
                cleanup_log.append("cleaned")
                raise

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive_with_cleanup)
        session.listen_signals = AsyncMock(side_effect=stay_alive_with_cleanup)

        async def long_main() -> None:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cleanup_log.append("main_cleaned")
                raise

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_main(), session, mock_surreal_connection
        )

        await asyncio.sleep(0.05)
        supervisor.cancel()

        try:
            await supervisor
        except asyncio.CancelledError:
            pass

        # Verify cleanup happened
        assert len(cleanup_log) >= 2  # At least heartbeat and listener cleanup

    @pytest.mark.asyncio
    async def test_cancelled_sets_status_correctly(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that cancellation sets CANCELLED status."""
        task_id = "cancel_status"
        mission_id = "missions:cancel_status"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        async def long_main() -> None:
            await asyncio.sleep(10)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_main(), session, mock_surreal_connection
        )

        await asyncio.sleep(0.02)
        supervisor.cancel()

        try:
            await supervisor
        except asyncio.CancelledError:
            pass

        assert session.status == TaskStatus.CANCELLED
        assert session.completed_at is not None


# ============================================================================
# Test: Concurrent Execution
# ============================================================================


class TestConcurrentExecution:
    """Tests for concurrent execution of three sub-tasks."""

    @pytest.mark.asyncio
    async def test_concurrent_execution_of_three_tasks(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
    ) -> None:
        """Test that main, heartbeat, and listener run concurrently."""
        task_id = "concurrent_test"
        mission_id = "missions:concurrent"
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

        supervisor = await task_executor.execute_task(
            task_id, mission_id, main_with_logs(), session, mock_surreal_connection
        )

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

    @pytest.mark.asyncio
    async def test_first_completed_wins(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that first task to complete determines the outcome."""
        task_id = "first_wins"
        mission_id = "missions:first_wins"

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def quick_listener() -> None:
            await asyncio.sleep(0.02)  # Finishes first

        async def slow_heartbeat() -> None:
            await asyncio.sleep(10)

        session.listen_signals = quick_listener
        session.generate_heartbeats = AsyncMock(side_effect=slow_heartbeat)

        async def slow_main() -> None:
            await asyncio.sleep(10)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, slow_main(), session, mock_surreal_connection
        )

        await supervisor

        # Listener finished first, so status should be CANCELLED
        assert session.status == TaskStatus.CANCELLED


# ============================================================================
# Test: Edge Cases
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and corner scenarios."""

    @pytest.mark.asyncio
    async def test_immediate_completion(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test task that completes immediately."""
        task_id = "immediate"
        mission_id = "missions:immediate"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        async def instant_task() -> None:
            pass  # Completes immediately

        supervisor = await task_executor.execute_task(
            task_id, mission_id, instant_task(), session, mock_surreal_connection
        )

        await supervisor

        assert session.status == TaskStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_empty_task_id(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test with empty task_id (valid but unusual)."""
        task_id = ""
        mission_id = "missions:empty"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        async def quick_task() -> None:
            await asyncio.sleep(0.01)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, quick_task(), session, mock_surreal_connection
        )

        await supervisor

        assert session.status == TaskStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_very_long_task_name(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test with very long task_id."""
        task_id = "x" * 1000
        mission_id = "missions:long"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        async def quick_task() -> None:
            await asyncio.sleep(0.01)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, quick_task(), session, mock_surreal_connection
        )

        await supervisor

        assert session.status == TaskStatus.COMPLETED


# ============================================================================
# Test: Supervisor Task Names
# ============================================================================


class TestSupervisorTaskNames:
    """Tests for proper task naming in asyncio."""

    @pytest.mark.asyncio
    async def test_supervisor_task_name(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that supervisor task has correct name."""
        task_id = "named_task"
        mission_id = "missions:named"
        running_event = asyncio.Event()

        async def stay_alive() -> None:
            await running_event.wait()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=stay_alive)
        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)

        async def quick_task() -> None:
            await asyncio.sleep(0.01)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, quick_task(), session, mock_surreal_connection
        )

        assert supervisor.get_name() == f"{task_id}_supervisor"

        await supervisor
