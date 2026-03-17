"""Comprehensive tests for TaskExecutor.

Tests the supervisor pattern implementation including:
- Two concurrent tasks (main + signal listener)
- Outcome determination (completed, failed, cancelled)
- Exception handling and propagation
- Cleanup on cancellation
- Timing precision
"""

import asyncio
import contextlib
import time
from typing import NoReturn
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

from digitalkin.core.task_manager.task_executor import TaskExecutor
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy

# Set timeout for all tests in this file (30 seconds)
pytestmark = pytest.mark.timeout(30)


# ============================================================================
# Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def mock_signal_service() -> Mock:
    """Create a mock TaskManagerStrategy with async methods."""
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
async def task_executor() -> TaskExecutor:
    """Standard TaskExecutor instance."""
    return TaskExecutor()


# ============================================================================
# Test: Main Task Completion Scenarios
# ============================================================================


class TestMainTaskCompletion:
    """Tests for normal main task completion."""

    @pytest.mark.asyncio
    async def test_main_task_completes_successfully(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test executor when main task completes successfully."""
        task_id = "main_success"
        mission_id = "missions:test"
        execution_log = []

        session = TaskSession(task_id, mission_id, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=_stay_alive)

        async def main_coro() -> None:
            execution_log.append("main_start")
            await asyncio.sleep(0.1)
            execution_log.append("main_end")

        supervisor = await task_executor.execute_task(
            task_id, mission_id, main_coro(), session
        )

        await supervisor

        assert session.status == "completed"
        assert "main_start" in execution_log
        assert "main_end" in execution_log
        assert session.started_at is not None
        assert session.completed_at is not None

    @pytest.mark.asyncio
    async def test_main_task_completion_timing_accuracy(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test accurate timing measurement for completed tasks."""
        task_id = "timing_test"
        mission_id = "missions:timing"

        session = TaskSession(task_id, mission_id, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=_stay_alive)

        async def job() -> None:
            await asyncio.sleep(0.08)

        start = time.monotonic()
        supervisor = await task_executor.execute_task(task_id, mission_id, job(), session)
        await supervisor
        elapsed = time.monotonic() - start

        assert session.status == "completed"
        assert session.started_at is not None
        assert session.completed_at is not None
        recorded = (session.completed_at - session.started_at).total_seconds()
        assert abs(recorded - elapsed) < 0.07


# ============================================================================
# Test: Exception Handling
# ============================================================================


class TestExceptionHandling:
    """Tests for exception handling in various scenarios."""

    @pytest.mark.asyncio
    async def test_main_task_raises_exception(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test executor when main task raises an exception."""
        task_id = "main_exception"
        mission_id = "missions:error"

        session = TaskSession(task_id, mission_id, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=_stay_alive)

        async def failing_coro() -> None:
            await asyncio.sleep(0.05)
            msg = "Intentional failure"
            raise ValueError(msg)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, failing_coro(), session
        )

        with pytest.raises(ValueError, match="Intentional failure"):
            await supervisor

        assert session.status == "failed"

    @pytest.mark.asyncio
    async def test_exception_sets_failed_status(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test that any exception sets status to 'failed'."""
        task_id = "fail"
        mission_id = "missions:fail"

        session = TaskSession(task_id, mission_id, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=_stay_alive)

        async def failing() -> NoReturn:
            msg = "boom"
            raise ValueError(msg)

        supervisor = await task_executor.execute_task(task_id, mission_id, failing(), session)

        with pytest.raises(ValueError):
            await supervisor
        assert session.status == "failed"

    @pytest.mark.asyncio
    async def test_exception_propagated_to_caller(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test that exceptions are properly propagated to the caller."""
        task_id = "propagate"
        mission_id = "missions:propagate"

        session = TaskSession(task_id, mission_id, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=_stay_alive)

        class CustomError(Exception):
            pass

        async def custom_failure() -> NoReturn:
            await asyncio.sleep(0.01)
            msg = "custom error message"
            raise CustomError(msg)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, custom_failure(), session
        )

        with pytest.raises(CustomError, match="custom error message"):
            await supervisor

    @pytest.mark.asyncio
    async def test_exception_records_traceback(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test that exceptions are recorded in session for signal reporting."""
        task_id = "traceback_test"
        mission_id = "missions:traceback"

        session = TaskSession(task_id, mission_id, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=_stay_alive)

        async def failing() -> NoReturn:
            msg = "detailed error"
            raise RuntimeError(msg)

        supervisor = await task_executor.execute_task(task_id, mission_id, failing(), session)

        with contextlib.suppress(RuntimeError):
            await supervisor

        assert session._last_exception == "detailed error"
        assert session._last_traceback is not None


# ============================================================================
# Test: Signal Listener Scenarios
# ============================================================================


class TestSignalListener:
    """Tests for signal listener behavior."""

    @pytest.mark.asyncio
    async def test_signal_listener_stops_task(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test executor when signal listener returns (stop signal)."""
        task_id = "signal_stop"
        mission_id = "missions:signal"

        async def signal_that_stops() -> None:
            await asyncio.sleep(0.05)

        session = TaskSession(task_id, mission_id, mock_base_module)
        session.listen_signals = signal_that_stops

        async def long_main() -> None:
            await asyncio.sleep(10)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_main(), session
        )

        await supervisor

        assert session.status == "cancelled"

    @pytest.mark.asyncio
    async def test_signal_wrapper_sends_start_and_stop(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
        mock_signal_service: Mock,
    ) -> None:
        """Test that signal wrapper sends START and STOP signals."""
        task_id = "signal_lifecycle"
        mission_id = "missions:lifecycle"

        session = TaskSession(task_id, mission_id, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=_stay_alive)

        async def quick_task() -> None:
            await asyncio.sleep(0.05)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, quick_task(), session
        )

        await supervisor

        # Verify START and STOP signals were sent
        calls = mock_signal_service.send_signal.call_args_list
        assert len(calls) >= 2

        # First call should be START
        start_data = calls[0][0][1]  # Second positional arg
        assert start_data["action"] == "start"

        # Last call should be STOP
        stop_data = calls[-1][0][1]
        assert stop_data["action"] == "stop"


# ============================================================================
# Test: Cancellation
# ============================================================================


class TestCancellation:
    """Tests for task cancellation scenarios."""

    @pytest.mark.asyncio
    async def test_supervisor_cancellation(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test executor handling external cancellation."""
        task_id = "external_cancel"
        mission_id = "missions:cancel"

        session = TaskSession(task_id, mission_id, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=_stay_alive)

        async def long_main() -> None:
            await asyncio.sleep(10)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_main(), session
        )

        await asyncio.sleep(0.05)
        supervisor.cancel()

        with pytest.raises(asyncio.CancelledError):
            await supervisor

        assert session.status == "cancelled"

    @pytest.mark.asyncio
    async def test_cancellation_cleanup(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test that cancellation properly cleans up all sub-tasks."""
        task_id = "cancel_cleanup"
        mission_id = "missions:cleanup"

        cleanup_log = []

        session = TaskSession(task_id, mission_id, mock_base_module)

        async def stay_alive_with_cleanup() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cleanup_log.append("listener_cleaned")
                raise

        session.listen_signals = stay_alive_with_cleanup

        async def long_main() -> None:
            try:
                await asyncio.sleep(10)
            except asyncio.CancelledError:
                cleanup_log.append("main_cleaned")
                raise

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_main(), session
        )

        await asyncio.sleep(0.05)
        supervisor.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await supervisor

        # At least one sub-task should have been cleaned up
        assert len(cleanup_log) >= 1

    @pytest.mark.asyncio
    async def test_cancelled_sets_timestamps(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test that cancellation sets both started_at and completed_at."""
        task_id = "cancel_timestamps"
        mission_id = "missions:cancel_ts"

        session = TaskSession(task_id, mission_id, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=_stay_alive)

        async def long_main() -> None:
            await asyncio.sleep(10)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_main(), session
        )

        await asyncio.sleep(0.02)
        supervisor.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await supervisor

        assert session.status == "cancelled"
        assert session.completed_at is not None


# ============================================================================
# Test: Concurrent Execution
# ============================================================================


class TestConcurrentExecution:
    """Tests for concurrent execution of two sub-tasks (main + listener)."""

    @pytest.mark.asyncio
    async def test_concurrent_execution_of_two_tasks(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test that main and listener run concurrently."""
        task_id = "concurrent_test"
        mission_id = "missions:concurrent"
        execution_timeline = []

        session = TaskSession(task_id, mission_id, mock_base_module)

        async def listener_with_logs() -> None:
            try:
                for i in range(5):
                    execution_timeline.append(f"listener_{i}")
                    await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                pass

        session.listen_signals = listener_with_logs

        async def main_with_logs() -> None:
            for i in range(3):
                execution_timeline.append(f"main_{i}")
                await asyncio.sleep(0.05)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, main_with_logs(), session
        )

        await supervisor

        # Verify interleaved execution
        assert len(execution_timeline) >= 3
        main_indices = [i for i, log in enumerate(execution_timeline) if "main" in log]
        listener_indices = [i for i, log in enumerate(execution_timeline) if "listener" in log]

        if len(main_indices) > 1 and len(listener_indices) > 1:
            assert not (max(main_indices) < min(listener_indices))

    @pytest.mark.asyncio
    async def test_first_completed_wins(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test that first task to complete determines the outcome."""
        task_id = "first_wins"
        mission_id = "missions:first_wins"

        session = TaskSession(task_id, mission_id, mock_base_module)

        async def quick_listener() -> None:
            await asyncio.sleep(0.02)  # Finishes first

        session.listen_signals = quick_listener

        async def slow_main() -> None:
            await asyncio.sleep(10)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, slow_main(), session
        )

        await supervisor

        # Listener finished first, so status should be cancelled
        assert session.status == "cancelled"


# ============================================================================
# Test: Edge Cases
# ============================================================================


class TestEdgeCases:
    """Tests for edge cases and corner scenarios."""

    @pytest.mark.asyncio
    async def test_immediate_completion(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test task that completes immediately."""
        task_id = "immediate"
        mission_id = "missions:immediate"

        session = TaskSession(task_id, mission_id, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=_stay_alive)

        async def instant_task() -> None:
            pass

        supervisor = await task_executor.execute_task(
            task_id, mission_id, instant_task(), session
        )

        await supervisor

        assert session.status == "completed"

    @pytest.mark.asyncio
    async def test_supervisor_task_name(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test that supervisor task has correct name."""
        task_id = "named_task"
        mission_id = "missions:named"

        session = TaskSession(task_id, mission_id, mock_base_module)
        session.listen_signals = AsyncMock(side_effect=_stay_alive)

        async def quick_task() -> None:
            await asyncio.sleep(0.01)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, quick_task(), session
        )

        assert supervisor.get_name() == f"{task_id}_supervisor"
        await supervisor


# ============================================================================
# Helpers
# ============================================================================


async def _stay_alive() -> None:
    """Block forever until cancelled."""
    await asyncio.Event().wait()
