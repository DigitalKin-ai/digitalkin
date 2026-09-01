"""Tests for TaskExecutor.

Covers task lifecycle (run main coro, status transitions, exception
handling, cancellation, timing). Signal dispatch lives in
``SharedRedisListener.dispatch_signal`` — the supervisor pattern with a
separate signal listener task is gone.
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
async def mock_signal_service() -> Mock:  # noqa: RUF029
    """Create a mock TaskManagerStrategy with async methods."""
    svc = Mock(spec=TaskManagerStrategy)
    svc.send_signal = AsyncMock(return_value={})
    svc.close = AsyncMock()
    return svc


@pytest_asyncio.fixture
async def mock_base_module(mock_signal_service: Mock) -> Mock:  # noqa: RUF029
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
async def task_executor() -> TaskExecutor:  # noqa: RUF029
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
        """Test executor when main task raises an exception. Exception is caught inside _run()."""
        task_id = "main_exception"
        mission_id = "missions:error"

        session = TaskSession(task_id, mission_id, mock_base_module)

        async def failing_coro() -> None:
            await asyncio.sleep(0.05)
            msg = "Intentional failure"
            raise ValueError(msg)

        task = await task_executor.execute_task(
            task_id, mission_id, failing_coro(), session
        )

        await task  # _run() catches the exception, task completes normally
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

        async def failing() -> NoReturn:  # noqa: RUF029
            msg = "boom"
            raise ValueError(msg)

        task = await task_executor.execute_task(task_id, mission_id, failing(), session)

        await task  # _run() catches the exception
        assert session.status == "failed"

    @pytest.mark.asyncio
    async def test_exception_propagated_to_session(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test that exceptions are recorded in session (not propagated to caller)."""
        task_id = "propagate"
        mission_id = "missions:propagate"

        session = TaskSession(task_id, mission_id, mock_base_module)

        async def custom_failure() -> NoReturn:
            await asyncio.sleep(0.01)
            msg = "custom error message"
            raise ValueError(msg)

        task = await task_executor.execute_task(
            task_id, mission_id, custom_failure(), session
        )

        await task  # _run() catches the exception
        assert session.status == "failed"
        assert session._last_exception == "custom error message"

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

        async def failing() -> NoReturn:  # noqa: RUF029
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


class TestSignalHandling:
    """Tests for signal handling via direct task cancellation."""

    @pytest.mark.asyncio
    async def test_task_cancel_sets_cancelled_status(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test that cancelling the task sets status to 'cancelled'."""
        task_id = "signal_cancel"
        mission_id = "missions:signal"

        session = TaskSession(task_id, mission_id, mock_base_module)

        async def long_main() -> None:
            await asyncio.sleep(10)

        task = await task_executor.execute_task(
            task_id, mission_id, long_main(), session
        )

        await asyncio.sleep(0.05)
        task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await task

        assert session.status == "cancelled"


# ============================================================================
# Test: Cancellation
# ============================================================================


class TestCancellation:
    """Tests for task cancellation scenarios."""

    @pytest.mark.asyncio
    async def test_task_cancellation(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test executor handling external cancellation."""
        task_id = "external_cancel"
        mission_id = "missions:cancel"

        session = TaskSession(task_id, mission_id, mock_base_module)

        async def long_main() -> None:
            await asyncio.sleep(10)

        task = await task_executor.execute_task(
            task_id, mission_id, long_main(), session
        )

        await asyncio.sleep(0.05)
        task.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await task

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
# Test: Outcome
# ============================================================================


class TestOutcome:
    """Tests for single-task outcome determination."""

    @pytest.mark.asyncio
    async def test_completion_determines_outcome(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Module completion sets the final status."""
        task_id = "outcome"
        mission_id = "missions:outcome"

        session = TaskSession(task_id, mission_id, mock_base_module)

        async def quick_main() -> None:
            await asyncio.sleep(0.02)

        task = await task_executor.execute_task(
            task_id, mission_id, quick_main(), session
        )

        await task

        assert session.status == "completed"


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

        async def instant_task() -> None:
            pass

        supervisor = await task_executor.execute_task(
            task_id, mission_id, instant_task(), session
        )

        await supervisor

        assert session.status == "completed"

    @pytest.mark.asyncio
    async def test_task_name(
        self,
        task_executor: TaskExecutor,
        mock_base_module: Mock,
    ) -> None:
        """Test that task has correct name."""
        task_id = "named_task"
        mission_id = "missions:named"

        session = TaskSession(task_id, mission_id, mock_base_module)

        async def quick_task() -> None:
            await asyncio.sleep(0.01)

        task = await task_executor.execute_task(
            task_id, mission_id, quick_task(), session
        )

        assert task.get_name() == f"{task_id}_main"
        await task
