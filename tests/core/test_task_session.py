"""Comprehensive test suite for TaskSession class.

Tests lifecycle management including initialization, signal handling,
cancellation logic, and cleanup. Uses TaskManagerStrategy (signal_service).
"""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
import pytest_asyncio

from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.task_monitor import CancellationReason
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy

# Set timeout for all tests in this file (60 seconds)
pytestmark = pytest.mark.timeout(60)


# ============================================================================
# Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def mock_signal_service() -> Mock:  # noqa: RUF029
    """Mock TaskManagerStrategy with all required async methods."""
    svc = Mock(spec=TaskManagerStrategy)
    svc.send_signal = AsyncMock(return_value={})
    svc.close = AsyncMock()
    return svc


@pytest_asyncio.fixture
async def mock_module(mock_signal_service: Mock) -> Mock:  # noqa: RUF029
    """Mock BaseModule with signal service in context."""
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
async def task_session(mock_module: Mock) -> TaskSession:  # noqa: RUF029
    """Create a standard TaskSession for testing."""
    return TaskSession(
        task_id="task_test_001",
        mission_id="missions:test",
        module=mock_module,
    )


# ============================================================================
# Test: Initialization
# ============================================================================


class TestInitialization:
    """Tests for TaskSession initialization."""

    def test_initial_state(self, task_session: TaskSession) -> None:
        """Test default state after initialization."""
        assert task_session.task_id == "task_test_001"
        assert task_session.mission_id == "missions:test"
        assert task_session.status == "pending"
        assert task_session.started_at is None
        assert task_session.completed_at is None
        assert not task_session.cancelled
        assert not task_session.stream_closed
        assert task_session._cleanup_done is False

    def test_signal_service_from_module_context(
        self, task_session: TaskSession, mock_signal_service: Mock,
    ) -> None:
        """Test that signal_service is derived from module.context.task_manager."""
        assert task_session.signal_service is mock_signal_service

    def test_cancellation_reason_defaults_unknown(self, task_session: TaskSession) -> None:
        """Test default cancellation reason is UNKNOWN."""
        assert task_session.cancellation_reason == CancellationReason.UNKNOWN

    def test_setup_id_property(self, task_session: TaskSession) -> None:
        """Test setup_id property returns from module context."""
        assert task_session.setup_id == "setup:test"

    def test_setup_version_id_property(self, task_session: TaskSession) -> None:
        """Test setup_version_id property returns from module context."""
        assert task_session.setup_version_id == "setup_version:test"

    def test_session_ids_property(self, task_session: TaskSession) -> None:
        """Test session_ids property returns structured IDs."""
        ids = task_session.session_ids
        assert "mission_id" in ids
        assert "setup_id" in ids

    def test_custom_queue_maxsize(self, mock_module: Mock) -> None:
        """Test custom queue_maxsize."""
        session = TaskSession("t1", "m1", mock_module, queue_maxsize=10)
        assert session.queue.maxsize == 10


# ============================================================================
# Test: Cancellation
# ============================================================================


class TestCancellation:
    """Tests for cancellation logic."""

    @pytest.mark.asyncio
    async def test_handle_cancel_sets_state(self, task_session: TaskSession) -> None:
        """Test _handle_cancel sets status and event."""
        await task_session._handle_cancel(CancellationReason.SIGNAL_SERVICE_CANCEL)

        assert task_session.cancelled
        assert task_session.status == "cancelled"
        assert task_session.cancellation_reason == CancellationReason.SIGNAL_SERVICE_CANCEL

    @pytest.mark.asyncio
    async def test_handle_cancel_idempotent(self, task_session: TaskSession) -> None:
        """Test _handle_cancel is idempotent (second call is no-op)."""
        await task_session._handle_cancel(CancellationReason.SIGNAL_SERVICE_CANCEL)
        await task_session._handle_cancel(CancellationReason.TIMEOUT)

        # First reason should stick
        assert task_session.cancellation_reason == CancellationReason.SIGNAL_SERVICE_CANCEL

    @pytest.mark.asyncio
    async def test_handle_cancel_sends_ack(
        self, task_session: TaskSession, mock_signal_service: Mock,
    ) -> None:
        """Test _handle_cancel sends ACK_CANCEL signal."""
        await task_session._handle_cancel(CancellationReason.SIGNAL_SERVICE_CANCEL)

        mock_signal_service.send_signal.assert_called_once()
        call_data = mock_signal_service.send_signal.call_args[0][1]
        assert call_data["action"] == "ack_cancel"
        assert call_data["cancellation_reason"] == "signal_service_cancel"

    @pytest.mark.asyncio
    async def test_handle_cancel_ack_failure_silent(
        self, task_session: TaskSession, mock_signal_service: Mock,
    ) -> None:
        """Test _handle_cancel doesn't raise if ack fails."""
        mock_signal_service.send_signal = AsyncMock(side_effect=Exception("ack failed"))

        # Should not raise
        await task_session._handle_cancel(CancellationReason.SIGNAL_SERVICE_CANCEL)
        assert task_session.cancelled

    @pytest.mark.asyncio
    async def test_cancel_cleanup_vs_signal_logging(self, task_session: TaskSession) -> None:
        """Test that cleanup reasons use debug level (via coverage)."""
        await task_session._handle_cancel(CancellationReason.SUCCESS_CLEANUP)
        assert task_session.cancellation_reason == CancellationReason.SUCCESS_CLEANUP


# ============================================================================
# Test: Stream Control
# ============================================================================


class TestStreamControl:
    """Tests for stream_closed and close_stream."""

    def test_stream_closed_initially_false(self, task_session: TaskSession) -> None:
        """Test stream_closed is False initially."""
        assert not task_session.stream_closed

    def test_close_stream_sets_event(self, task_session: TaskSession) -> None:
        """Test close_stream sets the stream_closed event."""
        task_session.close_stream()
        assert task_session.stream_closed

    def test_cancelled_property(self, task_session: TaskSession) -> None:
        """Test cancelled property reflects is_cancelled event."""
        assert not task_session.cancelled
        task_session.is_cancelled.set()
        assert task_session.cancelled


# ============================================================================
# Test: Exception Recording
# ============================================================================


class TestExceptionRecording:
    """Tests for record_exception."""

    def test_record_exception(self, task_session: TaskSession) -> None:
        """Test exception recording."""
        try:
            msg = "test error"
            raise ValueError(msg)  # noqa: TRY301
        except ValueError as e:
            task_session.record_exception(e)

        assert task_session._last_exception == "test error"
        assert task_session._last_traceback is not None
        assert "ValueError" in task_session._last_traceback

    def test_record_exception_none_by_default(self, task_session: TaskSession) -> None:
        """Test exception fields are None by default."""
        assert task_session._last_exception is None
        assert task_session._last_traceback is None


# ============================================================================
# Test: Cleanup
# ============================================================================


class TestCleanup:
    """Tests for cleanup()."""

    @pytest.mark.asyncio
    async def test_cleanup_idempotent(self, task_session: TaskSession) -> None:
        """Test cleanup() is idempotent."""
        await task_session.cleanup()
        assert task_session._cleanup_done

        # Second call should be a no-op
        await task_session.cleanup()

    @pytest.mark.asyncio
    async def test_cleanup_clears_queue(self, task_session: TaskSession) -> None:
        """Test cleanup clears the queue."""
        task_session.queue.put_nowait({"data": "test"})
        assert not task_session.queue.empty()

        await task_session.cleanup()
        assert task_session.queue.empty()

    @pytest.mark.asyncio
    async def test_cleanup_calls_module_context_cleanup(
        self, task_session: TaskSession, mock_module: Mock,
    ) -> None:
        """Test cleanup calls module.context.cleanup()."""
        await task_session.cleanup()
        mock_module.context.cleanup.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_stops_module(
        self, task_session: TaskSession, mock_module: Mock,
    ) -> None:
        """Test cleanup stops the module."""
        await task_session.cleanup()
        mock_module.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_nullifies_module_reference(
        self, task_session: TaskSession,
    ) -> None:
        """Test cleanup sets module to None for GC."""
        await task_session.cleanup()
        assert task_session.module is None

    @pytest.mark.asyncio
    async def test_cleanup_handles_context_cleanup_failure(
        self, task_session: TaskSession, mock_module: Mock,
    ) -> None:
        """Test cleanup continues even if context cleanup fails."""
        mock_module.context.cleanup = AsyncMock(side_effect=RuntimeError("cleanup boom"))

        await task_session.cleanup()

        # Should still stop module and nullify
        mock_module.stop.assert_called_once()
        assert task_session.module is None

    @pytest.mark.asyncio
    async def test_cleanup_handles_module_stop_failure(
        self, task_session: TaskSession, mock_module: Mock,
    ) -> None:
        """Test cleanup continues even if module.stop() fails."""
        mock_module.stop = AsyncMock(side_effect=RuntimeError("stop boom"))

        await task_session.cleanup()

        assert task_session.module is None
        assert task_session._cleanup_done


# ============================================================================
# Regression: config-setup TaskSession with no task_manager (the dev13 bug)
# ============================================================================


@pytest_asyncio.fixture
async def mock_module_no_task_manager() -> Mock:  # noqa: RUF029
    """Mock BaseModule mirroring the config-setup path: ``context.task_manager`` is ``None``.

    ``SingleJobManager.create_config_setup_instance_job`` constructs a TaskSession before
    any task_manager is wired (config jobs don't need signals); this fixture reproduces that.
    """
    module = Mock(spec=BaseModule)
    module.stop = AsyncMock()
    module.context = Mock()
    module.context.session = Mock()
    module.context.session.setup_id = "setup:cfg"
    module.context.session.setup_version_id = "setup_version:cfg"
    module.context.session.current_ids = Mock(return_value={
        "mission_id": "missions:test",
        "task_id": "cfg_task",
        "setup_id": "setup:cfg",
        "setup_version_id": "setup_version:cfg",
    })
    module.context.task_manager = None
    module.context.cleanup = AsyncMock()
    return module


class TestTaskSessionNoTaskManager:
    """A config-setup TaskSession must construct without an assert and degrade gracefully."""

    async def test_constructs_without_assertion_error(self, mock_module_no_task_manager: Mock) -> None:
        # Regression: a stray ``assert module.context.task_manager is not None`` previously broke
        # ``SingleJobManager.create_config_setup_instance_job`` for every config call.
        session = TaskSession(
            task_id="cfg_task",
            mission_id="missions:test",
            module=mock_module_no_task_manager,
        )
        assert session.signal_service is None

    async def test_handle_cancel_skips_send_signal(self, mock_module_no_task_manager: Mock) -> None:
        session = TaskSession(
            task_id="cfg_task",
            mission_id="missions:test",
            module=mock_module_no_task_manager,
        )
        # Must complete without raising AND without emitting the noisy "best-effort failed" WARNING.
        await session._handle_cancel(CancellationReason.SIGNAL_SERVICE_CANCEL)

    async def test_handle_stop_skips_send_signal(self, mock_module_no_task_manager: Mock) -> None:
        session = TaskSession(
            task_id="cfg_task",
            mission_id="missions:test",
            module=mock_module_no_task_manager,
        )
        await session._handle_stop()

    async def test_base_task_manager_send_signal_returns_false(self, mock_module_no_task_manager: Mock) -> None:
        from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
        mgr = LocalTaskManager()
        session = TaskSession(
            task_id="cfg_task",
            mission_id="missions:test",
            module=mock_module_no_task_manager,
        )
        mgr.tasks_sessions["cfg_task"] = session
        # No signal_service -> graceful False, mirroring the "task not found" branch.
        result = await mgr.send_signal("cfg_task", "missions:test", "STOP", {})
        assert result is False
