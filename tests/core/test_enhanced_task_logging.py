"""Comprehensive tests for enhanced task logging to database.

Tests the exception tracking and enhanced SignalMessage fields:
- Exception tracking (_last_exception, _last_traceback)
- SignalMessage enhanced fields (cancellation_reason, error_message, exception_traceback)
- DB persistence of error details
- Cancellation reason propagation through lifecycle

These tests validate production observability requirements:
- Error diagnosis from DB records
- Exception tracebacks preserved
- Cancellation reasons tracked
"""

import asyncio
import contextlib
import traceback
from typing import Any, NoReturn
from unittest.mock import AsyncMock, Mock, call

import pytest
import pytest_asyncio

from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.core.task_manager.task_executor import TaskExecutor
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.task_monitor import (
    CancellationReason,
    SignalMessage,
    SignalType,
    TaskStatus,
)
from digitalkin.modules._base_module import BaseModule

# Set timeout for all tests in this file
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
    conn.merge = AsyncMock()
    conn.close = AsyncMock()
    conn.stop_live = AsyncMock()
    conn._closed = False
    conn._live_queries = set()
    return conn


@pytest_asyncio.fixture
async def mock_base_module() -> Mock:
    """Mock BaseModule with async stop() method."""
    module = Mock(spec=BaseModule)
    module.stop = AsyncMock()
    module.context = Mock()
    module.context.session = Mock()
    module.context.session.setup_id = "setup:test"
    module.context.session.setup_version_id = "setup_version:test"
    module.context.session.current_ids = Mock(return_value={
        "job_id": "test_job",
        "mission_id": "missions:test",
        "setup_id": "setup:test",
        "setup_version_id": "setup_version:test",
    })
    return module


@pytest_asyncio.fixture
async def task_executor() -> TaskExecutor:
    """Standard TaskExecutor instance."""
    return TaskExecutor()


# ============================================================================
# Test: Exception Tracking Attributes
# ============================================================================


class TestExceptionTrackingAttributes:
    """Tests for _last_exception and _last_traceback attributes."""

    @pytest.mark.asyncio
    async def test_exception_tracking_initially_none(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that exception tracking attributes are None initially."""
        session = TaskSession(
            "exc_init", "missions:test", mock_surreal_connection, mock_base_module
        )

        assert session._last_exception is None
        assert session._last_traceback is None

    @pytest.mark.asyncio
    async def test_exception_captured_on_task_failure(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that exception details are captured when task fails."""
        task_id = "exc_capture"
        mission_id = "missions:exc"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def failing_main() -> NoReturn:
            await asyncio.sleep(0.02)
            msg = "Test exception message"
            raise ValueError(msg)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, failing_main(), session, mock_surreal_connection
        )

        with pytest.raises(ValueError):
            await supervisor

        # Exception details should be captured
        assert session._last_exception is not None
        assert "Test exception message" in session._last_exception

        assert session._last_traceback is not None
        assert "ValueError" in session._last_traceback
        assert "failing_main" in session._last_traceback

    @pytest.mark.asyncio
    async def test_exception_not_captured_on_success(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that exception details remain None on successful completion."""
        task_id = "exc_success"
        mission_id = "missions:success"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def successful_main() -> None:
            await asyncio.sleep(0.02)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, successful_main(), session, mock_surreal_connection
        )

        await supervisor

        # Exception details should remain None
        assert session._last_exception is None
        assert session._last_traceback is None
        assert session.status == TaskStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_exception_not_captured_on_cancellation(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that CancelledError doesn't set exception tracking."""
        task_id = "exc_cancel"
        mission_id = "missions:cancel"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def long_main() -> None:
            await asyncio.sleep(10)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_main(), session, mock_surreal_connection
        )

        await asyncio.sleep(0.05)
        supervisor.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await supervisor

        # CancelledError shouldn't set exception tracking
        assert session._last_exception is None
        assert session._last_traceback is None
        assert session.status == TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_nested_exception_captured(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that nested/chained exceptions are captured correctly."""
        task_id = "exc_nested"
        mission_id = "missions:nested"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def nested_failure() -> NoReturn:
            try:
                msg = "Inner exception"
                raise ValueError(msg)
            except ValueError as e:
                msg = "Outer exception"
                raise RuntimeError(msg) from e

        supervisor = await task_executor.execute_task(
            task_id, mission_id, nested_failure(), session, mock_surreal_connection
        )

        with pytest.raises(RuntimeError):
            await supervisor

        # Should capture the outer exception
        assert "Outer exception" in session._last_exception
        # Traceback should contain both exceptions
        assert "RuntimeError" in session._last_traceback
        # And the cause chain
        assert "ValueError" in session._last_traceback or "Inner exception" in session._last_traceback


# ============================================================================
# Test: SignalMessage Enhanced Fields
# ============================================================================


class TestSignalMessageEnhancedFields:
    """Tests for enhanced SignalMessage fields in DB records."""

    @pytest.mark.asyncio
    async def test_signal_message_has_new_fields(self) -> None:
        """Test that SignalMessage model has all enhanced fields."""
        msg = SignalMessage(
            task_id="test",
            mission_id="missions:test",
            status=TaskStatus.FAILED,
            action=SignalType.STOP,
            cancellation_reason=CancellationReason.HEARTBEAT_FAILURE,
            error_message="Test error",
            exception_traceback="Traceback...",
        )

        # Note: SignalMessage uses use_enum_values=True, enum values are strings

        assert msg.cancellation_reason== "heartbeat_failure"
        assert msg.error_message == "Test error"
        assert msg.exception_traceback == "Traceback..."

    @pytest.mark.asyncio
    async def test_signal_message_optional_fields_defaults(self) -> None:
        """Test that enhanced fields have correct defaults."""
        msg = SignalMessage(
            task_id="test",
            mission_id="missions:test",
            status=TaskStatus.RUNNING,
            action=SignalType.START,
        )

        assert msg.cancellation_reason == CancellationReason.UNKNOWN.value
        assert msg.error_message is None
        assert msg.exception_traceback is None

    @pytest.mark.asyncio
    async def test_signal_message_serialization(self) -> None:
        """Test that SignalMessage with enhanced fields serializes correctly."""
        msg = SignalMessage(
            task_id="test",
            mission_id="missions:test",
            status=TaskStatus.CANCELLED,
            action=SignalType.STOP,
            cancellation_reason=CancellationReason.SIGNAL,
            error_message="User requested cancellation",
            exception_traceback=None,
        )

        data = msg.model_dump()

        # Enum serialized to value string
        assert data["cancellation_reason"]== "signal"
        assert data["error_message"] == "User requested cancellation"
        assert data["exception_traceback"] is None


# ============================================================================
# Test: DB Persistence of Enhanced Fields
# ============================================================================


class TestDBPersistenceEnhancedFields:
    """Tests for DB persistence of enhanced logging fields."""

    @pytest.mark.asyncio
    async def test_stop_signal_includes_exception_details(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that STOP signal includes exception details on failure."""
        task_id = "db_exc"
        mission_id = "missions:db_exc"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def failing_main() -> NoReturn:
            await asyncio.sleep(0.02)
            msg = "Database error simulation"
            raise ConnectionError(msg)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, failing_main(), session, mock_surreal_connection
        )

        with pytest.raises(ConnectionError):
            await supervisor

        # Find the STOP signal creation call
        create_calls = mock_surreal_connection.create.call_args_list

        # Last call should be STOP signal
        stop_call = None
        for call_args in reversed(create_calls):
            args, kwargs = call_args
            if args[0] == "tasks" and args[1].get("action") == "stop":
                stop_call = args[1]
                break

        assert stop_call is not None, "STOP signal not found in create calls"

        # Verify enhanced fields
        assert stop_call["error_message"] is not None
        assert "Database error simulation" in stop_call["error_message"]
        assert stop_call["exception_traceback"] is not None
        assert "ConnectionError" in stop_call["exception_traceback"]

    @pytest.mark.asyncio
    async def test_stop_signal_includes_cancellation_reason(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that STOP signal includes cancellation reason when cancelled."""
        task_id = "db_cancel"
        mission_id = "missions:db_cancel"

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def signal_that_stops() -> None:
            """Simulate signal listener receiving cancel signal."""
            await asyncio.sleep(0.05)
            # Return normally to simulate stop signal
            session.status = TaskStatus.CANCELLED
            session.cancellation_reason = CancellationReason.SIGNAL

        async def long_heartbeat() -> None:
            await asyncio.sleep(10)

        session.listen_signals = signal_that_stops  # type: ignore
        session.generate_heartbeats = AsyncMock(side_effect=long_heartbeat)

        async def long_main() -> None:
            await asyncio.sleep(10)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_main(), session, mock_surreal_connection
        )

        await supervisor

        # Find the STOP signal creation call
        create_calls = mock_surreal_connection.create.call_args_list

        stop_call = None
        for call_args in reversed(create_calls):
            args, kwargs = call_args
            if args[0] == "tasks" and args[1].get("action") == "stop":
                stop_call = args[1]
                break

        assert stop_call is not None

        # Cancellation reason should be included
        assert stop_call["cancellation_reason"]== "signal"
        # No exception details for cancellation (excluded by exclude_none=True)
        assert "error_message" not in stop_call
        assert "exception_traceback" not in stop_call

    @pytest.mark.asyncio
    async def test_stop_signal_no_enhanced_fields_on_success(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that STOP signal has None enhanced fields on success."""
        task_id = "db_success"
        mission_id = "missions:db_success"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def successful_main() -> None:
            await asyncio.sleep(0.02)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, successful_main(), session, mock_surreal_connection
        )

        await supervisor

        # Find the STOP signal
        create_calls = mock_surreal_connection.create.call_args_list

        stop_call = None
        for call_args in reversed(create_calls):
            args, kwargs = call_args
            if args[0] == "tasks" and args[1].get("action") == "stop":
                stop_call = args[1]
                break

        assert stop_call is not None

        # Successful completion has COMPLETED reason, no error fields (excluded by exclude_none=True)
        assert stop_call["cancellation_reason"]== "completed"
        assert "error_message" not in stop_call
        assert "exception_traceback" not in stop_call


# ============================================================================
# Test: Cancellation Reason Propagation
# ============================================================================


class TestCancellationReasonPropagation:
    """Tests for cancellation reason tracking through the lifecycle."""

    @pytest.mark.asyncio
    async def test_handle_cancel_includes_reason_in_db(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that _handle_cancel includes reason in DB update."""
        session = TaskSession(
            "cancel_reason", "missions:cancel", mock_surreal_connection, mock_base_module
        )
        session.signal_record_id = "tasks:test_signal"

        await session._handle_cancel(CancellationReason.SIGNAL)

        # Verify update was called with cancellation_reason
        mock_surreal_connection.update.assert_called_once()
        call_args = mock_surreal_connection.update.call_args
        args, kwargs = call_args

        assert args[0] == "tasks"
        update_data = args[2]
        assert update_data["cancellation_reason"]== "signal"

    @pytest.mark.asyncio
    async def test_heartbeat_failure_reason_tracked(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that heartbeat failure sets correct cancellation reason."""
        session = TaskSession(
            "hb_fail", "missions:hb_fail", mock_surreal_connection, mock_base_module
        )
        session.signal_record_id = "tasks:test_signal"

        await session._handle_cancel(CancellationReason.HEARTBEAT_FAILURE)

        assert session.cancellation_reason == CancellationReason.HEARTBEAT_FAILURE

        call_args = mock_surreal_connection.update.call_args
        args, kwargs = call_args
        update_data = args[2]
        assert update_data["cancellation_reason"]== "heartbeat_failure"

    @pytest.mark.asyncio
    async def test_cleanup_reasons_tracked(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that cleanup reasons are tracked correctly."""
        session = TaskSession(
            "cleanup", "missions:cleanup", mock_surreal_connection, mock_base_module
        )
        session.signal_record_id = "tasks:test_signal"

        # Test success cleanup
        await session._handle_cancel(CancellationReason.SUCCESS_CLEANUP)

        assert session.cancellation_reason == CancellationReason.SUCCESS_CLEANUP

        # Reset for next test
        session.is_cancelled.clear()
        session.cancellation_reason = CancellationReason.UNKNOWN
        mock_surreal_connection.update.reset_mock()

        # Test failure cleanup
        await session._handle_cancel(CancellationReason.FAILURE_CLEANUP)

        assert session.cancellation_reason == CancellationReason.FAILURE_CLEANUP

    @pytest.mark.asyncio
    async def test_cancellation_reason_not_overwritten(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that original cancellation reason is preserved."""
        session = TaskSession(
            "preserve", "missions:preserve", mock_surreal_connection, mock_base_module
        )
        session.signal_record_id = "tasks:test_signal"

        # First cancellation with SIGNAL reason
        await session._handle_cancel(CancellationReason.SIGNAL)

        assert session.cancellation_reason == CancellationReason.SIGNAL

        # Second cancellation attempt should be ignored
        mock_surreal_connection.update.reset_mock()
        await session._handle_cancel(CancellationReason.HEARTBEAT_FAILURE)

        # Reason should still be SIGNAL
        assert session.cancellation_reason == CancellationReason.SIGNAL

        # Update should not have been called again
        mock_surreal_connection.update.assert_not_called()


# ============================================================================
# Test: Integration Tests
# ============================================================================


class TestEnhancedLoggingIntegration:
    """Integration tests for enhanced logging with full task lifecycle."""

    @pytest.mark.asyncio
    async def test_full_failure_logging_integration(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test complete logging of a failed task."""
        task_id = "full_fail"
        mission_id = "missions:full_fail"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def complex_failure() -> NoReturn:
            """Simulate a complex failure with context."""
            async def inner_function() -> NoReturn:
                msg = "Database connection timeout after 30s"
                raise TimeoutError(msg)

            await inner_function()

        supervisor = await task_executor.execute_task(
            task_id, mission_id, complex_failure(), session, mock_surreal_connection
        )

        with pytest.raises(TimeoutError):
            await supervisor

        # Verify comprehensive logging
        assert session.status == TaskStatus.FAILED
        assert session._last_exception is not None
        assert "timeout" in session._last_exception.lower()
        assert session._last_traceback is not None
        assert "complex_failure" in session._last_traceback
        assert "inner_function" in session._last_traceback

        # Verify DB record contains all details
        create_calls = mock_surreal_connection.create.call_args_list
        stop_call = None
        for call_args in reversed(create_calls):
            args, kwargs = call_args
            if args[0] == "tasks" and args[1].get("action") == "stop":
                stop_call = args[1]
                break

        assert stop_call is not None
        assert stop_call["status"] == "failed"
        assert stop_call["error_message"] is not None
        assert stop_call["exception_traceback"] is not None

    @pytest.mark.asyncio
    async def test_graceful_shutdown_logging(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test logging during graceful shutdown."""
        task_id = "graceful"
        mission_id = "missions:graceful"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def normal_task() -> None:
            await asyncio.sleep(0.05)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, normal_task(), session, mock_surreal_connection
        )

        await supervisor

        # Graceful completion should have clean logging
        assert session.status == TaskStatus.COMPLETED
        assert session._last_exception is None
        assert session._last_traceback is None

    @pytest.mark.asyncio
    async def test_error_details_help_diagnosis(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that error details provide enough info for diagnosis."""
        task_id = "diagnosis"
        mission_id = "missions:diagnosis"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        class CustomBusinessError(Exception):
            """Custom error with business context."""

            def __init__(self, operation: str, resource_id: str, details: str) -> None:
                self.operation = operation
                self.resource_id = resource_id
                super().__init__(f"Failed {operation} on {resource_id}: {details}")

        async def business_logic_failure() -> NoReturn:
            msg = "insufficient permissions"
            raise CustomBusinessError("update", "document:123", msg)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, business_logic_failure(), session, mock_surreal_connection
        )

        with pytest.raises(CustomBusinessError):
            await supervisor

        # Error message should contain business context
        assert "update" in session._last_exception
        assert "document:123" in session._last_exception
        assert "insufficient permissions" in session._last_exception

        # Traceback should show call chain
        assert "business_logic_failure" in session._last_traceback
        assert "CustomBusinessError" in session._last_traceback


# ============================================================================
# Test: Edge Cases
# ============================================================================


class TestEnhancedLoggingEdgeCases:
    """Edge cases for enhanced logging."""

    @pytest.mark.asyncio
    async def test_very_long_exception_message(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test handling of very long exception messages."""
        task_id = "long_exc"
        mission_id = "missions:long_exc"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        long_message = "x" * 10000  # 10KB message

        async def long_message_failure() -> NoReturn:
            raise ValueError(long_message)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_message_failure(), session, mock_surreal_connection
        )

        with pytest.raises(ValueError):
            await supervisor

        # Should capture the full message
        assert session._last_exception is not None
        assert len(session._last_exception) >= 10000

    @pytest.mark.asyncio
    async def test_exception_with_special_characters(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test handling of exceptions with special characters."""
        task_id = "special_chars"
        mission_id = "missions:special"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        special_message = "Error: <html>&nbsp;'quotes' \"double\" `backticks` \n\t\r"

        async def special_failure() -> NoReturn:
            raise ValueError(special_message)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, special_failure(), session, mock_surreal_connection
        )

        with pytest.raises(ValueError):
            await supervisor

        # Should capture special characters
        assert session._last_exception is not None
        assert "<html>" in session._last_exception
        assert "quotes" in session._last_exception

    @pytest.mark.asyncio
    async def test_unicode_in_exception(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test handling of Unicode in exception messages."""
        task_id = "unicode_exc"
        mission_id = "missions:unicode"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        unicode_message = "Error: 日本語 中文 한국어 العربية 🚀"

        async def unicode_failure() -> NoReturn:
            raise ValueError(unicode_message)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, unicode_failure(), session, mock_surreal_connection
        )

        with pytest.raises(ValueError):
            await supervisor

        # Should capture Unicode correctly
        assert session._last_exception is not None
        assert "日本語" in session._last_exception
        assert "🚀" in session._last_exception


# ============================================================================
# Test: SignalMessage Structure Verification
# ============================================================================


class TestSignalMessageStructure:
    """Tests verifying SignalMessage structure for API compatibility."""

    @pytest.mark.asyncio
    async def test_all_fields_present_in_model_dump(self) -> None:
        """Test that all expected fields are present in serialized output."""
        msg = SignalMessage(
            task_id="test_task",
            mission_id="missions:test",
            setup_id="setup:test",
            setup_version_id="setup_version:test",
            status=TaskStatus.FAILED,
            action=SignalType.STOP,
            cancellation_reason=CancellationReason.HEARTBEAT_FAILURE,
            error_message="Test error",
            exception_traceback="Test traceback",
        )

        data = msg.model_dump()

        expected_fields = {
            "task_id", "mission_id", "setup_id", "setup_version_id",
            "status", "action", "timestamp", "payload",
            "cancellation_reason", "error_message", "exception_traceback",
        }

        assert set(data.keys()) == expected_fields

    @pytest.mark.asyncio
    async def test_enum_values_serialized_correctly(self) -> None:
        """Test that enum values are serialized to their string values."""
        msg = SignalMessage(
            task_id="test",
            mission_id="missions:test",
            status=TaskStatus.CANCELLED,
            action=SignalType.ACK_CANCEL,
            cancellation_reason=CancellationReason.SIGNAL,
        )

        data = msg.model_dump()

        # Enums should be serialized to their values (strings)
        assert data["status"] == "cancelled"
        assert data["action"] == "ack_cancel"
        assert data["cancellation_reason"]== "signal"

    @pytest.mark.asyncio
    async def test_null_enhanced_fields_serialized(self) -> None:
        """Test that optional enhanced fields are serialized correctly."""
        msg = SignalMessage(
            task_id="test",
            mission_id="missions:test",
            status=TaskStatus.RUNNING,
            action=SignalType.START,
        )

        data = msg.model_dump()

        assert data["cancellation_reason"] == CancellationReason.UNKNOWN.value
        assert data["error_message"] is None
        assert data["exception_traceback"] is None

    @pytest.mark.asyncio
    async def test_model_dump_exclude_none_surrealdb_compatible(self) -> None:
        """Test that model_dump(exclude_none=True) produces SurrealDB-compatible output.

        SurrealDB's CBOR encoder cannot serialize Python enum instances.
        All enum fields must be serialized as primitive strings.
        The cancellation_reason field must always be present (not excluded).
        """
        msg = SignalMessage(
            task_id="test",
            mission_id="missions:test",
            status=TaskStatus.RUNNING,
            action=SignalType.START,
        )

        data = msg.model_dump(exclude_none=True)

        # cancellation_reason must be present (not excluded as None)
        assert "cancellation_reason" in data

        # All values must be primitives (str, int, float, bool, dict, list, None)
        # No enum instances allowed — SurrealDB CBOR encoder rejects them
        from enum import Enum

        for key, value in data.items():
            assert not isinstance(value, Enum), (
                f"Field '{key}' is an enum instance ({type(value).__name__}), "
                f"but SurrealDB CBOR encoder requires primitive types"
            )

    @pytest.mark.asyncio
    async def test_model_dump_exclude_none_with_explicit_reason(self) -> None:
        """Test that explicitly provided cancellation_reason is also serialized as string."""
        msg = SignalMessage(
            task_id="test",
            mission_id="missions:test",
            status=TaskStatus.CANCELLED,
            action=SignalType.STOP,
            cancellation_reason=CancellationReason.SIGNAL,
            error_message="User cancelled",
        )

        data = msg.model_dump(exclude_none=True)

        assert data["cancellation_reason"] == "signal"
        assert isinstance(data["cancellation_reason"], str)
        assert data["status"] == "cancelled"
        assert isinstance(data["status"], str)
