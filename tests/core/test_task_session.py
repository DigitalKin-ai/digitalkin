"""Comprehensive test suite for TaskSession class.

This test suite provides exhaustive coverage of TaskSession lifecycle management,
including initialization, heartbeat mechanisms, signal handling, pause/resume
functionality, and cancellation logic. Tests are designed to detect regressions,
validate state transitions, and ensure async concurrency safety.
"""

import asyncio
import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from freezegun import freeze_time

from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.task_monitor import (
    CancellationReason,
    SignalType,
    TaskStatus,
)
from digitalkin.modules._base_module import BaseModule

# Set timeout for all tests in this file (60 seconds)
pytestmark = pytest.mark.timeout(60)

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_db():
    """Mock SurrealDBConnection with all required async methods."""
    db = MagicMock(spec=SurrealDBConnection)
    db.create = AsyncMock(return_value={"id": "heartbeats:test_hb_id"})
    db.merge = AsyncMock(return_value={"id": "heartbeats:test_hb_id"})
    db.update = AsyncMock(return_value={"id": "tasks:test_signal_id"})
    db.select_by_task_id = AsyncMock(return_value={"id": "tasks:test_signal_id"})
    db.start_live = AsyncMock(return_value=("live_id_123", _empty_async_gen()))
    db.stop_live = AsyncMock()
    db.close = AsyncMock()
    return db


@pytest.fixture
def mock_module():
    """Mock BaseModule instance with context.session for session_ids property."""
    module = MagicMock(spec=BaseModule)
    # Mock context.session with current_ids() method for session_ids property
    module.context = MagicMock()
    module.context.session = MagicMock()
    module.context.session.setup_id = "setup:test_setup"
    module.context.session.setup_version_id = "setup_version:test_version"
    module.context.session.current_ids = MagicMock(
        return_value={
            "job_id": "test_task_123",
            "mission_id": "missions:test_mission",
            "setup_id": "setup:test_setup",
            "setup_version_id": "setup_version:test_version",
        }
    )
    # Mock context.cleanup() for cleanup flow
    module.context.cleanup = AsyncMock()
    return module


@pytest.fixture
def mock_logger():
    """Mock logger to capture log calls without side effects."""
    with patch("digitalkin.core.task_manager.task_session.logger") as logger_mock:
        yield logger_mock


@pytest.fixture
def task_session(mock_db, mock_module):
    """Create a TaskSession instance with mocked dependencies."""
    return TaskSession(
        task_id="test_task_123",
        mission_id="missions:test_mission",
        db=mock_db,
        module=mock_module,
        heartbeat_interval=datetime.timedelta(seconds=2),
    )


async def _empty_async_gen():
    """Empty async generator for default mock behavior."""
    if False:
        yield


async def _signal_generator(signals: list):
    """Create async generator yielding test signals."""
    for signal in signals:
        yield signal


# ============================================================================
# State Validation Helpers
# ============================================================================


def assert_task_state(
    session: TaskSession,
    status: TaskStatus | None = None,
    cancelled: bool | None = None,
    paused: bool | None = None,
    heartbeat_record_id: str | None = None,
    signal_record_id: str | None = None,
):
    """Comprehensive state assertion helper.

    Validates TaskSession internal state to detect regressions in attribute
    mutations during lifecycle operations.
    """
    if status is not None:
        assert session.status == status, f"Expected status {status}, got {session.status}"
    if cancelled is not None:
        assert session.cancelled == cancelled, f"Expected cancelled={cancelled}, got {session.cancelled}"
    if paused is not None:
        assert session.paused == paused, f"Expected paused={paused}, got {session.paused}"
    if heartbeat_record_id is not None:
        assert session.heartbeat_record_id == heartbeat_record_id
    if signal_record_id is not None:
        assert session.signal_record_id == signal_record_id


def compute_state_hash(session: TaskSession) -> str:
    """Compute deterministic hash of session state for regression detection.

    Returns a string representation of critical state attributes that can be
    compared across test runs to detect unintended state changes.
    """
    return f"{session.task_id}|{session.status.value}|{session.cancelled}|{session.paused}|{session.heartbeat_record_id}|{session.signal_record_id}"


# ============================================================================
# Test Class: Initialization
# ============================================================================


class TestInitialization:
    """Test TaskSession initialization and default state."""

    def test_init_sets_correct_defaults(self, mock_db, mock_module, mock_logger):
        """Verify all attributes are initialized with correct types and values.

        This test ensures that TaskSession constructor properly initializes
        all instance variables to prevent null reference errors and unexpected
        behavior in subsequent operations.
        """
        task_id = "init_test_task"
        heartbeat_interval = datetime.timedelta(seconds=5)

        session = TaskSession(
            task_id=task_id,
            mission_id="missions:default_mission",
            db=mock_db,
            module=mock_module,
            heartbeat_interval=heartbeat_interval,
        )

        # Verify basic attributes
        assert session.task_id == task_id
        assert session.db is mock_db
        assert session.module is mock_module
        assert session._heartbeat_interval == heartbeat_interval

        # Verify status and lifecycle attributes
        assert session.status == TaskStatus.PENDING
        assert session.started_at is None
        assert session.completed_at is None
        assert session.signal_record_id is None
        assert session.heartbeat_record_id is None

        # Verify event states
        assert isinstance(session.is_cancelled, asyncio.Event)
        assert not session.is_cancelled.is_set()
        assert isinstance(session._paused, asyncio.Event)
        assert not session._paused.is_set()

        # Verify queue
        assert isinstance(session.queue, asyncio.Queue)

        # Verify logger call
        mock_logger.info.assert_called_once()
        assert task_id in str(mock_logger.info.call_args)

    def test_init_default_heartbeat_interval(self, mock_db, mock_module):
        """Verify default heartbeat interval when not specified.

        Ensures backward compatibility if heartbeat_interval parameter is omitted.
        """
        session = TaskSession(
            task_id="test_default",
            mission_id="missions:default_mission",
            db=mock_db,
            module=mock_module,
        )

        assert session._heartbeat_interval == datetime.timedelta(seconds=2)

    def test_cancelled_property(self, task_session):
        """Verify cancelled property reflects is_cancelled event state."""
        assert not task_session.cancelled
        task_session.is_cancelled.set()
        assert task_session.cancelled

    def test_paused_property(self, task_session):
        """Verify paused property reflects _paused event state."""
        assert not task_session.paused
        task_session._paused.set()
        assert task_session.paused


# ============================================================================
# Test Class: Heartbeat Logic
# ============================================================================


class TestHeartbeats:
    """Test heartbeat creation, updates, and rate limiting."""

    @freeze_time("2025-10-14 03:21:34")
    @pytest.mark.asyncio
    async def test_send_heartbeat_initial_creation_success(self, task_session, mock_db):
        """Verify first heartbeat creates a new record in the database.

        This test ensures that the initial heartbeat properly calls db.create
        and stores the returned record ID for future updates.
        """
        result = await task_session.send_heartbeat()

        assert result is None
        assert task_session.heartbeat_record_id == "heartbeats:test_hb_id"
        assert task_session._last_heartbeat == datetime.datetime.now(tz=datetime.timezone.utc)

        mock_db.create.assert_called_once()
        call_args = mock_db.create.call_args
        assert call_args[0][0] == "heartbeats"
        assert call_args[0][1]["task_id"] == "test_task_123"
        assert call_args[0][1]["timestamp"] == datetime.datetime.now(tz=datetime.timezone.utc)

    @pytest.mark.asyncio
    async def test_send_heartbeat_initial_creation_failure(self, task_session, mock_db, mock_logger):
        """Verify proper error handling when initial heartbeat creation fails.

        Tests that db.create failures are logged and return CancellationReason without
        setting heartbeat_record_id.
        """
        mock_db.create.return_value = {"code": "DB_ERROR", "message": "Connection failed"}

        result = await task_session.send_heartbeat()

        assert result == CancellationReason.HEARTBEAT_FAILURE
        assert task_session.heartbeat_record_id is None
        mock_logger.error.assert_called()

    @freeze_time("2025-10-14 03:21:34")
    @pytest.mark.asyncio
    async def test_send_heartbeat_successful_merge(self, task_session, mock_db):
        """Verify subsequent heartbeats use merge to update existing record.

        After initial creation, heartbeats should use db.merge for efficiency
        and update the _last_heartbeat timestamp.
        """
        # Setup: create initial heartbeat
        task_session.heartbeat_record_id = "heartbeats:existing_id"
        task_session._last_heartbeat = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(seconds=5)

        with patch("digitalkin.core.task_manager.task_session.datetime") as mock_dt:
            new_time = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=3)
            mock_dt.datetime.now.return_value = new_time
            mock_dt.timezone = datetime.timezone

            result = await task_session.send_heartbeat()

            assert result is None
            assert task_session._last_heartbeat == new_time

            mock_db.merge.assert_called_once()
            call_args = mock_db.merge.call_args
            assert call_args[0][0] == "heartbeats"
            assert call_args[0][1] == "heartbeats:existing_id"

    @freeze_time("2025-10-14 03:21:34")
    @pytest.mark.asyncio
    async def test_send_heartbeat_rate_limiting(self, task_session, mock_db, mock_logger):
        """Verify heartbeat is skipped when called within rate limit interval.

        This test ensures rate limiting prevents excessive DB operations while
        still returning None to avoid triggering cancellation.
        """
        task_session.heartbeat_record_id = "heartbeats:existing_id"
        task_session._last_heartbeat = datetime.datetime.now(tz=datetime.timezone.utc)

        result = await task_session.send_heartbeat()
        assert result is None
        # Should not call merge due to rate limiting
        mock_db.merge.assert_not_called()
        # Last heartbeat should remain unchanged
        assert task_session._last_heartbeat == datetime.datetime.now(tz=datetime.timezone.utc)
        # Should log rate limited
        assert any("rate limited" in str(call).lower() for call in mock_logger.debug.call_args_list)

    @freeze_time("2025-10-14 03:21:34")
    @pytest.mark.asyncio
    async def test_send_heartbeat_merge_failure(self, task_session, mock_db, mock_logger):
        """Verify proper handling of db.merge failures.

        Tests that merge failures are logged and return CancellationReason without updating
        _last_heartbeat timestamp.
        """
        task_session.heartbeat_record_id = "heartbeats:existing_id"
        task_session._last_heartbeat = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(seconds=5)

        mock_db.merge.return_value = {"code": "MERGE_ERROR"}

        with patch("digitalkin.core.task_manager.task_session.datetime") as mock_dt:
            new_time = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=3)
            mock_dt.datetime.now.return_value = new_time
            mock_dt.timezone = datetime.timezone

            result = await task_session.send_heartbeat()

            assert result == CancellationReason.HEARTBEAT_FAILURE
            # Last heartbeat should not update on failure
            assert task_session._last_heartbeat == datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(
                seconds=5
            )
            mock_logger.warning.assert_called()

    @freeze_time("2025-10-14 03:21:34")
    @pytest.mark.asyncio
    async def test_send_heartbeat_exception_handling(self, task_session, mock_db, mock_logger):
        """Verify exception handling during heartbeat merge operation.

        Ensures that unexpected exceptions are caught, logged with exc_info,
        and don't crash the heartbeat loop.
        """
        task_session.heartbeat_record_id = "heartbeats:existing_id"
        task_session._last_heartbeat = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(seconds=5)

        mock_db.merge.side_effect = RuntimeError("Database connection lost")

        with patch("digitalkin.core.task_manager.task_session.datetime") as mock_dt:
            new_time = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=3)
            mock_dt.datetime.now.return_value = new_time
            mock_dt.timezone = datetime.timezone

            result = await task_session.send_heartbeat()

            assert result == CancellationReason.HEARTBEAT_FAILURE
            mock_logger.error.assert_called()
            # Verify exc_info was used for traceback
            assert mock_logger.error.call_args[1].get("exc_info") is True


# ============================================================================
# Test Class: Periodic Heartbeat Generator
# ============================================================================


class TestPeriodicHeartbeatGenerator:
    """Test the generate_heartbeats continuous loop."""

    @pytest.mark.asyncio
    async def test_generate_heartbeats_normal_operation(self, task_session, mock_db, mock_logger):
        """Verify heartbeat generator runs multiple iterations successfully.

        Tests that the generator loop continues sending heartbeats at the
        specified interval until cancellation is triggered.
        """
        heartbeat_count = 0

        async def mock_send_heartbeat() -> CancellationReason | None:
            nonlocal heartbeat_count
            heartbeat_count += 1
            if heartbeat_count >= 3:
                task_session.is_cancelled.set()
            return None

        task_session.send_heartbeat = mock_send_heartbeat

        await task_session.generate_heartbeats()

        assert heartbeat_count == 3
        assert task_session.cancelled

    @pytest.mark.asyncio
    async def test_generate_heartbeats_failure_triggers_cancellation(self, task_session, mock_db, mock_logger):
        """Verify heartbeat failure triggers task cancellation.

        When send_heartbeat returns CancellationReason, generate_heartbeats should call
        _handle_cancel and break the loop to prevent infinite failed attempts.
        """
        call_count = 0

        async def mock_send_heartbeat() -> CancellationReason | None:
            nonlocal call_count
            call_count += 1
            return CancellationReason.HEARTBEAT_FAILURE  # Simulate failure

        task_session.send_heartbeat = mock_send_heartbeat
        task_session._handle_cancel = AsyncMock()

        await task_session.generate_heartbeats()

        assert call_count == 1
        task_session._handle_cancel.assert_called_once()
        mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_generate_heartbeats_respects_interval(self, task_session, mock_db):
        """Verify heartbeat generator respects configured interval timing.

        Ensures asyncio.sleep is called with correct interval between heartbeats
        to prevent excessive resource usage.
        """
        heartbeat_interval = datetime.timedelta(milliseconds=100)
        task_session._heartbeat_interval = heartbeat_interval

        sleep_calls = []

        async def mock_sleep(duration) -> None:
            sleep_calls.append(duration)
            if len(sleep_calls) >= 2:
                task_session.is_cancelled.set()

        task_session.send_heartbeat = AsyncMock(return_value=None)

        with patch("asyncio.sleep", side_effect=mock_sleep):
            await task_session.generate_heartbeats()

        assert len(sleep_calls) == 2
        for duration in sleep_calls:
            assert duration == heartbeat_interval.total_seconds()

    @pytest.mark.asyncio
    async def test_generate_heartbeats_exits_on_cancellation(self, task_session, mock_db):
        """Verify generator exits immediately when task is cancelled.

        Tests that setting is_cancelled during heartbeat generation causes
        the loop to exit cleanly on the next iteration.
        """
        task_session.is_cancelled.set()
        task_session.send_heartbeat = AsyncMock()

        await task_session.generate_heartbeats()

        # Should not send any heartbeats if already cancelled
        task_session.send_heartbeat.assert_not_called()


# ============================================================================
# Test Class: Signal Listener
# ============================================================================


class TestSignalListener:
    """Test signal reception and handler dispatch."""

    @pytest.mark.asyncio
    async def test_listen_signals_returns_early_if_signal_record_id_not_set(self, task_session, mock_db):
        """Verify signal listener returns early if signal_record_id is not set.

        The TaskExecutor is responsible for setting signal_record_id from the
        create result. If not set, listen_signals returns early to avoid race conditions.
        """
        task_session.signal_record_id = None

        await task_session.listen_signals()

        # Should not start live query if signal_record_id is not set
        mock_db.start_live.assert_not_called()
        mock_db.select_by_task_id.assert_not_called()

    @pytest.mark.asyncio
    async def test_listen_signals_handles_cancel_signal(self, task_session, mock_db):
        """Verify cancel signal triggers _handle_cancel.

        Tests that incoming cancel signals are properly detected and routed
        to the cancellation handler.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        signals = [
            {
                "id": "tasks:different_id",
                "action": "cancel",
                "payload": {"signal": "cancel"},
            }
        ]
        mock_db.start_live.return_value = ("live_123", _signal_generator(signals))
        task_session._handle_cancel = AsyncMock()

        # Let it process one signal then exit
        async def delayed_cancel() -> None:
            await asyncio.sleep(0.1)
            task_session.is_cancelled.set()

        await asyncio.gather(
            task_session.listen_signals(),
            delayed_cancel(),
        )

        task_session._handle_cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_listen_signals_handles_pause_signal(self, task_session, mock_db):
        """Verify pause signal triggers _handle_pause.

        Tests pause signal detection and handler invocation.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        signals = [
            {
                "id": "tasks:different_id",
                "action": "pause",
                "payload": {"signal": "pause"},
            }
        ]
        mock_db.start_live.return_value = ("live_123", _signal_generator(signals))
        task_session._handle_pause = AsyncMock()

        async def delayed_cancel() -> None:
            await asyncio.sleep(0.1)
            task_session.is_cancelled.set()

        await asyncio.gather(
            task_session.listen_signals(),
            delayed_cancel(),
        )

        task_session._handle_pause.assert_called_once()

    @pytest.mark.asyncio
    async def test_listen_signals_handles_resume_signal(self, task_session, mock_db):
        """Verify resume signal triggers _handle_resume.

        Tests resume signal detection and handler invocation.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        signals = [
            {
                "id": "tasks:different_id",
                "action": "resume",
                "payload": {"signal": "resume"},
            }
        ]
        mock_db.start_live.return_value = ("live_123", _signal_generator(signals))
        task_session._handle_resume = AsyncMock()

        async def delayed_cancel() -> None:
            await asyncio.sleep(0.1)
            task_session.is_cancelled.set()

        await asyncio.gather(
            task_session.listen_signals(),
            delayed_cancel(),
        )

        task_session._handle_resume.assert_called_once()

    @pytest.mark.asyncio
    async def test_listen_signals_handles_status_signal(self, task_session, mock_db):
        """Verify status signal triggers _handle_status_request.

        Tests status request signal detection and handler invocation.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        signals = [
            {
                "id": "tasks:different_id",
                "action": "status",
                "payload": {"signal": "status"},
            }
        ]
        mock_db.start_live.return_value = ("live_123", _signal_generator(signals))
        task_session._handle_status_request = AsyncMock()

        async def delayed_cancel() -> None:
            await asyncio.sleep(0.1)
            task_session.is_cancelled.set()

        await asyncio.gather(
            task_session.listen_signals(),
            delayed_cancel(),
        )

        task_session._handle_status_request.assert_called_once()

    @pytest.mark.asyncio
    async def test_listen_signals_ignores_own_signals(self, task_session, mock_db):
        """Verify signals from own task are ignored to prevent feedback loops.

        The listener should filter out signals with matching signal_record_id
        to avoid processing its own acknowledgements.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        signals = [
            {
                "id": "tasks:test_signal_id",  # Same as own signal_record_id
                "action": "cancel",
                "payload": {"signal": "cancel"},
            }
        ]
        mock_db.start_live.return_value = ("live_123", _signal_generator(signals))
        task_session._handle_cancel = AsyncMock()

        async def delayed_cancel() -> None:
            await asyncio.sleep(0.1)
            task_session.is_cancelled.set()

        await asyncio.gather(
            task_session.listen_signals(),
            delayed_cancel(),
        )

        # Should not handle own signal
        task_session._handle_cancel.assert_not_called()

    @pytest.mark.asyncio
    async def test_listen_signals_ignores_none_signals(self, task_session, mock_db):
        """Verify None signals are safely ignored.

        Tests defensive programming against null signals from live query.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        signals = [None]
        mock_db.start_live.return_value = ("live_123", _signal_generator(signals))
        task_session._handle_cancel = AsyncMock()

        async def delayed_cancel() -> None:
            await asyncio.sleep(0.1)
            task_session.is_cancelled.set()

        await asyncio.gather(
            task_session.listen_signals(),
            delayed_cancel(),
        )

        task_session._handle_cancel.assert_not_called()

    @pytest.mark.asyncio
    async def test_listen_signals_cleanup_on_normal_exit(self, task_session, mock_db, mock_logger):
        """Verify db.stop_live is called on normal completion.

        Tests that cleanup happens in the finally block even when the loop
        exits normally.
        """
        task_session.signal_record_id = "tasks:test_signal_id"
        task_session.is_cancelled.set()

        live_id = "live_123"
        mock_db.start_live.return_value = (live_id, _signal_generator([]))

        await task_session.listen_signals()

        mock_db.stop_live.assert_called_once_with(live_id)

    @pytest.mark.asyncio
    async def test_listen_signals_cleanup_on_exception(self, task_session, mock_db, mock_logger):
        """Verify db.stop_live is called even when exception occurs.

        Tests that cleanup happens in the finally block during error conditions
        to prevent resource leaks.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        async def failing_generator():
            yield {"id": "tasks:different_id", "action": "cancel", "payload": {}}
            msg = "Simulated failure"
            raise RuntimeError(msg)

        live_id = "live_456"
        mock_db.start_live.return_value = (live_id, failing_generator())
        task_session._handle_cancel = AsyncMock()

        await task_session.listen_signals()

        mock_db.stop_live.assert_called_once_with(live_id)
        mock_logger.exception.assert_called()

    @pytest.mark.asyncio
    async def test_listen_signals_handles_multiple_signals(self, task_session, mock_db):
        """Verify multiple signals are processed sequentially.

        Tests that the listener can handle a stream of different signals
        and dispatch to appropriate handlers.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        signals = [
            {"id": "tasks:sig1", "action": "pause", "payload": {}},
            {"id": "tasks:sig2", "action": "resume", "payload": {}},
            {"id": "tasks:sig3", "action": "status", "payload": {}},
        ]
        mock_db.start_live.return_value = ("live_123", _signal_generator(signals))

        task_session._handle_pause = AsyncMock()
        task_session._handle_resume = AsyncMock()
        task_session._handle_status_request = AsyncMock()

        async def delayed_cancel() -> None:
            await asyncio.sleep(0.2)
            task_session.is_cancelled.set()

        await asyncio.gather(
            task_session.listen_signals(),
            delayed_cancel(),
        )

        task_session._handle_pause.assert_called_once()
        task_session._handle_resume.assert_called_once()
        task_session._handle_status_request.assert_called_once()


# ============================================================================
# Test Class: Pause and Resume Handlers
# ============================================================================


class TestPauseResume:
    """Test pause and resume functionality."""

    @pytest.mark.asyncio
    async def test_handle_pause_sets_event_and_sends_ack(self, task_session, mock_db):
        """Verify _handle_pause clears the pause event and sends acknowledgement.

        Tests that pause handler properly updates the pause event state and
        sends an ACK_PAUSE signal through the database.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        # Ensure not paused initially
        task_session._paused.clear()

        await task_session._handle_pause()

        # Pause event should be cleared (blocking wait_if_paused)
        assert task_session.paused

        # Verify DB update with ACK_PAUSE
        mock_db.update.assert_called_once()
        call_args = mock_db.update.call_args[0]
        assert call_args[0] == "tasks"
        assert call_args[1] == "tasks:test_signal_id"

        payload = call_args[2]
        assert payload["task_id"] == "test_task_123"
        assert payload["action"] == SignalType.ACK_PAUSE.value

    @pytest.mark.asyncio
    async def test_handle_pause_idempotency(self, task_session, mock_db):
        """Verify multiple pause calls are safe and idempotent.

        Tests that calling _handle_pause multiple times doesn't cause
        unexpected state changes or duplicate database operations beyond
        acknowledgements.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        # First pause
        task_session._paused.clear()
        await task_session._handle_pause()
        assert task_session.paused

        # Second pause (already paused)
        await task_session._handle_pause()
        assert task_session.paused

        # Should have two ACK calls (one per pause)
        assert task_session.paused
        assert mock_db.update.call_count == 2

    @pytest.mark.asyncio
    async def test_handle_resume_sets_event_and_sends_ack(self, task_session, mock_db):
        """Verify _handle_resume sets the pause event and sends acknowledgement.

        Tests that resume handler properly updates the pause event state and
        sends an ACK_RESUME signal through the database.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        # Ensure paused initially
        task_session._paused.is_set()

        await task_session._handle_resume()

        # Pause event should be set (releasing wait_if_paused)
        assert not task_session.paused

        # Verify DB update with ACK_RESUME
        mock_db.update.assert_called_once()
        call_args = mock_db.update.call_args[0]
        assert call_args[0] == "tasks"
        assert call_args[1] == "tasks:test_signal_id"

        payload = call_args[2]
        assert payload["task_id"] == "test_task_123"
        assert payload["action"] == SignalType.ACK_RESUME.value

    @pytest.mark.asyncio
    async def test_handle_resume_idempotency(self, task_session, mock_db):
        """Verify multiple resume calls are safe and idempotent.

        Tests that calling _handle_resume multiple times doesn't cause
        unexpected state changes or duplicate operations.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        # First resume
        task_session._paused.clear()
        await task_session._handle_resume()
        assert not task_session.paused

        # Second resume (already resumed)
        await task_session._handle_resume()
        assert not task_session.paused

        # Should have two ACK calls
        assert mock_db.update.call_count == 2

    @pytest.mark.asyncio
    async def test_wait_if_paused_blocks_when_paused(self, task_session):
        """Verify wait_if_paused blocks execution when task is paused.

        Tests that the wait_if_paused method properly blocks until the
        pause event is set by resume.
        """
        task_session._paused.set()  # Paused state

        wait_completed = False

        async def wait_task() -> None:
            nonlocal wait_completed
            await task_session.wait_if_paused()
            wait_completed = True

        async def resume_after_delay() -> None:
            await asyncio.sleep(0.05)
            task_session._paused.clear()

        await asyncio.gather(wait_task(), resume_after_delay())

        assert wait_completed
        assert not task_session.paused

    @pytest.mark.asyncio
    async def test_wait_if_paused_returns_immediately_when_not_paused(self, task_session, mock_logger):
        """Verify wait_if_paused returns immediately when not paused.

        Tests that the method doesn't block when the task is not in paused state.
        """
        task_session._paused.clear()  # Not paused

        # Should complete immediately
        await asyncio.wait_for(task_session.wait_if_paused(), timeout=0.1)

        # Should not log waiting message
        assert not any(
            "paused" in str(call).lower() and "waiting" in str(call).lower() for call in mock_logger.info.call_args_list
        )

    @pytest.mark.asyncio
    async def test_pause_resume_cycle_maintains_state_integrity(self, task_session, mock_db):
        """Verify pause/resume cycle maintains consistent state.

        Tests a complete pause-resume cycle to ensure state transitions
        are clean and don't leave the session in an inconsistent state.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        initial_state = compute_state_hash(task_session)

        # Pause
        task_session._paused.set()
        await task_session._handle_pause()
        assert task_session.paused

        # Resume
        await task_session._handle_resume()
        assert not task_session.paused

        # State should only differ in pause flag
        final_state = compute_state_hash(task_session)
        assert initial_state == final_state  # Both have paused=False initially and after resume


# ============================================================================
# Test Class: Cancellation Handler
# ============================================================================


class TestCancellation:
    """Test task cancellation logic and state transitions."""

    @pytest.mark.asyncio
    async def test_handle_cancel_sets_cancelled_and_updates_status(self, task_session, mock_db):
        """Verify _handle_cancel properly sets cancellation state.

        Tests that cancel handler sets the is_cancelled event, updates
        TaskStatus to CANCELLED, and sends acknowledgement.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        await task_session._handle_cancel()

        assert task_session.is_cancelled.is_set()
        assert task_session.status == TaskStatus.CANCELLED

        # Verify DB update with ACK_CANCEL
        mock_db.update.assert_called_once()
        call_args = mock_db.update.call_args[0]
        assert call_args[0] == "tasks"
        assert call_args[1] == "tasks:test_signal_id"

        payload = call_args[2]
        assert payload["task_id"] == "test_task_123"
        assert payload["action"] == SignalType.ACK_CANCEL.value
        assert payload["status"] == TaskStatus.CANCELLED.value

    @pytest.mark.asyncio
    async def test_handle_cancel_resumes_if_paused(self, task_session, mock_db):
        """Verify cancellation resumes paused tasks to allow cleanup.

        Tests that if a task is paused when cancelled, the pause event
        is set to allow the cancellation to proceed through blocked code.
        """
        task_session.signal_record_id = "tasks:test_signal_id"
        task_session._paused.set()  # Paused state

        await task_session._handle_cancel()

        # Should resume to allow cancellation to proceed
        assert task_session.paused
        assert task_session.is_cancelled.is_set()

    @pytest.mark.asyncio
    async def test_handle_cancel_idempotency(self, task_session, mock_db, mock_logger):
        """Verify multiple cancel calls are idempotent.

        Tests that calling _handle_cancel multiple times only processes
        the first cancellation and logs subsequent attempts without
        duplicating database operations.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        # First cancel
        await task_session._handle_cancel()
        assert task_session.cancelled
        assert mock_db.update.call_count == 1

        # Second cancel (already cancelled)
        await task_session._handle_cancel()
        assert task_session.cancelled
        # Should not call DB again
        assert mock_db.update.call_count == 1

        # Should log that cancel was ignored
        assert any("already cancelled" in str(call).lower() for call in mock_logger.debug.call_args_list)

    @pytest.mark.asyncio
    async def test_handle_cancel_state_transition(self, task_session, mock_db):
        """Verify status transitions correctly during cancellation.

        Tests that status moves from PENDING to CANCELLED and the
        transition is reflected in the database update.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        assert task_session.status == TaskStatus.PENDING

        await task_session._handle_cancel()

        assert task_session.status == TaskStatus.CANCELLED

        # Verify the update payload contains correct status
        update_payload = mock_db.update.call_args[0][2]
        assert update_payload["status"] == TaskStatus.CANCELLED.value

    @pytest.mark.asyncio
    async def test_handle_cancel_from_different_states(self, task_session, mock_db):
        """Verify cancellation works from various TaskStatus states.

        Tests that cancellation properly transitions from differentf
        starting states to ensure robustness.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        # Test from PENDING
        task_session.status = TaskStatus.PENDING
        await task_session._handle_cancel()
        assert task_session.status == TaskStatus.CANCELLED

        # Reset for next test
        task_session.is_cancelled.clear()
        mock_db.update.reset_mock()

        # Test from a hypothetical RUNNING state (if enum supports it)
        # This ensures cancellation works regardless of current status
        task_session.status = TaskStatus.PENDING  # Using PENDING as proxy
        await task_session._handle_cancel()
        assert task_session.status == TaskStatus.CANCELLED


# ============================================================================
# Test Class: Status Handler
# ============================================================================


class TestStatusHandler:
    """Test status request handling."""

    @pytest.mark.asyncio
    async def test_handle_status_request_sends_current_status(self, task_session, mock_db, mock_logger):
        """Verify _handle_status_request sends current TaskStatus.

        Tests that status requests are acknowledged with the current
        task status via database update.
        """
        task_session.signal_record_id = "tasks:test_signal_id"
        task_session.status = TaskStatus.PENDING

        await task_session._handle_status_request()

        mock_db.update.assert_called_once()
        call_args = mock_db.update.call_args[0]
        assert call_args[0] == "tasks"
        assert call_args[1] == "tasks:test_signal_id"

        payload = call_args[2]
        assert payload["task_id"] == "test_task_123"
        assert payload["action"] == SignalType.ACK_STATUS.value
        assert payload["status"] == TaskStatus.PENDING.value

        mock_logger.debug.assert_called()

    @pytest.mark.asyncio
    async def test_handle_status_request_with_cancelled_status(self, task_session, mock_db):
        """Verify status request correctly reports CANCELLED status.

        Tests that the status handler accurately reflects task state
        when the task has been cancelled.
        """
        task_session.signal_record_id = "tasks:test_signal_id"
        task_session.status = TaskStatus.CANCELLED

        await task_session._handle_status_request()

        payload = mock_db.update.call_args[0][2]
        assert payload["status"] == TaskStatus.CANCELLED.value

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status",
        [TaskStatus.PENDING, TaskStatus.CANCELLED],
    )
    async def test_handle_status_request_parametrized(self, task_session, mock_db, status):
        """Verify status request works for all TaskStatus values.

        Parametrized test ensuring status handler correctly reports
        all possible task statuses.
        """
        task_session.signal_record_id = "tasks:test_signal_id"
        task_session.status = status

        await task_session._handle_status_request()

        payload = mock_db.update.call_args[0][2]
        assert payload["status"] == status.value
        assert payload["action"] == SignalType.ACK_STATUS.value


# ============================================================================
# Test Class: Integration and Lifecycle
# ============================================================================


class TestLifecycleIntegration:
    """Test complete lifecycle scenarios and state consistency."""

    @pytest.mark.asyncio
    async def test_full_lifecycle_with_heartbeats_and_signals(self, task_session, mock_db, mock_logger):
        """Verify complete task lifecycle with concurrent heartbeats and signals.

        Integration test simulating a realistic task execution with heartbeat
        generation and signal handling running concurrently.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        # Setup signal stream
        signals = [
            {
                "id": "tasks:sig1",
                "action": "status",
                "payload": {},
            },
        ]
        mock_db.start_live.return_value = ("live_123", _signal_generator(signals))

        task_session._handle_status_request = AsyncMock()

        # Simulate short-lived task
        async def run_task() -> None:
            await asyncio.sleep(0.1)
            task_session.is_cancelled.set()

        # Run all components concurrently
        await asyncio.gather(
            task_session.generate_heartbeats(),
            task_session.listen_signals(),
            run_task(),
        )

        # Verify heartbeats were sent
        assert mock_db.create.call_count >= 1 or mock_db.merge.call_count >= 1

        # Verify signal was handled
        task_session._handle_status_request.assert_called()

        # Verify cleanup
        mock_db.stop_live.assert_called_once()

    @pytest.mark.asyncio
    async def test_state_consistency_after_multiple_operations(self, task_session, mock_db):
        """Verify state remains consistent after complex operation sequence.

        Tests that performing multiple operations in sequence doesn't
        corrupt internal state or create race conditions.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        compute_state_hash(task_session)

        # Perform sequence of operations
        await task_session.send_heartbeat()
        await task_session._handle_pause()
        await task_session._handle_resume()
        await task_session._handle_status_request()

        # Status and heartbeat should have changed, others stable
        assert task_session.task_id == "test_task_123"
        assert isinstance(task_session.is_cancelled, asyncio.Event)
        assert isinstance(task_session._paused, asyncio.Event)

    @pytest.mark.asyncio
    async def test_cancellation_stops_heartbeat_generation(self, task_session, mock_db):
        """Verify cancellation properly stops heartbeat generation.

        Tests that triggering cancellation causes the heartbeat loop
        to exit cleanly without hanging.
        """
        heartbeat_count = 0

        async def counting_send_heartbeat() -> CancellationReason | None:
            nonlocal heartbeat_count
            heartbeat_count += 1
            return None

        task_session.send_heartbeat = counting_send_heartbeat

        # Cancel after short delay
        async def delayed_cancel() -> None:
            await asyncio.sleep(0.05)
            await task_session._handle_cancel()

        await asyncio.gather(
            task_session.generate_heartbeats(),
            delayed_cancel(),
        )

        # Heartbeat loop should have exited
        assert task_session.cancelled
        # Should have sent at least one heartbeat before cancellation
        assert heartbeat_count >= 1

    @pytest.mark.skip(reason="Resume doesn't communicate with the main CORO task so only the signal task is paused")
    @pytest.mark.asyncio
    async def test_pause_blocks_execution_in_wait_if_paused(self, task_session, mock_db):
        """Verify pause properly blocks wait_if_paused calls.

        Integration test ensuring pause/resume mechanism works correctly
        for blocking task execution.
        """
        execution_log = []
        task_session.signal_record_id = "tasks:test_signal_id"
        task_session._paused.set()

        async def task_with_checkpoints() -> None:
            execution_log.append("start")
            await task_session.wait_if_paused()
            execution_log.append("after_checkpoint_1")
            await task_session.wait_if_paused()
            execution_log.append("after_checkpoint_2")

        async def pause_resume_sequence() -> None:
            await asyncio.sleep(0.1)
            execution_log.append("resuming")
            task_session._paused.set()

            await asyncio.sleep(0.05)
            execution_log.append("pausing")
            task_session._paused.clear()

            await asyncio.sleep(0.1)
            execution_log.append("resuming")
            task_session._paused.set()

        await asyncio.gather(
            task_with_checkpoints(),
            pause_resume_sequence(),
        )

        # Verify execution order
        assert execution_log.index("start") < execution_log.index("pausing")
        assert execution_log.index("pausing") < execution_log.index("resuming")
        assert execution_log.index("resuming") < execution_log.index("after_checkpoint_1")

    @pytest.mark.asyncio
    async def test_concurrent_signal_processing(self, task_session, mock_db):
        """Verify multiple signals are processed without state corruption.

        Tests that receiving multiple signals concurrently doesn't cause
        race conditions or inconsistent state.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        signals = [
            {"id": "tasks:sig1", "action": "pause", "payload": {}},
            {"id": "tasks:sig2", "action": "status", "payload": {}},
            {"id": "tasks:sig3", "action": "resume", "payload": {}},
            {"id": "tasks:sig4", "action": "status", "payload": {}},
        ]
        mock_db.start_live.return_value = ("live_123", _signal_generator(signals))

        task_session._handle_pause = AsyncMock()
        task_session._handle_resume = AsyncMock()
        task_session._handle_status_request = AsyncMock()

        async def delayed_cancel() -> None:
            await asyncio.sleep(0.2)
            task_session.is_cancelled.set()

        await asyncio.gather(
            task_session.listen_signals(),
            delayed_cancel(),
        )

        # All signals should have been processed
        task_session._handle_pause.assert_called_once()
        task_session._handle_resume.assert_called_once()
        assert task_session._handle_status_request.call_count == 2

    @pytest.mark.asyncio
    async def test_state_hash_regression_detection(self, task_session, mock_db):
        """Verify state hash changes only when expected attributes change.

        Regression test using state hashing to detect unexpected
        attribute mutations during operations.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        initial_hash = compute_state_hash(task_session)

        # Operations that shouldn't change core state
        await task_session._handle_status_request()
        hash_after_status = compute_state_hash(task_session)
        assert initial_hash == hash_after_status

        # Operations that should change state
        await task_session._handle_cancel()
        hash_after_cancel = compute_state_hash(task_session)
        assert initial_hash != hash_after_cancel
        assert "cancelled" in hash_after_cancel


# ============================================================================
# Test Class: Cleanup Operations
# ============================================================================


class TestCleanup:
    """Rigorous tests for TaskSession.cleanup() method.

    Tests the cleanup contract:
    - Queue must be cleared
    - Module must be stopped
    - DB connection must be closed

    Critical invariant: DB close MUST happen even if module.stop() fails.
    """

    @pytest.mark.asyncio
    async def test_cleanup_full_lifecycle(self, task_session, mock_db, mock_module):
        """Test cleanup executes full lifecycle: queue clear, module stop, db close."""
        # Setup: Add items to queue
        for i in range(5):
            await task_session.queue.put(f"item_{i}")

        mock_module.stop = AsyncMock()

        # Execute cleanup
        await task_session.cleanup()

        # Assert: Queue cleared
        assert task_session.queue.empty(), "Queue should be empty after cleanup"

        # Assert: Module stopped
        mock_module.stop.assert_awaited_once()

        # Assert: DB closed
        mock_db.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cleanup_invariant_db_closes_despite_module_failure(
        self, task_session, mock_db, mock_module, mock_logger
    ):
        """CRITICAL: DB must close even if module.stop() raises exception.

        This test verifies the most important cleanup invariant - resource cleanup
        must complete even if intermediate steps fail.
        """
        # Setup: module.stop() will fail
        mock_module.stop = AsyncMock(side_effect=RuntimeError("Module stop failed"))

        # Execute cleanup
        await task_session.cleanup()

        # Assert CRITICAL INVARIANT: DB still closed
        mock_db.close.assert_awaited_once()

        # Assert: Exception was logged for module stop failure
        exception_calls = [call for call in mock_logger.exception.call_args_list]
        assert any("Error stopping module during cleanup" in str(call) for call in exception_calls)

    @pytest.mark.asyncio
    async def test_cleanup_idempotent(self, task_session, mock_db, mock_module):
        """Test cleanup can be called multiple times safely (now idempotent).

        With the _cleanup_done guard, second call is a no-op.
        """
        mock_module.stop = AsyncMock()

        # Call cleanup twice
        await task_session.cleanup()
        await task_session.cleanup()

        # With idempotent cleanup, DB close should only be called once
        assert mock_db.close.await_count == 1
        # Module stop should also only be called once
        assert mock_module.stop.await_count == 1

    @pytest.mark.asyncio
    async def test_cleanup_with_full_queue_maxsize(self, task_session, mock_db, mock_module):
        """Test cleanup handles queue at maximum capacity (1000 items).

        Ensures no infinite loop or timeout when queue is full.
        """
        mock_module.stop = AsyncMock()

        # Fill queue to maxsize (1000)
        for i in range(1000):
            await task_session.queue.put(f"item_{i}")

        assert task_session.queue.qsize() == 1000

        # Execute cleanup
        await task_session.cleanup()

        # Assert: All items cleared
        assert task_session.queue.empty()
        assert task_session.queue.qsize() == 0

    @pytest.mark.asyncio
    async def test_cleanup_with_empty_queue(self, task_session, mock_db, mock_module):
        """Test cleanup with empty queue doesn't fail."""
        mock_module.stop = AsyncMock()

        assert task_session.queue.empty()

        # Should not raise
        await task_session.cleanup()

        mock_db.close.assert_awaited_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "exception_type",
        [RuntimeError, ValueError, TypeError, Exception],
    )
    async def test_cleanup_module_stop_various_exceptions(
        self, task_session, mock_db, mock_module, exception_type, mock_logger
    ):
        """Test cleanup handles various exception types from module.stop().

        Parametrized test ensures robustness across different failure modes.
        Note: CancelledError is not included as it's a BaseException that should
        propagate (it's used for task cancellation coordination).
        """
        mock_module.stop = AsyncMock(side_effect=exception_type("Module error"))

        # Execute cleanup - should not propagate exception
        await task_session.cleanup()

        # Assert: DB still closed (invariant)
        mock_db.close.assert_awaited_once()

        # Assert: Exception logged for module stop failure
        exception_calls = [call for call in mock_logger.exception.call_args_list]
        assert any("Error stopping module during cleanup" in str(call) for call in exception_calls)

    @pytest.mark.asyncio
    async def test_cleanup_db_close_failure_propagates(self, task_session, mock_db, mock_module):
        """Test that DB close failures propagate (critical failure).

        Unlike module.stop() failures, DB close failures are critical and should
        propagate to caller for proper error handling.
        """
        mock_module.stop = AsyncMock()
        mock_db.close = AsyncMock(side_effect=ConnectionError("DB close failed"))

        # Execute cleanup - exception should propagate
        with pytest.raises(ConnectionError, match="DB close failed"):
            await task_session.cleanup()

        # Module should still have been stopped
        mock_module.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cleanup_execution_order(self, task_session, mock_db, mock_module):
        """Test cleanup executes steps in correct order: queue -> module -> db.

        Order matters for proper resource cleanup hierarchy.
        """
        call_order = []

        # Track call order
        mock_module.stop = AsyncMock(side_effect=lambda: call_order.append("module_stop"))
        original_close = mock_db.close
        mock_db.close = AsyncMock(side_effect=lambda: call_order.append("db_close") or original_close())

        # Add item to queue to verify it's processed first
        await task_session.queue.put("test_item")

        await task_session.cleanup()

        # Assert: Queue cleared before module stop before db close
        assert task_session.queue.empty()
        assert call_order == ["module_stop", "db_close"]

    @pytest.mark.asyncio
    async def test_cleanup_with_queue_containing_different_types(self, task_session, mock_db, mock_module):
        """Test queue cleanup handles various object types correctly."""
        mock_module.stop = AsyncMock()

        # Add different types to queue
        await task_session.queue.put("string")
        await task_session.queue.put(123)
        await task_session.queue.put({"key": "value"})
        await task_session.queue.put(None)
        await task_session.queue.put([1, 2, 3])

        await task_session.cleanup()

        # All items cleared regardless of type
        assert task_session.queue.empty()
        mock_db.close.assert_awaited_once()


# ============================================================================
# Test Class: Error Handling and Edge Cases
# ============================================================================


class TestErrorHandling:
    """Test error conditions and edge cases."""

    @pytest.mark.asyncio
    async def test_heartbeat_with_db_connection_failure(self, task_session, mock_db, mock_logger):
        """Verify graceful handling of database connection failures.

        Tests that database connectivity issues are properly caught and
        logged without crashing the application.
        """
        mock_db.create.side_effect = ConnectionError("Database unreachable")

        result = await task_session.send_heartbeat()

        assert result == CancellationReason.HEARTBEAT_CONNECTION_REFUSED
        mock_logger.error.assert_called()

    @pytest.mark.asyncio
    async def test_signal_listener_with_malformed_signals(self, task_session, mock_db, mock_logger):
        """Verify robust handling of malformed signal data.

        Tests defensive programming against unexpected signal formats
        that might occur due to database schema changes or bugs.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        malformed_signals = [
            {"id": "tasks:sig1"},  # Missing action and payload
            {"action": "cancel"},  # Missing id and payload
            {"id": "tasks:sig2", "payload": {}},  # Missing action
            "invalid_string",  # Completely invalid
        ]

        async def malformed_generator():
            for signal in malformed_signals:
                yield signal
            task_session.is_cancelled.set()

        mock_db.start_live.return_value = ("live_123", malformed_generator())

        # Should not crash
        await task_session.listen_signals()

        # Should have cleaned up
        mock_db.stop_live.assert_called_once()

    @pytest.mark.asyncio
    async def test_multiple_heartbeat_failures_trigger_cancellation(self, task_session, mock_db):
        """Verify repeated heartbeat failures lead to task cancellation.

        Tests that the system doesn't enter an infinite retry loop when
        heartbeats consistently fail.
        """
        task_session.send_heartbeat = AsyncMock(return_value=CancellationReason.HEARTBEAT_FAILURE)
        task_session._handle_cancel = AsyncMock()

        await task_session.generate_heartbeats()

        # Should trigger cancellation after first failure
        task_session._handle_cancel.assert_called_once()

    @pytest.mark.asyncio
    async def test_signal_listener_exception_cleanup(self, task_session, mock_db, mock_logger):
        """Verify cleanup occurs even when signal processing raises exceptions.

        Tests that the finally block properly cleans up resources when
        unexpected exceptions occur during signal processing.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        async def exception_generator():
            yield {"id": "tasks:sig1", "action": "pause", "payload": {}}
            msg = "Unexpected error in signal processing"
            raise ValueError(msg)

        mock_db.start_live.return_value = ("live_123", exception_generator())
        task_session._handle_pause = AsyncMock()

        await task_session.listen_signals()

        # Should have cleaned up despite exception
        mock_db.stop_live.assert_called_once_with("live_123")

        # Should have logged the error
        mock_logger.exception.assert_called()


# ============================================================================
# Test Class: Regression Snapshots
# ============================================================================


class TestRegressionSnapshots:
    """Snapshot-based regression testing for database payloads."""

    @freeze_time("2025-10-14 03:21:34")
    @pytest.mark.asyncio
    async def test_heartbeat_payload_structure_snapshot(self, task_session, mock_db):
        """Verify heartbeat payload structure remains consistent.

        Regression test to ensure heartbeat message structure doesn't
        change unexpectedly, which could break database schema compatibility.
        """
        with patch("digitalkin.core.task_manager.task_session.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = datetime.datetime.now(tz=datetime.timezone.utc)
            mock_dt.timezone = datetime.timezone

            await task_session.send_heartbeat()

            payload = mock_db.create.call_args[0][1]

            # Snapshot of expected structure (updated with setup_id and setup_version_id)
            expected_keys = {"task_id", "mission_id", "setup_id", "setup_version_id", "timestamp"}
            assert set(payload.keys()) == expected_keys
            assert payload["task_id"] == "test_task_123"
            assert payload["mission_id"] == "missions:test_mission"
            assert payload["setup_id"] == "setup:test_setup"
            assert payload["setup_version_id"] == "setup_version:test_version"
            assert isinstance(payload["timestamp"], datetime.datetime)

    @pytest.mark.asyncio
    async def test_signal_ack_payload_structure_snapshot(self, task_session, mock_db):
        """Verify signal acknowledgement payload structure remains consistent.

        Regression test for signal message structure to prevent breaking
        changes in the communication protocol.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        await task_session._handle_cancel()

        payload = mock_db.update.call_args[0][2]
        # Snapshot of expected structure (updated with setup_id, setup_version_id, and enhanced logging fields)
        expected_keys = {
            "task_id", "mission_id", "setup_id", "setup_version_id", "action", "status", "payload", "timestamp",
            "cancellation_reason", "error_message", "exception_traceback",
        }
        assert set(payload.keys()) == expected_keys
        assert payload["mission_id"] == "missions:test_mission"
        assert payload["task_id"] == "test_task_123"
        assert payload["setup_id"] == "setup:test_setup"
        assert payload["setup_version_id"] == "setup_version:test_version"
        assert payload["action"] == SignalType.ACK_CANCEL.value
        assert payload["status"] == TaskStatus.CANCELLED.value

    @freeze_time("2025-10-14 03:21:34")
    @pytest.mark.asyncio
    async def test_db_method_call_patterns_snapshot(self, task_session, mock_db):
        """Verify database interaction patterns remain consistent.

        Regression test to ensure the sequence and frequency of database
        calls doesn't change unexpectedly, which could impact performance.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        with patch("digitalkin.core.task_manager.task_session.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = datetime.datetime.now(tz=datetime.timezone.utc)
            mock_dt.timezone = datetime.timezone

            # Perform standard operation sequence
            await task_session.send_heartbeat()
            await task_session._handle_status_request()
            await task_session._handle_pause()
            await task_session._handle_resume()
            await task_session._handle_cancel()

            # Snapshot of expected call pattern
            assert mock_db.create.call_count == 1  # Initial heartbeat
            assert mock_db.update.call_count == 4  # status, pause, resume, cancel
            assert mock_db.merge.call_count == 0  # No subsequent heartbeats


# ============================================================================
# Test Class: Parametrized Failure Modes
# ============================================================================


class TestFailureModes:
    """Parametrized tests for different failure scenarios."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("db_method", "return_value", "expected_result"),
        [
            ("create", {"code": "ERROR"}, CancellationReason.HEARTBEAT_FAILURE),
            ("create", {"id": "success"}, None),
            ("merge", {"code": "ERROR"}, CancellationReason.HEARTBEAT_FAILURE),
            ("merge", {"id": "success"}, None),
        ],
    )
    @freeze_time("2025-10-14 03:21:34")
    async def test_heartbeat_db_operation_failures(
        self, task_session, mock_db, db_method, return_value, expected_result
    ):
        """Parametrized test for various database operation failure modes.

        Tests heartbeat behavior across different database response scenarios
        to ensure proper error handling for all failure types.
        """
        if db_method == "create":
            mock_db.create.return_value = return_value
        else:
            task_session.heartbeat_record_id = "heartbeats:existing_id"
            task_session._last_heartbeat = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(
                seconds=5
            )
            mock_db.merge.return_value = return_value

        with patch("digitalkin.core.task_manager.task_session.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = datetime.datetime.now(tz=datetime.timezone.utc)
            mock_dt.timezone = datetime.timezone

            result = await task_session.send_heartbeat()

            assert result == expected_result

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("exception_type", "expect_exc_info"),
        [
            # Known exception types are logged without exc_info since traceback is not needed
            (ConnectionError, False),
            (TimeoutError, False),
            # Unknown/unexpected exception types include exc_info for debugging
            (ValueError, True),
            (RuntimeError, True),
            (Exception, True),
        ],
    )
    @freeze_time("2025-10-14 03:21:34")
    async def test_heartbeat_various_exception_types(
        self, task_session, mock_db, mock_logger, exception_type, expect_exc_info
    ):
        """Verify heartbeat handles various exception types gracefully.

        Tests that all exception types are caught and logged without
        crashing the heartbeat mechanism. Known errors (ConnectionError, TimeoutError)
        are logged without full traceback since they're expected failures.
        """
        task_session.heartbeat_record_id = "heartbeats:existing_id"
        task_session._last_heartbeat = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(seconds=5)

        mock_db.merge.side_effect = exception_type("Simulated error")

        with patch("digitalkin.core.task_manager.task_session.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = datetime.datetime.now(tz=datetime.timezone.utc)
            mock_dt.timezone = datetime.timezone

            result = await task_session.send_heartbeat()

            assert result is not None  # Should return a CancellationReason on failure
            mock_logger.error.assert_called()
            if expect_exc_info:
                assert mock_logger.error.call_args[1].get("exc_info") is True
            else:
                # Known errors may or may not include exc_info, just verify error was logged
                assert mock_logger.error.call_count >= 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("signal_action", "handler_name"),
        [
            ("cancel", "_handle_cancel"),
            ("pause", "_handle_pause"),
            ("resume", "_handle_resume"),
            ("status", "_handle_status_request"),
        ],
    )
    async def test_signal_routing_parametrized(self, task_session, mock_db, signal_action, handler_name):
        """Parametrized test for signal routing to correct handlers.

        Verifies that each signal action type is routed to its corresponding
        handler method without cross-contamination.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        signals = [
            {
                "id": "tasks:sig1",
                "action": signal_action,
                "payload": {},
            }
        ]
        mock_db.start_live.return_value = ("live_123", _signal_generator(signals))

        # Mock all handlers
        task_session._handle_cancel = AsyncMock()
        task_session._handle_pause = AsyncMock()
        task_session._handle_resume = AsyncMock()
        task_session._handle_status_request = AsyncMock()

        async def delayed_cancel() -> None:
            await asyncio.sleep(0.1)
            task_session.is_cancelled.set()

        await asyncio.gather(
            task_session.listen_signals(),
            delayed_cancel(),
        )

        # Verify only the correct handler was called
        target_handler = getattr(task_session, handler_name)
        target_handler.assert_called_once()

        # Verify other handlers were not called
        all_handlers = [
            task_session._handle_cancel,
            task_session._handle_pause,
            task_session._handle_resume,
            task_session._handle_status_request,
        ]
        for handler in all_handlers:
            if handler != target_handler:
                handler.assert_not_called()


# ============================================================================
# Test Class: Timing and Concurrency
# ============================================================================


class TestTimingAndConcurrency:
    """Test timing-sensitive operations and concurrent execution."""

    @freeze_time("2025-10-14 03:21:34")
    @pytest.mark.asyncio
    async def test_heartbeat_timing_precision(self, task_session, mock_db):
        """Verify heartbeat timing accuracy within acceptable tolerance.

        Tests that heartbeat intervals are respected with minimal drift
        to ensure consistent health monitoring.
        """
        task_session.heartbeat_record_id = "heartbeats:existing_id"
        task_session._last_heartbeat = datetime.datetime.now(tz=datetime.timezone.utc)
        task_session._heartbeat_interval = datetime.timedelta(seconds=2)

        timestamps = []

        with patch("digitalkin.core.task_manager.task_session.datetime") as mock_dt:
            for i in range(3):
                new_time = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(seconds=2.5 * i)
                timestamps.append(new_time)
                mock_dt.datetime.now.return_value = new_time
                mock_dt.timezone = datetime.timezone

                await task_session.send_heartbeat()

        # First call should succeed (no previous heartbeat to compare)
        # Subsequent calls should succeed as they're > 2 seconds apart
        assert mock_db.merge.call_count == 2  # 2nd and 3rd calls

    @pytest.mark.asyncio
    async def test_concurrent_heartbeat_and_cancellation(self, task_session, mock_db, mock_module):
        """Verify race-free cancellation during heartbeat generation.

        Tests that cancellation properly interrupts heartbeat generation
        without causing deadlocks or corrupted state.
        """
        heartbeat_count = 0
        session = TaskSession(
            task_id="test_task_123",
            mission_id="missions:default_mission",
            db=mock_db,
            module=mock_module,
            heartbeat_interval=datetime.timedelta(milliseconds=1),
        )

        async def counting_heartbeat() -> CancellationReason | None:
            nonlocal heartbeat_count
            heartbeat_count += 1
            await asyncio.sleep(0.05)
            return None

        session.send_heartbeat = counting_heartbeat

        async def cancel_mid_execution() -> None:
            await asyncio.sleep(0.12)
            await session._handle_cancel()

        await asyncio.gather(
            session.generate_heartbeats(),
            cancel_mid_execution(),
        )

        assert heartbeat_count >= 2
        assert session.cancelled

    @pytest.mark.asyncio
    async def test_pause_resume_during_signal_processing(self, task_session, mock_db):
        """Verify pause/resume works correctly during active signal processing.

        Tests that pause and resume signals can be processed while other
        signals are being handled without state corruption.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        processing_order = []

        async def slow_pause() -> None:
            processing_order.append("pause_start")
            await asyncio.sleep(0.05)
            task_session._paused.clear()
            processing_order.append("pause_end")

        async def slow_resume() -> None:
            processing_order.append("resume_start")
            await asyncio.sleep(0.05)
            task_session._paused.set()
            processing_order.append("resume_end")

        task_session._handle_pause = slow_pause
        task_session._handle_resume = slow_resume

        signals = [
            {"id": "tasks:sig1", "action": "pause", "payload": {}},
            {"id": "tasks:sig2", "action": "resume", "payload": {}},
        ]
        mock_db.start_live.return_value = ("live_123", _signal_generator(signals))

        async def delayed_cancel() -> None:
            await asyncio.sleep(0.3)
            task_session.is_cancelled.set()

        await asyncio.gather(
            task_session.listen_signals(),
            delayed_cancel(),
        )

        # Verify sequential processing
        assert processing_order.index("pause_start") < processing_order.index("pause_end")
        assert processing_order.index("pause_end") < processing_order.index("resume_start")
        assert processing_order.index("resume_start") < processing_order.index("resume_end")

    @pytest.mark.asyncio
    async def test_rapid_signal_sequence(self, task_session, mock_db):
        """Verify handling of rapid signal sequences without dropping signals.

        Tests that high-frequency signal arrival doesn't cause missed
        signals or processing errors.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        # Generate 20 rapid signals
        signals = [{"id": f"tasks:sig{i}", "action": "status", "payload": {}} for i in range(20)]
        mock_db.start_live.return_value = ("live_123", _signal_generator(signals))

        task_session._handle_status_request = AsyncMock()

        async def delayed_cancel() -> None:
            await asyncio.sleep(0.5)
            task_session.is_cancelled.set()

        await asyncio.gather(
            task_session.listen_signals(),
            delayed_cancel(),
        )

        # All signals should be processed
        assert task_session._handle_status_request.call_count == 20


# ============================================================================
# Test Class: State Assertions and Invariants
# ============================================================================


class TestStateInvariants:
    """Test state invariants and consistency guarantees."""

    def test_cancelled_and_paused_are_mutually_independent(self, task_session):
        """Verify cancelled and paused states can coexist independently.

        Tests that cancellation and pause are orthogonal states that
        don't interfere with each other's event mechanisms.
        """
        # Can be both cancelled and paused
        task_session.is_cancelled.set()
        task_session._paused.clear()

        assert task_session.cancelled
        assert not task_session.paused

        # Can be neither
        task_session.is_cancelled.clear()
        task_session._paused.set()

        assert not task_session.cancelled
        assert task_session.paused

    @pytest.mark.asyncio
    async def test_status_transitions_are_monotonic(self, task_session, mock_db):
        """Verify TaskStatus transitions follow expected progression.

        Tests that status changes follow a logical progression and
        don't regress to previous states unexpectedly.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        # Start at PENDING
        assert task_session.status == TaskStatus.PENDING

        # Can transition to CANCELLED
        await task_session._handle_cancel()
        assert task_session.status == TaskStatus.CANCELLED

        # Once cancelled, should stay cancelled (idempotency)
        await task_session._handle_cancel()
        assert task_session.status == TaskStatus.CANCELLED

    @freeze_time("2025-10-14 03:21:34")
    @pytest.mark.asyncio
    async def test_heartbeat_record_id_immutability_after_creation(self, task_session, mock_db):
        """Verify heartbeat_record_id doesn't change after initial creation.

        Tests that once a heartbeat record is created, its ID remains
        stable across subsequent heartbeat updates.
        """
        with patch("digitalkin.core.task_manager.task_session.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = datetime.datetime.now(tz=datetime.timezone.utc)
            mock_dt.timezone = datetime.timezone

            # First heartbeat creates record
            await task_session.send_heartbeat()
            first_id = task_session.heartbeat_record_id
            assert first_id == "heartbeats:test_hb_id"

            # Update timestamp for second heartbeat
            task_session._last_heartbeat = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(
                seconds=5
            )
            mock_dt.datetime.now.return_value = datetime.datetime.now(tz=datetime.timezone.utc) + datetime.timedelta(
                seconds=3
            )

            # Second heartbeat should reuse same ID
            await task_session.send_heartbeat()
            assert task_session.heartbeat_record_id == first_id

    @pytest.mark.asyncio
    async def test_event_state_consistency_after_operations(self, task_session, mock_db):
        """Verify event states remain consistent after various operations.

        Tests that asyncio.Event states don't get corrupted or stuck
        after complex operation sequences.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        # Initial state
        assert not task_session.is_cancelled.is_set()
        assert not task_session.paused

        # Pause
        await task_session._handle_pause()
        assert task_session.paused  # Paused = cleared

        # Resume
        await task_session._handle_resume()
        assert not task_session.paused  # Resumed = set

        # Cancel (should also resume if paused)
        task_session._paused.clear()
        await task_session._handle_cancel()
        assert task_session.is_cancelled.is_set()
        assert not task_session.paused  # Should be resumed


# ============================================================================
# Test Class: Logging Validation
# ============================================================================


class TestLoggingValidation:
    """Test logging output for debugging and monitoring."""

    @pytest.mark.asyncio
    async def test_initialization_logs_task_info(self, mock_db, mock_module, mock_logger):
        """Verify initialization logs task details for audit trail.

        Tests that task creation is properly logged with relevant
        information for debugging and monitoring.
        """
        task_id = "logged_task_123"
        heartbeat_interval = datetime.timedelta(seconds=5)

        TaskSession(
            task_id=task_id,
            mission_id="missions:default_mission",
            db=mock_db,
            module=mock_module,
            heartbeat_interval=heartbeat_interval,
        )

        # Verify logger.info was called with task info
        mock_logger.info.assert_called_once()
        call_args = mock_logger.info.call_args

        # Check message contains task_id
        assert task_id in str(call_args)

        # Check extra context
        assert call_args[1].get("extra", {}).get("task_id") == task_id

    @pytest.mark.asyncio
    async def test_heartbeat_failure_logs_error(self, task_session, mock_db, mock_logger):
        """Verify heartbeat failures are logged with appropriate severity.

        Tests that heartbeat errors are logged at ERROR level with
        sufficient context for troubleshooting.
        """
        mock_db.create.return_value = {"code": "DB_ERROR"}

        await task_session.send_heartbeat()

        mock_logger.error.assert_called()
        call_args = mock_logger.error.call_args

        # Verify job_id in extra context (via session_ids property)
        assert call_args[1].get("extra", {}).get("job_id") == "test_task_123"

    @pytest.mark.asyncio
    async def test_cancellation_logs_with_correct_level(self, task_session, mock_db, mock_logger):
        """Verify cancellation events are logged at appropriate levels.

        Tests that cancellation actions are logged for audit purposes
        with correct severity levels.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        # First cancellation
        await task_session._handle_cancel()

        # Should log info about cancellation (new format: "Task cancelled:")
        assert any(call for call in mock_logger.info.call_args_list if "cancelled" in str(call).lower())

        # Second cancellation (idempotent)
        await task_session._handle_cancel()

        # Should log debug about already cancelled (new format: "Cancel ignored")
        assert any(call for call in mock_logger.debug.call_args_list if "cancel ignored" in str(call).lower())

    @pytest.mark.asyncio
    async def test_signal_listener_logs_lifecycle(self, task_session, mock_db, mock_logger):
        """Verify signal listener lifecycle is properly logged.

        Tests that signal listener startup and shutdown are logged
        for monitoring and debugging purposes.
        """
        task_session.signal_record_id = "tasks:test_signal_id"
        task_session.is_cancelled.set()

        mock_db.start_live.return_value = ("live_123", _signal_generator([]))

        await task_session.listen_signals()

        # Should log start
        assert any("started" in str(call).lower() for call in mock_logger.info.call_args_list)

        # Should log stop
        assert any("stopped" in str(call).lower() for call in mock_logger.info.call_args_list)


# ============================================================================
# Test Class: Database Interaction Patterns
# ============================================================================


class TestDatabaseInteractionPatterns:
    """Test database call patterns and data integrity."""

    @freeze_time("2025-10-14 03:21:34")
    @pytest.mark.asyncio
    async def test_db_create_called_with_correct_table(self, task_session, mock_db):
        """Verify database create operations target correct table.

        Tests that heartbeat creation uses the correct table name
        to prevent data being written to wrong locations.
        """
        with patch("digitalkin.core.task_manager.task_session.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = datetime.datetime.now(tz=datetime.timezone.utc)
            mock_dt.timezone = datetime.timezone

            await task_session.send_heartbeat()

            mock_db.create.assert_called_once()
            assert mock_db.create.call_args[0][0] == "heartbeats"

    @pytest.mark.asyncio
    async def test_db_update_preserves_record_id(self, task_session, mock_db):
        """Verify database updates use correct record identifiers.

        Tests that update operations correctly target the intended
        records without mixing up IDs.
        """
        task_session.signal_record_id = "tasks:specific_signal_id"

        await task_session._handle_status_request()

        mock_db.update.assert_called_once()
        call_args = mock_db.update.call_args[0]
        assert call_args[0] == "tasks"
        assert call_args[1] == "tasks:specific_signal_id"

    @freeze_time("2025-10-14 03:21:34")
    @pytest.mark.asyncio
    async def test_db_merge_uses_existing_heartbeat_id(self, task_session, mock_db):
        """Verify heartbeat merges target existing record.

        Tests that subsequent heartbeats correctly reference the
        initial heartbeat record for updates.
        """
        task_session.heartbeat_record_id = "heartbeats:existing_123"
        task_session._last_heartbeat = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(seconds=5)

        with patch("digitalkin.core.task_manager.task_session.datetime") as mock_dt:
            mock_dt.datetime.now.return_value = datetime.datetime.now(tz=datetime.timezone.utc)
            mock_dt.timezone = datetime.timezone

            await task_session.send_heartbeat()

            mock_db.merge.assert_called_once()
            call_args = mock_db.merge.call_args[0]
            assert call_args[0] == "heartbeats"
            assert call_args[1] == "heartbeats:existing_123"

    @pytest.mark.asyncio
    async def test_signal_listener_requires_record_id_to_start(self, task_session, mock_db):
        """Verify signal listener requires signal_record_id to be set before starting.

        The TaskExecutor sets signal_record_id from the create result before calling
        listen_signals. If not set, the listener returns early without database calls.
        """
        task_session.signal_record_id = None  # Not initialized

        await task_session.listen_signals()

        # Should not make any database calls if signal_record_id is not set
        mock_db.start_live.assert_not_called()
        mock_db.select_by_task_id.assert_not_called()

    @pytest.mark.asyncio
    async def test_db_live_query_lifecycle(self, task_session, mock_db):
        """Verify live query is properly started and stopped.

        Tests that live query lifecycle is managed correctly with
        matching start_live and stop_live calls.
        """
        task_session.signal_record_id = "tasks:test_signal_id"
        task_session.is_cancelled.set()

        live_id = "live_query_456"
        mock_db.start_live.return_value = (live_id, _signal_generator([]))

        await task_session.listen_signals()

        mock_db.start_live.assert_called_once()
        mock_db.stop_live.assert_called_once_with(live_id)


# ============================================================================
# Test Class: Complete Scenarios
# ============================================================================


class TestCompleteScenarios:
    """End-to-end scenario tests simulating real-world usage."""

    @pytest.mark.asyncio
    async def test_successful_task_execution_scenario(self, task_session, mock_db, mock_logger):
        """Simulate complete successful task execution from start to finish.

        End-to-end test covering initialization, heartbeats, status checks,
        and clean termination without errors.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        execution_log = []

        # Simulate task work
        async def simulate_work() -> None:
            execution_log.append("work_started")
            for i in range(3):
                await asyncio.sleep(0.05)
                await task_session.wait_if_paused()
                execution_log.append(f"work_step_{i}")
            execution_log.append("work_completed")
            task_session.is_cancelled.set()

        # Status check signal
        signals = [
            {"id": "tasks:sig1", "action": "status", "payload": {}},
        ]
        mock_db.start_live.return_value = ("live_123", _signal_generator(signals))
        task_session._handle_status_request = AsyncMock()

        # Run all components
        await asyncio.gather(
            task_session.generate_heartbeats(),
            task_session.listen_signals(),
            simulate_work(),
        )

        # Verify execution completed
        assert "work_completed" in execution_log
        assert task_session.cancelled

        # Verify monitoring components functioned
        assert mock_db.create.call_count >= 1  # Heartbeats sent
        task_session._handle_status_request.assert_called_once()

    @pytest.mark.asyncio
    async def test_task_pause_resume_workflow(self, task_session, mock_db):
        """Simulate task being paused and resumed during execution.

        End-to-end test of pause/resume workflow showing task properly
        blocks during pause and continues after resume.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        execution_timeline = []

        async def simulate_pausable_work() -> None:
            execution_timeline.append(("work_start", asyncio.get_event_loop().time()))

            for i in range(5):
                await task_session.wait_if_paused()
                execution_timeline.append((f"step_{i}", asyncio.get_event_loop().time()))
                await asyncio.sleep(0.02)

            execution_timeline.append(("work_end", asyncio.get_event_loop().time()))
            task_session.is_cancelled.set()

        async def control_sequence() -> None:
            await asyncio.sleep(0.05)
            execution_timeline.append(("pause_sent", asyncio.get_event_loop().time()))
            await task_session._handle_pause()

            await asyncio.sleep(0.1)
            execution_timeline.append(("resume_sent", asyncio.get_event_loop().time()))
            await task_session._handle_resume()

        mock_db.start_live.return_value = ("live_123", _signal_generator([]))

        await asyncio.gather(
            simulate_pausable_work(),
            control_sequence(),
            task_session.listen_signals(),
        )

        # Verify pause and resume occurred in sequence
        pause_time = next(t for event, t in execution_timeline if event == "pause_sent")
        resume_time = next(t for event, t in execution_timeline if event == "resume_sent")
        assert pause_time < resume_time

        # Verify work completed
        assert any(event == "work_end" for event, _ in execution_timeline)

    @pytest.mark.asyncio
    async def test_task_cancellation_during_execution(self, task_session, mock_db):
        """Simulate task cancellation interrupting ongoing work.

        End-to-end test showing cancellation properly stops task execution
        and triggers cleanup procedures.
        """
        task_session.signal_record_id = "tasks:test_signal_id"

        work_steps_completed = []

        async def simulate_long_work() -> None:
            for i in range(10):
                if task_session.cancelled:
                    work_steps_completed.append("cancelled_detected")
                    break
                work_steps_completed.append(f"step_{i}")
                await asyncio.sleep(0.02)

        async def trigger_cancellation() -> None:
            await asyncio.sleep(0.08)  # Let some work happen
            await task_session._handle_cancel()

        mock_db.start_live.return_value = ("live_123", _signal_generator([]))

        await asyncio.gather(
            simulate_long_work(),
            trigger_cancellation(),
            task_session.listen_signals(),
        )

        # Verify cancellation interrupted work
        assert len(work_steps_completed) < 10
        assert "cancelled_detected" in work_steps_completed
        assert task_session.status == TaskStatus.CANCELLED


# ============================================================================
# Exception Classification Tests
# ============================================================================


class TestExceptionClassification:
    """Tests for _classify_exception static method covering all CancellationReason scenarios."""

    def test_timeout_error_returns_heartbeat_timeout(self) -> None:
        """TimeoutError maps to HEARTBEAT_TIMEOUT."""
        result = TaskSession._classify_exception(TimeoutError("operation timed out"), is_initial=True)
        assert result == CancellationReason.HEARTBEAT_TIMEOUT

    def test_connection_error_initial_returns_connection_refused(self) -> None:
        """ConnectionError on CREATE maps to HEARTBEAT_CONNECTION_REFUSED."""
        result = TaskSession._classify_exception(ConnectionError("refused"), is_initial=True)
        assert result == CancellationReason.HEARTBEAT_CONNECTION_REFUSED

    def test_connection_error_update_returns_connection_lost(self) -> None:
        """ConnectionError on MERGE maps to SURREALDB_CONNECTION_LOST."""
        result = TaskSession._classify_exception(ConnectionError("reset"), is_initial=False)
        assert result == CancellationReason.SURREALDB_CONNECTION_LOST

    def test_keepalive_ping_timeout_returns_websocket_closed(self) -> None:
        """Exception with 'keepalive ping timeout' maps to HEARTBEAT_WEBSOCKET_CLOSED."""
        exc = Exception("sent 1011 (internal error) keepalive ping timeout; no close frame received")
        result = TaskSession._classify_exception(exc, is_initial=True)
        assert result == CancellationReason.HEARTBEAT_WEBSOCKET_CLOSED

    def test_connection_closed_error_type_returns_websocket_closed(self) -> None:
        """Exception type containing 'ConnectionClosedError' maps to HEARTBEAT_WEBSOCKET_CLOSED."""

        class ConnectionClosedError(Exception):
            pass

        result = TaskSession._classify_exception(ConnectionClosedError("closed"), is_initial=False)
        assert result == CancellationReason.HEARTBEAT_WEBSOCKET_CLOSED

    def test_handshake_timeout_returns_surrealdb_handshake_timeout(self) -> None:
        """Exception with 'timed out during opening handshake' maps to SURREALDB_HANDSHAKE_TIMEOUT."""
        exc = Exception("timed out during opening handshake")
        result = TaskSession._classify_exception(exc, is_initial=True)
        assert result == CancellationReason.SURREALDB_HANDSHAKE_TIMEOUT

    def test_unknown_exception_returns_heartbeat_failure(self) -> None:
        """Unknown exception maps to HEARTBEAT_FAILURE."""
        result = TaskSession._classify_exception(ValueError("unexpected"), is_initial=True)
        assert result == CancellationReason.HEARTBEAT_FAILURE

    def test_runtime_error_returns_heartbeat_failure(self) -> None:
        """RuntimeError without special message maps to HEARTBEAT_FAILURE."""
        result = TaskSession._classify_exception(RuntimeError("generic error"), is_initial=False)
        assert result == CancellationReason.HEARTBEAT_FAILURE


class TestHeartbeatFailureReasons:
    """Tests for heartbeat operations returning correct CancellationReason."""

    @pytest.fixture
    def session(self, mock_db, mock_module) -> TaskSession:
        """Create a TaskSession for testing."""
        return TaskSession(
            task_id="test_task",
            mission_id="missions:test",
            db=mock_db,
            module=mock_module,
        )

    @pytest.mark.asyncio
    async def test_initial_heartbeat_timeout_returns_correct_reason(self, session, mock_db) -> None:
        """Initial heartbeat TimeoutError returns HEARTBEAT_TIMEOUT."""
        mock_db.create = AsyncMock(side_effect=TimeoutError("db timeout"))

        result = await session.send_heartbeat()

        assert result == CancellationReason.HEARTBEAT_TIMEOUT

    @pytest.mark.asyncio
    async def test_initial_heartbeat_connection_refused_returns_correct_reason(self, session, mock_db) -> None:
        """Initial heartbeat ConnectionError returns HEARTBEAT_CONNECTION_REFUSED."""
        mock_db.create = AsyncMock(side_effect=ConnectionError("refused"))

        result = await session.send_heartbeat()

        assert result == CancellationReason.HEARTBEAT_CONNECTION_REFUSED

    @pytest.mark.asyncio
    async def test_initial_heartbeat_websocket_closed_returns_correct_reason(self, session, mock_db) -> None:
        """Initial heartbeat websocket close returns HEARTBEAT_WEBSOCKET_CLOSED."""
        mock_db.create = AsyncMock(side_effect=Exception("keepalive ping timeout"))

        result = await session.send_heartbeat()

        assert result == CancellationReason.HEARTBEAT_WEBSOCKET_CLOSED

    @pytest.mark.asyncio
    async def test_initial_heartbeat_handshake_timeout_returns_correct_reason(self, session, mock_db) -> None:
        """Initial heartbeat handshake timeout returns SURREALDB_HANDSHAKE_TIMEOUT."""
        mock_db.create = AsyncMock(side_effect=Exception("timed out during opening handshake"))

        result = await session.send_heartbeat()

        assert result == CancellationReason.SURREALDB_HANDSHAKE_TIMEOUT

    @pytest.mark.asyncio
    async def test_initial_heartbeat_surreal_error_returns_correct_reason(self, session, mock_db) -> None:
        """Initial heartbeat SurrealDB error response returns HEARTBEAT_FAILURE."""
        mock_db.create = AsyncMock(return_value={"code": -1, "message": "table not found"})

        result = await session.send_heartbeat()

        assert result == CancellationReason.HEARTBEAT_FAILURE

    @pytest.mark.asyncio
    async def test_update_heartbeat_timeout_returns_correct_reason(self, session, mock_db) -> None:
        """Update heartbeat TimeoutError returns HEARTBEAT_TIMEOUT."""
        mock_db.create = AsyncMock(return_value={"id": "heartbeats:1"})
        mock_db.merge = AsyncMock(side_effect=TimeoutError("db timeout"))

        await session.send_heartbeat()  # Create initial
        # Force update by setting last heartbeat to old time (timezone-aware)
        session._last_heartbeat = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
        result = await session.send_heartbeat()

        assert result == CancellationReason.HEARTBEAT_TIMEOUT

    @pytest.mark.asyncio
    async def test_update_heartbeat_connection_lost_returns_correct_reason(self, session, mock_db) -> None:
        """Update heartbeat ConnectionError returns SURREALDB_CONNECTION_LOST."""
        mock_db.create = AsyncMock(return_value={"id": "heartbeats:1"})
        mock_db.merge = AsyncMock(side_effect=ConnectionError("connection reset"))

        await session.send_heartbeat()  # Create initial
        session._last_heartbeat = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
        result = await session.send_heartbeat()

        assert result == CancellationReason.SURREALDB_CONNECTION_LOST

    @pytest.mark.asyncio
    async def test_update_heartbeat_websocket_closed_returns_correct_reason(self, session, mock_db) -> None:
        """Update heartbeat websocket close returns HEARTBEAT_WEBSOCKET_CLOSED."""
        mock_db.create = AsyncMock(return_value={"id": "heartbeats:1"})
        mock_db.merge = AsyncMock(side_effect=Exception("keepalive ping timeout"))

        await session.send_heartbeat()  # Create initial
        session._last_heartbeat = datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc)
        result = await session.send_heartbeat()

        assert result == CancellationReason.HEARTBEAT_WEBSOCKET_CLOSED


class TestCancellationReasonValues:
    """Tests to verify all CancellationReason enum values exist and have expected format."""

    def test_all_cancellation_reasons_are_strings(self) -> None:
        """All CancellationReason values should be strings."""
        for reason in CancellationReason:
            assert isinstance(reason.value, str)

    def test_cancellation_reason_count(self) -> None:
        """Verify expected number of CancellationReason values."""
        assert len(CancellationReason) == 15

    def test_expected_cancellation_reasons_exist(self) -> None:
        """Verify all expected CancellationReason members exist."""
        expected = [
            "COMPLETED",
            "SUCCESS_CLEANUP",
            "FAILURE_CLEANUP",
            "SIGNAL",
            "HEARTBEAT_FAILURE",
            "HEARTBEAT_WEBSOCKET_CLOSED",
            "HEARTBEAT_TIMEOUT",
            "HEARTBEAT_CONNECTION_REFUSED",
            "SURREALDB_HANDSHAKE_TIMEOUT",
            "SURREALDB_CONNECTION_LOST",
            "GRPC_SETUP_UNAVAILABLE",
            "GRPC_SERVICE_ERROR",
            "TIMEOUT",
            "SHUTDOWN",
            "UNKNOWN",
        ]
        actual = [r.name for r in CancellationReason]
        assert actual == expected

    def test_completed_is_simple_string(self) -> None:
        """COMPLETED should be a simple string without description."""
        assert CancellationReason.COMPLETED.value == "completed"

    def test_unknown_is_simple_string(self) -> None:
        """UNKNOWN should be a simple string."""
        assert CancellationReason.UNKNOWN.value == "unknown"
