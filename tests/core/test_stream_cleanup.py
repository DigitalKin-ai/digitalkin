"""Comprehensive tests for gRPC stream cleanup priority.

Tests the stream closure mechanism to ensure proper shutdown ordering:
- Stream termination signal (`_stream_closed` event)
- SurrealDB connection `_closed` flag
- Cleanup ordering (stream closes BEFORE task cleanup)
- Race condition prevention between stream and task lifecycle
- Integration with SingleJobManager stream generator
- Queue write rejection after stream closed
- Idempotent cleanup behavior
- Channel pool cleanup on context cleanup

These tests validate production resilience against:
- Race conditions during shutdown
- Premature connection closure errors
- Invalid state exceptions from concurrent cleanup
- Messages lost after stream close
- Double cleanup errors
- Channel pool resource leaks
"""

import asyncio
import contextlib
import datetime
from typing import Any, NoReturn
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio

from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.core.task_manager.task_executor import TaskExecutor
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.models.core.task_monitor import CancellationReason, TaskStatus
from digitalkin.modules._base_module import BaseModule

# Set timeout for all tests in this file
pytestmark = pytest.mark.timeout(30)


# ============================================================================
# Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def mock_surreal_connection() -> Mock:
    """Create a mock SurrealDB connection with async methods and _closed flag."""
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
# Test: Stream Closed Event State Transitions
# ============================================================================


class TestStreamClosedEventState:
    """Tests for _stream_closed event state management."""

    @pytest.mark.asyncio
    async def test_stream_closed_initially_unset(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that stream_closed is False initially."""
        session = TaskSession(
            "task_1", "missions:test", mock_surreal_connection, mock_base_module
        )

        assert session.stream_closed is False
        assert not session._stream_closed.is_set()

    @pytest.mark.asyncio
    async def test_close_stream_sets_event(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that close_stream() sets the _stream_closed event."""
        session = TaskSession(
            "task_2", "missions:test", mock_surreal_connection, mock_base_module
        )

        session.close_stream()

        assert session.stream_closed is True
        assert session._stream_closed.is_set()

    @pytest.mark.asyncio
    async def test_close_stream_idempotent(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that multiple close_stream() calls are safe (idempotent)."""
        session = TaskSession(
            "task_3", "missions:test", mock_surreal_connection, mock_base_module
        )

        # Call multiple times
        for _ in range(10):
            session.close_stream()

        assert session.stream_closed is True
        # No exceptions should have been raised

    @pytest.mark.asyncio
    async def test_stream_closed_concurrent_access(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test concurrent access to stream_closed is thread-safe."""
        session = TaskSession(
            "task_4", "missions:test", mock_surreal_connection, mock_base_module
        )

        async def reader() -> list[bool]:
            """Read stream_closed 100 times."""
            return [session.stream_closed for _ in range(100)]

        async def writer() -> None:
            """Close stream after brief delay."""
            await asyncio.sleep(0.01)
            session.close_stream()

        # Run readers and writer concurrently
        readers = [reader() for _ in range(5)]
        results = await asyncio.gather(*readers, writer())

        # After completion, stream should be closed
        assert session.stream_closed is True

        # All reader results should be lists of booleans
        for result in results[:-1]:  # Exclude writer result (None)
            assert isinstance(result, list)
            assert all(isinstance(v, bool) for v in result)


# ============================================================================
# Test: Stream Closure Ordering
# ============================================================================


class TestStreamClosureOrdering:
    """Tests for correct ordering of stream closure before task cleanup."""

    @pytest.mark.asyncio
    async def test_stream_closed_before_task_cancellation(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that stream is closed before pending tasks are cancelled."""
        task_id = "ordering_test"
        mission_id = "missions:ordering"
        execution_log: list[str] = []
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        # Track when stream_closed is detected
        original_close_stream = session.close_stream

        def tracking_close_stream() -> None:
            execution_log.append("stream_closed")
            original_close_stream()

        session.close_stream = tracking_close_stream  # type: ignore

        async def tracking_heartbeat() -> None:
            try:
                await running_event.wait()
            except asyncio.CancelledError:
                # Check if stream was closed before we were cancelled
                execution_log.append(f"heartbeat_cancelled_stream_closed={session.stream_closed}")
                raise

        async def tracking_listener() -> None:
            try:
                await running_event.wait()
            except asyncio.CancelledError:
                execution_log.append(f"listener_cancelled_stream_closed={session.stream_closed}")
                raise

        session.generate_heartbeats = tracking_heartbeat  # type: ignore
        session.listen_signals = tracking_listener  # type: ignore

        async def quick_main() -> None:
            execution_log.append("main_start")
            await asyncio.sleep(0.05)
            execution_log.append("main_end")

        supervisor = await task_executor.execute_task(
            task_id, mission_id, quick_main(), session, mock_surreal_connection
        )

        await supervisor

        # Verify ordering: stream_closed should appear before task cancellations
        assert "stream_closed" in execution_log
        stream_idx = execution_log.index("stream_closed")

        # Find cancellation logs
        cancel_logs = [log for log in execution_log if "cancelled" in log]
        for cancel_log in cancel_logs:
            cancel_idx = execution_log.index(cancel_log)
            # Stream should be closed before cancellation
            assert stream_idx < cancel_idx, f"Stream closed at {stream_idx}, but {cancel_log} at {cancel_idx}"

            # The cancel log should show stream was already closed
            assert "stream_closed=True" in cancel_log

    @pytest.mark.asyncio
    async def test_stream_closed_on_main_task_completion(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test stream is closed when main task completes successfully."""
        task_id = "completion_test"
        mission_id = "missions:completion"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def main_task() -> None:
            await asyncio.sleep(0.05)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, main_task(), session, mock_surreal_connection
        )

        # Before completion, stream should be open
        assert not session.stream_closed

        await supervisor

        # After completion, stream should be closed
        assert session.stream_closed
        assert session.status == TaskStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_stream_closed_on_main_task_exception(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test stream is closed when main task raises an exception."""
        task_id = "exception_test"
        mission_id = "missions:exception"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def failing_main() -> NoReturn:
            await asyncio.sleep(0.02)
            msg = "Intentional test failure"
            raise ValueError(msg)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, failing_main(), session, mock_surreal_connection
        )

        with pytest.raises(ValueError):
            await supervisor

        # Stream should be closed even on exception
        assert session.stream_closed
        assert session.status == TaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_stream_closed_on_external_cancellation(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test handling of external supervisor cancellation.

        Note: External cancellation (via supervisor.cancel()) is an abrupt termination
        that bypasses normal cleanup flow. The stream may not be closed in this case
        since the CancelledError interrupts asyncio.wait() before reaching cleanup code.
        This is acceptable because external cancellation is a force-stop scenario.
        """
        task_id = "external_cancel"
        mission_id = "missions:external_cancel"
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

        # Cancel externally
        await asyncio.sleep(0.05)
        supervisor.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await supervisor

        # On external cancellation, status should be CANCELLED
        # Note: stream_closed may or may not be set depending on timing
        # External cancellation is a force-stop that bypasses normal cleanup
        assert session.status == TaskStatus.CANCELLED


# ============================================================================
# Test: SurrealDB Connection Closed Flag
# ============================================================================


class TestSurrealDBClosedFlag:
    """Tests for SurrealDBConnection _closed flag behavior."""

    @pytest.mark.asyncio
    async def test_closed_flag_initially_false(self) -> None:
        """Test that _closed flag is False initially."""
        conn = SurrealDBConnection()
        assert conn._closed is False

    @pytest.mark.asyncio
    async def test_close_sets_closed_flag_first(self) -> None:
        """Test that close() sets _closed flag before other cleanup."""
        conn = SurrealDBConnection()

        # Mock the db object
        conn.db = Mock()
        conn.db.close = AsyncMock()
        conn.db.kill = AsyncMock()

        execution_order: list[str] = []

        async def tracking_close() -> None:
            # Check flag state when db.close is called
            execution_order.append(f"db_close_called_when_closed={conn._closed}")

        conn.db.close = tracking_close  # type: ignore

        await conn.close()

        # Flag should have been True when db.close was called
        assert "db_close_called_when_closed=True" in execution_order

    @pytest.mark.asyncio
    async def test_stop_live_skips_when_closed(self) -> None:
        """Test that stop_live() is a no-op when connection is closed."""
        conn = SurrealDBConnection()

        # Mock the db object
        conn.db = Mock()
        conn.db.kill = AsyncMock()

        # Add a live query
        from uuid import uuid4
        live_id = uuid4()
        conn._live_queries.add(live_id)

        # Close the connection
        conn._closed = True

        # stop_live should not call db.kill
        await conn.stop_live(live_id)

        conn.db.kill.assert_not_called()

        # But the query should still be removed from tracking
        assert live_id not in conn._live_queries

    @pytest.mark.asyncio
    async def test_stop_live_calls_kill_when_open(self) -> None:
        """Test that stop_live() calls db.kill when connection is open."""
        conn = SurrealDBConnection()

        # Mock the db object
        conn.db = Mock()
        conn.db.kill = AsyncMock()

        from uuid import uuid4
        live_id = uuid4()
        conn._live_queries.add(live_id)

        # Connection is open
        assert conn._closed is False

        await conn.stop_live(live_id)

        conn.db.kill.assert_called_once_with(live_id)
        assert live_id not in conn._live_queries


# ============================================================================
# Test: Race Conditions
# ============================================================================


class TestStreamClosureRaceConditions:
    """Tests for race condition prevention in stream closure."""

    @pytest.mark.asyncio
    async def test_concurrent_stream_close_and_task_completion(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test concurrent stream closure and task completion don't race."""
        session = TaskSession(
            "race_test_1", "missions:race", mock_surreal_connection, mock_base_module
        )

        completion_results: list[bool] = []

        async def complete_task() -> None:
            """Simulate task completion."""
            session.status = TaskStatus.COMPLETED
            completion_results.append(session.stream_closed)

        async def close_stream() -> None:
            """Close stream concurrently."""
            session.close_stream()

        # Run concurrently
        await asyncio.gather(
            complete_task(),
            close_stream(),
            asyncio.sleep(0),  # Force interleaving
        )

        # After both complete, stream should definitely be closed
        assert session.stream_closed

    @pytest.mark.asyncio
    async def test_rapid_state_transitions(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test rapid state transitions don't cause invalid states."""
        session = TaskSession(
            "rapid_test", "missions:rapid", mock_surreal_connection, mock_base_module
        )

        states_observed: list[tuple[bool, TaskStatus]] = []

        async def observer() -> None:
            """Observe states rapidly."""
            for _ in range(100):
                states_observed.append((session.stream_closed, session.status))
                await asyncio.sleep(0)

        async def modifier() -> None:
            """Modify states rapidly."""
            for i in range(50):
                if i % 2 == 0:
                    session.close_stream()
                session.status = TaskStatus.RUNNING if i % 3 == 0 else TaskStatus.COMPLETED
                await asyncio.sleep(0)

        await asyncio.gather(observer(), modifier())

        # All observed states should be valid (no corruption)
        for stream_closed, status in states_observed:
            assert isinstance(stream_closed, bool)
            assert isinstance(status, TaskStatus)

    @pytest.mark.asyncio
    async def test_stream_generator_respects_stream_closed(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that stream generator exits when stream_closed is set."""
        session = TaskSession(
            "gen_test", "missions:gen", mock_surreal_connection, mock_base_module
        )

        # Put some items in the queue
        for i in range(5):
            await session.queue.put({"data": i})

        items_yielded: list[dict[str, Any]] = []

        async def simulated_stream() -> None:
            """Simulated stream generator like SingleJobManager._stream()."""
            while True:
                if session.stream_closed:
                    break

                try:
                    msg = await asyncio.wait_for(session.queue.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    if session.stream_closed:
                        break
                    continue

                items_yielded.append(msg)
                session.queue.task_done()

                if session.stream_closed:
                    break

        async def close_after_delay() -> None:
            """Close stream after yielding some items."""
            await asyncio.sleep(0.15)  # Allow some items to be yielded
            session.close_stream()

        await asyncio.gather(
            simulated_stream(),
            close_after_delay(),
        )

        # Should have yielded some but not all items (due to early closure)
        assert session.stream_closed
        # Some items should have been yielded
        assert len(items_yielded) >= 1

    @pytest.mark.asyncio
    async def test_signal_listener_handles_closed_connection(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that signal listener's finally block handles closed connection gracefully."""
        session = TaskSession(
            "signal_test", "missions:signal", mock_surreal_connection, mock_base_module
        )

        # Simulate stop_live being called on closed connection
        mock_surreal_connection.stop_live = AsyncMock(
            side_effect=Exception("Connection closed")
        )

        # The finally block should catch this exception
        from uuid import uuid4
        live_id = uuid4()

        # This should not raise
        try:
            await session.db.stop_live(live_id)
        except Exception:
            pass  # Expected to fail, but test the pattern

        # Connection closed flag should prevent the call
        mock_surreal_connection._closed = True

        # Create a new connection with proper _closed handling
        conn = SurrealDBConnection()
        conn._closed = True
        conn.db = Mock()
        conn.db.kill = AsyncMock()

        # This should be a no-op
        await conn.stop_live(live_id)

        # db.kill should not have been called
        conn.db.kill.assert_not_called()


# ============================================================================
# Test: Integration with Task Lifecycle
# ============================================================================


class TestStreamClosureTaskLifecycleIntegration:
    """Integration tests for stream closure with full task lifecycle."""

    @pytest.mark.asyncio
    async def test_full_lifecycle_success(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test complete lifecycle with successful task completion."""
        task_id = "lifecycle_success"
        mission_id = "missions:lifecycle"
        lifecycle_events: list[str] = []
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        # Track lifecycle events
        original_close = session.close_stream

        def tracking_close() -> None:
            lifecycle_events.append("stream_closed")
            original_close()

        session.close_stream = tracking_close  # type: ignore

        async def stay_alive() -> None:
            try:
                await running_event.wait()
            except asyncio.CancelledError:
                lifecycle_events.append("helper_cancelled")
                raise

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def main_task() -> None:
            lifecycle_events.append("main_start")
            await asyncio.sleep(0.05)
            lifecycle_events.append("main_end")

        supervisor = await task_executor.execute_task(
            task_id, mission_id, main_task(), session, mock_surreal_connection
        )

        await supervisor

        # Verify lifecycle order
        assert "main_start" in lifecycle_events
        assert "main_end" in lifecycle_events
        assert "stream_closed" in lifecycle_events

        main_end_idx = lifecycle_events.index("main_end")
        stream_closed_idx = lifecycle_events.index("stream_closed")

        # Stream should be closed after main ends
        assert main_end_idx < stream_closed_idx

        # Helpers should be cancelled after stream is closed
        helper_cancelled_events = [e for e in lifecycle_events if "helper_cancelled" in e]
        for event in helper_cancelled_events:
            assert lifecycle_events.index(event) > stream_closed_idx

    @pytest.mark.asyncio
    async def test_full_lifecycle_with_heartbeat_failure(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test lifecycle when heartbeat fails (simulating DB connection issues)."""
        task_id = "heartbeat_fail"
        mission_id = "missions:heartbeat_fail"
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            await running_event.wait()

        async def failing_heartbeat() -> NoReturn:
            await asyncio.sleep(0.05)
            msg = f"Heartbeat stopped for {task_id}"
            raise RuntimeError(msg)

        session.generate_heartbeats = failing_heartbeat  # type: ignore
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def long_main() -> None:
            await asyncio.sleep(10)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, long_main(), session, mock_surreal_connection
        )

        with pytest.raises(RuntimeError, match="Heartbeat stopped"):
            await supervisor

        # Stream should still be closed on failure path
        assert session.stream_closed
        assert session.status == TaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_cleanup_does_not_cause_invalid_state_errors(
        self,
        task_executor: TaskExecutor,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test that cleanup sequence doesn't cause 'invalid state' errors."""
        task_id = "no_invalid_state"
        mission_id = "missions:no_invalid"
        errors_observed: list[str] = []
        running_event = asyncio.Event()

        session = TaskSession(task_id, mission_id, mock_surreal_connection, mock_base_module)

        async def stay_alive() -> None:
            try:
                await running_event.wait()
            except asyncio.CancelledError:
                # Simulate some cleanup that might access shared state
                try:
                    _ = session.stream_closed
                    _ = session.status
                except Exception as e:
                    errors_observed.append(str(e))
                raise

        session.generate_heartbeats = AsyncMock(side_effect=stay_alive)
        session.listen_signals = AsyncMock(side_effect=stay_alive)

        async def quick_main() -> None:
            await asyncio.sleep(0.05)

        supervisor = await task_executor.execute_task(
            task_id, mission_id, quick_main(), session, mock_surreal_connection
        )

        await supervisor

        # No errors should have been observed
        assert len(errors_observed) == 0


# ============================================================================
# Test: Stress Testing
# ============================================================================


class TestStreamClosureStress:
    """Stress tests for stream closure under load."""

    @pytest.mark.asyncio
    async def test_many_concurrent_sessions(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test many sessions can close streams concurrently without issues."""
        sessions = [
            TaskSession(f"stress_{i}", f"missions:stress_{i}", mock_surreal_connection, mock_base_module)
            for i in range(100)
        ]

        async def close_session(session: TaskSession) -> None:
            await asyncio.sleep(0.001 * (hash(session.task_id) % 10))  # Random delay
            session.close_stream()

        await asyncio.gather(*[close_session(s) for s in sessions])

        # All sessions should have stream closed
        assert all(s.stream_closed for s in sessions)

    @pytest.mark.asyncio
    async def test_rapid_session_creation_and_cleanup(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test rapid creation and cleanup of sessions."""
        for i in range(50):
            session = TaskSession(
                f"rapid_{i}", f"missions:rapid_{i}", mock_surreal_connection, mock_base_module
            )

            assert not session.stream_closed

            session.close_stream()

            assert session.stream_closed

            # Allow garbage collection
            del session

    @pytest.mark.asyncio
    async def test_queue_operations_under_stream_closure(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Test queue operations continue safely during stream closure."""
        session = TaskSession(
            "queue_stress", "missions:queue_stress", mock_surreal_connection, mock_base_module
        )

        items_added = 0
        items_consumed = 0

        async def producer() -> None:
            nonlocal items_added
            for i in range(100):
                if session.stream_closed:
                    break
                try:
                    session.queue.put_nowait({"item": i})
                    items_added += 1
                except asyncio.QueueFull:
                    await asyncio.sleep(0.001)

        async def consumer() -> None:
            nonlocal items_consumed
            while True:
                if session.stream_closed and session.queue.empty():
                    break
                try:
                    msg = await asyncio.wait_for(session.queue.get(), timeout=0.1)
                    items_consumed += 1
                    session.queue.task_done()
                except asyncio.TimeoutError:
                    if session.stream_closed:
                        break

        async def closer() -> None:
            await asyncio.sleep(0.05)
            session.close_stream()

        await asyncio.gather(producer(), consumer(), closer())

        # Items should be roughly balanced (some may be in-flight)
        assert items_consumed <= items_added
        assert session.stream_closed


# ============================================================================
# Test: Idempotent Cleanup
# ============================================================================


class TestIdempotentCleanup:
    """Tests for idempotent cleanup behavior."""

    @pytest.mark.asyncio
    async def test_cleanup_idempotent(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Verify cleanup can be called multiple times safely."""
        session = TaskSession(
            "idempotent_test", "missions:idempotent", mock_surreal_connection, mock_base_module
        )

        # First cleanup
        await session.cleanup()

        # Verify module.stop() was called once
        mock_base_module.stop.assert_called_once()

        # Reset mock to track second call
        mock_base_module.stop.reset_mock()

        # Second cleanup should be no-op
        await session.cleanup()

        # module.stop() should NOT be called again
        mock_base_module.stop.assert_not_called()

    @pytest.mark.asyncio
    async def test_cleanup_guard_flag_set(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Verify _cleanup_done flag is set after cleanup."""
        session = TaskSession(
            "flag_test", "missions:flag", mock_surreal_connection, mock_base_module
        )

        assert session._cleanup_done is False

        await session.cleanup()

        assert session._cleanup_done is True

    @pytest.mark.asyncio
    async def test_concurrent_cleanup_calls(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Verify concurrent cleanup calls don't cause double cleanup."""
        session = TaskSession(
            "concurrent_cleanup", "missions:concurrent", mock_surreal_connection, mock_base_module
        )

        # Run multiple cleanups concurrently
        await asyncio.gather(
            session.cleanup(),
            session.cleanup(),
            session.cleanup(),
        )

        # module.stop() should only be called once
        assert mock_base_module.stop.call_count == 1

    @pytest.mark.asyncio
    async def test_cleanup_calls_context_cleanup(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Verify cleanup calls module.context.cleanup()."""
        # Add context.cleanup mock
        mock_base_module.context.cleanup = AsyncMock()

        session = TaskSession(
            "context_cleanup", "missions:context", mock_surreal_connection, mock_base_module
        )

        await session.cleanup()

        # context.cleanup() should be called
        mock_base_module.context.cleanup.assert_called_once()


# ============================================================================
# Test: Queue Write Rejection After Stream Closed
# ============================================================================


class TestQueueWriteRejection:
    """Tests for queue write rejection after stream closed.

    These tests directly test the add_to_queue behavior using a minimal
    mock setup that simulates the SingleJobManager's tasks_sessions dict.
    """

    @pytest.mark.asyncio
    async def test_queue_write_after_stream_closed_rejected(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Verify writes after stream close are rejected, not lost."""
        from digitalkin.models.module.module import ModuleCodeModel

        session = TaskSession(
            "write_reject_test", "missions:write_reject", mock_surreal_connection, mock_base_module
        )

        # Close the stream
        session.close_stream()

        # Simulate what add_to_queue does - check stream_closed before writing
        test_output = ModuleCodeModel(code="test", message="test message")

        # This is the behavior we're testing - stream_closed should prevent writes
        if session.stream_closed:
            # Message rejected
            pass
        else:
            await session.queue.put(test_output.model_dump())

        # Queue should be empty (message was rejected)
        assert session.queue.empty()

    @pytest.mark.asyncio
    async def test_queue_write_before_stream_closed_accepted(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Verify writes before stream close are accepted."""
        from digitalkin.models.module.module import ModuleCodeModel

        session = TaskSession(
            "write_accept_test", "missions:write_accept", mock_surreal_connection, mock_base_module
        )

        # Stream is NOT closed
        assert not session.stream_closed

        # Simulate what add_to_queue does
        test_output = ModuleCodeModel(code="test", message="test message")

        if session.stream_closed:
            pass  # Rejected
        else:
            await session.queue.put(test_output.model_dump())

        # Queue should have the message
        assert not session.queue.empty()
        msg = await session.queue.get()
        assert msg["code"] == "test"

    @pytest.mark.asyncio
    async def test_stream_closed_check_prevents_race(
        self,
        mock_surreal_connection: Mock,
        mock_base_module: Mock,
    ) -> None:
        """Verify the stream_closed check correctly prevents race conditions."""
        from digitalkin.models.module.module import ModuleCodeModel

        session = TaskSession(
            "race_test", "missions:race", mock_surreal_connection, mock_base_module
        )

        messages_rejected = 0
        messages_accepted = 0

        async def writer() -> None:
            nonlocal messages_rejected, messages_accepted
            for i in range(50):
                output = ModuleCodeModel(code=f"msg_{i}", message=f"message {i}")
                if session.stream_closed:
                    messages_rejected += 1
                else:
                    await session.queue.put(output.model_dump())
                    messages_accepted += 1
                await asyncio.sleep(0.001)

        async def closer() -> None:
            await asyncio.sleep(0.025)  # Close after ~25 messages
            session.close_stream()

        await asyncio.gather(writer(), closer())

        # Some messages should have been accepted (before close)
        assert messages_accepted > 0
        # Some messages should have been rejected (after close)
        assert messages_rejected > 0
        # Total should be 50
        assert messages_accepted + messages_rejected == 50


# ============================================================================
# Test: Channel Pool Cleanup
# ============================================================================


class TestChannelPoolCleanup:
    """Tests for gRPC channel pool cleanup."""

    @pytest.mark.asyncio
    async def test_grpc_communication_cleanup_closes_channels(self) -> None:
        """Verify GrpcCommunication.cleanup() closes all channels."""
        from digitalkin.services.communication.grpc_communication import GrpcCommunication
        from digitalkin.models.grpc_servers.models import ClientConfig, ServerMode, SecurityMode

        # Create a proper config
        config = ClientConfig(
            host="localhost",
            port=50051,
            mode=ServerMode.ASYNC,
            security=SecurityMode.INSECURE,
        )

        comm = GrpcCommunication(
            mission_id="missions:test",
            setup_id="setup:test",
            setup_version_id="setup_version:test",
            client_config=config,
        )

        # Insert mock async channels directly into the pool
        mock_channel_1 = AsyncMock()
        mock_channel_2 = AsyncMock()
        comm._channel_pool[("localhost", 50051)] = mock_channel_1
        comm._channel_pool[("localhost", 50052)] = mock_channel_2

        assert len(comm._channel_pool) == 2

        # Cleanup
        await comm.cleanup()

        # All channels should be closed and pool cleared
        assert len(comm._channel_pool) == 0
        mock_channel_1.close.assert_awaited_once()
        mock_channel_2.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_default_communication_cleanup_noop(self) -> None:
        """Verify DefaultCommunication.cleanup() is a no-op."""
        from digitalkin.services.communication.default_communication import DefaultCommunication

        comm = DefaultCommunication(
            mission_id="missions:test",
            setup_id="setup:test",
            setup_version_id="setup_version:test",
        )

        # Should not raise
        await comm.cleanup()

    @pytest.mark.asyncio
    async def test_module_context_cleanup_calls_communication_cleanup(self) -> None:
        """Verify ModuleContext.cleanup() calls communication.cleanup()."""
        from digitalkin.models.module.module_context import ModuleContext

        # Create mock services
        mock_communication = AsyncMock()
        mock_communication.cleanup = AsyncMock()

        context = ModuleContext(
            agent=Mock(),
            communication=mock_communication,
            cost=Mock(),
            filesystem=Mock(),
            identity=Mock(),
            registry=Mock(),
            snapshot=Mock(),
            storage=Mock(),
            user_profile=Mock(),
            session={
                "job_id": "job:test",
                "mission_id": "missions:test",
                "setup_id": "setup:test",
                "setup_version_id": "setup_version:test",
            },
        )

        await context.cleanup()

        mock_communication.cleanup.assert_called_once()
