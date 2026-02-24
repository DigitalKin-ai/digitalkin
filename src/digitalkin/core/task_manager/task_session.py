"""Task session easing task lifecycle management."""

import asyncio
import contextlib
import datetime
import traceback
from collections.abc import AsyncGenerator

from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.logger import logger
from digitalkin.models.core.task_monitor import (
    CancellationReason,
    HeartbeatMessage,
    SignalMessage,
    SignalType,
    TaskStatus,
)
from digitalkin.modules._base_module import BaseModule


class TaskSession:
    """Task Session with lifecycle management.

    The Session defined the whole lifecycle of a task as an epheneral context.
    """

    db: SurrealDBConnection
    module: BaseModule

    status: TaskStatus
    signal_queue: AsyncGenerator | None

    task_id: str
    mission_id: str
    signal_record_id: str | None
    heartbeat_record_id: str | None

    started_at: datetime.datetime | None
    completed_at: datetime.datetime | None

    is_cancelled: asyncio.Event
    cancellation_reason: CancellationReason
    _stream_closed: asyncio.Event
    _heartbeat_interval: datetime.timedelta
    _last_heartbeat: datetime.datetime

    # Exception tracking for enhanced DB logging
    _last_exception: str | None
    _last_traceback: str | None

    # Cleanup guard for idempotent cleanup
    _cleanup_done: bool

    # Connection ownership: when False, cleanup() skips db.close() (shared connection)
    _owns_connection: bool

    def __init__(
        self,
        task_id: str,
        mission_id: str,
        db: SurrealDBConnection,
        module: BaseModule,
        heartbeat_interval: datetime.timedelta = datetime.timedelta(seconds=2),
        queue_maxsize: int = 1000,
        *,
        owns_connection: bool = True,
    ) -> None:
        """Initialize Task Session.

        Args:
            task_id: Unique task identifier
            mission_id: Mission identifier
            db: SurrealDB connection
            module: Module instance
            heartbeat_interval: Interval between heartbeats
            queue_maxsize: Maximum size for the queue (0 = unlimited)
            owns_connection: If True (default), cleanup() closes the DB connection.
                If False, the connection is shared and cleanup() leaves it open.
        """
        self.db = db
        self.module = module

        self.status = TaskStatus.PENDING
        # Bounded queue to prevent unbounded memory growth (max 1000 items)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)

        self.task_id = task_id
        self.mission_id = mission_id

        self.heartbeat = None
        self.started_at = None
        self.completed_at = None

        self.signal_record_id = None
        self.heartbeat_record_id = None

        self.is_cancelled = asyncio.Event()
        self.cancellation_reason = CancellationReason.UNKNOWN
        self._stream_closed = asyncio.Event()
        self._heartbeat_interval = heartbeat_interval

        # Exception tracking
        self._last_exception = None
        self._last_traceback = None

        # Cleanup guard
        self._cleanup_done = False

        # Connection ownership
        self._owns_connection = owns_connection

        logger.debug(
            "TaskSession initialized (heartbeat_interval=%.1fs)",
            heartbeat_interval.total_seconds(),
            extra={"task_id": task_id, "mission_id": mission_id},
        )

    @property
    def cancelled(self) -> bool:
        """Task cancellation status."""
        return self.is_cancelled.is_set()

    @property
    def stream_closed(self) -> bool:
        """Check if stream termination was signaled."""
        return self._stream_closed.is_set()

    def close_stream(self) -> None:
        """Signal that the stream should terminate."""
        self._stream_closed.set()

    @property
    def setup_id(self) -> str:
        """Get setup_id from module context."""
        return self.module.context.session.setup_id

    @property
    def setup_version_id(self) -> str:
        """Get setup_version_id from module context."""
        return self.module.context.session.setup_version_id

    @property
    def session_ids(self) -> dict[str, str]:
        """Get all session IDs from module context for structured logging."""
        return self.module.context.session.current_ids()

    def record_exception(self, exc: Exception) -> None:
        """Record exception details for DB logging.

        Args:
            exc: The exception that caused the task to fail.
        """
        self._last_exception = str(exc)
        self._last_traceback = traceback.format_exc()

    async def send_heartbeat(self) -> CancellationReason | None:
        """Rate-limited heartbeat with connection resilience and detailed error tracking.

        Returns:
            None if heartbeat was successful, CancellationReason on failure.
        """
        heartbeat = HeartbeatMessage(
            task_id=self.task_id,
            mission_id=self.mission_id,
            setup_id=self.setup_id,
            setup_version_id=self.setup_version_id,
            timestamp=datetime.datetime.now(datetime.timezone.utc),
        )

        if self.heartbeat_record_id is None:
            return await self._send_initial_heartbeat(heartbeat)

        if (heartbeat.timestamp - self._last_heartbeat) < self._heartbeat_interval:
            return None

        return await self._send_heartbeat_update(heartbeat)

    @staticmethod
    def _classify_exception(e: Exception, *, is_initial: bool) -> CancellationReason:
        """Classify exception to CancellationReason.

        Args:
            e: The exception to classify.
            is_initial: True for CREATE, False for MERGE (affects ConnectionError mapping).

        Returns:
            CancellationReason based on exception type and message.
        """
        if isinstance(e, TimeoutError):
            return CancellationReason.HEARTBEAT_TIMEOUT
        if isinstance(e, ConnectionError):
            if is_initial:
                return CancellationReason.HEARTBEAT_CONNECTION_REFUSED
            return CancellationReason.SURREALDB_CONNECTION_LOST
        error_msg = str(e)
        error_type = type(e).__name__
        if "keepalive ping timeout" in error_msg or "ConnectionClosedError" in error_type:
            return CancellationReason.HEARTBEAT_WEBSOCKET_CLOSED
        if "timed out during opening handshake" in error_msg:
            return CancellationReason.SURREALDB_HANDSHAKE_TIMEOUT
        return CancellationReason.HEARTBEAT_FAILURE

    async def _send_initial_heartbeat(self, heartbeat: HeartbeatMessage) -> CancellationReason | None:
        """Send the initial heartbeat CREATE to SurrealDB.

        Args:
            heartbeat: The heartbeat message to create.

        Returns:
            None if successful, CancellationReason on failure.
        """
        try:
            result = await self.db.create("heartbeats", heartbeat.model_dump())
        except Exception as e:
            reason = self._classify_exception(e, is_initial=True)
            logger.error(
                "Heartbeat CREATE exception (%s): %s",
                reason.value,
                e,
                extra=self.session_ids,
                exc_info=reason == CancellationReason.HEARTBEAT_FAILURE,
            )
            return reason
        else:
            if not isinstance(result, dict):
                return CancellationReason.HEARTBEAT_FAILURE
            if "code" not in result:
                self.heartbeat_record_id = result.get("id")
                self._last_heartbeat = heartbeat.timestamp
                logger.debug("Heartbeat CREATE ok (record_id=%s)", self.heartbeat_record_id, extra=self.session_ids)
                return None
            logger.error(
                "Heartbeat CREATE failed [%s]: %s",
                result.get("code"),
                result.get("message", result.get("information", "unknown")),
                extra=self.session_ids,
            )
            return CancellationReason.HEARTBEAT_FAILURE

    async def _send_heartbeat_update(self, heartbeat: HeartbeatMessage) -> CancellationReason | None:
        """Send a heartbeat MERGE/UPDATE to SurrealDB.

        Args:
            heartbeat: The heartbeat message to merge.

        Returns:
            None if successful, CancellationReason on failure.
        """
        try:
            result = await self.db.merge(
                "heartbeats",
                self.heartbeat_record_id,  # type: ignore[arg-type]
                heartbeat.model_dump(),
            )
        except Exception as e:
            reason = self._classify_exception(e, is_initial=False)
            logger.error(
                "Heartbeat MERGE exception (%s): %s",
                reason.value,
                e,
                extra=self.session_ids,
                exc_info=reason == CancellationReason.HEARTBEAT_FAILURE,
            )
            return reason
        else:
            if not isinstance(result, dict):
                return CancellationReason.HEARTBEAT_FAILURE
            if "code" not in result:
                self._last_heartbeat = heartbeat.timestamp
                return None
            logger.warning(
                "Heartbeat MERGE failed [%s]: %s",
                result.get("code"),
                result.get("message", result.get("information", "unknown")),
                extra=self.session_ids,
            )
            return CancellationReason.HEARTBEAT_FAILURE

    async def generate_heartbeats(self) -> None:
        """Periodic heartbeat generator with cancellation support and detailed failure reasons."""
        logger.debug("Heartbeat generator started", extra=self.session_ids)
        while not self.cancelled:
            failure_reason = await self.send_heartbeat()
            if failure_reason is not None:
                logger.error("Heartbeat failed, cancelling task (%s)", failure_reason.name, extra=self.session_ids)
                await self._handle_cancel(failure_reason)
                break
            await asyncio.sleep(self._heartbeat_interval.total_seconds())

    async def listen_signals(self) -> None:
        """Signal listener for cancel and status signals.

        Filters signals by task_id so each listener only processes its own signals.
        AsyncSurreal multiplexes operations over a single WebSocket via JSON-RPC
        request IDs, so concurrent heartbeat + signal operations on self.db are safe.

        Raises:
            CancelledError: If task is cancelled during signal listening.
        """
        logger.info("Signal listener started", extra=self.session_ids)

        # signal_record_id must be set by TaskExecutor before calling this method.
        # If not set, we cannot filter signals correctly - abort early.
        if self.signal_record_id is None:
            logger.error(
                "signal_record_id not set - cannot start signal listener without valid record ID",
                extra=self.session_ids,
            )
            return

        live_id, live_signals = await self.db.start_live("tasks")
        try:
            async for signal in live_signals:
                logger.debug("Signal received: %s", signal, extra=self.session_ids)
                # Check both cancelled and stream_closed to ensure clean shutdown
                if self.cancelled or self.stream_closed:
                    break

                if (
                    signal is None
                    or signal["id"] == self.signal_record_id
                    or "payload" not in signal
                    or signal.get("task_id") != self.task_id
                ):
                    continue

                if signal["action"] == "cancel":
                    await self._handle_cancel(CancellationReason.SIGNAL)
                elif signal["action"] == "status":
                    await self._handle_status_request()

        except asyncio.CancelledError:
            logger.debug("Signal listener cancelled", extra=self.session_ids)
            raise
        except Exception:
            logger.exception("Signal listener fatal error", extra=self.session_ids)
        finally:
            with contextlib.suppress(Exception):  # Connection may already be closed
                await self.db.stop_live(live_id)
            logger.info("Signal listener stopped", extra=self.session_ids)

    async def _handle_cancel(self, reason: CancellationReason = CancellationReason.UNKNOWN) -> None:
        """Idempotent cancellation with acknowledgment and reason tracking.

        Args:
            reason: The reason for cancellation (signal, heartbeat failure, cleanup, etc.)
        """
        if self.cancelled:
            logger.debug(
                "Cancel ignored - already cancelled (existing=%s, new=%s)",
                self.cancellation_reason.value,
                reason.value,
                extra=self.session_ids,
            )
            return

        self.cancellation_reason = reason
        self.status = TaskStatus.CANCELLED
        self.is_cancelled.set()

        # Log with appropriate level based on reason
        if reason in {CancellationReason.SUCCESS_CLEANUP, CancellationReason.FAILURE_CLEANUP}:
            logger.debug("Task cancelled (%s)", reason.value, extra=self.session_ids)
        else:
            logger.info("Task cancelled (%s)", reason.value, extra=self.session_ids)

        try:
            await self.db.update(
                "tasks",
                self.signal_record_id,  # type: ignore # Guaranteed non-None by guard in listen_signals
                SignalMessage(
                    task_id=self.task_id,
                    mission_id=self.mission_id,
                    setup_id=self.setup_id,
                    setup_version_id=self.setup_version_id,
                    action=SignalType.ACK_CANCEL,
                    status=self.status,
                    cancellation_reason=reason,
                ).model_dump(exclude_none=True),
            )
        except Exception:
            logger.warning("Cancel ack failed (best-effort)", extra=self.session_ids)

    async def _handle_status_request(self) -> None:
        """Send current task status."""
        try:
            await self.db.update(
                "tasks",
                self.signal_record_id,  # type: ignore # Guaranteed non-None by guard in listen_signals
                SignalMessage(
                    task_id=self.task_id,
                    mission_id=self.mission_id,
                    setup_id=self.setup_id,
                    setup_version_id=self.setup_version_id,
                    status=self.status,
                    action=SignalType.ACK_STATUS,
                ).model_dump(exclude_none=True),
            )
        except Exception:
            logger.warning("Status ack failed (best-effort)", extra=self.session_ids)

        logger.debug("Status report sent", extra=self.session_ids)

    async def cleanup(self) -> None:
        """Clean up task session resources.

        This method is idempotent - safe to call multiple times.
        Second and subsequent calls are no-ops.

        This includes:
        - Clearing queue to free memory
        - Cleaning up module context services
        - Stopping module
        - Closing database connection
        - Clearing module reference
        """
        # Use basic IDs for logging since module may already be None from previous cleanup
        ids = {"task_id": self.task_id, "mission_id": self.mission_id}

        if self._cleanup_done:
            logger.debug("Cleanup already done", extra=ids)
            return
        self._cleanup_done = True

        # Clear queue to free memory
        try:
            while not self.queue.empty():
                self.queue.get_nowait()
        except asyncio.QueueEmpty:
            pass

        # Clean up module context services (e.g., gRPC channel pool)
        if self.module is not None and self.module.context is not None:
            try:
                await self.module.context.cleanup()
            except Exception:
                logger.exception("Error cleaning up module context", extra=ids)

        # Stop module
        try:
            await self.module.stop()
        except Exception:
            logger.exception("Error stopping module during cleanup", extra=ids)

        # Close DB connection only if this session owns it (kills all live queries)
        if self._owns_connection:
            await self.db.close()

        # Clear module reference to allow garbage collection
        self.module = None  # type: ignore[assignment]  # Allow GC; typed as BaseModule but set to None after cleanup
