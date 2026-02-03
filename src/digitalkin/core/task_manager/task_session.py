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
    _paused: asyncio.Event
    _stream_closed: asyncio.Event
    _heartbeat_interval: datetime.timedelta
    _last_heartbeat: datetime.datetime

    # Exception tracking for enhanced DB logging
    _last_exception: str | None
    _last_traceback: str | None

    # Cleanup guard for idempotent cleanup
    _cleanup_done: bool

    def __init__(
        self,
        task_id: str,
        mission_id: str,
        db: SurrealDBConnection,
        module: BaseModule,
        heartbeat_interval: datetime.timedelta = datetime.timedelta(seconds=2),
        queue_maxsize: int = 1000,
    ) -> None:
        """Initialize Task Session.

        Args:
            task_id: Unique task identifier
            mission_id: Mission identifier
            db: SurrealDB connection
            module: Module instance
            heartbeat_interval: Interval between heartbeats
            queue_maxsize: Maximum size for the queue (0 = unlimited)
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
        self._paused = asyncio.Event()
        self._stream_closed = asyncio.Event()
        self._heartbeat_interval = heartbeat_interval

        # Exception tracking
        self._last_exception = None
        self._last_traceback = None

        # Cleanup guard
        self._cleanup_done = False

        logger.info(
            "TaskSession initialized",
            extra={
                "task_id": task_id,
                "mission_id": mission_id,
                "heartbeat_interval": str(heartbeat_interval),
            },
        )

    @property
    def cancelled(self) -> bool:
        """Task cancellation status."""
        return self.is_cancelled.is_set()

    @property
    def paused(self) -> bool:
        """Task paused status."""
        return self._paused.is_set()

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

    async def send_heartbeat(self) -> bool:
        """Rate-limited heartbeat with connection resilience.

        Returns:
            bool: True if heartbeat was successful, False otherwise
        """
        heartbeat = HeartbeatMessage(
            task_id=self.task_id,
            mission_id=self.mission_id,
            setup_id=self.setup_id,
            setup_version_id=self.setup_version_id,
            timestamp=datetime.datetime.now(datetime.timezone.utc),
        )

        if self.heartbeat_record_id is None:
            try:
                success = await self.db.create("heartbeats", heartbeat.model_dump())
                if "code" not in success:
                    self.heartbeat_record_id = success.get("id")  # type: ignore
                    self._last_heartbeat = heartbeat.timestamp
                    return True
            except Exception as e:
                logger.error(
                    "Heartbeat exception",
                    extra={**self.session_ids, "error": str(e)},
                    exc_info=True,
                )
            logger.error("Initial heartbeat failed", extra=self.session_ids)
            return False

        if (heartbeat.timestamp - self._last_heartbeat) < self._heartbeat_interval:
            logger.debug(
                "Heartbeat skipped due to rate limiting",
                extra={**self.session_ids, "delta": str(heartbeat.timestamp - self._last_heartbeat)},
            )
            return True

        try:
            success = await self.db.merge("heartbeats", self.heartbeat_record_id, heartbeat.model_dump())
            if "code" not in success:
                self._last_heartbeat = heartbeat.timestamp
                return True
        except Exception as e:
            logger.error(
                "Heartbeat exception",
                extra={**self.session_ids, "error": str(e)},
                exc_info=True,
            )
        logger.warning("Heartbeat failed", extra=self.session_ids)
        return False

    async def generate_heartbeats(self) -> None:
        """Periodic heartbeat generator with cancellation support."""
        logger.debug("Heartbeat generator started", extra=self.session_ids)
        while not self.cancelled:
            logger.debug(
                "Heartbeat tick",
                extra={**self.session_ids, "cancelled": self.cancelled},
            )
            success = await self.send_heartbeat()
            if not success:
                logger.error("Heartbeat failed, cancelling task", extra=self.session_ids)
                await self._handle_cancel(CancellationReason.HEARTBEAT_FAILURE)
                break
            await asyncio.sleep(self._heartbeat_interval.total_seconds())

    async def wait_if_paused(self) -> None:
        """Block execution if task is paused."""
        if self._paused.is_set():
            logger.info("Task paused, waiting for resume", extra=self.session_ids)
            await self._paused.wait()

    async def listen_signals(self) -> None:  # noqa: C901
        """Enhanced signal listener with comprehensive handling.

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
                logger.debug("Signal received", extra={**self.session_ids, "signal": signal})
                # Check both cancelled and stream_closed to ensure clean shutdown
                if self.cancelled or self.stream_closed:
                    break

                if signal is None or signal["id"] == self.signal_record_id or "payload" not in signal:
                    continue

                if signal["action"] == "cancel":
                    await self._handle_cancel(CancellationReason.SIGNAL)
                elif signal["action"] == "pause":
                    await self._handle_pause()
                elif signal["action"] == "resume":
                    await self._handle_resume()
                elif signal["action"] == "status":
                    await self._handle_status_request()

        except asyncio.CancelledError:
            logger.debug("Signal listener cancelled", extra=self.session_ids)
            raise
        except Exception as e:
            logger.error(
                "Signal listener fatal error",
                extra={**self.session_ids, "error": str(e)},
                exc_info=True,
            )
        finally:
            with contextlib.suppress(Exception):  # Connection may already be closed
                await self.db.stop_live(live_id)
            logger.info("Signal listener stopped", extra=self.session_ids)

    async def _handle_cancel(self, reason: CancellationReason = CancellationReason.UNKNOWN) -> None:
        """Idempotent cancellation with acknowledgment and reason tracking.

        Args:
            reason: The reason for cancellation (signal, heartbeat failure, cleanup, etc.)
        """
        if self.is_cancelled.is_set():
            logger.debug(
                "Cancel ignored - already cancelled",
                extra={
                    **self.session_ids,
                    "existing_reason": self.cancellation_reason.value,
                    "new_reason": reason.value,
                },
            )
            return

        self.cancellation_reason = reason
        self.status = TaskStatus.CANCELLED
        self.is_cancelled.set()

        # Log with appropriate level based on reason
        if reason in {CancellationReason.SUCCESS_CLEANUP, CancellationReason.FAILURE_CLEANUP}:
            logger.debug(
                "Task cancelled (cleanup)",
                extra={**self.session_ids, "cancellation_reason": reason.value},
            )
        else:
            logger.info(
                "Task cancelled",
                extra={**self.session_ids, "cancellation_reason": reason.value},
            )

        # Resume if paused so cancellation can proceed
        if self._paused.is_set():
            self._paused.set()

        await self.db.update(
            "tasks",
            self.signal_record_id,  # type: ignore
            SignalMessage(
                task_id=self.task_id,
                mission_id=self.mission_id,
                setup_id=self.setup_id,
                setup_version_id=self.setup_version_id,
                action=SignalType.ACK_CANCEL,
                status=self.status,
                cancellation_reason=reason,
            ).model_dump(),
        )

    async def _handle_pause(self) -> None:
        """Pause task execution."""
        if not self._paused.is_set():
            logger.info("Task paused", extra=self.session_ids)
            self._paused.set()

        await self.db.update(
            "tasks",
            self.signal_record_id,  # type: ignore
            SignalMessage(
                task_id=self.task_id,
                mission_id=self.mission_id,
                setup_id=self.setup_id,
                setup_version_id=self.setup_version_id,
                action=SignalType.ACK_PAUSE,
                status=self.status,
            ).model_dump(),
        )

    async def _handle_resume(self) -> None:
        """Resume paused task."""
        if self._paused.is_set():
            logger.info("Task resumed", extra=self.session_ids)
            self._paused.clear()

        await self.db.update(
            "tasks",
            self.signal_record_id,  # type: ignore
            SignalMessage(
                task_id=self.task_id,
                mission_id=self.mission_id,
                setup_id=self.setup_id,
                setup_version_id=self.setup_version_id,
                action=SignalType.ACK_RESUME,
                status=self.status,
            ).model_dump(),
        )

    async def _handle_status_request(self) -> None:
        """Send current task status."""
        await self.db.update(
            "tasks",
            self.signal_record_id,  # type: ignore
            SignalMessage(
                task_id=self.task_id,
                mission_id=self.mission_id,
                setup_id=self.setup_id,
                setup_version_id=self.setup_version_id,
                status=self.status,
                action=SignalType.ACK_STATUS,
            ).model_dump(),
        )

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
        if self._cleanup_done:
            logger.debug(
                "Cleanup already done, skipping",
                extra={"task_id": self.task_id, "mission_id": self.mission_id},
            )
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
                logger.exception(
                    "Error cleaning up module context",
                    extra={"mission_id": self.mission_id, "task_id": self.task_id},
                )

        # Stop module
        try:
            await self.module.stop()
        except Exception:
            logger.exception(
                "Error stopping module during cleanup",
                extra={"mission_id": self.mission_id, "task_id": self.task_id},
            )

        # Close DB connection (kills all live queries)
        await self.db.close()

        # Clear module reference to allow garbage collection
        self.module = None  # type: ignore
