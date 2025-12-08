"""Task session easing task lifecycle management."""

import asyncio
import datetime
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
    _heartbeat_interval: datetime.timedelta
    _last_heartbeat: datetime.datetime

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
        self._heartbeat_interval = heartbeat_interval

        logger.info(
            "TaskContext initialized for task: '%s'",
            task_id,
            extra={"task_id": task_id, "mission_id": mission_id, "heartbeat_interval": heartbeat_interval},
        )

    @property
    def cancelled(self) -> bool:
        """Task cancellation status."""
        return self.is_cancelled.is_set()

    @property
    def paused(self) -> bool:
        """Task paused status."""
        return self._paused.is_set()

    async def send_heartbeat(self) -> bool:
        """Rate-limited heartbeat with connection resilience.

        Returns:
            bool: True if heartbeat was successful, False otherwise
        """
        heartbeat = HeartbeatMessage(
            task_id=self.task_id,
            mission_id=self.mission_id,
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
                    "Heartbeat exception for task: '%s'",
                    self.task_id,
                    extra={"task_id": self.task_id, "error": str(e)},
                    exc_info=True,
                )
            logger.error(
                "Initial heartbeat failed for task: '%s'",
                self.task_id,
                extra={"task_id": self.task_id},
            )
            return False

        if (heartbeat.timestamp - self._last_heartbeat) < self._heartbeat_interval:
            logger.debug(
                "Heartbeat skipped due to rate limiting for task: '%s' | delta=%s",
                self.task_id,
                heartbeat.timestamp - self._last_heartbeat,
            )
            return True

        try:
            success = await self.db.merge("heartbeats", self.heartbeat_record_id, heartbeat.model_dump())
            if "code" not in success:
                self._last_heartbeat = heartbeat.timestamp
                return True
        except Exception as e:
            logger.error(
                "Heartbeat exception for task: '%s'",
                self.task_id,
                extra={"task_id": self.task_id, "error": str(e)},
                exc_info=True,
            )
        logger.warning(
            "Heartbeat failed for task: '%s'",
            self.task_id,
            extra={"task_id": self.task_id},
        )
        return False

    async def generate_heartbeats(self) -> None:
        """Periodic heartbeat generator with cancellation support."""
        logger.debug(
            "Heartbeat generator started for task: '%s'",
            self.task_id,
            extra={"task_id": self.task_id, "mission_id": self.mission_id},
        )
        while not self.cancelled:
            logger.debug(
                "Heartbeat tick for task: '%s', cancelled=%s",
                self.task_id,
                self.cancelled,
                extra={"task_id": self.task_id, "mission_id": self.mission_id},
            )
            success = await self.send_heartbeat()
            if not success:
                logger.error(
                    "Heartbeat failed, cancelling task: '%s'",
                    self.task_id,
                    extra={"task_id": self.task_id, "mission_id": self.mission_id},
                )
                await self._handle_cancel(CancellationReason.HEARTBEAT_FAILURE)
                break
            await asyncio.sleep(self._heartbeat_interval.total_seconds())

    async def wait_if_paused(self) -> None:
        """Block execution if task is paused."""
        if self._paused.is_set():
            logger.info(
                "Task paused, waiting for resume: '%s'",
                self.task_id,
                extra={"task_id": self.task_id},
            )
            await self._paused.wait()

    async def listen_signals(self) -> None:  # noqa: C901
        """Enhanced signal listener with comprehensive handling.

        Raises:
            CancelledError: Asyncio when task cancelling
        """
        logger.info(
            "Signal listener started for task: '%s'",
            self.task_id,
            extra={"task_id": self.task_id},
        )
        if self.signal_record_id is None:
            self.signal_record_id = (await self.db.select_by_task_id("tasks", self.task_id)).get("id")

        live_id, live_signals = await self.db.start_live("tasks")
        try:
            async for signal in live_signals:
                logger.debug("Signal received for task '%s': %s", self.task_id, signal)
                if self.cancelled:
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
            logger.debug(
                "Signal listener cancelled for task: '%s'",
                self.task_id,
                extra={"task_id": self.task_id},
            )
            raise
        except Exception as e:
            logger.error(
                "Signal listener fatal error for task: '%s'",
                self.task_id,
                extra={"task_id": self.task_id, "error": str(e)},
                exc_info=True,
            )
        finally:
            await self.db.stop_live(live_id)
            logger.info(
                "Signal listener stopped for task: '%s'",
                self.task_id,
                extra={"task_id": self.task_id},
            )

    async def _handle_cancel(self, reason: CancellationReason = CancellationReason.UNKNOWN) -> None:
        """Idempotent cancellation with acknowledgment and reason tracking.

        Args:
            reason: The reason for cancellation (signal, heartbeat failure, cleanup, etc.)
        """
        if self.is_cancelled.is_set():
            logger.debug(
                "Cancel ignored - task already cancelled: '%s' (existing reason: %s, new reason: %s)",
                self.task_id,
                self.cancellation_reason.value,
                reason.value,
                extra={
                    "task_id": self.task_id,
                    "mission_id": self.mission_id,
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
                "Task cancelled (cleanup): '%s', reason: %s",
                self.task_id,
                reason.value,
                extra={
                    "task_id": self.task_id,
                    "mission_id": self.mission_id,
                    "cancellation_reason": reason.value,
                },
            )
        else:
            logger.info(
                "Task cancelled: '%s', reason: %s",
                self.task_id,
                reason.value,
                extra={
                    "task_id": self.task_id,
                    "mission_id": self.mission_id,
                    "cancellation_reason": reason.value,
                },
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
                action=SignalType.ACK_CANCEL,
                status=self.status,
            ).model_dump(),
        )

    async def _handle_pause(self) -> None:
        """Pause task execution."""
        if not self._paused.is_set():
            logger.info(
                "Pausing task: '%s'",
                self.task_id,
                extra={"task_id": self.task_id},
            )
            self._paused.set()

        await self.db.update(
            "tasks",
            self.signal_record_id,  # type: ignore
            SignalMessage(
                task_id=self.task_id,
                mission_id=self.mission_id,
                action=SignalType.ACK_PAUSE,
                status=self.status,
            ).model_dump(),
        )

    async def _handle_resume(self) -> None:
        """Resume paused task."""
        if self._paused.is_set():
            logger.info(
                "Resuming task: '%s'",
                self.task_id,
                extra={"task_id": self.task_id},
            )
            self._paused.clear()

        await self.db.update(
            "tasks",
            self.signal_record_id,  # type: ignore
            SignalMessage(
                task_id=self.task_id,
                mission_id=self.mission_id,
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
                mission_id=self.mission_id,
                task_id=self.task_id,
                status=self.status,
                action=SignalType.ACK_STATUS,
            ).model_dump(),
        )

        logger.debug(
            "Status report sent for task: '%s'",
            self.task_id,
            extra={"task_id": self.task_id},
        )

    async def cleanup(self) -> None:
        """Clean up task session resources.

        This includes:
        - Clearing queue to free memory
        - Stopping module
        - Closing database connection
        - Clearing module reference
        """
        # Clear queue to free memory
        try:
            while not self.queue.empty():
                self.queue.get_nowait()
        except asyncio.QueueEmpty:
            pass

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
