"""."""

import asyncio
import datetime
from collections.abc import AsyncGenerator

from digitalkin.logger import logger
from digitalkin.models.module.task_monitor import HeartbeatMessage, SignalMessage, SignalType, TaskStatus
from digitalkin.modules._base_module import BaseModule
from digitalkin.modules.job_manager.surrealdb_repository import SurrealDBConnection


class TaskSession:
    """Task Session with lifecycle management.

    The Session defined the whole lifecycle of a task as an epheneral context.
    """

    db: SurrealDBConnection
    module: BaseModule

    status: TaskStatus
    signal_queue: AsyncGenerator | None

    task_id: str
    signal_record_id: str | None
    heartbeat_record_id: str | None

    started_at: datetime.datetime | None
    completed_at: datetime.datetime | None

    is_cancelled: asyncio.Event
    _paused: asyncio.Event
    _heartbeat_interval: datetime.timedelta
    _last_heartbeat: datetime.datetime

    def __init__(
        self,
        task_id: str,
        db: SurrealDBConnection,
        module: BaseModule,
        heartbeat_interval: datetime.timedelta = datetime.timedelta(seconds=2),
    ) -> None:
        """."""
        self.db = db
        self.module = module

        self.status = TaskStatus.PENDING
        self.queue: asyncio.Queue = asyncio.Queue()

        self.task_id = task_id
        self.heartbeat = None
        self.started_at = None
        self.completed_at = None

        self.signal_record_id = None
        self.heartbeat_record_id = None

        self.is_cancelled = asyncio.Event()
        self._paused = asyncio.Event()
        self._heartbeat_interval = heartbeat_interval

        logger.info(
            "TaskContext initialized for task: '%s'",
            task_id,
            extra={"task_id": task_id, "heartbeat_interval": heartbeat_interval},
        )

    @property
    def cancelled(self) -> bool:
        """."""
        return self.is_cancelled.is_set()

    @property
    def paused(self) -> bool:
        """."""
        return self._paused.is_set()

    async def send_heartbeat(self) -> bool:
        """Rate-limited heartbeat with connection resilience.

        Returns:
            bool: True if heartbeat was successful, False otherwise
        """
        heartbeat = HeartbeatMessage(
            task_id=self.task_id,
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
        logger.debug("Heartbeat started")
        while not self.cancelled:
            logger.debug(f"Heartbeat tick for task: '{self.task_id}' | {self.cancelled=}")
            success = await self.send_heartbeat()
            if not success:
                logger.error("Heartbeat failed, cancelling task: '%s'", self.task_id, extra={"task_id": self.task_id})
                await self._handle_cancel()
                break
            await asyncio.sleep(self._heartbeat_interval.total_seconds())

    async def wait_if_paused(self) -> None:
        """Block execution if task is paused."""
        if self._paused.is_set():
            logger.info("Task paused, waiting for resume: '%s'", self.task_id, extra={"task_id": self.task_id})
            await self._paused.wait()

    async def listen_signals(self) -> None:  # noqa: C901
        """Enhanced signal listener with comprehensive handling.

        Raises:
            CancelledError: Asyncio when task cancelling
        """
        logger.info("Signal listener started for task: '%s'", self.task_id, extra={"task_id": self.task_id})
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
                    await self._handle_cancel()
                elif signal["action"] == "pause":
                    await self._handle_pause()
                elif signal["action"] == "resume":
                    await self._handle_resume()
                elif signal["action"] == "status":
                    await self._handle_status_request()

        except asyncio.CancelledError:
            logger.debug("Signal listener cancelled for task: '%s'", self.task_id, extra={"task_id": self.task_id})
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
            logger.info("Signal listener stopped for task: '%s'", self.task_id, extra={"task_id": self.task_id})

    async def _handle_cancel(self) -> None:
        """Idempotent cancellation with acknowledgment."""
        logger.debug("Handle cancel called")
        if self.is_cancelled.is_set():
            logger.debug(
                "Cancel signal ignored - task already cancelled: '%s'", self.task_id, extra={"task_id": self.task_id}
            )
            return

        logger.info("Cancelling task: '%s'", self.task_id, extra={"task_id": self.task_id})

        self.status = TaskStatus.CANCELLED
        self.is_cancelled.set()

        # Resume if paused so cancellation can proceed
        if self._paused.is_set():
            self._paused.set()

        await self.db.update(
            "tasks",
            self.signal_record_id,  # type: ignore
            SignalMessage(
                task_id=self.task_id,
                action=SignalType.ACK_CANCEL,
                status=self.status,
            ).model_dump(),
        )

    async def _handle_pause(self) -> None:
        """Pause task execution."""
        if not self._paused.is_set():
            logger.info("Pausing task: '%s'", self.task_id, extra={"task_id": self.task_id})
            self._paused.set()

        await self.db.update(
            "tasks",
            self.signal_record_id,  # type: ignore
            SignalMessage(
                task_id=self.task_id,
                action=SignalType.ACK_PAUSE,
                status=self.status,
            ).model_dump(),
        )

    async def _handle_resume(self) -> None:
        """Resume paused task."""
        if self._paused.is_set():
            logger.info("Resuming task: '%s'", self.task_id, extra={"task_id": self.task_id})
            self._paused.clear()

        await self.db.update(
            "tasks",
            self.signal_record_id,  # type: ignore
            SignalMessage(
                task_id=self.task_id,
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
                action=SignalType.ACK_STATUS,
                task_id=self.task_id,
                status=self.status,
            ).model_dump(),
        )

        logger.debug(
            "Status report sent for task: '%s'",
            self.task_id,
            extra={"task_id": self.task_id},
        )
