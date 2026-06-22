"""Task session lifecycle: status, cancellation, cleanup."""

from __future__ import annotations

import asyncio
import datetime
import time
import traceback
from typing import TYPE_CHECKING

from digitalkin.logger import logger
from digitalkin.models.core.task_monitor import (
    CancellationReason,
    SignalMessage,
    SignalType,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from digitalkin.core.task_manager.redis.redis_state import RedisStateManager
    from digitalkin.modules._base_module import BaseModule
    from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy


class TaskSession:
    """Ephemeral lifecycle context for one task, optionally persisted to Redis."""

    signal_service: TaskManagerStrategy | None
    module: BaseModule

    _status: str
    signal_queue: AsyncGenerator | None

    task_id: str
    mission_id: str

    created_at: datetime.datetime
    started_at: datetime.datetime | None
    completed_at: datetime.datetime | None

    is_cancelled: asyncio.Event
    cancellation_reason: CancellationReason
    stream_closed_event: asyncio.Event

    _last_exception: str | None
    _last_traceback: str | None
    _cleanup_done: bool
    _state_manager: RedisStateManager | None
    _pending_redis_tasks: set[asyncio.Task[None]]

    pending_signal_action: str = ""
    last_signal_published_ns: int = 0

    def __init__(
        self,
        task_id: str,
        mission_id: str,
        module: BaseModule,
        queue_maxsize: int = 1000,
        state_manager: RedisStateManager | None = None,
    ) -> None:
        """Initialize Task Session.

        Args:
            task_id: Unique task identifier
            mission_id: Mission identifier
            module: Module instance
            queue_maxsize: Maximum size for the queue (0 = unlimited)
            state_manager: Optional Redis state manager for persistent status tracking
        """
        # signal_service is None for config-setup TaskSessions (no signals to dispatch); see
        # SingleJobManager.create_config_setup_instance_job. Real-task sessions get it wired
        # by preload_instance setting context.task_manager before _create_session runs.
        self.signal_service = module.context.task_manager
        self.module = module
        self._state_manager = state_manager

        self._status = "pending"
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)

        self.task_id = task_id
        self.mission_id = mission_id

        self.created_at = datetime.datetime.now(datetime.timezone.utc)
        self.started_at = None
        self.completed_at = None

        self.is_cancelled = asyncio.Event()
        self.cancellation_reason = CancellationReason.UNKNOWN
        self.stream_closed_event = asyncio.Event()

        self._last_exception = None
        self._last_traceback = None
        self._cleanup_done = False
        self._write_lock = asyncio.Lock()
        self.pending_signal_action = ""
        self.last_signal_published_ns = 0

        logger.debug(
            "TaskSession initialized",
            extra={"task_id": task_id, "mission_id": mission_id},
        )

    @property
    def status(self) -> str:
        """Current task status. Use ``set_status()`` to update."""
        return self._status

    async def set_status(self, value: str) -> None:
        """Set status; persist to Redis if a state_manager is configured.

        Args:
            value: New status (e.g., "running", "completed", "cancelled").
        """
        self._status = value
        if self._state_manager is None:
            return
        try:
            await self._state_manager.set_status(self.task_id, value)
        except Exception:
            logger.warning(
                "Redis status write failed: task_id=%s status=%s",
                self.task_id,
                value,
                exc_info=True,
            )

    @property
    def cancelled(self) -> bool:
        """Task cancellation status."""
        return self.is_cancelled.is_set()

    @property
    def stream_closed(self) -> bool:
        """Check if stream termination was signaled."""
        return self.stream_closed_event.is_set()

    def close_stream(self) -> None:
        """Signal that the stream should terminate."""
        self.stream_closed_event.set()

    @property
    def setup_id(self) -> str:
        """The setup_id from the module context."""
        return self.module.context.session.setup_id

    @property
    def setup_version_id(self) -> str:
        """The setup_version_id from the module context."""
        return self.module.context.session.setup_version_id

    @property
    def session_ids(self) -> dict[str, str]:
        """All session IDs from the module context for structured logging."""
        return self.module.context.session.current_ids()

    def record_exception(self, exc: Exception) -> None:
        """Record exception details for logging.

        Args:
            exc: The exception that caused the task to fail.
        """
        self._last_exception = str(exc)
        self._last_traceback = traceback.format_exc()

    async def _handle_cancel(self, reason: CancellationReason = CancellationReason.UNKNOWN) -> None:
        """Idempotent cancellation with acknowledgment and reason tracking.

        Args:
            reason: The reason for cancellation (signal, cleanup, etc.)
        """
        t0 = time.perf_counter_ns()
        if self.cancelled:
            logger.debug(
                "Cancel ignored - already cancelled (existing=%s, new=%s)",
                self.cancellation_reason.value,
                reason.value,
                extra=self.session_ids,
            )
            return

        self.cancellation_reason = reason
        await self.set_status("cancelled")
        self.is_cancelled.set()
        body_ns = time.perf_counter_ns() - t0

        ack_t0 = time.perf_counter_ns()
        ack_ok = False
        if self.signal_service is not None:
            try:
                await self.signal_service.send_signal(
                    self.task_id,
                    SignalMessage(
                        task_id=self.task_id,
                        mission_id=self.mission_id,
                        setup_id=self.setup_id,
                        setup_version_id=self.setup_version_id,
                        action=SignalType.ACK_CANCEL,
                        cancellation_reason=reason,
                    ).model_dump(exclude_none=True),
                )
                ack_ok = True
            except Exception:
                logger.warning("Cancel ack failed (best-effort)", extra=self.session_ids)
        ack_ns = time.perf_counter_ns() - ack_t0

        pub_ns = self.last_signal_published_ns
        e2e_ms = (time.time_ns() - pub_ns) / 1e6 if pub_ns else 0.0
        self.last_signal_published_ns = 0
        logger.debug(
            "[perf] signal_handle: handler=cancel reason=%s e2e_ms=%.2f "
            "body_ms=%.2f ack_send_ms=%.2f ack_ok=%s task_id=%s",
            reason.value,
            e2e_ms,
            body_ns / 1e6,
            ack_ns / 1e6,
            ack_ok,
            self.task_id,
            extra=self.session_ids,
        )

    async def _handle_stop(self) -> None:
        """Idempotent graceful-stop with acknowledgment.

        Mirrors _handle_cancel: marks the task as cancelled with
        SIGNAL_SERVICE_STOP reason and sends ACK_STOP to the signal service.
        """
        t0 = time.perf_counter_ns()
        if self.cancelled:
            logger.debug(
                "Stop ignored - already cancelled (existing=%s)",
                self.cancellation_reason.value,
                extra=self.session_ids,
            )
            return

        self.cancellation_reason = CancellationReason.SIGNAL_SERVICE_STOP
        await self.set_status("cancelled")
        self.is_cancelled.set()
        body_ns = time.perf_counter_ns() - t0

        ack_t0 = time.perf_counter_ns()
        ack_ok = False
        if self.signal_service is not None:
            try:
                await self.signal_service.send_signal(
                    self.task_id,
                    SignalMessage(
                        task_id=self.task_id,
                        mission_id=self.mission_id,
                        setup_id=self.setup_id,
                        setup_version_id=self.setup_version_id,
                        action=SignalType.ACK_STOP,
                        cancellation_reason=CancellationReason.SIGNAL_SERVICE_STOP,
                    ).model_dump(exclude_none=True),
                )
                ack_ok = True
            except Exception:
                logger.warning("Stop ack failed (best-effort)", extra=self.session_ids)
        ack_ns = time.perf_counter_ns() - ack_t0

        pub_ns = self.last_signal_published_ns
        e2e_ms = (time.time_ns() - pub_ns) / 1e6 if pub_ns else 0.0
        self.last_signal_published_ns = 0
        logger.debug(
            "[perf] signal_handle: handler=stop reason=%s e2e_ms=%.2f "
            "body_ms=%.2f ack_send_ms=%.2f ack_ok=%s task_id=%s",
            CancellationReason.SIGNAL_SERVICE_STOP.value,
            e2e_ms,
            body_ns / 1e6,
            ack_ns / 1e6,
            ack_ok,
            self.task_id,
            extra=self.session_ids,
        )

    async def cleanup(self) -> None:
        """Drain queue, release services, stop the module. Idempotent."""
        ids = {"task_id": self.task_id, "mission_id": self.mission_id}

        if self._cleanup_done:
            logger.debug("Cleanup already done", extra=ids)
            return
        self._cleanup_done = True

        logger.debug("Cleanup: draining queue (queue_size=%d)", self.queue.qsize(), extra=ids)
        try:
            while not self.queue.empty():
                self.queue.get_nowait()
                self.queue.task_done()
        except asyncio.QueueEmpty:
            pass

        if self.module is not None and self.module.context is not None:
            try:
                await self.module.context.cleanup()
            except Exception:
                logger.exception("Error cleaning up module context", extra=ids)

        try:
            await self.module.stop()
        except Exception:
            logger.exception("Error stopping module during cleanup", extra=ids)

        self.module = None  # type: ignore[assignment]
