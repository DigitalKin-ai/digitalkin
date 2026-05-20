"""Task session easing task lifecycle management.

Status transitions are persisted via ``await session.set_status(value)``.
When a ``RedisStateManager`` is configured the awaited call writes to
Redis before returning; with no manager the call updates only in-memory
state. The async API replaces the previous sync property setter that
spawned a fire-and-forget task per status change.
"""

from __future__ import annotations

import asyncio
import datetime
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
    """Task Session with lifecycle management.

    The Session defines the whole lifecycle of a task as an ephemeral context.
    Status transitions are optionally persisted to Redis via ``state_manager``.
    """

    signal_service: TaskManagerStrategy
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

    # Exception tracking for enhanced logging
    _last_exception: str | None
    _last_traceback: str | None

    # Cleanup guard for idempotent cleanup
    _cleanup_done: bool

    # Signal listener failure tracking
    _signal_listener_failed: bool

    # Optional Redis state persistence (P1: Redis-first writes)
    _state_manager: RedisStateManager | None

    # Tracked fire-and-forget tasks — prevents GC before completion
    _pending_redis_tasks: set[asyncio.Task[None]]

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
        self.signal_service = module.context.task_manager
        self.module = module
        self._state_manager = state_manager

        self._status = "pending"
        # Bounded queue to prevent unbounded memory growth (max 1000 items)
        self.queue: asyncio.Queue = asyncio.Queue(maxsize=queue_maxsize)

        self.task_id = task_id
        self.mission_id = mission_id

        self.created_at = datetime.datetime.now(datetime.timezone.utc)
        self.started_at = None
        self.completed_at = None

        self.is_cancelled = asyncio.Event()
        self.cancellation_reason = CancellationReason.UNKNOWN
        self.stream_closed_event = asyncio.Event()

        # Exception tracking
        self._last_exception = None
        self._last_traceback = None

        # Cleanup guard
        self._cleanup_done = False

        # Write lock — serialises final queue writes with session cleanup
        self._write_lock = asyncio.Lock()

        # Signal listener failure tracking
        self._signal_listener_failed = False

        logger.debug(
            "TaskSession initialized",
            extra={"task_id": task_id, "mission_id": mission_id},
        )

    @property
    def status(self) -> str:
        """Current task status. Use ``set_status()`` to update."""
        return self._status

    async def set_status(self, value: str) -> None:
        """Set status, persisting to Redis if state_manager is configured.

        Async to avoid the sync-setter-spawning-task pattern: callers
        ``await`` directly so a Redis hiccup is observable, no fire-and-
        forget tasks accumulate, and the in-memory state matches what's
        in Redis once the call returns.

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
        """Record exception details for logging.

        Args:
            exc: The exception that caused the task to fail.
        """
        self._last_exception = str(exc)
        self._last_traceback = traceback.format_exc()

    async def listen_signals(self) -> None:
        """Signal listener for cancel signals via TaskManagerStrategy.

        Subscribes to signal updates for this task_id and processes cancel signals.

        Raises:
            CancelledError: If task is cancelled during signal listening.
        """
        logger.info("Signal listener started", extra=self.session_ids)

        sub_id, live_signals = await self.signal_service.subscribe_signals(self.task_id)
        try:
            async for signal in live_signals:
                logger.info("Signal received: %s", signal, extra=self.session_ids)
                if self.cancelled or self.stream_closed:
                    break

                if signal is None or signal.get("task_id") != self.task_id:
                    continue

                if signal.get("action") == "cancel":
                    await self._handle_cancel(CancellationReason.SIGNAL_SERVICE_CANCEL)
                elif signal.get("action") == "stop":
                    await self._handle_stop()

        except asyncio.CancelledError:
            logger.info("Signal listener cancelled", extra=self.session_ids)
            raise
        except Exception:
            self._signal_listener_failed = True
            logger.exception("Signal listener fatal error", extra=self.session_ids)
        finally:
            try:
                await self.signal_service.unsubscribe_signals(sub_id)
            except Exception:
                logger.warning("Failed to unsubscribe signals", extra=self.session_ids, exc_info=True)
            logger.info("Signal listener stopped", extra=self.session_ids)

    async def _handle_cancel(self, reason: CancellationReason = CancellationReason.UNKNOWN) -> None:
        """Idempotent cancellation with acknowledgment and reason tracking.

        Args:
            reason: The reason for cancellation (signal, cleanup, etc.)
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
        await self.set_status("cancelled")
        self.is_cancelled.set()

        # Log with appropriate level based on reason
        if reason in {CancellationReason.SUCCESS_CLEANUP, CancellationReason.FAILURE_CLEANUP}:
            logger.debug("Task cancelled (%s)", reason.value, extra=self.session_ids)
        else:
            logger.info("Task cancelled (%s)", reason.value, extra=self.session_ids)

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
        except Exception:
            logger.warning("Cancel ack failed (best-effort)", extra=self.session_ids)

    async def _handle_stop(self) -> None:
        """Idempotent graceful-stop with acknowledgment.

        Mirrors _handle_cancel: marks the task as cancelled with
        SIGNAL_SERVICE_STOP reason and sends ACK_STOP to the signal service.
        """
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
        logger.info("Task stop requested via signal", extra=self.session_ids)

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
        except Exception:
            logger.warning("Stop ack failed (best-effort)", extra=self.session_ids)

    async def cleanup(self) -> None:
        """Clean up task session resources.

        This method is idempotent - safe to call multiple times.
        Second and subsequent calls are no-ops.

        This includes:
        - Clearing queue to free memory
        - Cleaning up module context services
        - Stopping module
        - Clearing module reference
        """
        # Use basic IDs for logging since module may already be None from previous cleanup
        ids = {"task_id": self.task_id, "mission_id": self.mission_id}

        if self._cleanup_done:
            logger.debug("Cleanup already done", extra=ids)
            return
        self._cleanup_done = True

        # (Status writes are now awaited inline via set_status — no
        # fire-and-forget tasks to drain.)

        # Clear queue to free memory
        logger.debug("Cleanup: draining queue", extra={"task_id": self.task_id, "queue_size": self.queue.qsize()})
        try:
            while not self.queue.empty():
                self.queue.get_nowait()
                self.queue.task_done()
        except asyncio.QueueEmpty:
            pass

        # Clean up module context services (e.g., gRPC channel pool, task_manager)
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

        # Clear module reference to allow garbage collection
        self.module = None  # type: ignore[assignment]  # Allow GC; typed as BaseModule but set to None after cleanup
