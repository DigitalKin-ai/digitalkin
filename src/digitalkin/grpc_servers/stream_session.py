"""Per-task session descriptor for Gateway inter-module brokering.

The session is a thin descriptor:
all stream data (consumer→module input, module→consumer output)
flows through Redis Streams. The session only carries identity, a
stop event for graceful cancellation, and the dial-back orchestrator
task handle so teardown can cancel it cleanly.
"""

from __future__ import annotations

import asyncio
import contextlib

from digitalkin.logger import logger


class StreamSession:
    """Per-task session descriptor in the Gateway.

    No queues. Input and output both flow through Redis Streams
    (``task:{task_id}:input`` and ``task:{task_id}:stream``).

    Attributes:
        task_id: Client-provided reference ID (universal key).
    """

    task_id: str
    _stop_event: asyncio.Event
    _forward_task: asyncio.Task[None] | None

    def __init__(self, task_id: str) -> None:
        """Initialize a session descriptor.

        Args:
            task_id: Client-provided task reference ID.
        """
        self.task_id = task_id
        self._stop_event = asyncio.Event()
        self._forward_task = None

    def stop(self) -> None:
        """Signal graceful stop to readers."""
        self._stop_event.set()

    async def teardown(self) -> None:
        """Cancel the dial-back orchestrator task if still running."""
        self._stop_event.set()
        if self._forward_task is not None and not self._forward_task.done():
            self._forward_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._forward_task
            self._forward_task = None
        logger.debug("StreamSession teardown: task_id=%s", self.task_id)
