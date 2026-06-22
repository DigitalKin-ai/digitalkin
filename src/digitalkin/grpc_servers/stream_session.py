"""Per-task session descriptor for Gateway inter-module brokering.

The session is a thin descriptor:
all stream data (consumer→module input, module→consumer output)
flows through Redis Streams. The session only carries identity and a
stop event for graceful cancellation.
"""

from __future__ import annotations

import asyncio

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

    def __init__(self, task_id: str) -> None:
        """Initialize a session descriptor.

        Args:
            task_id: Client-provided task reference ID.
        """
        self.task_id = task_id
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        """Signal graceful stop to readers."""
        self._stop_event.set()

    async def teardown(self) -> None:
        """Signal stop to readers (the dial-back task is reaped by the registry)."""
        self._stop_event.set()
        logger.debug("StreamSession teardown: task_id=%s", self.task_id)
