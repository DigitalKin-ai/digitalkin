"""Bidirectional stream session for Gateway inter-module brokering.

Each ``StreamSession`` represents one active task in the Gateway.
It holds two bounded queues:

- ``output_queue``: producer output → Gateway → consumer (downstream)
- ``input_queue``: consumer data → Gateway → producer (upstream)

The Gateway reads from one queue and writes to the other, brokering
all inter-module communication. Modules never talk to each other directly.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from digitalkin.grpc_servers.gateway_constants import (
    DEFAULT_INPUT_QUEUE_SIZE,
    DEFAULT_OUTPUT_QUEUE_SIZE,
    ENQUEUE_TIMEOUT_S,
)
from digitalkin.logger import logger


class StreamSession:
    """Tracks one bidirectional session in the Gateway.

    Attributes:
        task_id: Client-provided reference ID (universal key).
        output_queue: Producer → Gateway → Consumer (downstream).
        input_queue: Consumer → Gateway → Producer (upstream).
    """

    task_id: str
    _stop_event: asyncio.Event
    output_queue: asyncio.Queue[dict[str, Any] | None]
    input_queue: asyncio.Queue[dict[str, Any] | None]
    _forward_task: asyncio.Task[None] | None

    def __init__(
        self,
        task_id: str,
        output_queue_size: int = DEFAULT_OUTPUT_QUEUE_SIZE,
        input_queue_size: int = DEFAULT_INPUT_QUEUE_SIZE,
    ) -> None:
        """Initialize a bidirectional stream session.

        Args:
            task_id: Client-provided task reference ID.
            output_queue_size: Max items from producer to consumer.
            input_queue_size: Max items from consumer to producer.
        """
        self.task_id = task_id
        self._stop_event = asyncio.Event()
        self.output_queue = asyncio.Queue(maxsize=output_queue_size)
        self.input_queue = asyncio.Queue(maxsize=input_queue_size)
        self._forward_task = None

    async def enqueue_output(self, data: dict[str, Any], timeout: float = ENQUEUE_TIMEOUT_S) -> bool:
        """Put producer output on the downstream queue.

        Args:
            data: Output data to enqueue.
            timeout: Max seconds to wait for queue space.

        Returns:
            True if enqueued, False if dropped due to timeout.
        """
        try:
            self.output_queue.put_nowait(data)
        except asyncio.QueueFull:
            logger.warning("Output queue full for task %s, waiting %.1fs", self.task_id, timeout)
            try:
                await asyncio.wait_for(self.output_queue.put(data), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("Output queue still full after %.1fs, dropping item for task %s", timeout, self.task_id)
                return False
        return True

    async def enqueue_input(self, data: dict[str, Any], timeout: float = ENQUEUE_TIMEOUT_S) -> bool:
        """Put consumer data on the upstream queue.

        Args:
            data: Input data from the consumer.
            timeout: Max seconds to wait for queue space.

        Returns:
            True if enqueued, False if dropped due to timeout.
        """
        try:
            self.input_queue.put_nowait(data)
        except asyncio.QueueFull:
            logger.warning("Input queue full for task %s, waiting %.1fs", self.task_id, timeout)
            try:
                await asyncio.wait_for(self.input_queue.put(data), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning("Input queue still full after %.1fs, dropping item for task %s", timeout, self.task_id)
                return False
        return True

    def stop(self) -> None:
        """Signal graceful stop — terminates both directions."""
        self._stop_event.set()

    async def teardown(self) -> None:
        """Clean up: cancel forward task, drain both queues."""
        self._stop_event.set()

        # Cancel the background forwarding task if still running
        if self._forward_task is not None and not self._forward_task.done():
            self._forward_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._forward_task
            self._forward_task = None

        # Drain both queues to free memory
        for q in (self.output_queue, self.input_queue):
            while not q.empty():
                with contextlib.suppress(asyncio.QueueEmpty):
                    q.get_nowait()

        logger.debug("StreamSession teardown: task_id=%s", self.task_id)
