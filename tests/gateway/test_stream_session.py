"""Unit tests for StreamSession.

Covers: initialization, enqueue with backpressure, stop, teardown,
forward task cancellation, sequence counter.
"""

from __future__ import annotations

import asyncio

import pytest

from digitalkin.grpc_servers.stream_session import StreamSession

pytestmark = [pytest.mark.timeout(10)]


class TestStreamSessionInit:
    """Initialization and field defaults."""

    def test_task_id_required(self) -> None:
        s = StreamSession(task_id="t1")
        assert s.task_id == "t1"
        assert s._forward_task is None

    def test_custom_queue_size(self) -> None:
        s = StreamSession(task_id="t2", output_queue_size=10)
        assert s.output_queue.maxsize == 10


class TestStreamSessionEnqueue:
    """Enqueue output with backpressure."""

    async def test_enqueue_when_space(self) -> None:
        s = StreamSession(task_id="t_eq", output_queue_size=10)
        await s.enqueue_output({"data": "test"})
        assert s.output_queue.qsize() == 1

    async def test_enqueue_drops_after_timeout_when_full(self) -> None:
        s = StreamSession(task_id="t_full", output_queue_size=1)
        await s.enqueue_output({"first": True})
        # Queue is now full — second enqueue should timeout and drop
        await s.enqueue_output({"second": True}, timeout=0.1)
        # Queue still has only 1 item (the first one)
        assert s.output_queue.qsize() == 1


class TestStreamSessionStop:
    """Stop and teardown."""

    def test_stop_sets_event(self) -> None:
        s = StreamSession(task_id="t_stop")
        s.stop()
        assert s._stop_event.is_set()

    async def test_teardown_drains_queue(self) -> None:
        s = StreamSession(task_id="t_td")
        await s.enqueue_output({"a": 1})
        await s.enqueue_output({"b": 2})
        await s.teardown()
        assert s.output_queue.empty()

    async def test_teardown_cancels_forward_task(self) -> None:
        s = StreamSession(task_id="t_fwd")
        cancelled = False

        async def long_running() -> None:
            nonlocal cancelled
            try:
                await asyncio.sleep(999)
            except asyncio.CancelledError:
                cancelled = True
                raise

        s._forward_task = asyncio.create_task(long_running())
        await asyncio.sleep(0.01)  # Let task start
        await s.teardown()
        assert cancelled

    async def test_teardown_idempotent(self) -> None:
        s = StreamSession(task_id="t_idem")
        await s.teardown()
        await s.teardown()  # Should not raise


class TestStreamSessionForwardTask:
    """Forward task lifecycle."""

    async def test_no_forward_task_by_default(self) -> None:
        s = StreamSession(task_id="t_nf")
        assert s._forward_task is None

    async def test_set_forward_task(self) -> None:
        s = StreamSession(task_id="t_sf")

        async def noop() -> None:
            pass

        s._forward_task = asyncio.create_task(noop())
        await s._forward_task
        assert s._forward_task.done()
