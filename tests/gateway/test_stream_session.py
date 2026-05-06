"""Unit tests for StreamSession.

Phase 4.A — StreamSession is now a thin descriptor (task_id + stop event
+ optional forward task). All stream data flows through Redis Streams.
Queue tests removed.
"""

from __future__ import annotations

import asyncio

import pytest

from digitalkin.grpc_servers.stream_session import StreamSession

pytestmark = [pytest.mark.timeout(10)]


class TestStreamSessionInit:
    """Initialization."""

    def test_task_id_required(self) -> None:
        s = StreamSession(task_id="t1")
        assert s.task_id == "t1"
        assert s._forward_task is None  # noqa: SLF001
        assert not s._stop_event.is_set()  # noqa: SLF001


class TestStreamSessionStop:
    """Stop and teardown."""

    def test_stop_sets_event(self) -> None:
        s = StreamSession(task_id="t_stop")
        s.stop()
        assert s._stop_event.is_set()  # noqa: SLF001

    async def test_teardown_sets_stop_event(self) -> None:
        s = StreamSession(task_id="t_td")
        await s.teardown()
        assert s._stop_event.is_set()  # noqa: SLF001

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

        s._forward_task = asyncio.create_task(long_running())  # noqa: SLF001
        await asyncio.sleep(0.01)  # Let task start
        await s.teardown()
        assert cancelled

    async def test_teardown_idempotent(self) -> None:
        s = StreamSession(task_id="t_idem")
        await s.teardown()
        await s.teardown()  # Must not raise


class TestStreamSessionForwardTask:
    """Forward task lifecycle."""

    async def test_no_forward_task_by_default(self) -> None:
        s = StreamSession(task_id="t_nf")
        assert s._forward_task is None  # noqa: SLF001

    async def test_set_forward_task(self) -> None:
        s = StreamSession(task_id="t_sf")

        async def noop() -> None:
            pass

        s._forward_task = asyncio.create_task(noop())  # noqa: SLF001
        await s._forward_task  # noqa: SLF001
        assert s._forward_task.done()  # noqa: SLF001
