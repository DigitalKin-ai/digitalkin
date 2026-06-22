"""Unit tests for StreamSession.

Phase 4.A — StreamSession is now a thin descriptor (task_id + stop event).
All stream data flows through Redis Streams. Queue tests removed.
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


    async def test_teardown_idempotent(self) -> None:
        s = StreamSession(task_id="t_idem")
        await s.teardown()
        await s.teardown()  # Must not raise
