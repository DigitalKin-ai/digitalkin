"""Tests for TaskWrapper ContextVar lifecycle management.

Validates trace context isolation, reset on completion, and
prevention of context leak between reused coroutines.
"""

from __future__ import annotations

import asyncio

import pytest

from digitalkin.core.task_manager.task_wrapper import TRACE_CTX, TaskWrapper, TraceContext

pytestmark = pytest.mark.timeout(10)


class TestTraceContext:
    """TraceContext data class."""

    def test_frozen_and_slots(self) -> None:
        ctx = TraceContext(trace_id="t1", session_id="s1")
        assert ctx.trace_id == "t1"
        assert ctx.session_id == "s1"
        assert ctx.job_id == ""  # noqa: PLC1901
        assert ctx.mission_id == ""  # noqa: PLC1901

    def test_immutable(self) -> None:
        ctx = TraceContext(trace_id="t1", session_id="s1")
        with pytest.raises(AttributeError):
            ctx.trace_id = "t2"  # type: ignore[misc]


class TestTaskWrapper:
    """TaskWrapper.run() context isolation."""

    async def test_sets_trace_context_during_coroutine(self) -> None:
        ctx = TraceContext(trace_id="trace_1", session_id="sess_1", job_id="j1")
        captured: list[TraceContext | None] = []

        async def work() -> None:
            captured.append(TRACE_CTX.get())

        await TaskWrapper.run(work(), ctx)
        assert captured[0] is ctx

    async def test_resets_context_after_coroutine(self) -> None:
        ctx = TraceContext(trace_id="trace_2", session_id="sess_2")

        async def work() -> None:
            pass

        assert TRACE_CTX.get() is None
        await TaskWrapper.run(work(), ctx)
        assert TRACE_CTX.get() is None

    async def test_resets_context_on_exception(self) -> None:
        ctx = TraceContext(trace_id="trace_3", session_id="sess_3")

        async def failing() -> None:
            msg = "boom"
            raise ValueError(msg)

        with pytest.raises(ValueError, match="boom"):
            await TaskWrapper.run(failing(), ctx)
        assert TRACE_CTX.get() is None

    async def test_isolation_between_concurrent_tasks(self) -> None:
        """Two concurrent TaskWrapper.run() calls don't leak context."""
        results: dict[str, str] = {}
        barrier = asyncio.Event()

        async def task_a() -> None:
            ctx = TRACE_CTX.get()
            assert ctx is not None
            results["a_before"] = ctx.trace_id
            barrier.set()
            await asyncio.sleep(0.01)
            ctx_after = TRACE_CTX.get()
            assert ctx_after is not None
            results["a_after"] = ctx_after.trace_id

        async def task_b() -> None:
            await barrier.wait()
            ctx = TRACE_CTX.get()
            assert ctx is not None
            results["b"] = ctx.trace_id

        ctx_a = TraceContext(trace_id="A", session_id="sa")
        ctx_b = TraceContext(trace_id="B", session_id="sb")

        await asyncio.gather(
            TaskWrapper.run(task_a(), ctx_a),
            TaskWrapper.run(task_b(), ctx_b),
        )

        assert results["a_before"] == "A"
        assert results["a_after"] == "A"
        assert results["b"] == "B"


class TestTaskWrapperHelpers:
    """current() and current_ids() helpers."""

    async def test_current_returns_none_outside_wrapper(self) -> None:
        assert TaskWrapper.current() is None

    async def test_current_returns_context_inside_wrapper(self) -> None:
        ctx = TraceContext(trace_id="t", session_id="s", job_id="j", mission_id="m")

        async def check() -> TraceContext | None:
            return TaskWrapper.current()

        result = await TaskWrapper.run(check(), ctx)
        assert result is ctx

    async def test_current_ids_empty_outside(self) -> None:
        assert TaskWrapper.current_ids() == {}

    async def test_current_ids_returns_dict_inside(self) -> None:
        ctx = TraceContext(trace_id="t1", session_id="s1", job_id="j1", mission_id="m1")

        async def check() -> dict:
            return TaskWrapper.current_ids()

        result = await TaskWrapper.run(check(), ctx)
        assert result == {"trace_id": "t1", "session_id": "s1", "job_id": "j1", "mission_id": "m1"}
