"""ContextVar lifecycle management for trace propagation.

``TaskWrapper`` is the single point of ContextVar management. Every
coroutine that runs in the context of a session goes through
``TaskWrapper.run()``. Nothing else sets or resets ``TRACE_CTX``.

This prevents context leak when coroutines are reused (e.g., in pools)
and ensures every log line, gRPC call, and span carries the correct
trace_id and session_id.
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Coroutine


@dataclass(frozen=True, slots=True)
class TraceContext:
    """Immutable trace context carrying IDs for the current session.

    Attributes:
        trace_id: Root trace identifier for the session.
        session_id: Session identifier.
        job_id: Job identifier.
        mission_id: Mission identifier.
    """

    trace_id: str
    session_id: str
    job_id: str = ""
    mission_id: str = ""


TRACE_CTX: contextvars.ContextVar[TraceContext | None] = contextvars.ContextVar("TRACE_CTX", default=None)


class TaskWrapper:
    """Single entry point for ContextVar lifecycle.

    Every coroutine that needs trace context runs through ``TaskWrapper.run()``.
    The wrapper:
    1. Copies the current context (isolation from ambient state).
    2. Sets ``TRACE_CTX`` with the session's trace context.
    3. Runs the coroutine inside the copied context.
    4. Resets ``TRACE_CTX`` to pre-call state via token (not to default).

    This prevents the leak described in §5.1: a pooled coroutine carrying
    stale trace context from a previous request.
    """

    @staticmethod
    async def run(coro: Coroutine[Any, Any, Any], trace_ctx: TraceContext) -> Any:
        """Run a coroutine with isolated trace context.

        Args:
            coro: The coroutine to execute.
            trace_ctx: Trace context for this session.

        Returns:
            The coroutine's return value.
        """
        token = TRACE_CTX.set(trace_ctx)
        try:
            return await coro
        finally:
            TRACE_CTX.reset(token)

    @staticmethod
    def current() -> TraceContext | None:
        """Get the current trace context.

        Returns:
            The active TraceContext, or None if not in a traced coroutine.
        """
        return TRACE_CTX.get()

    @staticmethod
    def current_ids() -> dict[str, str]:
        """Get the current trace IDs as a dict for log ``extra``.

        Returns:
            Dict with trace_id, session_id, job_id, mission_id — or empty if no context.
        """
        ctx = TRACE_CTX.get()
        if ctx is None:
            return {}
        return {
            "trace_id": ctx.trace_id,
            "session_id": ctx.session_id,
            "job_id": ctx.job_id,
            "mission_id": ctx.mission_id,
        }
