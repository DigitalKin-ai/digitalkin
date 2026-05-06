"""Tiny helper: log unhandled exceptions on fire-and-forget asyncio tasks."""

from __future__ import annotations

import asyncio
from typing import Any

from digitalkin.logger import logger


def log_unhandled(task: asyncio.Task[Any]) -> None:
    """Done-callback that logs uncaught exceptions on a fire-and-forget task.

    Cancellation and clean exits are silent. Anything else is logged at
    error level with the task name and traceback — this replaces asyncio's
    opaque ``Task exception was never retrieved`` warning with an
    actionable log line.

    Usage:

        task = asyncio.create_task(coro, name="my_daemon")
        task.add_done_callback(log_unhandled)

    Args:
        task: The done asyncio task to inspect.
    """
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.error(
            "Background task '%s' failed with %s: %s",
            task.get_name(),
            type(exc).__name__,
            exc,
            exc_info=exc,
        )
