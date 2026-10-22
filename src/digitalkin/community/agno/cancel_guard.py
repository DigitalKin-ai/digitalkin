"""Agno tool hook that refuses to run a tool once the owning task has been cancelled.

A cancelled task can keep calling tools after the module logged "cancelled": each call then fails
at the gateway with "No active parent task". The guard sits under Agno's tool chain so the call is
refused before any m2m round trip, and it logs which tool the leaked run was about to call.
"""

from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Any

from agno.exceptions import RunCancelledException

from digitalkin.logger import logger

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable


class CancelGuard:
    """Agno tool hook bound to a task-wide cancellation event.

    Attach ``guard.tool_hook`` (the bound method) to an Agent's or Team's ``tool_hooks``.
    """

    def __init__(self, event: asyncio.Event, owner: str) -> None:
        """Bind the guard to a cancellation event.

        Args:
            event: Set by the caller when the task is cancelled.
            owner: Agent name used in the log line, e.g. ``leader`` or a member name.
        """
        self._event = event
        self._owner = owner

    async def tool_hook(self, function_name: str, function_call: Callable[..., Any], arguments: dict[str, Any]) -> Any:
        """Run the next link in the hook chain unless the task was cancelled.

        Args:
            function_name: Name of the tool being called.
            function_call: The next link in Agno's hook chain (sync or async).
            arguments: The arguments the model supplied.

        Returns:
            Whatever the tool returned.

        Raises:
            RunCancelledException: If the task was cancelled before this call. Agno's own
                cancellation signal, handled cleanly by its run loop, unlike
                ``asyncio.CancelledError``.
        """
        if self._event.is_set():
            logger.warning("Tool call refused after task cancellation: owner=%s tool=%s", self._owner, function_name)
            msg = f"task cancelled before tool '{function_name}'"
            raise RunCancelledException(msg)
        result = function_call(**arguments)
        if inspect.isawaitable(result):
            return await result
        return result
