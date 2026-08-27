"""Shared base for DigitalKin agno toolkits.

Codifies the format/return conventions established by
:class:`~digitalkin.community.agno.module_toolkit.ModuleToolkit`: a canonical
``{"output"|"error", "metadata"}`` JSON envelope (:meth:`_ok`/:meth:`_fail`) and
best-effort AG-UI custom-event notifications on the agent's own stream
(:meth:`_notify`). Every toolkit returns consistently and never raises into the
agent loop.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from ag_ui.core.events import CustomEvent as AgUiCustomEvent
from agno.tools import Toolkit

from digitalkin.logger import logger
from digitalkin.models.module.ag_ui import AgUiCustomEventOutput, AgUiOutput

if TYPE_CHECKING:
    from digitalkin.models.module import ModuleContext


class DkToolkit(Toolkit):
    """Base class for DigitalKin agno toolkits.

    Subclasses register bound async tool methods and return via :meth:`_ok`/
    :meth:`_fail` so the agent always receives the same envelope and the tool
    never raises. Passing a :class:`ModuleContext` enables :meth:`_notify`,
    which pushes AG-UI custom events onto the caller's gRPC stream.
    """

    def __init__(
        self,
        name: str,
        tools: list[Any],
        context: ModuleContext | None = None,
        external_execution_required_tools: list[str] | None = None,
    ) -> None:
        """Initialize the toolkit.

        Args:
            name: Toolkit name registered with Agno.
            tools: Bound tool callables to expose to the agent.
            context: Module context; when present, :meth:`_notify` can emit AG-UI events.
            external_execution_required_tools: Tool names Agno must pause on (executed
                outside the agent loop) instead of running their entrypoint.
        """
        self._ctx = context
        super().__init__(
            name=name,
            tools=tools,
            external_execution_required_tools=external_execution_required_tools or [],
        )

    @staticmethod
    def _ok(output: Any, **metadata: Any) -> str:
        """Build the canonical success envelope.

        Args:
            output: The tool result payload (JSON-serializable).
            metadata: Extra metadata fields (e.g. ``tool``).

        Returns:
            JSON string ``{"output": ..., "metadata": {"success": true, ...}}``.
        """
        return json.dumps({"output": output, "metadata": {"success": True, **metadata}}, ensure_ascii=False)

    @staticmethod
    def _fail(error: str, **metadata: Any) -> str:
        """Build the canonical error envelope.

        Args:
            error: Human/LLM-readable error message.
            metadata: Extra metadata fields (e.g. ``tool``).

        Returns:
            JSON string ``{"error": ..., "metadata": {"success": false, ...}}``.
        """
        return json.dumps({"error": error, "metadata": {"success": False, **metadata}}, ensure_ascii=False)

    @staticmethod
    def _nest_action(action: Any, fields: dict[str, Any]) -> Any:
        """Normalise the three shapes a model sends a nested ``action`` object in.

        The tool schema declares a single ``action`` property holding a discriminated union, but
        models routinely flatten it — sending ``{"action": "search", "query": ""}`` instead of
        ``{"action": {"action": "search", "query": ""}}`` — or serialise the inner object as a
        JSON string. Both are the model reading a nested union schema the obvious way, not a
        malformed call, so they are accepted rather than refused.

        Args:
            action: The ``action`` argument as received: the object, a JSON string of it, or the
                bare discriminator when the model flattened the call.
            fields: Any sibling keyword arguments, i.e. the flattened action's own fields.

        Returns:
            The payload to validate: a JSON string is passed through for ``validate_json``,
            anything else comes back as a dict.
        """
        if isinstance(action, str):
            # A JSON string is the inner object already; anything else is the bare discriminator,
            # whose fields were flattened alongside it (empty when the action takes none).
            return action if action.lstrip().startswith("{") else {"action": action, **fields}
        if fields and isinstance(action, dict):
            return {**action, **fields}
        return fields if action is None and fields else action

    async def _notify(self, name: str, value: Any) -> None:
        """Emit an AG-UI custom event on the agent's output stream (best-effort).

        No-op when there is no context or ``send_message`` is not installed (e.g. outside a
        running job). Never raises — a notification failure must not fail the tool call.

        Args:
            name: Custom event name.
            value: Custom event payload (JSON-serializable).
        """
        if self._ctx is None:
            return
        # callbacks is a dict-driven SimpleNamespace; send_message is attached during prepare().
        send_message = vars(self._ctx.callbacks).get("send_message")
        if send_message is None:
            return
        try:
            await send_message(AgUiOutput(root=AgUiCustomEventOutput(event=AgUiCustomEvent(name=name, value=value))))
        except Exception:
            logger.exception("Failed to emit custom event '%s' to the agent stream", name)
