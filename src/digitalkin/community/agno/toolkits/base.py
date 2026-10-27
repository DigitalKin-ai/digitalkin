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
import uuid
from typing import TYPE_CHECKING, Annotated, Any, get_args, get_origin

from ag_ui.core.events import ActivitySnapshotEvent
from ag_ui.core.events import CustomEvent as AgUiCustomEvent
from agno.tools import Toolkit
from pydantic import ValidationError

from digitalkin.logger import logger
from digitalkin.models.events import SourceCitation, ToolInfo
from digitalkin.models.module.ag_ui import AgUiActivitySnapshotOutput, AgUiCustomEventOutput, AgUiOutput

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
        display_name: str | None = None,
    ) -> None:
        """Initialize the toolkit.

        Args:
            name: Toolkit name registered with Agno.
            tools: Bound tool callables to expose to the agent.
            context: Module context; when present, :meth:`_notify` can emit AG-UI events.
            external_execution_required_tools: Tool names Agno must pause on (executed
                outside the agent loop) instead of running their entrypoint.
            display_name: Human label of the toolkit shown to users; defaults to the humanized ``name``.
        """
        self._ctx = context
        self.display_name = display_name or ToolInfo.humanize(name)
        super().__init__(
            name=name,
            tools=tools,
            external_execution_required_tools=external_execution_required_tools or [],
        )

    def display_labels(self, function_name: str, tool_args: dict[str, Any] | str | None) -> tuple[str, str | None]:  # ruff: ignore[unused-method-argument]
        """Resolve the human labels of one call to this toolkit.

        Args:
            function_name: Technical name of the called function.
            tool_args: Call arguments; used by toolkits whose label depends on them.

        Returns:
            ``(tool label, function label)``. The function label is the function's ``title``, else its
            humanized name when the toolkit has several functions, else ``None``.
        """
        function = self.functions.get(function_name) or self.async_functions.get(function_name)
        if function is None:
            return self.display_name, None
        if function.title is not None:
            return self.display_name, function.title
        several = len(self.functions.keys() | self.async_functions.keys()) > 1
        return self.display_name, ToolInfo.humanize(function_name) if several else None

    @staticmethod
    def action_label(tool_args: dict[str, Any] | str | None, actions: Any) -> str | None:
        """Label of the action a manager call selects through its ``action`` discriminator.

        The action arrives as a dict, a JSON string of that dict, or a bare discriminator when the
        model flattened the call. Only a clean identifier is ever labelled, never a raw payload.

        Args:
            tool_args: Call arguments of a manager tool.
            actions: The manager's action class, or the ``Annotated`` discriminated union of them.

        Returns:
            The matching action's ``display_name``, the humanized discriminator for an unknown or
            unlabelled action, or ``None`` when no discriminator resolves.
        """
        action = tool_args.get("action") if isinstance(tool_args, dict) else None
        if isinstance(action, str) and action.lstrip().startswith("{"):
            try:
                action = json.loads(action)
            except ValueError:
                action = None
        if isinstance(action, dict):
            action = action.get("action")
        if not isinstance(action, str) or not action.isidentifier():
            return None
        members = get_args(get_args(actions)[0]) if get_origin(actions) is Annotated else (actions,)
        label = next((cls.display_name for cls in members if cls.model_fields["action"].default == action), None)
        return label or ToolInfo.humanize(action)

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

    async def _cite(self, url: str, title: str | None = None, description: str | None = None) -> None:
        """Add one source the agent used to the run's ``sources`` activity (best-effort).

        Inside an agent's AG-UI run the source is merged into that run's list. A tool module
        without a run of its own emits a one-source ``sources`` snapshot instead, which the
        calling agent's relay merges into its list.

        Args:
            url: Link to the source.
            title: Short label of the source.
            description: What the source supports in the answer.
        """
        if self._ctx is None:
            return
        try:
            citation = SourceCitation.model_validate({"url": url, "title": title, "description": description})
        except ValidationError as exc:
            logger.warning(
                "Dropped source citation url=%r title=%.300r description=%.300r: %s", url, title, description, exc
            )
            return
        try:  # ruff: ignore[too-many-statements-in-try-clause]
            if self._ctx.agui_run is not None:
                await self._ctx.agui_run.cite(self._ctx, citation, None)
                return
            send_message = vars(self._ctx.callbacks).get("send_message")
            if send_message is None:
                logger.warning("Dropped source citation %s: no send_message on the context", citation.url)
                return
            await send_message(
                AgUiOutput(
                    root=AgUiActivitySnapshotOutput(
                        event=ActivitySnapshotEvent(
                            message_id=f"sources-{uuid.uuid4()}",
                            activity_type="sources",
                            content={"sources": [citation.model_dump(mode="json", exclude_none=True)]},
                            replace=True,
                        )
                    )
                )
            )
        except Exception:
            logger.exception("Failed to emit source citation %s", citation.url)
