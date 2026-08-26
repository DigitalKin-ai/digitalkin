"""``load_manager`` — external-execution tool that loads discovered objects into the agent.

Unlike the CRUD managers (``tools_manager``/``services_manager``/``kins_manager``, which run
in-process), ``load_manager`` is an **external-execution** tool: Agno pauses when the model
calls it, and the bound :class:`~digitalkin.community.agno.hitl.AgnoHitlRunner` runs the load
(:meth:`LoadManager.run_paused`) and auto-continues — so the loaded object is callable in the
same turn. For now the only action is ``tool``: the model discovers a tool via ``tools_manager``
(search / get), then loads it here to make it callable.

Every result — the loaded confirmation, a failure, or the "unavailable"/invalid-payload guards —
goes through the same ``{output|error, metadata: {success, tool}}`` envelope as the CRUD
managers, so a caller reads ``metadata.success`` instead of pattern-matching the message text.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, get_args

from pydantic import TypeAdapter, ValidationError

from digitalkin.community.agno.toolkits.base import DkToolkit
from digitalkin.community.agno.toolkits.registry.loader.action import LoadActionCtx, LoadActions
from digitalkin.logger import logger

if TYPE_CHECKING:
    from digitalkin.models.module import ModuleContext


class LoadManager(DkToolkit):
    """Expose ``load_manager`` — an external-execution tool that loads objects into the agent.

    The tool itself never executes: it is registered as external-execution so the run pauses
    when the model calls it. The bound :class:`AgnoHitlRunner` then invokes :meth:`run_paused`,
    which validates the paused action and runs its :meth:`LoadAction.execute`.
    """

    def __init__(self, context: ModuleContext | None = None) -> None:
        """Register the ``load_manager`` external-execution tool.

        Args:
            context: Module context; supplies ``resolve_tool`` and AG-UI notifications.
        """
        super().__init__(
            name="load_manager",
            tools=[self.load_manager],
            context=context,
            external_execution_required_tools=[self.load_manager.__name__],
        )
        # The live list the agent's tools factory splats; bound by DefaultToolkits.build.
        self._base_tools: list[Any] | None = None

    @property
    def tool_name(self) -> str:
        """The external tool name the runner pauses on and routes to :meth:`run_paused`."""
        return self.load_manager.__name__

    def bind_tools(self, base_tools: list[Any]) -> None:
        """Bind the live tool list that a load appends newly-loaded tools to.

        Args:
            base_tools: The exact list the agent's ``make_tools_factory`` closes over, so an
                appended toolkit is visible on the next run.
        """
        self._base_tools = base_tools

    async def load_manager(self, action: LoadActions) -> str:  # noqa: ARG002 — schema-only stub, never run
        """Load a discovered object into the agent so it becomes usable right now.

        Loading is a two-step flow, and this is step two: first DISCOVER the object with its
        manager (e.g. use ``tools_manager`` to ``search``/``get`` a tool and obtain its
        ``setup_id``), THEN call ``load_manager`` with that id to load it. For a tool, use the
        ``tool`` action — this is the ONLY way to make a discovered tool actually callable; the
        managers merely administer setups, they never run them. You do NOT need to ask the user,
        and you can call the loaded tool in your very next step.

        Args:
            action: The load action — currently ``tool`` with a ``setup_id`` taken from a
                ``tools_manager`` search or get result.

        Returns:
            The canonical envelope; on success ``output`` names the now-callable functions, on
            failure ``error`` carries a distinct message. Check ``metadata.success``.
        """
        # Never executed: registered as external-execution, so the run pauses here and
        # AgnoHitlRunner calls run_paused() instead. Kept for a correct LLM-facing schema.
        return self._ok({"status": "pending"}, tool="load_manager")

    async def run_paused(self, tool_args: dict[str, Any]) -> str:
        """Validate a paused ``load_manager`` call and run the load (runner entry point).

        Generic over the action union: it validates the payload into a concrete
        :class:`LoadAction`, runs its :meth:`~LoadAction.execute`, and wraps the resulting
        :class:`LoadOutcome` in the canonical envelope — so a new loader is just a new
        action, no change here.

        Args:
            tool_args: The raw tool arguments from the paused call (``{"action": {...}}``).

        Returns:
            The canonical ``{output|error, metadata}`` envelope the runner writes back as the
            tool result.
        """
        if self._ctx is None or self._base_tools is None:
            return self._fail("tool loading is unavailable in this context", tool="load")
        try:
            action = TypeAdapter(LoadActions).validate_python(tool_args.get("action"))
        except ValidationError:
            # Name the accepted action tags (like the CRUD managers do) so a caller that sent an
            # out-of-union action — e.g. 'service' — can self-correct, instead of getting an opaque
            # "invalid" with no enumeration. ``LoadActions`` is a single action class today and a
            # discriminated union once a second loader exists; handle both.
            union = get_args(LoadActions)
            members = get_args(union[0]) if union else (LoadActions,)
            accepted = sorted(str(member.model_fields["action"].default) for member in members)
            return self._fail(f"invalid load action; accepted actions: {', '.join(accepted)}", tool="load")
        ctx = LoadActionCtx(context=self._ctx, base_tools=self._base_tools, notify=self._notify)
        try:
            outcome = await action.execute(ctx)
        except Exception as error:
            # execute is written never to raise, but a backend surprise (a gRPC error the action
            # didn't anticipate) must still not crash the module — the exception would otherwise
            # propagate through the HITL runner into the module run lifecycle. Fail-safe envelope.
            logger.exception("LoadManager: load failed unexpectedly")
            return self._fail(f"could not load: {type(error).__name__}: {error}", tool="load")
        if not outcome.ok:
            return self._fail(outcome.message, tool=action.action)
        return self._ok(
            {
                "status": outcome.status,
                "tool_name": outcome.tool_name,
                "loaded_functions": outcome.loaded_functions,
                "message": outcome.message,
            },
            tool=action.action,
        )
