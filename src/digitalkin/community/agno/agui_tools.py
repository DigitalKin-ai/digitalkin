"""AG-UI frontend tools → Agno external Functions.

The AG-UI protocol lets the client declare its own tools in
``RunAgentInput.tools``, meant to be executed on the frontend rather than
by the agent process. :class:`AguiTools` exposes them to an Agno agent as
:class:`~agno.tools.function.Function` objects marked
``external_execution=True``: when the LLM "calls" one, Agno pauses the run
instead of executing an entrypoint, letting the caller stream the tool-call
events to the front and resume later.

See ``examples/`` and the :class:`~digitalkin.community.agno.AgnoHitlRunner`
docstring for end-to-end usage.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from ag_ui.core.types import Tool as AgUiTool
    from agno.run.base import RunContext
    from agno.tools.function import Function


class AguiTools:
    """Convert AG-UI frontend tool declarations into Agno external Functions."""

    @staticmethod
    def _unreachable_entrypoint(**_: Any) -> None:
        """Placeholder — never invoked because ``external_execution=True`` pauses the run."""

    @staticmethod
    def agui_tool_to_external_function(tool: AgUiTool) -> Function:
        """Wrap an AG-UI tool definition as an Agno external ``Function``.

        The resulting :class:`Function` carries the AG-UI schema as-is and is
        marked ``external_execution=True`` so Agno emits the tool-call events
        but skips the entrypoint and pauses the run when the LLM invokes it.

        Args:
            tool: An :class:`ag_ui.core.types.Tool` from ``RunAgentInput.tools``.

        Returns:
            An :class:`agno.tools.function.Function` ready to plug into an agent.
        """
        from agno.tools.function import Function  # pyright: ignore[reportMissingImports]

        parameters = tool.parameters or {"type": "object", "properties": {}, "required": []}
        return Function(
            name=tool.name,
            description=tool.description,
            parameters=parameters,
            entrypoint=AguiTools._unreachable_entrypoint,
            external_execution=True,
            skip_entrypoint_processing=True,
        )

    @staticmethod
    def make_tools_factory(
        base_tools: list[Any],
        dependency_key: str = "agui_tools",
    ) -> Callable[[RunContext], list[Any]]:
        """Build an Agno ``tools`` factory merging base tools with per-run AG-UI tools.

        The returned callable is the value passed to ``Agent(tools=...)``. On
        every run Agno resolves it with the current ``RunContext``; the factory
        reads ``run_context.dependencies[dependency_key]`` (the per-run AG-UI
        tool list), converts them to external Functions, and concatenates them
        with ``base_tools``.

        Args:
            base_tools: Toolkits / Functions always available, passed through.
            dependency_key: Key in ``run_context.dependencies`` for the per-run
                AG-UI tool list. Defaults to ``"agui_tools"``.

        Returns:
            A callable for ``Agent(tools=...)``. Set ``cache_callables=False``
            so it is re-invoked every run.
        """

        def factory(run_context: RunContext | None = None) -> list[Any]:
            if run_context is None:
                return list(base_tools)
            deps = getattr(run_context, "dependencies", None) or {}
            agui_tools: list[AgUiTool] = deps.get(dependency_key) or []
            return [*base_tools, *[AguiTools.agui_tool_to_external_function(t) for t in agui_tools]]

        return factory
