"""AG-UI frontend tools → Agno external Functions.

The AG-UI protocol lets the client declare its own tools in
``RunAgentInput.tools``. Those tools are meant to be executed on the
frontend (a UI widget, a browser-local API call, a user prompt, …) rather
than by the agent process. This module provides the glue to expose them
to an Agno :class:`~agno.agent.Agent` as regular :class:`~agno.tools.function.Function`
objects marked with ``external_execution=True``: when the LLM "calls" one,
Agno pauses the run (via :class:`~agno.run.agent.RunPausedEvent`) instead
of executing an entrypoint — letting the caller stream the tool-call
events to the front and resume later via :meth:`~agno.agent.Agent.acontinue_run`.

Usage::

    from digitalkin.community.agno import make_tools_factory
    from agno.agent import Agent

    agent = Agent(
        tools=make_tools_factory([AsyncDuckDuckGoTools()]),
        cache_callables=False,           # critical — see make_tools_factory
        ...
    )

    async for ev in agent.arun(
        message,
        dependencies={"agui_tools": input_data.tools},
        stream=True,
        stream_events=True,
    ):
        ...

Notes:
    ``dependencies`` is Agno's standard per-run injection bus. We use it
    as a transport channel to hand the frontend tools to the tools
    factory on every run — the tools themselves are actually registered
    through the ``tools=factory`` mechanism, not through ``dependencies``.
    ``cache_callables=False`` is required so the factory is re-invoked on
    each run (otherwise the first resolved tool list is cached forever and
    subsequent requests would not see new frontend tools).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from ag_ui.core.types import Tool as AgUiTool
    from agno.run.base import RunContext
    from agno.tools.function import Function

_DEFAULT_DEPENDENCY_KEY = "agui_tools"


def _unreachable_entrypoint(**_: Any) -> None:
    """Placeholder — never invoked because ``external_execution=True`` pauses the run."""


def agui_tool_to_external_function(tool: AgUiTool) -> Function:
    """Wrap an AG-UI tool definition as an Agno external ``Function``.

    The resulting :class:`Function` carries the AG-UI schema as-is (Agno
    accepts raw JSON Schema via ``parameters``) and is marked with
    ``external_execution=True`` so Agno emits the tool-call events but
    skips the entrypoint and pauses the run when the LLM invokes it.

    Args:
        tool: An :class:`ag_ui.core.types.Tool` from ``RunAgentInput.tools``.

    Returns:
        An :class:`agno.tools.function.Function` ready to be plugged into
        an Agno agent's tool list.
    """
    from agno.tools.function import Function  # pyright: ignore[reportMissingImports]

    parameters = tool.parameters or {"type": "object", "properties": {}, "required": []}
    return Function(
        name=tool.name,
        description=tool.description,
        parameters=parameters,
        entrypoint=_unreachable_entrypoint,
        external_execution=True,
        skip_entrypoint_processing=True,
    )


def make_tools_factory(
    base_tools: list[Any],
    dependency_key: str = _DEFAULT_DEPENDENCY_KEY,
) -> Callable[[RunContext], list[Any]]:
    """Build an Agno ``tools`` factory that merges base tools with per-run AG-UI tools.

    The returned callable is the value you pass to ``Agent(tools=...)``. On
    every run, Agno resolves the factory with the current
    :class:`~agno.run.base.RunContext` (see
    :func:`agno.utils.callables.aresolve_callable_tools`). The factory
    reads ``run_context.dependencies[dependency_key]`` — the list of
    :class:`~ag_ui.core.types.Tool` you passed via
    ``agent.arun(dependencies={dependency_key: [...]})`` — converts them to
    external :class:`Function` objects, and concatenates them with the
    ``base_tools``.

    Args:
        base_tools: Toolkits / Functions always available to the agent
            (e.g. ``AsyncDuckDuckGoTools()``). Passed through unchanged.
        dependency_key: The key in ``run_context.dependencies`` under which
            the caller places the per-run AG-UI tool list. Defaults to
            ``"agui_tools"``.

    Returns:
        A callable suitable for :class:`agno.agent.Agent`'s ``tools=``
        parameter. Set ``cache_callables=False`` on the ``Agent`` so this
        factory is re-invoked on every run.
    """

    def factory(run_context: RunContext) -> list[Any]:
        deps = getattr(run_context, "dependencies", None) or {}
        agui_tools: list[AgUiTool] = deps.get(dependency_key) or []
        return [*base_tools, *[agui_tool_to_external_function(t) for t in agui_tools]]

    return factory
