"""Toolkit for dynamically loading a discovered setup as a live, callable tool.

``use_setup`` is an external-execution tool: when the model invokes it, Agno pauses
the run (rather than executing an entrypoint), handing control to
:class:`~digitalkin.community.agno.hitl.AgnoHitlRunner`. The runner calls
:meth:`ToolLoaderTools.load`, which resolves the setup into a
:class:`~digitalkin.community.agno.module_toolkit.ModuleToolkit`, appends it to the
live ``base_tools`` list the agent's tools factory closes over, and auto-continues —
so discover → load → use looks like a single turn to the user.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from digitalkin.community.agno.toolkits.base import DkToolkit
from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.logger import logger

if TYPE_CHECKING:
    from collections.abc import Callable

    from digitalkin.models.module import ModuleContext


class ToolLoaderTools(DkToolkit):
    """Expose ``use_setup`` — an external-execution tool that loads a setup on demand.

    The tool itself never executes: it is registered as external-execution so the run
    pauses when the model calls it. The bound :class:`AgnoHitlRunner` then invokes
    :meth:`load` and auto-continues with the enlarged tool list.
    """

    def __init__(self, context: ModuleContext | None = None) -> None:
        """Register the ``use_setup`` external-execution tool.

        Args:
            context: Module context; supplies ``resolve_tool`` and AG-UI notifications.
        """
        super().__init__(
            name="tool_loader_tools",
            tools=[self.use_setup],
            context=context,
            external_execution_required_tools=[self.use_setup.__name__],
        )
        # The live list the agent's tools factory splats; bound by DefaultToolkits.build.
        self._base_tools: list[Any] | None = None

    @property
    def tool_name(self) -> str:
        """The external tool name the runner pauses on and routes to :meth:`load`."""
        return self.use_setup.__name__

    def bind_tools(self, base_tools: list[Any]) -> None:
        """Bind the live tool list that :meth:`load` appends newly-loaded tools to.

        Args:
            base_tools: The exact list the agent's ``make_tools_factory`` closes over,
                so an appended toolkit is visible on the next run.
        """
        self._base_tools = base_tools

    @staticmethod
    def find(tools: list[Any] | Callable[..., list[Any]] | None) -> ToolLoaderTools | None:
        """Locate the ToolLoaderTools instance within a tools list or factory.

        Args:
            tools: The tools list, or a ``make_tools_factory`` callable.

        Returns:
            The first ToolLoaderTools found, or ``None``.
        """
        if callable(tools) and not isinstance(tools, list):
            tools = tools(None)
        if not isinstance(tools, list):
            return None
        for tool in tools:
            if isinstance(tool, ToolLoaderTools):
                return tool
        return None

    async def use_setup(self, setup_id: str) -> str:
        """Load a discovered setup as a live tool you can call immediately.

        Pass a ``setup_id`` returned by ``search_setups`` to make that tool available for
        the rest of this conversation. The tool is loaded right away — you do NOT need to
        ask the user — and you can call it in your very next step. This returns a short
        confirmation (or an error if the setup could not be loaded).

        Args:
            setup_id: The setup id (from ``search_setups``) to load as an invocable tool.

        Returns:
            A confirmation that the tool is loaded, or an error message.
        """
        # Never executed: registered as external-execution, so the run pauses here and
        # AgnoHitlRunner calls load() instead. Kept for a correct LLM-facing schema.
        return self._ok({"setup_id": setup_id, "status": "pending"}, tool="use_setup")

    async def load(self, setup_id: str) -> str:
        """Resolve ``setup_id`` into a ModuleToolkit and append it to the live tool list.

        Called by the runner on a ``use_setup`` pause. Idempotent per ``setup_id`` (a tool
        already loaded is not duplicated). Never raises — resolution/permission failures
        return a message the model reads as the tool result.

        Args:
            setup_id: The setup id to load as an invocable tool.

        Returns:
            A short status string ("loaded: …" / "permission denied: …" / "could not load …").
        """
        if self._ctx is None or self._base_tools is None:
            return "tool loading is unavailable in this context"
        try:
            info = await self._ctx.resolve_tool(setup_id)
        except PermissionDeniedError:
            return f"permission denied: cannot load setup {setup_id}"
        except Exception as error:
            logger.warning("ToolLoaderTools: failed to resolve setup %s: %s", setup_id, error)
            return f"could not load setup {setup_id}"
        if info is None:
            return f"could not load setup {setup_id}: not found"
        if not info.tools:
            return f"could not load setup {setup_id}: module exposes no callable tools"

        # Imported here, not at module top: ModuleToolkit requires the optional agno
        # dependency at import time (see its module docstring), while this toolkit must
        # stay importable without it — same convention as the rest of community.agno.
        from digitalkin.community.agno.module_toolkit import ModuleToolkit

        name = info.tool_name or info.module_name or info.slug
        already = any(
            isinstance(tool, ModuleToolkit) and tool.tool_module_info.setup_id == setup_id for tool in self._base_tools
        )
        if not already:
            self._base_tools.append(ModuleToolkit(self._ctx, info))
        await self._notify("tool_loaded", {"setup_id": setup_id, "tool_name": name})
        return f"loaded: '{name}' is now available as a tool; call it directly to use it"
