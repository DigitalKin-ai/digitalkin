"""Build :class:`ModuleToolkit` instances from a module context's tool cache."""

from __future__ import annotations

from typing import TYPE_CHECKING

from digitalkin.logger import logger

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable

    from digitalkin.community.agno.module_toolkit import ModuleToolkit
    from digitalkin.models.module import ModuleContext
    from digitalkin.models.module.tool_cache import ToolModuleInfo
    from digitalkin.models.module.tool_reference import ToolSelection


class ModuleToolkits:
    """Turn tool selections and runtime-loaded tools into agent toolkits.

    ``tool_cache`` holds one shared ``ToolModuleInfo`` per setup; every toolkit filters it to its
    own triggers without mutating it, so agents sharing a setup each see only their selection.
    """

    @staticmethod
    def for_selections(context: ModuleContext, selections: Iterable[ToolSelection]) -> list[ModuleToolkit]:
        """Build one toolkit per selection, keeping only the triggers enabled in it.

        Args:
            context: Module context carrying the tool cache.
            selections: The agent's tool selections (setup id + trigger flags).

        Returns:
            The toolkits that resolved to at least one tool.
        """
        toolkits = []
        for selection in selections:
            info = context.tool_cache.entries.get(selection.setup_id)
            if info is None:
                logger.warning("No tool found in cache for setup_id='%s'", selection.setup_id)
                continue
            allowed = {name for name, enabled in selection.triggers.items() if enabled}
            if not allowed:
                logger.info("All triggers disabled for setup_id='%s', skipping", selection.setup_id)
                continue
            toolkit = ModuleToolkits._toolkit(context, info, allowed)
            if toolkit is not None:
                toolkits.append(toolkit)
        return toolkits

    @staticmethod
    def for_loaded(context: ModuleContext, exclude: Collection[str] = ()) -> list[ModuleToolkit]:
        """Build a toolkit for each tool the agent loaded at runtime during this mission.

        The tool cache's mission-scoped ``dynamic`` layer is rehydrated from storage at the start
        of every turn, so re-attaching it is what makes a load outlive the turn that made it. A
        loaded tool is loaded whole: no trigger filter applies.

        Args:
            context: Module context carrying the tool cache.
            exclude: Setup ids already attached through a selection; loading one again would
                duplicate its function names.

        Returns:
            The toolkits that resolved to at least one tool.
        """
        toolkits = []
        for setup_id, info in context.tool_cache.dynamic.items():
            if setup_id in exclude:
                continue
            toolkit = ModuleToolkits._toolkit(context, info, None)
            if toolkit is not None:
                toolkits.append(toolkit)
        return toolkits

    @staticmethod
    def build(context: ModuleContext, selections: Collection[ToolSelection]) -> list[ModuleToolkit]:
        """Build the selected toolkits followed by the mission's runtime-loaded ones.

        Args:
            context: Module context carrying the tool cache.
            selections: The agent's tool selections.

        Returns:
            :meth:`for_selections` then :meth:`for_loaded`, excluding the selected setups.
        """
        return [
            *ModuleToolkits.for_selections(context, selections),
            *ModuleToolkits.for_loaded(context, exclude={selection.setup_id for selection in selections}),
        ]

    @staticmethod
    def _toolkit(context: ModuleContext, info: ToolModuleInfo, allowed: set[str] | None) -> ModuleToolkit | None:
        """Wrap one cache entry, or ``None`` when it fails or keeps no tool.

        Args:
            context: Module context for the tool calls.
            info: The cache entry to wrap.
            allowed: Tool names to keep; ``None`` keeps them all.

        Returns:
            The toolkit, or ``None``.
        """
        # Lazy: module_toolkit imports this package (toolkits.base), so a top-level import here
        # breaks ``from digitalkin.community.agno.module_toolkit import ModuleToolkit`` (cycle).
        from digitalkin.community.agno.module_toolkit import ModuleToolkit

        try:
            toolkit = ModuleToolkit(context=context, tool_module_info=info, allowed_tools=allowed)
        except Exception:
            logger.warning(
                "Failed to create toolkit for module_id='%s' setup_id='%s'",
                info.module_id,
                info.setup_id,
                exc_info=True,
            )
            return None
        if not toolkit.tools:
            logger.warning("Toolkit for setup_id='%s' has no tools, not attached", info.setup_id)
            return None
        return toolkit
