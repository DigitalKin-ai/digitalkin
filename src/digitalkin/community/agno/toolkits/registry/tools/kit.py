"""``tools_manager`` — one agent-facing tool grouping Tool-setup CRUD as actions.

This administers tool **setups** (search / get / update / delete / change visibility); it
never makes a tool callable. To actually **use** a discovered tool, load it with the separate
``load_manager`` tool (``load_tool`` action).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from digitalkin.community.agno.toolkits.registry.base import RegistryObjectToolKit

# Runtime import (not TYPE_CHECKING): the union is passed to the base as ``actions=`` to build the
# LLM schema and validate calls, so it must exist at runtime.
from digitalkin.community.agno.toolkits.registry.tools.action import ToolActions
from digitalkin.models.services.registry import RegistryModuleType

if TYPE_CHECKING:
    from digitalkin.models.module import ModuleContext
    from digitalkin.services.registry.registry_strategy import RegistryStrategy
    from digitalkin.services.setup.setup_strategy import SetupStrategy


class ToolsManager(RegistryObjectToolKit):
    """Manage Tool setups (TOOL_MODULE): search, get, update, delete, change visibility, versions."""

    module_type: ClassVar[RegistryModuleType] = RegistryModuleType.TOOL_MODULE

    def __init__(self, setup: SetupStrategy, registry: RegistryStrategy, context: ModuleContext | None = None) -> None:
        """Initialize the manager with the module's setup and registry services.

        Args:
            setup: The setup service strategy.
            registry: The registry service strategy.
            context: Module context; enables AG-UI notifications via the base toolkit.
        """
        super().__init__(
            setup,
            registry,
            context,
            name="tools_manager",
            actions=ToolActions,
            description=(
                "Administer tool SETUPS — find and manage tools, but do NOT run them. Use it to "
                "discover tool setups (search), read one (get), or administer a tool (update / "
                "delete / change_visibility / list_versions / set_version). It only touches a "
                "tool's setup (metadata, "
                "configuration, visibility, lifecycle) — it never makes a tool callable. To actually "
                "USE a discovered tool, take its setup_id and load it with the separate load_manager "
                "tool (the 'tool' action)."
            ),
            entrypoint=self.tools_manager,
        )

    async def tools_manager(self, action: ToolActions | None = None, **fields: Any) -> str:
        """Administer tool setups (search / get / update / delete / change_visibility).

        Args:
            action: A discriminated Tool action; its type selects the operation.
            fields: Absorbs a flattened call — some models send the action's own fields as
                siblings of ``action`` rather than inside it. :meth:`_run` re-nests them.

        Returns:
            The canonical success envelope, or a fail envelope on rejection/invalid input.
        """
        return await self._run(action, **fields)
