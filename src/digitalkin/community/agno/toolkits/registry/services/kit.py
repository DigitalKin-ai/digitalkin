"""``services_manager`` — one agent-facing tool grouping Service CRUD + create + load."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from digitalkin.community.agno.toolkits.registry.base import RegistryObjectToolKit

# Runtime import (not TYPE_CHECKING): the union is passed to the base as ``actions=`` to build the
# LLM schema and validate calls, so it must exist at runtime.
from digitalkin.community.agno.toolkits.registry.services.action import ServiceActions
from digitalkin.models.services.registry import RegistryModuleType

if TYPE_CHECKING:
    from digitalkin.models.module import ModuleContext
    from digitalkin.services.registry.registry_strategy import RegistryStrategy
    from digitalkin.services.setup.setup_strategy import SetupStrategy


class ServicesManager(RegistryObjectToolKit):
    """Manage Service setups (SERVICE): create, search, load, update, patch, append, remove, delete, versions."""

    module_type: ClassVar[RegistryModuleType] = RegistryModuleType.SERVICE

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
            name="services_manager",
            actions=ServiceActions,
            description=(
                "Manage Service SETUPS: create, search, structure, load, update, patch, append, "
                "remove, delete, change_visibility, plus list_versions / set_version to inspect the "
                "configuration history and undo a bad update. The action discriminator selects the "
                "operation. To change one element use 'patch' (the path must exist), to add one "
                "use 'append' (a new key, or one more item of a list), and to take one out use "
                "'remove' (a key or a list item; 'delete' removes the whole service). patch and "
                "append take a key path, a value and the structure entries describing what they "
                "write; remove takes only the path and updates the map itself. All three keep the "
                "rest of the configuration, its documentation and the map entries outside the "
                "edited element, and cut a new version. Use 'update' only to rewrite the whole "
                "configuration. "
                "Configurations can be large, so read them in two steps: 'search' returns each "
                "result with a 'structure' map of key path -> what lives there ('structure' fetches "
                "the same map for an id you already hold), then 'load' with a key copied verbatim "
                "from it returns just that part. Copy the key, never invent one: a key the "
                "configuration does not have fails with not found. Load without a key only when "
                "the whole document is genuinely needed. Every action that writes content (create, "
                "update, patch, append) requires that map: you write it yourself, describing what "
                "each key is for — it is how other agents find their way around the configuration."
            ),
            entrypoint=self.services_manager,
            display_name="Services Manager",
        )

    async def services_manager(self, action: ServiceActions | None = None, **fields: Any) -> str:
        """Dispatch a Service operation (create, search, load, update, patch, append, remove, delete, versions).

        Args:
            action: A discriminated Service action; its type selects the operation.
            fields: Absorbs a flattened call — some models send the action's own fields as
                siblings of ``action`` rather than inside it. :meth:`_run` re-nests them.

        Returns:
            The canonical success envelope, or a fail envelope on rejection/invalid input.
        """
        return await self._run(action, **fields)
