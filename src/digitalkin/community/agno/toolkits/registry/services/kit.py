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
    """Manage Service setups (SERVICE): create, search, load, update, delete, visibility, versions."""

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
                "Manage Service SETUPS: create, search, load, update, delete, change_visibility, plus "
                "list_versions / set_version to inspect the configuration history and undo a bad "
                "update. The "
                "action discriminator selects the operation; 'load' returns a service's configuration "
                "content for use."
            ),
            entrypoint=self.services_manager,
        )

    async def services_manager(self, action: ServiceActions | None = None, **fields: Any) -> str:
        """Dispatch a Service operation (create / search / load / update / delete / visibility / versions).

        Args:
            action: A discriminated Service action; its type selects the operation.
            fields: Absorbs a flattened call — some models send the action's own fields as
                siblings of ``action`` rather than inside it. :meth:`_run` re-nests them.

        Returns:
            The canonical success envelope, or a fail envelope on rejection/invalid input.
        """
        return await self._run(action, **fields)
