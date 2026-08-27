"""``kins_manager`` — one agent-facing tool grouping Kin CRUD + search as actions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

from digitalkin.community.agno.toolkits.registry.base import RegistryObjectToolKit

# Runtime import (not TYPE_CHECKING): the union is passed to the base as ``actions=`` to build the
# LLM schema and validate calls, so it must exist at runtime.
from digitalkin.community.agno.toolkits.registry.kins.action import KinActions
from digitalkin.models.services.registry import RegistryModuleType

if TYPE_CHECKING:
    from digitalkin.models.module import ModuleContext
    from digitalkin.services.registry.registry_strategy import RegistryStrategy
    from digitalkin.services.setup.setup_strategy import SetupStrategy


class KinsManager(RegistryObjectToolKit):
    """Manage Kin setups (ARCHETYPE): search, update, delete, change visibility, roll back a version."""

    module_type: ClassVar[RegistryModuleType] = RegistryModuleType.ARCHETYPE

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
            name="kins_manager",
            actions=KinActions,
            description=(
                "Manage Kin SETUPS (archetypes): search, update, delete, change_visibility, plus "
                "list_versions / set_version to inspect the configuration history and undo a bad "
                "update. The action discriminator selects the operation."
            ),
            entrypoint=self.kins_manager,
        )

    async def kins_manager(self, action: KinActions | None = None, **fields: Any) -> str:
        """Dispatch a Kin operation (search / update / delete / change_visibility / versions).

        Args:
            action: A discriminated Kin action; its type selects the operation.
            fields: Absorbs a flattened call — some models send the action's own fields as
                siblings of ``action`` rather than inside it. :meth:`_run` re-nests them.

        Returns:
            The canonical success envelope, or a fail envelope on rejection/invalid input.
        """
        return await self._run(action, **fields)
