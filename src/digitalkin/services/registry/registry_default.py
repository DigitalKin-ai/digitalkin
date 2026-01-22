"""Default registry implementation."""

from typing import ClassVar

from digitalkin.exception.registry import RegistryModuleNotFoundError
from digitalkin.models.services.modules import ModuleInfo, ModuleStatus, ModuleType
from digitalkin.models.services.setup import SetupInfo
from digitalkin.services.registry.registry_strategy import RegistryStrategy


class DefaultRegistry(RegistryStrategy):
    """Default registry strategy using in-memory storage."""

    _modules: ClassVar[dict[str, ModuleInfo]] = {}

    # ══════════════════════════════════ Public Methods ══════════════════════════════════ #

    async def search(
        self,
        name: str | None = None,
        module_type: ModuleType | None = None,
        _organization_id: str | None = None,
    ) -> list[ModuleInfo]:
        """Search modules in local registry by name or type.

        Returns:
            The module stub.
        """
        results = list(self._modules.values())

        if name:
            results = [m for m in results if name in m.name]

        if module_type:
            results = [m for m in results if m.type == module_type]

        return results

    async def get(self, module_id: str) -> ModuleInfo:
        """Get module info by ID from local registry.

        Returns:
            The module stub.

        Raises:
            RegistryModuleNotFoundError: If module not found.
        """
        if module_id not in self._modules:
            raise RegistryModuleNotFoundError(module_id)
        return self._modules[module_id]

    async def get_status(self, module_id: str) -> ModuleStatus:
        """Get module status from local registry.

        Returns:
            The module stub.

        Raises:
            RegistryModuleNotFoundError: If module not found.
        """
        if module_id not in self._modules:
            raise RegistryModuleNotFoundError(module_id)

        module = self._modules[module_id]
        return module.status or ModuleStatus.UNSPECIFIED

    async def register(
        self,
        module_id: str,
        address: str,
        port: int,
        version: str,
    ) -> ModuleInfo | None:
        """Register or update a module in local registry.

        Returns:
            The module address.
        """
        existing = self._modules.get(module_id)
        self._modules[module_id] = ModuleInfo(
            id=module_id,
            type=existing.type if existing else ModuleType.UNSPECIFIED,
            address=address,
            port=port,
            version=version,
            name=existing.name if existing else module_id,
            status=ModuleStatus.ACTIVE,
        )
        return self._modules[module_id]

    async def heartbeat(self, module_id: str) -> ModuleStatus:
        """Send heartbeat and return ACTIVE status.

        Returns:
            The module class.

        Raises:
            RegistryModuleNotFoundError: If module not found.
        """
        if module_id not in self._modules:
            raise RegistryModuleNotFoundError(module_id)

        module = self._modules[module_id]
        # Update status to ACTIVE on heartbeat
        self._modules[module_id] = ModuleInfo(
            id=module.id,
            type=module.type,
            address=module.address,
            port=module.port,
            version=module.version,
            name=module.name,
            status=ModuleStatus.ACTIVE,
        )
        return ModuleStatus.ACTIVE

    async def deregister(self, module_id: str) -> bool:
        """Remove module from local registry.

        Returns:
            List of registered modules.
        """
        if module_id in self._modules:
            del self._modules[module_id]
            return True
        return False

    async def get_setup(self, setup_id: str) -> SetupInfo | None:
        """Not implemented.

        Returns:
            True after successful deletion.
        """
        return await super().get_setup(setup_id=setup_id)
