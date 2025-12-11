"""Default registry implementation."""

from typing import ClassVar

from digitalkin.models.services.registry import (
    ModuleInfo,
    ModuleStatusInfo,
    RegistryModuleStatus,
    RegistryModuleType,
)
from digitalkin.services.registry.exceptions import RegistryModuleNotFoundError
from digitalkin.services.registry.registry_strategy import RegistryStrategy


class DefaultRegistry(RegistryStrategy):
    """Default registry strategy using in-memory storage."""

    _modules: ClassVar[dict[str, ModuleInfo]] = {}

    def discover_by_id(self, module_id: str) -> ModuleInfo:
        """Get module info by ID.

        Args:
            module_id: The module identifier.

        Returns:
            ModuleInfo with module details.

        Raises:
            RegistryModuleNotFoundError: If module not found.
        """
        if module_id not in self._modules:
            raise RegistryModuleNotFoundError(module_id)
        return self._modules[module_id]

    def search(
        self,
        name: str | None = None,
        module_type: str | None = None,
        organization_id: str | None = None,  # noqa: ARG002
    ) -> list[ModuleInfo]:
        """Search for modules by criteria.

        Args:
            name: Filter by name (partial match).
            module_type: Filter by type (archetype, tool).
            organization_id: Filter by organization (not used in local storage).

        Returns:
            List of matching modules.
        """
        results = list(self._modules.values())

        if name:
            results = [m for m in results if name in m.name]

        if module_type:
            results = [m for m in results if m.module_type == module_type]

        return results

    def get_status(self, module_id: str) -> ModuleStatusInfo:
        """Get module status.

        Args:
            module_id: The module identifier.

        Returns:
            ModuleStatusInfo with current status.

        Raises:
            RegistryModuleNotFoundError: If module not found.
        """
        if module_id not in self._modules:
            raise RegistryModuleNotFoundError(module_id)

        module = self._modules[module_id]
        return ModuleStatusInfo(
            module_id=module_id,
            status=module.status or RegistryModuleStatus.UNSPECIFIED,
        )

    def register(
        self,
        module_id: str,
        address: str,
        port: int,
        version: str,
    ) -> ModuleInfo | None:
        """Register a module with the registry.

        Note: Updates existing module or creates new one in local storage.

        Args:
            module_id: Unique module identifier.
            address: Network address.
            port: Network port.
            version: Module version.

        Returns:
            ModuleInfo if successful, None otherwise.
        """
        existing = self._modules.get(module_id)
        self._modules[module_id] = ModuleInfo(
            module_id=module_id,
            module_type=existing.module_type if existing else RegistryModuleType.UNSPECIFIED,
            address=address,
            port=port,
            version=version,
            name=existing.name if existing else module_id,
            status=RegistryModuleStatus.ACTIVE,
        )
        return self._modules[module_id]

    def heartbeat(self, module_id: str) -> RegistryModuleStatus:
        """Send heartbeat to keep module active.

        Args:
            module_id: The module identifier.

        Returns:
            Current module status after heartbeat.

        Raises:
            RegistryModuleNotFoundError: If module not found.
        """
        if module_id not in self._modules:
            raise RegistryModuleNotFoundError(module_id)

        module = self._modules[module_id]
        # Update status to ACTIVE on heartbeat
        self._modules[module_id] = ModuleInfo(
            module_id=module.module_id,
            module_type=module.module_type,
            address=module.address,
            port=module.port,
            version=module.version,
            name=module.name,
            status=RegistryModuleStatus.ACTIVE,
        )
        return RegistryModuleStatus.ACTIVE
