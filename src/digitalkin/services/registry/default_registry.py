"""Default registry implementation."""

from typing import Any

from digitalkin.models.services.registry import (
    ModuleInfo,
    RegistryModuleStatus,
    RegistryModuleType,
    RegistrySetupStatus,
    RegistryVisibility,
    SetupInfo,
    SetupSummary,
)
from digitalkin.services.registry.exceptions import RegistryModuleNotFoundError
from digitalkin.services.registry.registry_models import ModuleStatusInfo
from digitalkin.services.registry.registry_strategy import RegistryStrategy


class DefaultRegistry(RegistryStrategy):
    """Default registry strategy using in-memory storage."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize with per-instance module and setup stores."""
        super().__init__(*args, **kwargs)
        self._modules: dict[str, ModuleInfo] = {}
        self._setups: dict[str, SetupInfo] = {}

    async def wait_for_ready(self, timeout: float = 1.0) -> bool:  # noqa: ARG002, PLR6301
        """Local registry is always ready (in-memory store).

        Args:
            timeout: Ignored for local registry.

        Returns:
            Always True — no network dependency.
        """
        return True

    async def discover_by_id(self, module_id: str) -> ModuleInfo:
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

    async def search(
        self,
        name: str | None = None,
        module_type: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[ModuleInfo]:
        """Search the module catalog (module blueprints; needs a setup to be invocable).

        Args:
            name: Case-insensitive free text matched against module name AND documentation.
            module_type: Filter by type (archetype, tool_module, service).
            limit: Max results (1-100).
            offset: Pagination offset.

        Returns:
            List of matching modules.
        """
        results = list(self._modules.values())

        if name:
            needle = name.lower()
            results = [
                m for m in results if needle in m.module_name.lower() or needle in (m.documentation or "").lower()
            ]

        if module_type:
            results = [m for m in results if m.module_type == module_type]

        return results[offset : offset + limit]

    async def get_status(self, module_id: str) -> ModuleStatusInfo:
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

    async def register(
        self,
        module_id: str,
        address: str,
        port: int,
        version: str,
        module_type: RegistryModuleType = RegistryModuleType.UNSPECIFIED,
        documentation: str = "",
    ) -> ModuleInfo | None:
        """Register a module with the registry.

        Note: Updates existing module or creates new one in local storage.

        Args:
            module_id: Unique module identifier.
            address: Network address.
            port: Network port.
            version: Module version.
            module_type: Declared module type; UNSPECIFIED preserves the existing record's type.
            documentation: Internal documentation for registry index search.

        Returns:
            ModuleInfo if successful, None otherwise.
        """
        existing = self._modules.get(module_id)
        self._modules[module_id] = ModuleInfo(
            module_id=module_id,
            module_type=module_type
            if module_type != RegistryModuleType.UNSPECIFIED
            else (existing.module_type if existing else RegistryModuleType.UNSPECIFIED),
            address=address,
            port=port,
            version=version,
            module_name=existing.module_name if existing else module_id,
            documentation=documentation or (existing.documentation if existing else None),
            status=RegistryModuleStatus.ACTIVE,
        )
        return self._modules[module_id]

    async def heartbeat(self, module_id: str) -> RegistryModuleStatus:
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
            module_name=module.module_name,
            status=RegistryModuleStatus.ACTIVE,
        )
        return RegistryModuleStatus.ACTIVE

    async def deregister(self, module_id: str) -> bool:
        """Deregister a module from the registry.

        Args:
            module_id: The module identifier to deregister.

        Returns:
            True if module was removed, False if not found.
        """
        if module_id in self._modules:
            del self._modules[module_id]
            return True
        return False

    async def get_setup(self, setup_id: str) -> SetupInfo | None:
        """Get setup info from the in-memory store.

        Args:
            setup_id: The setup identifier.

        Returns:
            SetupInfo if present, None otherwise.
        """
        return self._setups.get(setup_id)

    def add_setup(self, setup: SetupInfo) -> None:
        """Add a setup to the in-memory store (helper for testing).

        Args:
            setup: The setup to store, keyed by its setup_id.
        """
        self._setups[setup.setup_id] = setup

    async def search_setups(
        self,
        query: str | None = None,
        setup_ids: list[str] | None = None,
        module_ids: list[str] | None = None,
        module_types: list[RegistryModuleType] | None = None,
        statuses: list[RegistrySetupStatus] | None = None,
        visibilities: list[RegistryVisibility] | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[SetupSummary]:
        """Search the setup catalog (configured, invocable module instances).

        Args:
            query: Case-insensitive free text matched against setup name AND documentation.
            setup_ids: Restrict to these setup ids.
            module_ids: Restrict to setups backed by these modules.
            module_types: Filter by backing module type (tool_module, archetype, service).
            statuses: Filter by setup status. None = no filter.
            visibilities: Filter by visibility.
            limit: Max results (1-100).
            offset: Pagination offset.

        Returns:
            Matching setups as ``SetupSummary`` (no ``config`` field by construction).
        """
        results = list(self._setups.values())
        if setup_ids:
            results = [s for s in results if s.setup_id in setup_ids]
        if module_ids:
            results = [s for s in results if s.module_id in module_ids]
        if module_types:
            results = [s for s in results if s.module_type in module_types]
        if statuses:
            results = [s for s in results if s.status in statuses]
        if visibilities:
            results = [s for s in results if s.visibility in visibilities]
        if query:
            needle = query.lower()
            results = [s for s in results if needle in s.name.lower() or needle in (s.documentation or "").lower()]
        return [
            SetupSummary(
                setup_id=s.setup_id,
                name=s.name,
                documentation=s.documentation,
                status=s.status,
                visibility=s.visibility,
                organization_id=s.organization_id,
                module_id=s.module_id,
                module_name=s.module_name,
                module_type=s.module_type,
                setup_version_id=s.setup_version_id,
                setup_version=s.setup_version,
            )
            for s in results[offset : offset + limit]
        ]
