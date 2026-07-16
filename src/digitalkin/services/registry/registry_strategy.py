"""Abstract base class for registry strategies."""

from abc import ABC, abstractmethod
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
from digitalkin.services.base_strategy import BaseStrategy
from digitalkin.services.registry.registry_models import ModuleStatusInfo


class RegistryStrategy(BaseStrategy, ABC):
    """Abstract base class for registry strategies.

    Defines the interface for registry operations including module discovery,
    registration, and status management.
    """

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        config: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the strategy."""
        super().__init__(mission_id, setup_id, setup_version_id)
        self.config = config

    @abstractmethod
    async def discover_by_id(self, module_id: str) -> ModuleInfo:
        """Get module info by ID."""
        ...

    @abstractmethod
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
            List of matching modules as trimmed ModuleInfo (address/port are never
            populated by search — resolve via discover_by_id when wiring communication).
        """
        ...

    async def search_tools(
        self,
        name: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[ModuleInfo]:
        """Tool registry view: modules of type TOOL_MODULE.

        Args:
            name: Case-insensitive free text matched against module name AND documentation.
            limit: Max results (1-100).
            offset: Pagination offset.

        Returns:
            List of matching tool modules.
        """
        return await self.search(
            name=name,
            module_type=RegistryModuleType.TOOL_MODULE.value,
            limit=limit,
            offset=offset,
        )

    async def search_kins(
        self,
        name: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[ModuleInfo]:
        """Kin registry view: modules of type ARCHETYPE.

        Args:
            name: Case-insensitive free text matched against module name AND documentation.
            limit: Max results (1-100).
            offset: Pagination offset.

        Returns:
            List of matching archetype (kin) modules.
        """
        return await self.search(
            name=name,
            module_type=RegistryModuleType.ARCHETYPE.value,
            limit=limit,
            offset=offset,
        )

    async def search_services(
        self,
        name: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[ModuleInfo]:
        """Service registry view: modules of type SERVICE.

        Args:
            name: Case-insensitive free text matched against module name AND documentation.
            limit: Max results (1-100).
            offset: Pagination offset.

        Returns:
            List of matching service modules.
        """
        return await self.search(
            name=name,
            module_type=RegistryModuleType.SERVICE.value,
            limit=limit,
            offset=offset,
        )

    @abstractmethod
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
            statuses: Filter by setup status. None = no filter; agent-facing callers
                should pass READY/CONFIGURATION_SUCCEEDED for invocable setups.
            visibilities: Filter by visibility.
            limit: Max results (1-100).
            offset: Pagination offset.

        Returns:
            Matching setups as ``SetupSummary`` (no ``config`` field by construction).
        """
        ...

    @abstractmethod
    async def get_status(self, module_id: str) -> ModuleStatusInfo:
        """Get module status."""
        ...

    @abstractmethod
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

        Note: The module must already exist in the registry database; registration
        updates its address/port/version and declares its type.

        Args:
            module_id: Unique module identifier.
            address: Network address.
            port: Network port.
            version: Module version.
            module_type: Declared module type (tool or archetype/kin).
            documentation: Internal documentation for registry index search.

        Returns:
            ModuleInfo if successful, None otherwise.
        """
        ...

    @abstractmethod
    async def heartbeat(self, module_id: str) -> RegistryModuleStatus:
        """Send heartbeat to keep module active.

        Args:
            module_id: The module identifier.

        Returns:
            Current module status after heartbeat.

        Raises:
            RegistryModuleNotFoundError: If module not found.
        """
        ...

    @abstractmethod
    async def get_setup(self, setup_id: str) -> SetupInfo | None:
        """Get setup info."""
        ...

    async def get_service_setup(self, setup_id: str) -> dict[str, Any] | None:
        """Fetch a service setup's setup_version content JSON.

        The id comes from chat-driven discovery (``search_setups`` + user acceptance),
        not from configuration. Goes through ``get_setup`` on every call — the registry
        stays the permission gate; nothing cached. Content always reflects the latest
        setup version.

        Args:
            setup_id: The discovered service setup id.

        Returns:
            The setup_version content, or None when the setup is missing or has no content.
        """
        setup = await self.get_setup(setup_id)
        return setup.config if setup else None

    async def wait_for_ready(self, timeout: float = 1.0) -> bool:  # noqa: PLR6301
        """Check if the registry backend is reachable.

        Args:
            timeout: Max seconds to wait for connectivity.

        Returns:
            True if ready. Default implementation always returns True.
        """
        _ = timeout
        return True

    @abstractmethod
    async def deregister(self, module_id: str) -> bool:
        """Deregister a module from the registry.

        Args:
            module_id: The module identifier to deregister.

        Returns:
            True if deregistration was successful, False otherwise.
        """
        ...
