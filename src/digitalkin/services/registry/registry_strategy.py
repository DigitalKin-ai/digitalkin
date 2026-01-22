"""Abstract base class for registry strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.models.base_strategy import BaseStrategy
from digitalkin.models.services.modules import ModuleInfo, ModuleStatus, ModuleType
from digitalkin.models.services.setup import SetupInfo


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

    # ════════════════════════════════ Overriding Methods ════════════════════════════════ #

    @abstractmethod
    async def search(
        self,
        name: str | None = None,
            module_type: ModuleType | None = None,
        organization_id: str | None = None,
    ) -> list[ModuleInfo]:
        """Search for modules by criteria.

        Args:
            name: Filter by name (partial match via query).
            module_type: Filter by type (archetype, tool).
            organization_id: Filter by organization.

        Returns:
            list[ModuleInfo]: List of matching modules.
        """
        return await super().search()

    @abstractmethod
    async def get(self, module_id: str) -> ModuleInfo:
        """Get module information by its unique identifier.

        Args:
            module_id: Unique module identifier.

        Returns:
            ModuleInfo: If module with the given ID is found in the registry.

        Raises:
             RegistryModuleNotFoundError: If module with the given ID is not found in the registry.
        """
        return await super().get()

    # ════════════════════════════════ Abstracts Methods ═════════════════════════════════ #

    @abstractmethod
    async def register(
        self,
        module_id: str,
        address: str,
        port: int,
        version: str,
    ) -> ModuleInfo | None:
        """Register a module with the registry.

        Note: The new proto only updates address/port/version for an existing module.
        The module must already exist in the registry database.

        Args:
            module_id: Unique module identifier.
            address: Network address.
            port: Network port.
            version: Module version.

        Returns:
            ModuleInfo: If registration successful
        """
        msg = "Register method not implemented yet."
        raise NotImplementedError(msg)

    @abstractmethod
    async def heartbeat(self, module_id: str) -> ModuleStatus:
        """Send heartbeat to keep module active.

        Args:
            module_id: The module identifier.

        Returns:
            ModuleStatus: Current module status after heartbeat.

        Raises:
            RegistryModuleNotFoundError: If module not found.
        """
        msg = "Heartbeat method not implemented yet."
        raise NotImplementedError(msg)

    @abstractmethod
    async def get_status(self, module_id: str) -> ModuleInfo:
        """Get the current status of a module.

        Args:
            module_id: The module identifier.

        Returns:
            ModuleInfo: Current module information including status.

        Raises:
            RegistryModuleNotFoundError: If module not found.
        """
        msg = "Get status method not implemented yet."
        raise NotImplementedError(msg)

    @abstractmethod
    async def get_setup(self, setup_id: str) -> SetupInfo | None:
        """Get setup info.

        Args:
            setup_id: The setup identifier.

        Returns:
            SetupInfo if successful, None otherwise.

        Raises:
            RegistryServiceError: If gRPC call fails.
        """
        msg = "Get setup method not implemented yet."
        raise NotImplementedError(msg)

    @abstractmethod
    async def deregister(self, module_id: str) -> bool:
        """Deregister a module from the registry.

        Note: The registry protocol uses heartbeat expiration for deregistration.
        When a module stops sending heartbeats, it becomes inactive. This method
        logs the deregistration intent for observability.

        Args:
            module_id: The module identifier to deregister.

        Returns:
            True always (heartbeat expiration handles actual deregistration).
        """
        msg = "Deregister method not implemented yet."
        raise NotImplementedError(msg)

    # ════════════════════════════ Unimplemented Methods ═════════════════════════════ #

    async def create(self, *args: Any, **kwargs: Any) -> Any:
        return await super().create()

    async def list(self, *args: Any, **kwargs: Any) -> Any:
        return await super().list()

    async def delete(self, *args: Any, **kwargs: Any) -> Any:
        return await super().delete()

    async def update(self, *args: Any, **kwargs: Any) -> Any:
        return await super().update()

    async def upload(self, *args: Any, **kwargs: Any) -> Any:
        return await super().upload()
