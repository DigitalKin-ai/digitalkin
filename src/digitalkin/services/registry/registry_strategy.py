"""Abstract base class for registry strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.services.base_strategy import BaseStrategy
from digitalkin.services.registry.registry_models import ModuleInfo, ModuleStatus, ModuleType


class RegistryStrategy(BaseStrategy, ABC):
    """Abstract base class for registry strategies."""

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
    def search(
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
        return super().search()

    @abstractmethod
    def get(self, module_id: str) -> ModuleInfo:
        """Get module information by its unique identifier.

        Args:
            module_id: Unique module identifier.

        Returns:
            ModuleInfo: If module with the given ID is found in the registry.

        Raises:
             RegistryModuleNotFoundError: If module with the given ID is not found in the registry.
        """
        return super().get()

    # ════════════════════════════════ Abstracts Methods ═════════════════════════════════ #

    @abstractmethod
    def register(
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
    def heartbeat(self, module_id: str) -> ModuleStatus:
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
    def get_status(self, module_id: str) -> ModuleInfo:
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

    # ════════════════════════════ Unimplemented Methods ═════════════════════════════ #

    def create(self, *args: Any, **kwargs: Any) -> Any:
        return super().create()

    def list(self, *args: Any, **kwargs: Any) -> Any:
        return super().list()

    def delete(self, *args: Any, **kwargs: Any) -> Any:
        return super().delete()

    def update(self, *args: Any, **kwargs: Any) -> Any:
        return super().update()

    def upload(self, *args: Any, **kwargs: Any) -> Any:
        return super().upload()
