"""Abstract base class for registry strategies."""

from abc import ABC, abstractmethod
from typing import Any

from digitalkin.models.services.registry import (
    ModuleInfo,
    ModuleStatusInfo,
    RegistryModuleStatus,
)
from digitalkin.services.base_strategy import BaseStrategy


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
    def discover_by_id(self, module_id: str) -> ModuleInfo:
        """Get module info by ID."""
        raise NotImplementedError

    @abstractmethod
    def search(
        self,
        name: str | None = None,
        module_type: str | None = None,
        organization_id: str | None = None,
    ) -> list[ModuleInfo]:
        """Search for modules by criteria.

        Args:
            name: Filter by name (partial match via query).
            module_type: Filter by type (archetype, tool).
            organization_id: Filter by organization.

        Returns:
            List of matching modules.
        """
        raise NotImplementedError

    @abstractmethod
    def get_status(self, module_id: str) -> ModuleStatusInfo:
        """Get module status."""
        raise NotImplementedError

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
            ModuleInfo if successful, None otherwise.
        """
        raise NotImplementedError

    @abstractmethod
    def heartbeat(self, module_id: str) -> RegistryModuleStatus:
        """Send heartbeat to keep module active.

        Args:
            module_id: The module identifier.

        Returns:
            Current module status after heartbeat.

        Raises:
            RegistryModuleNotFoundError: If module not found.
        """
        raise NotImplementedError
