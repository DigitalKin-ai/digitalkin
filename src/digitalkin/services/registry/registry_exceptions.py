"""Registry-specific exceptions.

This module contains custom exceptions for registry service operations.
"""


class RegistryServiceError(Exception):
    """Base exception for registry service errors."""


class RegistryModuleNotFoundError(RegistryServiceError):
    """Raised when a module is not found in the registry."""

    def __init__(self, module_id: str) -> None:
        """Initialize the exception.

        Args:
            module_id: The ID of the module that was not found.
        """
        self.module_id = module_id
        super().__init__(f"Module '{module_id}' not found in registry")


class ModuleAlreadyExistsError(RegistryServiceError):
    """Raised when attempting to register an already-registered module."""

    def __init__(self, module_id: str) -> None:
        """Initialize the exception.

        Args:
            module_id: The ID of the module that already exists.
        """
        self.module_id = module_id
        super().__init__(f"Module '{module_id}' already registered")


class InvalidStatusError(RegistryServiceError):
    """Raised when an invalid status is provided."""

    def __init__(self, status: int) -> None:
        """Initialize the exception.

        Args:
            status: The invalid status value.
        """
        self.status = status
        super().__init__(f"Invalid module status: {status}")
