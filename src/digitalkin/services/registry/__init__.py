"""This module is responsible for handling the registry service."""

from digitalkin.models.services.registry import (
    ModuleInfo,
    ModuleStatusInfo,
    RegistryModuleStatus,
    RegistryModuleType,
)
from digitalkin.services.registry.default_registry import DefaultRegistry
from digitalkin.services.registry.exceptions import (
    RegistryModuleNotFoundError,
    RegistryServiceError,
)
from digitalkin.services.registry.grpc_registry import GrpcRegistry
from digitalkin.services.registry.registry_strategy import RegistryStrategy

__all__ = [
    "DefaultRegistry",
    "GrpcRegistry",
    "ModuleInfo",
    "ModuleStatusInfo",
    "RegistryModuleNotFoundError",
    "RegistryModuleStatus",
    "RegistryModuleType",
    "RegistryServiceError",
    "RegistryStrategy",
]
