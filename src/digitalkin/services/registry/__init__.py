"""This module is responsible for handling the registry service."""

from digitalkin.exception.registry import (
    RegistryModuleNotFoundError,
    RegistryServiceError,
)
from digitalkin.models.services.modules import ModuleInfo, ModuleStatus, ModuleType
from digitalkin.services.registry.registry_default import DefaultRegistry
from digitalkin.services.registry.registry_grpc import GrpcRegistry
from digitalkin.services.registry.registry_strategy import RegistryStrategy

__all__ = [
    "DefaultRegistry",
    "GrpcRegistry",
    "ModuleInfo",
    "ModuleStatus",
    "ModuleType",
    "RegistryModuleNotFoundError",
    "RegistryServiceError",
    "RegistryStrategy",
]
