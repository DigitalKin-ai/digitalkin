"""This module is responsible for handling the registry service."""

from digitalkin.services.registry.default_registry import DefaultRegistry
from digitalkin.services.registry.registry_strategy import RegistryStrategy

__all__ = ["DefaultRegistry", "RegistryStrategy"]
