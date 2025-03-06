"""This module is responsible for handling the registry service."""

from .default_registry import DefaultRegistry
from .registry_strategy import RegistryStrategy

__all__ = ["DefaultRegistry", "RegistryStrategy"]
