"""Registry data models.

This module contains Pydantic models for registry service data structures.
"""

from enum import IntEnum

from pydantic import BaseModel


class RegistryModuleStatus(IntEnum):
    """Module status in the registry.

    Maps to proto ModuleStatus enum values.
    """

    UNKNOWN = 0
    READY = 1
    ACTIVE = 2
    OFFLINE = 3


class ModuleInfo(BaseModel):
    """Complete module information from registry.

    Maps to proto ModuleDescriptor message.
    """

    module_id: str
    module_type: str
    address: str
    port: int
    version: str
    name: str = ""
    documentation: str | None = None
    status: RegistryModuleStatus | None = None


class ModuleStatusInfo(BaseModel):
    """Module status response."""

    module_id: str
    status: RegistryModuleStatus
