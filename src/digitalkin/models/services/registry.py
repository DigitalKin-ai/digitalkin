"""Registry data models."""

from enum import Enum

from pydantic import BaseModel


class RegistryModuleStatus(str, Enum):
    """Module status in the registry."""

    UNSPECIFIED = "unspecified"
    READY = "ready"
    ACTIVE = "active"
    ARCHIVED = "archived"


class RegistryModuleType(str, Enum):
    """Module type in the registry."""

    UNSPECIFIED = "unspecified"
    ARCHETYPE = "archetype"
    TOOL = "tool"


class ModuleInfo(BaseModel):
    """Module information from registry."""

    module_id: str
    module_type: RegistryModuleType
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
