"""Registry data models.

This module contains Pydantic models for registry service data structures.
"""

from enum import Enum

from agentic_mesh_protocol.module.v1.module_enums_pb2 import ModuleStatus as ModuleStatusProto
from agentic_mesh_protocol.module.v1.module_enums_pb2 import ModuleType as ModuleTypeProto
from pydantic import BaseModel

from digitalkin.services.base_enum import BaseEnum


class ModuleStatus(BaseEnum[ModuleStatusProto], Enum):
    """Module status in the registry."""

    UNSPECIFIED = "UNSPECIFIED"
    READY = "READY"
    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"


class ModuleType(BaseEnum[ModuleTypeProto], Enum):
    """Module type in the registry."""

    UNSPECIFIED = "UNSPECIFIED"
    ARCHETYPE = "ARCHETYPE"
    TOOL = "TOOL"


class ModuleInfo(BaseModel):
    """Module information from registry."""

    id: str
    type: ModuleType = ModuleType.UNSPECIFIED
    address: str = ""
    port: int = 0
    version: str = ""
    name: str = ""
    documentation: str | None = None
    status: ModuleStatus | None
