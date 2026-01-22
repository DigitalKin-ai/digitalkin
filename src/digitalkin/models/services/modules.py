"""Registry data models.

This module contains Pydantic models for registry service data structures.
"""

from enum import Enum

from agentic_mesh_protocol.module.v1.module_enums_pb2 import ModuleStatus as ModuleStatusProto
from agentic_mesh_protocol.module.v1.module_enums_pb2 import ModuleType as ModuleTypeProto
from pydantic import BaseModel, Field

from digitalkin.models.base_enum import BaseEnum


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

    id: str = Field(description="Unique identifier for the module.")
    type: ModuleType = Field(default=ModuleType.UNSPECIFIED, description="Type of the module.")
    address: str = Field(default="", description="Address of the module.")
    port: int = Field(default=0, description="Port number of the module.")
    version: str = Field(default="", description="Version of the module.")
    name: str = Field(default="", description="Name of the module.")
    documentation: str | None = Field(default=None, description="Documentation for the module.")
    status: ModuleStatus | None = Field(default=None, description="Current status of the module.")
