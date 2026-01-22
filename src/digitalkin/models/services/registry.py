"""Registry data models."""

from enum import Enum
from typing import Any

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

    module_id: str = ""
    module_type: RegistryModuleType = RegistryModuleType.UNSPECIFIED
    address: str = ""
    port: int = 0
    version: str = ""
    module_name: str = ""
    documentation: str | None = None
    status: RegistryModuleStatus | None = None


class RegistrySetupStatus(str, Enum):
    """Setup status in the registry."""

    UNSPECIFIED = "unspecified"
    DRAFT = "draft"
    WAITING_FOR_APPROVAL = "waiting_for_approval"
    READY = "ready"
    PAUSED = "paused"
    FAILED = "failed"
    ARCHIVED = "archived"
    NEEDS_CONFIGURATION = "needs_configuration"
    CONFIGURATION_FAILED = "configuration_failed"
    CONFIGURATION_SUCCEEDED = "configuration_succeeded"


class RegistryVisibility(str, Enum):
    """Visibility in the registry."""

    UNSPECIFIED = "unspecified"
    PUBLIC = "public"
    PRIVATE = "private"
    INTERNAL = "internal"


class SetupInfo(BaseModel):
    """Setup information from registry."""

    setup_id: str
    name: str
    documentation: str | None = None
    status: RegistrySetupStatus | None = None
    visibility: RegistryVisibility | None = None
    organization_id: str | None = None
    owner_id: str | None = None
    card_id: str | None = None
    module_id: str | None = None
    setup_version_id: str | None = None
    setup_version: str | None = None
    config: dict[str, Any] | None = None
