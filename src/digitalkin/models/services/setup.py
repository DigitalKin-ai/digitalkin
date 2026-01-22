import datetime
from enum import Enum
from typing import Any

from agentic_mesh_protocol.registry.v1.registry_enums_pb2 import Visibility as VisibilityProto
from agentic_mesh_protocol.setup.v1.setup_enums_pb2 import SetupStatus as SetupStatusProto
from pydantic import BaseModel, Field

from digitalkin.models.base_enum import BaseEnum


class Visibility(BaseEnum[VisibilityProto], Enum):
    """Visibility in the registry."""

    UNSPECIFIED = "UNSPECIFIED"
    PUBLIC = "PUBLIC"
    PRIVATE = "PRIVATE"
    INTERNAL = "INTERNAL"


class SetupStatus(BaseEnum[SetupStatusProto], Enum):
    """Setup status in the registry."""

    UNSPECIFIED = "UNSPECIFIED"
    DRAFT = "DRAFT"
    WAITING_FOR_APPROVAL = "WAITING_FOR_APPROVAL"
    READY = "READY"
    PAUSED = "PAUSED"
    FAILED = "FAILED"
    ARCHIVED = "ARCHIVED"
    NEEDS_CONFIGURATION = "NEEDS_CONFIGURATION"
    CONFIGURATION_FAILED = "CONFIGURATION_FAILED"
    CONFIGURATION_SUCCEEDED = "CONFIGURATION_SUCCEEDED"


class SetupInfo(BaseModel):
    """Setup information from registry."""

    setup_id: str = Field(description="Unique identifier for the setup.")
    name: str = Field(description="Name of the setup.")
    documentation: str | None = Field(default=None, description="Documentation for the setup.")
    status: SetupStatus | None = Field(default=None, description="Current status of the setup.")
    visibility: Visibility | None = Field(default=None, description="Visibility level of the setup.")
    organization_id: str | None = Field(
        default=None, description="Identifier for the organization that owns the setup."
    )
    owner_id: str | None = Field(default=None, description="Identifier for the owner of the setup.")
    card_id: str | None = Field(default=None, description="Identifier for the card associated with the setup.")
    module_id: str | None = Field(default=None, description="Identifier for the module associated with the setup.")
    setup_version_id: str | None = Field(default=None, description="Identifier for the setup version.")
    setup_version: str | None = Field(default=None, description="Version of the setup.")
    config: dict[str, Any] | None = Field(default=None, description="Configuration for the setup.")


class SetupVersionData(BaseModel):
    """Pydantic model for SetupVersion data validation."""

    id: str = Field(description="Unique identifier for the setup version.")
    setup_id: str = Field(description="Identifier for the setup associated with this version.")
    version: str = Field(description="Version string for the setup version.")
    content: dict[str, Any] = Field(description="Content/configuration for the setup version.")
    created_at: datetime.datetime = Field(description="Timestamp when the setup version was created.")


class SetupData(BaseModel):
    """Pydantic model for Setup data validation."""

    id: str = Field(description="Unique identifier for the setup.")
    name: str = Field(description="Name of the setup.")
    organization_id: str = Field(description="Identifier for the organization that owns the setup.")
    owner_id: str = Field(description="Identifier for the owner of the setup.")
    module_id: str = Field(description="Identifier for the module associated with the setup.")
    current_setup_version: SetupVersionData = Field(description="Current version of the setup.")
