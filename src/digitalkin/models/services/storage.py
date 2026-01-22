"""Storage model."""

import datetime
from enum import Enum
from typing import Any

from agentic_mesh_protocol.storage.v1.storage_enums_pb2 import DataType as DataTypeProto
from pydantic import BaseModel, Field

from digitalkin.models.base_enum import BaseEnum


class BaseRole(str, Enum):
    """Officially supported Role Enum for chat messages."""

    ASSISTANT = "assistant"
    USER = "user"
    SYSTEM = "system"


Role = BaseRole | str


class BaseMessage(BaseModel):
    """Base Model representing a simple message in the chat history."""

    role: Role = Field(..., description="Role of the message sender")
    content: Any = Field(..., description="The content of the message | preferably a BaseModel.")


class ChatHistory(BaseModel):
    """Storage chat history model for the OpenAI Archetype module."""

    messages: list[BaseMessage] = Field(..., description="List of messages in the chat history")


class FileModel(BaseModel):
    """File model."""

    file_id: str = Field(..., description="ID of the file")
    name: str = Field(..., description="Name of the file")
    metadata: dict[str, Any] = Field(..., description="Metadata of the file")


class FileHistory(BaseModel):
    """File history model."""

    files: list[FileModel] = Field(..., description="List of files")


class DataType(BaseEnum[DataTypeProto], Enum):
    """Enum defining the types of data that can be stored."""

    UNSPECIFIED = "UNSPECIFIED"
    OUTPUT = "OUTPUT"
    VIEW = "VIEW"
    LOGS = "LOGS"
    OTHER = "OTHER"


class StorageRecord(BaseModel):
    """A single record stored in a collection, with metadata."""

    mission_id: str = Field(..., description="ID of the mission (bucket) this doc belongs to")
    collection: str = Field(..., description="Logical collection name")
    record_id: str = Field(..., description="Unique ID of this record in its collection")
    data_type: DataType = Field(default=DataType.OUTPUT, description="Category of the data of this record")
    data: BaseModel = Field(..., description="The typed payload of this record")
    created_at: datetime.datetime | None = Field(default=None, description="When this record was first created")
    updated_at: datetime.datetime | None = Field(default=None, description="When this record was last modified")
