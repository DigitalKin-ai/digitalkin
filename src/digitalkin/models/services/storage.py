"""Storage model."""

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


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
