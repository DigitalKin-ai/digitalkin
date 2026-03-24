from typing import Literal

from pydantic import BaseModel, Field


class ServiceHealthStatus(BaseModel):
    """Health status of a single service."""

    name: str = Field(..., description="Name of the service")
    status: Literal["healthy", "unhealthy", "unknown"] = Field(
        ...,
        description="Health status of the service",
    )
    message: str | None = Field(
        default=None,
        description="Optional message about the service status",
    )


class DefaultChatHistory(BaseModel):
    """Storage model for chat history persistence."""

    messages: list[str] = Field(
        default_factory=list,
        description="List of messages in the chat history.",
    )
