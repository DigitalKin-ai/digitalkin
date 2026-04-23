"""Data models for gRPC server configurations."""

from typing import Any

from pydantic import BaseModel, Field, field_validator

from digitalkin.grpc_servers.utils.exceptions import ConfigurationError
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode


class ChannelConfig(BaseModel):
    """Base configuration for gRPC channels.

    Attributes:
        host: Host address
        port: Port to listen on
        mode: communication operation mode (sync/async)
        security: Security mode (secure/insecure)
        credentials: Client credentials for secure mode
    """

    host: str = Field(
        "0.0.0.0",  # noqa: S104
        description="Host address to bind the client to",
    )  # Bind to all interfaces by design
    port: int = Field(50051, description="Port to listen on")
    mode: ControlFlow = Field(ControlFlow.SYNC, description="Client operation mode (sync/async)")
    security: SecurityMode = Field(SecurityMode.INSECURE, description="Security mode (secure/insecure)")

    # Enable __slots__ for memory efficiency
    model_config = {
        "extra": "forbid",
        "arbitrary_types_allowed": True,
        "validate_assignment": True,
    }

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        """Validate that the port is in a valid range.

        Args:
            v: Port number to validate

        Returns:
            The validated port number

        Raises:
            ConfigurationError: If port is outside valid range
        """
        if not 0 < v < 65536:  # TCP port range constant # noqa: PLR2004
            msg = f"Port must be between 1 and 65535, got {v}"
            raise ConfigurationError(msg)
        return v

    @property
    def address(self) -> str:
        """Get the server address.

        Returns:
            The formatted address string
        """
        return f"{self.host}:{self.port}"


class ClientConfig(ChannelConfig):
    """Base configuration for gRPC clients.

    Attributes:
        host: Host address to bind the client to
        port: Port to listen on
        mode: Client operation mode (sync/async)
        security: Security mode (secure/insecure)
    """

    def __init__(self, /, **data: Any) -> None:
        """Client config constructor."""
        super().__init__(**data)
