"""This file define channelBase for grpc config."""

from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from digitalkin.grpc_servers.exceptions import ConfigurationError, SecurityError


class ControlFlow(str, Enum):
    """Enum for server operation mode."""

    SYNC = "sync"
    ASYNC = "async"


class SecurityMode(str, Enum):
    """Enum for server security mode."""

    SECURE = "secure"
    INSECURE = "insecure"


class Credentials(BaseModel):
    """Model for server credentials in secure mode.

    Attributes:
        key_path: Path to the server private key
        cert_path: Path to the server certificate
        root_cert_path: Optional path to the root certificate
    """

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True, validate_assignment=True, frozen=True)

    key_path: Path | None = Field(default=None, description="Path to the private key")
    cert_path: Path | None = Field(default=None, description="Path to the certificate")
    root_cert_path: Path | None = Field(default=None, description="Path to the root certificate")

    def __init__(self, /, **data: Any) -> None:
        """Initialize the Credentials model."""
        super().__init__(**data)

    @field_validator("key_path", "cert_path", "root_cert_path")
    @classmethod
    def check_path_exists(cls, v: Path | None) -> Path | None:
        """Validate that the file path exists.

        Args:
            v: Path to validate

        Returns:
            The validated path

        Raises:
            SecurityError: If the path does not exist
        """
        if v is not None and not v.exists():
            msg = f"File not found: {v}"
            raise SecurityError(msg)
        return v


class BaseChannelSettings(BaseSettings):
    """Base settings model for gRPC channel configuration."""

    model_config = SettingsConfigDict(extra="forbid", arbitrary_types_allowed=True, validate_assignment=True)

    host: str = Field("[::]", description="Host address to bind the client to")
    port: NonNegativeInt = Field(50055, description="Port to listen on")
    communication_mode: ControlFlow = Field(ControlFlow.ASYNC, description="Client/Server operation mode (sync/async)")
    credentials: Credentials | None = Field(None, description="Client credentials for secure mode")
    security: SecurityMode = Field(SecurityMode.INSECURE, description="Security mode (secure/insecure)")
    mtls: bool = Field(default=False, description="Enable mutual TLS")

    def __init__(self, **values: Any) -> None:
        """Initialize the BaseChannelSettings model."""
        super().__init__(**values)

    @property
    def address(self) -> str:
        """Get the server address.

        Returns:
            The formatted address string
        """
        return f"{self.host}:{self.port}"

    @model_validator(mode="after")
    def validate_credentials(self) -> "BaseChannelSettings":
        """Validate that credentials are provided when in secure mode.

        Returns:
            The validated credentials

        Raises:
            ConfigurationError: If credentials are missing in secure mode
        """
        # Access security mode from the info.data dictionary
        if self.security == SecurityMode.SECURE and self.credentials is None:
            msg = "Credentials must be provided when using secure mode"
            raise ConfigurationError(msg)
        return self

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

    @field_validator("communication_mode", mode="before")
    @classmethod
    def _normalize_mode(cls, v: str | ControlFlow) -> ControlFlow:
        """Normalize mode value.

        Returns:
            The normalized ServerMode.
        """
        if isinstance(v, ControlFlow):
            return v
        return ControlFlow.SYNC if str(v).lower() == "sync" else ControlFlow.ASYNC

    @field_validator("security", mode="before")
    @classmethod
    def _normalize_security(cls, v: str | SecurityMode) -> SecurityMode:
        """Normalize security value.

        Returns:
            The normalized SecurityMode.
        """
        if isinstance(v, SecurityMode):
            return v
        return SecurityMode.SECURE if str(v).lower() == "secure" else SecurityMode.INSECURE
