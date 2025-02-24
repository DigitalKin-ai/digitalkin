"""Data models for gRPC server configurations."""

from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, field_validator


class ServerMode(str, Enum):
    """Enum for server operation mode."""

    SYNC = "sync"
    ASYNC = "async"


class SecurityMode(str, Enum):
    """Enum for server security mode."""

    SECURE = "secure"
    INSECURE = "insecure"


class ServerCredentials(BaseModel):
    """Model for server credentials in secure mode."""

    server_key_path: Path = Field(..., description="Path to the server private key")
    server_cert_path: Path = Field(..., description="Path to the server certificate")
    root_cert_path: Path | None = Field(None, description="Path to the root certificate")

    @field_validator("server_key_path", "server_cert_path", "root_cert_path")
    @classmethod
    def check_path_exists(cls, v: Path | None) -> Path | None:
        """Validate that the file path exists.

        Args:
            v: Path to validate

        Returns:
            The validated path

        Raises:
            ValueError: If the path does not exist
        """
        if v is not None and not v.exists():
            raise ValueError(f"File not found: {v}")
        return v


class ServerConfig(BaseModel):
    """Base configuration for gRPC servers."""

    host: str = Field("0.0.0.0", description="Host address to bind the server to")  # noqa: S104
    port: int = Field(50051, description="Port to listen on")
    max_workers: int = Field(10, description="Maximum number of workers for sync mode")
    mode: ServerMode = Field(ServerMode.SYNC, description="Server operation mode (sync/async)")
    security: SecurityMode = Field(SecurityMode.INSECURE, description="Security mode (secure/insecure)")
    credentials: ServerCredentials | None = Field(None, description="Server credentials for secure mode")
    server_options: list[tuple[str, Any]] = Field(default_factory=list, description="Additional server options")

    @field_validator("credentials")
    @classmethod
    def validate_credentials(cls, v: ServerCredentials | None, values: dict[str, Any]) -> ServerCredentials | None:
        """Validate that credentials are provided when in secure mode.

        Args:
            v: The credentials value
            values: All other field values

        Returns:
            The validated credentials

        Raises:
            ValueError: If credentials are missing in secure mode
        """
        if values.get("security") == SecurityMode.SECURE and v is None:
            raise ValueError("Credentials must be provided when using secure mode")
        return v

    @property
    def address(self) -> str:
        """Get the server address.

        Returns:
            The formatted address string
        """
        return f"{self.host}:{self.port}"


class ModuleServerConfig(ServerConfig):
    """Configuration for Module gRPC server."""

    registry_address: str | None = Field(None, description="Address of the registry server")


class RegistryServerConfig(ServerConfig):
    """Configuration for Registry gRPC server."""

    database_url: str | None = Field(None, description="Database URL for registry data storage")
