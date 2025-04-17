"""Data models for gRPC server configurations."""

from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from digitalkin.grpc_servers.utils.exceptions import ConfigurationError, SecurityError


class ServerMode(str, Enum):
    """Enum for server operation mode."""

    SYNC = "sync"
    ASYNC = "async"


class SecurityMode(str, Enum):
    """Enum for server security mode."""

    SECURE = "secure"
    INSECURE = "insecure"


class ServerCredentials(BaseModel):
    """Model for server credentials in secure mode.

    Attributes:
        server_key_path: Path to the server private key
        server_cert_path: Path to the server certificate
        root_cert_path: Optional path to the root certificate
    """

    server_key_path: Path = Field(..., description="Path to the server private key")
    server_cert_path: Path = Field(..., description="Path to the server certificate")
    root_cert_path: Path | None = Field(None, description="Path to the root certificate")

    # Enable __slots__ for memory efficiency
    model_config = {
        "extra": "forbid",
        "arbitrary_types_allowed": True,
        "validate_assignment": True,
        "use_enum_values": True,
        "frozen": True,  # Make immutable
    }

    @field_validator("server_key_path", "server_cert_path", "root_cert_path")
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


class ClientCredentials(BaseModel):
    """Model for client credentials in secure mode.

    Attributes:
        root_cert_path: path to the root certificate
        client_key_path: Path to the client private key
        client_cert_path: Path to the client certificate
    """

    root_cert_path: Path = Field(..., description="Path to the root certificate")
    client_key_path: Path | None = Field(None, description="Path to the client private key | mTLS enable")
    client_cert_path: Path | None = Field(None, description="Path to the client certificate | mTLS enable")

    # Enable __slots__ for memory efficiency
    model_config = {
        "extra": "forbid",
        "arbitrary_types_allowed": True,
        "validate_assignment": True,
        "use_enum_values": True,
        "frozen": True,  # Make immutable
    }

    @field_validator("client_key_path", "client_cert_path", "root_cert_path")
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


class ChannelConfig(BaseModel):
    """Base configuration for gRPC channels.

    Attributes:
        host: Host address
        port: Port to listen on
        mode: communication operation mode (sync/async)
        security: Security mode (secure/insecure)
        credentials: Client credentials for secure mode
    """

    host: str = Field("0.0.0.0", description="Host address to bind the client to")  # noqa: S104
    port: int = Field(50051, description="Port to listen on")
    mode: ServerMode = Field(ServerMode.SYNC, description="Client operation mode (sync/async)")
    security: SecurityMode = Field(SecurityMode.INSECURE, description="Security mode (secure/insecure)")

    # Enable __slots__ for memory efficiency
    model_config = {
        "extra": "forbid",
        "arbitrary_types_allowed": True,
        "validate_assignment": True,
        "use_enum_values": True,
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
        if not 0 < v < 65536:  # noqa: PLR2004
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
        credentials: Client credentials for secure mode
    """

    credentials: ClientCredentials | None = Field(None, description="Client credentials for secure mode")

    @field_validator("credentials")
    @classmethod
    def validate_credentials(cls, v: ClientCredentials | None, info: ValidationInfo) -> ClientCredentials | None:
        """Validate that credentials are provided when in secure mode.

        Args:
            v: The credentials value
            info: ValidationInfo containing other field values

        Returns:
            The validated credentials

        Raises:
            ConfigurationError: If credentials are missing in secure mode
        """
        # Access security mode from the info.data dictionary
        security = info.data.get("security")

        if security == SecurityMode.SECURE and v is None:
            msg = "Credentials must be provided when using secure mode"
            raise ConfigurationError(msg)
        return v


class ServerConfig(ChannelConfig):
    """Base configuration for gRPC servers.

    Attributes:
        host: Host address to bind the server to
        port: Port to listen on
        max_workers: Maximum number of workers for sync mode
        mode: Server operation mode (sync/async)
        security: Security mode (secure/insecure)
        credentials: Server credentials for secure mode
        server_options: Additional server options
        enable_reflection: Enable reflection for the server
    """

    max_workers: int = Field(10, description="Maximum number of workers for sync mode")
    credentials: ServerCredentials | None = Field(None, description="Server credentials for secure mode")
    server_options: list[tuple[str, Any]] = Field(default_factory=list, description="Additional server options")
    enable_reflection: bool = Field(default=True, description="Enable reflection for the server")
    enable_health_check: bool = Field(default=True, description="Enable health check service")

    @field_validator("credentials")
    @classmethod
    def validate_credentials(cls, v: ServerCredentials | None, info: ValidationInfo) -> ServerCredentials | None:
        """Validate that credentials are provided when in secure mode.

        Args:
            v: The credentials value
            info: ValidationInfo containing other field values

        Returns:
            The validated credentials

        Raises:
            ConfigurationError: If credentials are missing in secure mode
        """
        # Access security mode from the info.data dictionary
        security = info.data.get("security")

        if security == SecurityMode.SECURE and v is None:
            msg = "Credentials must be provided when using secure mode"
            raise ConfigurationError(msg)
        return v


class ModuleServerConfig(ServerConfig):
    """Configuration for Module gRPC server.

    Attributes:
        registry_address: Address of the registry server
    """

    registry_address: str | None = Field(None, description="Address of the registry server")


class RegistryServerConfig(ServerConfig):
    """Configuration for Registry gRPC server.

    Attributes:
        database_url: Database URL for registry data storage
    """

    database_url: str | None = Field(None, description="Database URL for registry data storage")
