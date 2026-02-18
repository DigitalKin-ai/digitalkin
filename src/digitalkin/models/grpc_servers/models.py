"""Data models for gRPC server configurations."""

from enum import Enum
from pathlib import Path
from typing import Any

import grpc
from pydantic import BaseModel, Field, ValidationInfo, field_validator

from digitalkin.grpc_servers.utils.exceptions import ConfigurationError, SecurityError


class GrpcCompression(str, Enum):
    """gRPC compression algorithm.

    Attributes:
        NONE: No compression
        GZIP: Gzip compression
        DEFLATE: Deflate compression
    """

    NONE = "none"
    GZIP = "gzip"
    DEFLATE = "deflate"

    def to_grpc(self) -> grpc.Compression:
        """Convert to grpc.Compression enum.

        Returns:
            The corresponding grpc.Compression value.
        """
        match self:
            case GrpcCompression.NONE:
                return grpc.Compression.NoCompression
            case GrpcCompression.GZIP:
                return grpc.Compression.Gzip
            case GrpcCompression.DEFLATE:
                return grpc.Compression.Deflate


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
        "frozen": True,
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


class RetryPolicy(BaseModel):
    """gRPC retry policy configuration for resilient connections.

    Attributes:
        max_attempts: Maximum retry attempts including the original call
        initial_backoff: Initial backoff duration (e.g., "0.1s")
        max_backoff: Maximum backoff duration (e.g., "10s")
        backoff_multiplier: Multiplier for exponential backoff
        retryable_status_codes: gRPC status codes that trigger retry
    """

    max_attempts: int = Field(default=5, ge=1, le=10, description="Maximum retry attempts including the original call")
    initial_backoff: str = Field(default="0.1s", description="Initial backoff duration (e.g., '0.1s')")
    max_backoff: str = Field(default="10s", description="Maximum backoff duration (e.g., '10s')")
    backoff_multiplier: float = Field(default=2.0, ge=1.0, description="Multiplier for exponential backoff")
    retryable_status_codes: list[str] = Field(
        default_factory=lambda: ["UNAVAILABLE", "RESOURCE_EXHAUSTED"],
        description="gRPC status codes that trigger retry",
    )

    model_config = {"extra": "forbid", "frozen": True}

    def to_service_config_json(self) -> str:
        """Serialize to gRPC service config JSON string.

        Returns:
            JSON string for grpc.service_config channel option.
        """
        codes = "[" + ",".join(f'"{c}"' for c in self.retryable_status_codes) + "]"
        return (
            f'{{"methodConfig":[{{"name":[{{}}],"retryPolicy":{{"maxAttempts":{self.max_attempts},'
            f'"initialBackoff":"{self.initial_backoff}","maxBackoff":"{self.max_backoff}",'
            f'"backoffMultiplier":{self.backoff_multiplier},"retryableStatusCodes":{codes}}}}}]}}'
        )


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
        "frozen": True,
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

    host: str = Field(
        "0.0.0.0",  # noqa: S104
        description="Host address to bind the client to",
    )  # Bind to all interfaces by design
    port: int = Field(50051, description="Port to listen on")
    mode: ServerMode = Field(ServerMode.SYNC, description="Client operation mode (sync/async)")
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
        credentials: Client credentials for secure mode
        channel_options: Additional channel options
        retry_policy: Retry policy for failed RPCs
        compression: gRPC compression algorithm for channel-level compression
    """

    credentials: ClientCredentials | None = Field(None, description="Client credentials for secure mode")
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy, description="Retry policy for failed RPCs")
    compression: GrpcCompression = Field(GrpcCompression.GZIP, description="gRPC compression algorithm")
    channel_options: list[tuple[str, Any]] = Field(
        default_factory=lambda: [
            ("grpc.max_receive_message_length", 100 * 1024 * 1024),
            ("grpc.max_send_message_length", 100 * 1024 * 1024),
            # === DNS Re-resolution (Railway redeployments change IPs) ===
            ("grpc.dns_min_time_between_resolutions_ms", 500),
            ("grpc.initial_reconnect_backoff_ms", 500),
            ("grpc.max_reconnect_backoff_ms", 5000),
            ("grpc.min_reconnect_backoff_ms", 250),
            # === Keepalive (Keep streams alive through Railway proxy) ===
            # Both client and server ping at 30s — ensures continuous HTTP/2
            # frames even when stream produces no application data for minutes
            ("grpc.keepalive_time_ms", 30000),
            ("grpc.keepalive_timeout_ms", 10000),
            ("grpc.keepalive_permit_without_calls", True),
            ("grpc.http2.min_time_between_pings_ms", 20000),
            # === Retries ===
            ("grpc.enable_retries", 1),
        ],
        description="gRPC channel options optimized for Railway with long-lived streams",
    )

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

    @property
    def grpc_options(self) -> list[tuple[str, Any]]:
        """Get channel options with retry policy service config.

        Returns:
            Full list of gRPC channel options.
        """
        return [*self.channel_options, ("grpc.service_config", self.retry_policy.to_service_config_json())]


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
        compression: gRPC compression algorithm for server-level compression
    """

    max_workers: int = Field(10, description="Maximum number of workers for sync mode")
    credentials: ServerCredentials | None = Field(None, description="Server credentials for secure mode")
    compression: GrpcCompression = Field(GrpcCompression.GZIP, description="gRPC compression algorithm")
    server_options: list[tuple[str, Any]] = Field(
        default_factory=lambda: [
            ("grpc.max_receive_message_length", 100 * 1024 * 1024),
            ("grpc.max_send_message_length", 100 * 1024 * 1024),
            # === Server-Side Keepalive (Keep Railway proxy from dropping silent streams) ===
            # Server pings clients every 30s — keeps long-lived silent streams
            # alive through Railway's proxy which drops idle HTTP/2 connections
            ("grpc.keepalive_time_ms", 30000),
            # Wait 10s for pong before declaring connection dead
            ("grpc.keepalive_timeout_ms", 10000),
            # Send keepalive pings even when no RPCs are active
            ("grpc.keepalive_permit_without_calls", True),
            # Minimum client ping interval server tolerates (10s)
            ("grpc.http2.min_ping_interval_without_data_ms", 10000),
        ],
        description="gRPC server options with keepalive for long-lived streams",
    )
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
        advertise_host: Public hostname/IP sent to registry for discovery. Falls back to host if not set.
    """

    advertise_host: str | None = Field(
        None, description="Public hostname/IP sent to registry for discovery. Falls back to host if not set."
    )


class RegistryServerConfig(ServerConfig):
    """Configuration for Registry gRPC server.

    Attributes:
        database_url: Database URL for registry data storage
    """

    database_url: str | None = Field(None, description="Database URL for registry data storage")
