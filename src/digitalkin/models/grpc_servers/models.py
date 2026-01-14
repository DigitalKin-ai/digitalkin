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
        channel_options: Additional channel options
        retry_policy: Retry policy for failed RPCs
    """

    credentials: ClientCredentials | None = Field(None, description="Client credentials for secure mode")
    retry_policy: RetryPolicy = Field(default_factory=lambda: RetryPolicy(), description="Retry policy for failed RPCs")  # noqa: PLW0108
    channel_options: list[tuple[str, Any]] = Field(
        default_factory=lambda: [
            ("grpc.max_receive_message_length", 100 * 1024 * 1024),
            ("grpc.max_send_message_length", 100 * 1024 * 1024),
            # === DNS Re-resolution (Critical for Container Environments) ===
            # Minimum milliseconds between DNS re-resolution attempts (500 ms)
            # When connection fails, gRPC will re-query DNS after this interval
            # Solves: Container restarts with new IPs causing "No route to host"
            ("grpc.dns_min_time_between_resolutions_ms", 500),
            # Initial delay before first reconnection attempt (1 second)
            ("grpc.initial_reconnect_backoff_ms", 1000),
            # Maximum delay between reconnection attempts (10 seconds)
            # Prevents overwhelming the network during extended outages
            ("grpc.max_reconnect_backoff_ms", 10000),
            # Minimum delay between reconnection attempts (500ms)
            # Ensures rapid recovery for brief network glitches
            ("grpc.min_reconnect_backoff_ms", 500),
            # === Keepalive Settings (Detect Dead Connections) ===
            # Send keepalive ping every 60 seconds when connection is idle
            # Proactively detects dead connections before RPC calls fail
            ("grpc.keepalive_time_ms", 60000),
            # Wait 20 seconds for keepalive response before declaring connection dead
            # Triggers reconnection (with DNS re-resolution) if pong not received
            ("grpc.keepalive_timeout_ms", 20000),
            # Send keepalive pings even when no RPCs are in flight
            # Essential for long-lived connections that may sit idle
            ("grpc.keepalive_permit_without_calls", True),
            # Minimum interval between HTTP/2 pings (30 seconds)
            # Must be >= server's grpc.http2.min_ping_interval_without_data_ms (10s)
            ("grpc.http2.min_time_between_pings_ms", 30000),
            # === Retry Configuration ===
            # Enable automatic retry for failed RPCs (1 = enabled)
            # Works with retryable status codes: UNAVAILABLE, RESOURCE_EXHAUSTED
            ("grpc.enable_retries", 1),
        ],
        description="Resilient gRPC channel options with DNS re-resolution, keepalive, and retries",
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
    """

    max_workers: int = Field(10, description="Maximum number of workers for sync mode")
    credentials: ServerCredentials | None = Field(None, description="Server credentials for secure mode")
    server_options: list[tuple[str, Any]] = Field(
        default_factory=lambda: [
            ("grpc.max_receive_message_length", 100 * 1024 * 1024),
            ("grpc.max_send_message_length", 100 * 1024 * 1024),
            # === Keepalive Permission (Required for Client Keepalive) ===
            # Allow clients to send keepalive pings without active RPCs
            # Without this, server rejects client keepalives with GOAWAY
            ("grpc.keepalive_permit_without_calls", True),
            # Minimum interval server allows between client pings (10 seconds)
            # Prevents "too_many_pings" GOAWAY errors
            # Must match or be less than client's http2.min_time_between_pings_ms
            ("grpc.http2.min_ping_interval_without_data_ms", 10000),
        ],
        description="gRPC server options with keepalive support",
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
        registry_address: Address of the registry server
    """

    registry_address: str = Field(..., description="Address of the registry server")


class RegistryServerConfig(ServerConfig):
    """Configuration for Registry gRPC server.

    Attributes:
        database_url: Database URL for registry data storage
    """

    database_url: str | None = Field(None, description="Database URL for registry data storage")
