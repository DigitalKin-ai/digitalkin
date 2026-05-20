"""Data models for gRPC server configurations."""

from enum import Enum
from pathlib import Path
from typing import Any

import grpc
from pydantic import BaseModel, Field, ValidationInfo, field_validator

from digitalkin.grpc_servers.exceptions import ConfigurationError, SecurityError
from digitalkin.models.settings.grpc_client import GrpcChannelSettings, GrpcRetrySettings
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode


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


class RetryPolicy(BaseModel):
    """gRPC retry policy configuration for resilient connections.

    Attributes:
        max_attempts: Maximum retry attempts including the original call
        initial_backoff: Initial backoff duration (e.g., "0.1s")
        max_backoff: Maximum backoff duration (e.g., "10s")
        backoff_multiplier: Multiplier for exponential backoff
        retryable_status_codes: gRPC status codes that trigger retry
    """

    max_attempts: int = Field(
        default=5,
        ge=1,
        le=10,
        description="Maximum retry attempts including the original call",
    )
    initial_backoff: str = Field(
        default="0.1s",
        description="Initial backoff duration (e.g., '0.1s')",
    )
    max_backoff: str = Field(
        default="10s",
        description="Maximum backoff duration (e.g., '10s')",
    )
    backoff_multiplier: float = Field(
        default=2.0,
        ge=1.0,
        description="Multiplier for exponential backoff",
    )
    retryable_status_codes: list[str] = Field(
        default_factory=lambda: ["UNAVAILABLE", "RESOURCE_EXHAUSTED", "DEADLINE_EXCEEDED"],
        description="gRPC status codes that trigger retry",
    )

    model_config = {"extra": "forbid", "frozen": True}

    @classmethod
    def from_settings(cls) -> "RetryPolicy":
        """Build a retry policy with backoff values sourced from the environment.

        Returns:
            Retry policy populated from ``GrpcRetrySettings``.
        """
        settings = GrpcRetrySettings()
        return cls(
            max_attempts=settings.max_attempts,
            initial_backoff=settings.initial_backoff,
            max_backoff=settings.max_backoff,
            backoff_multiplier=settings.backoff_multiplier,
        )

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
        credentials: Client credentials for secure mode
        channel_options: Additional channel options
        retry_policy: Retry policy for failed RPCs
        compression: gRPC compression algorithm for channel-level compression
    """

    credentials: ClientCredentials | None = Field(None, description="Client credentials for secure mode")
    retry_policy: RetryPolicy = Field(
        default_factory=RetryPolicy.from_settings, description="Retry policy for failed RPCs"
    )
    compression: GrpcCompression = Field(GrpcCompression.GZIP, description="gRPC compression algorithm")
    channel_options: list[tuple[str, Any]] = Field(
        default_factory=lambda: GrpcChannelSettings().to_channel_options(),
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
