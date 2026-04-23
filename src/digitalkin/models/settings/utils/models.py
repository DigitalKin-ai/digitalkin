"""Models for settings."""

from enum import Enum
from pathlib import Path
from typing import Any

import grpc
from pydantic import BaseModel, ConfigDict, Field, field_validator

from digitalkin.grpc_servers.utils.exceptions import SecurityError


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
