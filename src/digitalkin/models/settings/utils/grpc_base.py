"""Shared gRPC settings models."""

from typing import Any

from pydantic import Field, NonNegativeInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseGrpcSettings(BaseSettings):
    """Base settings shared by client and server gRPC configurations."""

    model_config = SettingsConfigDict(extra="forbid", arbitrary_types_allowed=True, validate_assignment=True)

    # ── Options ───────────────────────────────────────────────────────────────────── #

    max_receive_message_lenght: NonNegativeInt = Field(
        default=100 * 1024 * 1024, description="Maximum message size the server can receive, in bytes."
    )
    max_send_message_length: NonNegativeInt = Field(
        default=100 * 1024 * 1024, description="Maximum message size the server can send, in bytes."
    )
    keepalive_time: NonNegativeInt = Field(default=120000, description="Interval for server keepalive pings.")
    keepalive_timeout: NonNegativeInt = Field(default=20000, description="Timeout for server keepalive pings.")
    keepalive_permit_without_calls: bool = Field(
        default=True,
        description="Allow clients to send keepalive pings even when there are no active RPCs. "
        "This is important for keeping connections "
        "alive through proxies and detecting dead clients.",
    )

    # ── Functions ─────────────────────────────────────────────────────────────────── #

    def __init__(self, **values: Any) -> None:
        """Default constructor."""
        super().__init__(**values)

    @property
    def options(self) -> list[tuple[str, Any]]:
        """Convert settings to gRPC options format.

        Returns:
            List of tuples containing gRPC options and their corresponding values.
        """
        return [
            ("grpc.max_receive_message_length", self.max_receive_message_lenght),
            ("grpc.max_send_message_length", self.max_send_message_length),
            ("grpc.keepalive_time_ms", self.keepalive_time),
            ("grpc.keepalive_timeout_ms", self.keepalive_timeout),
            ("grpc.keepalive_permit_without_calls", self.keepalive_permit_without_calls),
            *self._specific_options,
        ]

    @property
    def _specific_options(self) -> list[tuple[str, Any]]:
        """Return settings specific to a gRPC side.

        Returns:
            List of tuples containing side-specific gRPC options.
        """
        return []
