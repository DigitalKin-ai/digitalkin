"""gRPC client settings for the SDK."""

from typing import Any

from pydantic import Field, NonNegativeInt
from pydantic_settings import BaseSettings, SettingsConfigDict

from digitalkin.models.settings.utils.channel import GrpcCompression


class GrpcClientRetrySettings(BaseSettings):
    """gRPC service-config retry policy (channel-level).

    Attributes:
        max_attempts (int): Max retry attempts including the original call.
        initial_backoff (str): Initial backoff duration (e.g. '0.1s').
        max_backoff (str): Maximum backoff duration (e.g. '10s').
        backoff_multiplier (float): Exponential backoff multiplier.

    """

    model_config = SettingsConfigDict(
        env_prefix="CLIENT_GRPC_RETRY_",
        extra="forbid",
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

    max_attempts: int = Field(default=5, ge=1, le=10, description="Max retry attempts including the original call.")
    initial_backoff: str = Field(default="0.1s", description="Initial backoff duration (e.g. '0.1s').")
    max_backoff: str = Field(default="10s", description="Maximum backoff duration (e.g. '10s').")
    backoff_multiplier: float = Field(default=2.0, ge=1.0, description="Exponential backoff multiplier.")


class GrpcClientSettings(BaseSettings):
    """gRPC tuning settings on the SDK client side.

    Attributes:
        compression (GrpcCompression): gRPC compression algorithm to use for client requests.
        keepalive_time (NonNegativeInt): Interval for client keepalive pings, in milliseconds.
        keepalive_timeout (NonNegativeInt): Timeout for client keepalive pings, in milliseconds.
        min_ping_interval (NonNegativeInt): Minimum interval between HTTP/2 pings on the client side, in milliseconds.
        max_receive_message_length (NonNegativeInt): Maximum message size the client can receive, in bytes.
        max_send_message_length (NonNegativeInt): Maximum message size the client can send, in bytes.
        keepalive_permit_without_calls (bool): Send keepalive pings even when there are no active RPCs.
        dns_resolution_ms (NonNegativeInt): Minimum interval between DNS re-resolutions, in milliseconds.
        initial_reconnect_ms (NonNegativeInt): Initial reconnect backoff, in milliseconds.
        max_reconnect_ms (NonNegativeInt): Maximum reconnect backoff, in milliseconds.
        min_reconnect_ms (NonNegativeInt): Minimum reconnect backoff, in milliseconds.
        enable_retries (bool): Enable the gRPC C++ retry layer on top of the Python-level retry loop.

    """

    model_config = SettingsConfigDict(
        env_prefix="CLIENT_GRPC_",
        extra="forbid",
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

    compression: GrpcCompression = Field(
        default=GrpcCompression.GZIP,
        description="gRPC compression algorithm",
    )

    keepalive_time: NonNegativeInt = Field(
        default=15000,
        description="Interval for client keepalive pings.",
        alias="CLIENT_GRPC_OPTIONS_KEEPALIVE_TIME",
    )
    keepalive_timeout: NonNegativeInt = Field(
        default=5000,
        description="Timeout for client keepalive pings.",
        alias="CLIENT_GRPC_OPTIONS_KEEPALIVE_TIMEOUT",
    )
    min_ping_interval: NonNegativeInt = Field(
        default=10000,
        description="Minimum interval between HTTP/2 pings on the client side. Must be >= the server minimum.",
        alias="CLIENT_GRPC_OPTIONS_MIN_PING_INTERVAL",
    )
    max_receive_message_length: NonNegativeInt = Field(
        default=100 * 1024 * 1024,
        description="Maximum message size the client can receive, in bytes.",
        alias="CLIENT_GRPC_OPTIONS_MAX_RECEIVE_MESSAGE_LENGTH",
    )
    max_send_message_length: NonNegativeInt = Field(
        default=100 * 1024 * 1024,
        description="Maximum message size the client can send, in bytes.",
        alias="CLIENT_GRPC_OPTIONS_MAX_SEND_MESSAGE_LENGTH",
    )
    keepalive_permit_without_calls: bool = Field(
        default=True,
        description="Send keepalive pings even when there are no active RPCs. "
        "This is important for keeping connections "
        "alive through proxies and detecting dead servers.",
        alias="CLIENT_GRPC_OPTIONS_KEEPALIVE_PERMIT_WITHOUT_CALLS",
    )
    dns_resolution_ms: NonNegativeInt = Field(
        default=500,
        description="Minimum interval between DNS re-resolutions. Critical when services restart with new IPs.",
        alias="CLIENT_GRPC_OPTIONS_DNS_RESOLUTION_MS",
    )
    initial_reconnect_ms: NonNegativeInt = Field(
        default=1000,
        description="Initial reconnect backoff.",
        alias="CLIENT_GRPC_OPTIONS_INITIAL_RECONNECT_MS",
    )
    max_reconnect_ms: NonNegativeInt = Field(
        default=10000,
        description="Maximum reconnect backoff.",
        alias="CLIENT_GRPC_OPTIONS_MAX_RECONNECT_MS",
    )
    min_reconnect_ms: NonNegativeInt = Field(
        default=500,
        description="Minimum reconnect backoff.",
        alias="CLIENT_GRPC_OPTIONS_MIN_RECONNECT_MS",
    )
    enable_retries: bool = Field(
        default=False,
        description="Enable the gRPC C++ retry layer. Disabled by default: the per-channel service_config retry "
        "policy would otherwise stack on top of the Python-level retry loop in `exec_grpc_query`. "
        "On a cold registry channel returning UNAVAILABLE for multiple tool refs, the C++ retries "
        "caused an 8 s wall-clock gap (see monitoring/reports/sdk-8sec-gap-rootcause.md). The Python "
        "loop already covers the same retryable status codes (UNAVAILABLE, INTERNAL, "
        "DEADLINE_EXCEEDED) with a more conservative budget.",
        alias="CLIENT_GRPC_OPTIONS_ENABLE_RETRIES",
    )

    @property
    def options(self) -> list[tuple[str, Any]]:
        """Convert settings to gRPC channel options format.

        Returns:
            List of tuples containing gRPC channel options and their corresponding values.
        """
        return [
            ("grpc.max_receive_message_length", self.max_receive_message_length),
            ("grpc.max_send_message_length", self.max_send_message_length),
            ("grpc.dns_min_time_between_resolutions_ms", self.dns_resolution_ms),
            ("grpc.initial_reconnect_backoff_ms", self.initial_reconnect_ms),
            ("grpc.max_reconnect_backoff_ms", self.max_reconnect_ms),
            ("grpc.min_reconnect_backoff_ms", self.min_reconnect_ms),
            ("grpc.keepalive_time_ms", self.keepalive_time),
            ("grpc.keepalive_timeout_ms", self.keepalive_timeout),
            ("grpc.keepalive_permit_without_calls", self.keepalive_permit_without_calls),
            ("grpc.http2.min_time_between_pings_ms", self.min_ping_interval),
            ("grpc.enable_retries", int(self.enable_retries)),
        ]
