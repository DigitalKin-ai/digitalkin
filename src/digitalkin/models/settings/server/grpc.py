"""gRPC server settings for the SDK."""

from typing import Any

from pydantic import Field, NonNegativeInt
from pydantic_settings import BaseSettings, SettingsConfigDict

from digitalkin.models.grpc_servers.models import GrpcCompression


class GrpcServerSettings(BaseSettings):
    """gRPC tuning settings on the SDK side.

    Attributes:
        compression (GrpcCompression): gRPC compression algorithm to use for server responses.
        keepalive_time (NonNegativeInt): Interval for server keepalive pings, in milliseconds.
        keepalive_timeout (NonNegativeInt): Timeout for server keepalive pings, in milliseconds.
        min_ping_interval (NonNegativeInt): Minimum interval between HTTP/2 pings on the server side, in milliseconds.
        max_receive_message_lenght (NonNegativeInt): Maximum message size the server can receive, in bytes.
        max_send_message_length (NonNegativeInt): Maximum message size the server can send, in bytes.
        max_pings_without_data (NonNegativeInt): Maximum number of pings the server allows without receiving any data.
        keepalive_permit_without_calls (bool): Allow clients to send keepalive pings even when there are no active RPCs.

    """

    model_config = SettingsConfigDict(
        env_prefix="SERVER_GRPC_",
        extra="forbid",
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

    compression: GrpcCompression = Field(
        GrpcCompression.GZIP,
        description="gRPC compression algorithm",
    )

    # ── Options ───────────────────────────────────────────────────────────────────── #

    keepalive_time: NonNegativeInt = Field(
        120000,
        description="Interval for server keepalive pings.",
        alias="SERVER_GRPC_OPTIONS_KEEPALIVE_TIME",
    )
    keepalive_timeout: NonNegativeInt = Field(
        20000,
        description="Timeout for server keepalive pings.",
        alias="SERVER_GRPC_OPTIONS_KEEPALIVE_TIMEOUT",
    )
    min_ping_interval: NonNegativeInt = Field(
        10000,
        description="Minimum interval between HTTP/2 pings on the server side.",
        alias="SERVER_GRPC_OPTIONS_MIN_PING_INTERVAL",
    )
    max_receive_message_lenght: NonNegativeInt = Field(
        100 * 1024 * 1024,
        description="Maximum message size the server can receive, in bytes.",
        alias="SERVER_GRPC_OPTIONS_MAX_RECEIVE_MESSAGE_LENGTH",
    )
    max_send_message_length: NonNegativeInt = Field(
        100 * 1024 * 1024,
        description="Maximum message size the server can send, in bytes.",
        alias="SERVER_GRPC_OPTIONS_MAX_SEND_MESSAGE_LENGTH",
    )
    max_pings_without_data: NonNegativeInt = Field(
        0,
        description="Maximum number of pings the server allows without receiving any data. "
        "Setting to 0 allows unlimited pings, "
        "which is important for long-running streams.",
        alias="SERVER_GRPC_OPTIONS_MAX_PINGS_WITHOUT_DATA",
    )
    keepalive_permit_without_calls: bool = Field(
        default=True,
        description="Allow clients to send keepalive pings even when there are no active RPCs. "
        "This is important for keeping connections "
        "alive through proxies and detecting dead clients.",
        alias="SERVER_GRPC_OPTIONS_KEEPALIVE_PERMIT_WITHOUT_CALLS",
    )

    @property
    def options(self) -> list[tuple[str, Any]]:
        """Convert settings to gRPC server options format.

        Returns:
            List of tuples containing gRPC server options and their corresponding values.
        """
        return [
            ("grpc.max_receive_message_length", self.max_receive_message_lenght),
            ("grpc.max_send_message_length", self.max_send_message_length),
            # === Server-Side Keepalive (Keeps Connections Alive Through Proxies) ===
            # Server sends keepalive pings to detect dead clients and keep
            # proxy connections (e.g. Railway) alive during long-running RPCs.
            ("grpc.keepalive_time_ms", self.keepalive_time),
            ("grpc.keepalive_timeout_ms", self.keepalive_timeout),
            # === Keepalive Permission (Required for Client Keepalive) ===
            # Allow clients to send keepalive pings without active RPCs
            # Without this, server rejects client keepalives with GOAWAY
            ("grpc.keepalive_permit_without_calls", self.keepalive_permit_without_calls),
            # Allow unlimited pings without data (required for long-running streams)
            ("grpc.http2.max_pings_without_data", self.max_pings_without_data),
            # Minimum interval server allows between client pings
            # Prevents "too_many_pings" GOAWAY errors
            # Must match or be less than client's http2.min_time_between_pings_ms
            ("grpc.http2.min_ping_interval_without_data_ms", self.min_ping_interval),
        ]

    def __init__(self, **values: Any) -> None:
        """Initialize gRPC server settings."""
        super().__init__(**values)
