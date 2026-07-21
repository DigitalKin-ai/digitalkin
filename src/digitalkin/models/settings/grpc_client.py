"""gRPC client-side settings — circuit breaker, query retry, channel options."""

from functools import lru_cache
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CircuitBreakerSettings(BaseSettings):
    """Per-service circuit-breaker thresholds."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_CB_", case_sensitive=False)

    fail_max: int = Field(default=5, description="Consecutive failures before the circuit opens.")
    reset_timeout: float = Field(default=30.0, description="Seconds the circuit stays open before a half-open probe.")


class GrpcClientSettings(BaseSettings):
    """Retry/backoff for unary gRPC queries via GrpcClientWrapper."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_GRPC_QUERY_", case_sensitive=False)

    max_retries: int = Field(default=2, description="Retry attempts for a failed unary query.")
    backoff_base_ms: float = Field(default=50.0, description="Base backoff in milliseconds for query retries.")
    timeout: float = Field(default=30.0, description="Default per-query deadline in seconds.")


class GrpcRetrySettings(BaseSettings):
    """gRPC service-config retry policy (channel-level)."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_GRPC_RETRY_", case_sensitive=False)

    max_attempts: int = Field(default=5, ge=1, le=10, description="Max retry attempts including the original call.")
    initial_backoff: str = Field(default="0.1s", description="Initial backoff duration (e.g. '0.1s').")
    max_backoff: str = Field(default="10s", description="Maximum backoff duration (e.g. '10s').")
    backoff_multiplier: float = Field(default=2.0, ge=1.0, description="Exponential backoff multiplier.")


class GrpcChannelSettings(BaseSettings):
    """gRPC channel keepalive and reconnect tuning."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_GRPC_", case_sensitive=False)

    dns_resolution_ms: int = Field(default=500, description="Min ms between DNS re-resolutions.")
    initial_reconnect_ms: int = Field(default=1000, description="Initial reconnect backoff in ms.")
    max_reconnect_ms: int = Field(default=10000, description="Max reconnect backoff in ms.")
    min_reconnect_ms: int = Field(default=500, description="Min reconnect backoff in ms.")
    keepalive_time_ms: int = Field(default=15000, description="Keepalive ping interval in ms.")
    keepalive_timeout_ms: int = Field(default=5000, description="Keepalive ping timeout in ms.")
    min_ping_interval_ms: int = Field(default=10000, description="Min HTTP/2 ping interval in ms.")

    def to_channel_options(self) -> list[tuple[str, Any]]:
        """Build the resilient gRPC channel-options list.

        Returns:
            Channel options with message-size limits, DNS re-resolution, keepalive, and retries.
        """
        return [
            ("grpc.max_receive_message_length", 100 * 1024 * 1024),
            ("grpc.max_send_message_length", 100 * 1024 * 1024),
            ("grpc.dns_min_time_between_resolutions_ms", self.dns_resolution_ms),
            ("grpc.initial_reconnect_backoff_ms", self.initial_reconnect_ms),
            ("grpc.max_reconnect_backoff_ms", self.max_reconnect_ms),
            ("grpc.min_reconnect_backoff_ms", self.min_reconnect_ms),
            ("grpc.keepalive_time_ms", self.keepalive_time_ms),
            ("grpc.keepalive_timeout_ms", self.keepalive_timeout_ms),
            ("grpc.keepalive_permit_without_calls", True),
            ("grpc.http2.min_time_between_pings_ms", self.min_ping_interval_ms),
            # gRPC C++ retry layer disabled: the per-channel service_config retry
            # policy (max_attempts=5, max_backoff=10s) would otherwise stack on top
            # of the Python-level retry loop in `exec_grpc_query` (max_retries=2,
            # backoff_base_ms=50). On a cold registry channel returning UNAVAILABLE
            # for multiple tool refs, the C++ retries caused an 8 s wall-clock gap
            # (see monitoring/reports/sdk-8sec-gap-rootcause.md). The Python loop
            # already covers the same retryable status codes (UNAVAILABLE,
            # INTERNAL, DEADLINE_EXCEEDED) with a more conservative budget.
            ("grpc.enable_retries", 0),
        ]


@lru_cache(maxsize=1)
def get_circuit_breaker_settings() -> CircuitBreakerSettings:
    """Process-wide ``CircuitBreakerSettings`` singleton.

    Tests must call ``get_circuit_breaker_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``CircuitBreakerSettings`` instance.
    """
    return CircuitBreakerSettings()


@lru_cache(maxsize=1)
def get_grpc_client_settings() -> GrpcClientSettings:
    """Process-wide ``GrpcClientSettings`` singleton.

    Tests must call ``get_grpc_client_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``GrpcClientSettings`` instance.
    """
    return GrpcClientSettings()


@lru_cache(maxsize=1)
def get_grpc_retry_settings() -> GrpcRetrySettings:
    """Process-wide ``GrpcRetrySettings`` singleton.

    Tests must call ``get_grpc_retry_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``GrpcRetrySettings`` instance.
    """
    return GrpcRetrySettings()


@lru_cache(maxsize=1)
def get_grpc_channel_settings() -> GrpcChannelSettings:
    """Process-wide ``GrpcChannelSettings`` singleton.

    Tests must call ``get_grpc_channel_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``GrpcChannelSettings`` instance.
    """
    return GrpcChannelSettings()
