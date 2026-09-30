"""Client settings for the DigitalKin application."""

from functools import lru_cache

from pydantic import Field, NonNegativeFloat, NonNegativeInt
from pydantic_settings import BaseSettings, SettingsConfigDict

from digitalkin.models.settings.client.channel import ClientChannelSettings
from digitalkin.models.settings.client.circuit_breaker import CircuitBreakerSettings
from digitalkin.models.settings.client.grpc import GrpcClientRetrySettings, GrpcClientSettings


class ClientSettings(BaseSettings):
    """Settings for the DigitalKin gRPC clients.

    Attributes:
        channel (ClientChannelSettings): Settings for the client channel.
        grpc (GrpcClientSettings): Settings for the gRPC channel options.
        retry (GrpcClientRetrySettings): Channel-level gRPC retry policy.
        circuit_breaker (CircuitBreakerSettings): Per-service circuit-breaker thresholds.
        max_retries (NonNegativeInt): Retry attempts for a failed unary query.
        backoff_base_ms (NonNegativeFloat): Base backoff in milliseconds for query retries.
        timeout (NonNegativeFloat): Default per-query deadline in seconds.

    """

    model_config = SettingsConfigDict(env_prefix="CLIENT_", case_sensitive=False)

    channel: ClientChannelSettings = Field(default_factory=ClientChannelSettings)

    grpc: GrpcClientSettings = Field(default_factory=GrpcClientSettings)

    retry: GrpcClientRetrySettings = Field(default_factory=GrpcClientRetrySettings)

    circuit_breaker: CircuitBreakerSettings = Field(default_factory=CircuitBreakerSettings)

    max_retries: NonNegativeInt = Field(default=2, description="Retry attempts for a failed unary query.")
    backoff_base_ms: NonNegativeFloat = Field(
        default=50.0, description="Base backoff in milliseconds for query retries."
    )
    timeout: NonNegativeFloat = Field(default=30.0, description="Default per-query deadline in seconds.")


@lru_cache(maxsize=1)
def get_client_settings() -> ClientSettings:
    """Process-wide ``ClientSettings`` singleton.

    Nested settings accessed via composition: ``.channel``, ``.grpc``,
    ``.retry``, ``.circuit_breaker``. Tests must call
    ``get_client_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``ClientSettings`` instance.
    """
    return ClientSettings()
