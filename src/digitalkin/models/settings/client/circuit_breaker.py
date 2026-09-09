"""Client circuit-breaker settings."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class CircuitBreakerSettings(BaseSettings):
    """Per-service circuit-breaker thresholds.

    Attributes:
        fail_max (int): Consecutive failures before the circuit opens.
        reset_timeout (float): Seconds the circuit stays open before a half-open probe.

    """

    model_config = SettingsConfigDict(env_prefix="CLIENT_CIRCUIT_BREAKER_", case_sensitive=False)

    fail_max: int = Field(default=5, description="Consecutive failures before the circuit opens.")
    reset_timeout: float = Field(default=30.0, description="Seconds the circuit stays open before a half-open probe.")
