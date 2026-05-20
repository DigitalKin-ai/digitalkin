"""Resilience subsystem settings — bulkhead, watchdog, reaper, shutdown."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BulkheadSettings(BaseSettings):
    """Per-service concurrency-limiter defaults.

    The per-service ``DIGITALKIN_BULKHEAD_{SERVICE_ID}_MAX`` override has a
    dynamic suffix and is read directly in ``Bulkhead.for_service``.
    """

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_BULKHEAD_", case_sensitive=False)

    default_max: int = Field(default=50, description="Default max concurrent calls per service.")
    timeout: float = Field(default=2.0, description="Seconds to wait for a slot before raising BulkheadFullError.")


class ResilienceSettings(BaseSettings):
    """Watchdog, session-reaper, and graceful-shutdown timings."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_RESILIENCE_", case_sensitive=False)

    shutdown_timeout: float = Field(default=30.0, description="Max seconds for the graceful-shutdown sequence.")
    session_reaper_ttl: float = Field(default=300.0, description="Idle seconds before the reaper drops a session.")
    session_reaper_interval: float = Field(default=60.0, description="Seconds between session-reaper scans.")
    watchdog_stall_threshold: float = Field(
        default=5.0, description="Seconds without loop progress before declaring a stall."
    )
    watchdog_check_interval: float = Field(default=1.0, description="Seconds between watchdog health checks.")
