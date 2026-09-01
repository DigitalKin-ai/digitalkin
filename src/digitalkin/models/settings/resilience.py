"""Resilience subsystem settings — bulkhead."""

from functools import lru_cache

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


@lru_cache(maxsize=1)
def get_bulkhead_settings() -> BulkheadSettings:
    """Process-wide ``BulkheadSettings`` singleton.

    Tests must call ``get_bulkhead_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``BulkheadSettings`` instance.
    """
    return BulkheadSettings()
