"""Resilience subsystem settings — bulkhead."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BulkheadServiceSettings(BaseSettings):
    """Per-service bulkhead override, read under a prefix built at runtime.

    ``DIGITALKIN_BULKHEAD_{SERVICE_ID}_MAX`` carries the service id in the
    variable name, so the prefix is built from the id at instantiation:
    ``BulkheadServiceSettings("storage")`` reads
    ``DIGITALKIN_BULKHEAD_STORAGE_MAX``.

    Attributes:
        max (int | None): Max concurrent calls for this service. ``None`` keeps the default.

    """

    model_config = SettingsConfigDict(case_sensitive=False)

    max: int | None = Field(
        default=None,
        gt=0,
        description="Max concurrent calls for this service. Unset keeps DIGITALKIN_BULKHEAD_DEFAULT_MAX.",
    )

    def __init__(self, service_id: str) -> None:
        """Read the override declared for one service.

        Args:
            service_id: Service identifier forming the variable's dynamic segment.
        """
        super().__init__(_env_prefix=f"DIGITALKIN_BULKHEAD_{service_id.upper()}_")


class BulkheadSettings(BaseSettings):
    """Per-service concurrency-limiter defaults.

    The per-service override lives in ``BulkheadServiceSettings``, whose
    prefix is built from the service id.
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
