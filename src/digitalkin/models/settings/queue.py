"""Queue factory settings."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class QueueSettings(BaseSettings):
    """Defaults for asyncio queues created via QueueFactory."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_QUEUE_", case_sensitive=False)

    max_size: int = Field(default=1000, description="Default bounded-queue max size (0 = unbounded).")


@lru_cache(maxsize=1)
def get_queue_settings() -> QueueSettings:
    """Process-wide ``QueueSettings`` singleton.

    Tests must call ``get_queue_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``QueueSettings`` instance.
    """
    return QueueSettings()
