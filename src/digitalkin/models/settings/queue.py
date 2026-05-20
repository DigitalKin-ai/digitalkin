"""Queue factory settings."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class QueueSettings(BaseSettings):
    """Defaults for asyncio queues created via QueueFactory."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_QUEUE_", case_sensitive=False)

    max_size: int = Field(default=1000, description="Default bounded-queue max size (0 = unbounded).")
