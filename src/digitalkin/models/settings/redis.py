"""Redis connection and pool settings."""

import os

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisPoolSettings(BaseSettings):
    """Redis connection pool configuration."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_REDIS_", case_sensitive=False)

    url: str = Field(
        default_factory=lambda: os.environ.get("DIGITALKIN_REDIS_URL", "redis://localhost:6379/0"),
        description="Redis connection URL",
    )
    pool_size: int = Field(default=2000, description="Total Redis connection pool size")
    pool_size_default: int = Field(default=0, description="Non-blocking pool size (0 = pool_size // 2)")
    pool_size_blocking: int = Field(default=0, description="Blocking pool size for XREAD (0 = pool_size // 2)")

    def get_default_pool_size(self) -> int:
        """Non-blocking pool size, defaults to half of total.

        Returns:
            Pool size for non-blocking commands.
        """
        return self.pool_size_default or self.pool_size // 2

    def get_blocking_pool_size(self) -> int:
        """Blocking pool size for XREAD, defaults to half of total.

        Returns:
            Pool size for blocking commands.
        """
        return self.pool_size_blocking or self.pool_size // 2


class RedisSignalSettings(BaseSettings):
    """Redis signal delivery configuration."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_SIGNAL_", case_sensitive=False)

    queue_size: int = Field(default=512, description="Per-task signal queue max size")
    max_tasks: int = Field(default=10000, description="Max registered signal tasks")
    flush_interval: float = Field(default=0.1, description="Signal batch flush interval in seconds")
    max_batch_size: int = Field(default=50, description="Max signals per batch flush")
    max_pending: int = Field(default=5000, description="Max pending signals in send buffer")


class RedisSettings(BaseSettings):
    """Top-level Redis configuration."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_REDIS_", case_sensitive=False)

    pool: RedisPoolSettings = Field(default_factory=RedisPoolSettings)
    signal: RedisSignalSettings = Field(default_factory=RedisSignalSettings)
    task_ttl: int = Field(default=86400, description="Task state TTL in seconds (1 day)")
    checkpoint_ttl: int = Field(default=300, description="Checkpoint TTL in seconds")
    idempotency_ttl: int = Field(default=3600, description="Idempotency claim TTL in seconds")
