"""Redis connection and pool settings."""

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisPoolSettings(BaseSettings):
    """Redis connection pool configuration."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_REDIS_", case_sensitive=False)

    url: SecretStr = Field(default=SecretStr("redis://localhost:6379/0"), description="Redis connection URL")
    pool_size: int = Field(default=2000, gt=0, description="Total Redis connection pool size")
    pool_size_default: int = Field(default=0, description="Non-blocking pool size (0 = pool_size // 2)")
    pool_size_blocking: int = Field(default=0, description="Blocking pool size for XREAD (0 = pool_size // 2)")
    health_check_timeout: float = Field(default=5.0, description="Max seconds to wait for a PING during health check.")
    health_check_interval: int = Field(
        default=15,
        description="Seconds between connection-level PINGs; 0 disables. Catches silently-dead sockets.",
    )

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

    max_tasks: int = Field(default=10000, gt=0, description="Max registered signal tasks")


class RedisSettings(BaseSettings):
    """Top-level Redis configuration."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_REDIS_", case_sensitive=False)

    pool: RedisPoolSettings = Field(default_factory=RedisPoolSettings)
    signal: RedisSignalSettings = Field(default_factory=RedisSignalSettings)
    task_ttl: int = Field(default=86400, description="Task state TTL in seconds (1 day)")
    idem_ttl: int = Field(default=3600, description="Idempotency claim TTL in seconds")


@lru_cache(maxsize=1)
def get_redis_settings() -> RedisSettings:
    """Process-wide ``RedisSettings`` singleton.

    Nested settings are accessed via composition: ``get_redis_settings().pool``
    and ``.signal``. Tests must call ``get_redis_settings.cache_clear()`` after
    mutating env.

    Returns:
        The shared ``RedisSettings`` instance.
    """
    return RedisSettings()
