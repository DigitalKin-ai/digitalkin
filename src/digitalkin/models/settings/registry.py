"""Registry-scope runtime settings."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RegistrySettings(BaseSettings):
    """Registry client runtime configuration."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_REGISTRY_", case_sensitive=False)

    search_timeout_s: float = Field(
        default=10.0,
        description="Per-call deadline for agent-facing registry searches (shorter than the global gRPC default).",
    )


@lru_cache(maxsize=1)
def get_registry_settings() -> RegistrySettings:
    """Process-wide ``RegistrySettings`` singleton.

    Tests must call ``get_registry_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``RegistrySettings`` instance.
    """
    return RegistrySettings()
