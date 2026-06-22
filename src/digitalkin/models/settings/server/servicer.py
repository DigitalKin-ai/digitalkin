"""Module servicer settings."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ModuleServicerSettings(BaseSettings):
    """Caching and timeout settings for ModuleServicer."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_MODULE_SERVICER_", case_sensitive=False)

    setup_cache_max: int = Field(default=100, description="Max entries in the per-setup-version cache.")
    setup_cache_ttl: float = Field(default=600.0, description="TTL in seconds for the per-setup-version cache.")
    completion_timeout: float = Field(default=300.0, description="Max seconds to await module completion.")


@lru_cache(maxsize=1)
def get_module_servicer_settings() -> ModuleServicerSettings:
    """Process-wide ``ModuleServicerSettings`` singleton.

    Tests must call ``get_module_servicer_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``ModuleServicerSettings`` instance.
    """
    return ModuleServicerSettings()
