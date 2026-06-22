"""Module-scope runtime settings."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ModuleSettings(BaseSettings):
    """Per-module runtime configuration."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_MODULE_", case_sensitive=False)

    id: str = Field(default="", description="Module identifier. Empty falls back to metadata module_id.")
    timezone: str = Field(default="Europe/Paris", description="IANA timezone for module session timestamps.")
    tool_resolve_timeout: float = Field(default=10.0, description="Per-tool resolution deadline in seconds.")
    file_history_flush_threshold: int = Field(
        default=10, description="Dirty-entry count that triggers a file-history flush."
    )


@lru_cache(maxsize=1)
def get_module_settings() -> ModuleSettings:
    """Process-wide ``ModuleSettings`` singleton.

    Tests must call ``get_module_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``ModuleSettings`` instance.
    """
    return ModuleSettings()
