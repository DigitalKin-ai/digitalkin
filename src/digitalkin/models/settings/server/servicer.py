"""Module servicer settings."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ModuleServicerSettings(BaseSettings):
    """Caching and timeout settings for ModuleServicer."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_MODULE_SERVICER_", case_sensitive=False)

    setup_cache_max: int = Field(default=100, description="Max entries in the per-setup-version cache.")
    completion_timeout: float = Field(default=300.0, description="Max seconds to await module completion.")
