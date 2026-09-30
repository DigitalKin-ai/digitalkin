"""Module-scope runtime settings."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from digitalkin.models.services.services import ServicesMode


class ModuleSettings(BaseSettings):
    """Per-module runtime configuration.

    Env prefix ``DIGITALKIN_MODULE_``; ``services_mode`` reads the unprefixed
    ``SERVICE_MODE`` the deployment and the ``--dev-mode`` flag share.
    """

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_MODULE_", case_sensitive=False)

    id: str = Field(
        default="",
        description="Module identifier. Empty falls back to metadata module_id.",
        json_schema_extra={"env_required": True, "env_example": "modules:xxx"},
    )
    services_mode: ServicesMode = Field(
        default=ServicesMode.REMOTE,
        validation_alias="SERVICE_MODE",
        description="Whether service strategies run against local stubs or remote gRPC services.",
    )
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
