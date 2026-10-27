"""AG-UI producer limits."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AgUiSettings(BaseSettings):
    """Bounds on what the SDK puts on, or accepts from, an AG-UI stream."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_AGUI_", case_sensitive=False)

    max_sources: int = Field(default=50, gt=0, description="Sources kept per run in the sources activity.")
    max_source_title_chars: int = Field(default=300, gt=0, description="Characters kept of a source title.")
    max_source_description_chars: int = Field(
        default=1000, gt=0, description="Characters kept of a source description."
    )
    max_inline_b64_chars: int = Field(
        default=13_981_016,
        gt=0,
        description="Largest base64 inline image accepted in a run input (checked before decoding; ~10 MiB).",
    )
    mission_title_attempts: int = Field(
        default=3, gt=0, description="Runs that may try to title a mission before a null title is emitted."
    )


@lru_cache(maxsize=1)
def get_agui_settings() -> AgUiSettings:  # dk-allow-global (settings factory, docs/environment_variables.md)
    """Process-wide ``AgUiSettings`` singleton.

    Returns:
        The shared ``AgUiSettings`` instance.
    """
    return AgUiSettings()
