"""``services_manager`` toolkit limits."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ServicesManagerSettings(BaseSettings):
    """How much of a service the ``services_manager`` toolkit hands the model at once."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_SERVICES_MANAGER_", case_sensitive=False)

    structure_max_chars: int = Field(
        default=20000, gt=0, description="Largest structure map (JSON characters) returned whole, and its page budget."
    )
    load_page_chars: int = Field(
        default=20000,
        gt=0,
        description="Largest configuration part (JSON characters) returned whole, and its page budget.",
    )
    text_overlap_chars: int = Field(
        default=500,
        ge=0,
        description="Characters a text page shares with the next one, so the model keeps its place between pages.",
    )
    writer_concurrency: int = Field(default=4, gt=0, description="Sections a generate call writes in parallel.")


@lru_cache(maxsize=1)
def get_services_manager_settings() -> ServicesManagerSettings:  # dk-allow-global (settings factory)
    """Process-wide ``ServicesManagerSettings`` singleton.

    Returns:
        The shared ``ServicesManagerSettings`` instance.
    """
    return ServicesManagerSettings()
