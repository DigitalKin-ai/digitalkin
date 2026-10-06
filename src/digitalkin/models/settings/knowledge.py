"""Knowledge-scope runtime settings."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KnowledgeSettings(BaseSettings):
    """Knowledge client runtime configuration."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_KNOWLEDGE_", case_sensitive=False)

    search_timeout_s: float = Field(
        default=10.0,
        gt=0,
        description="Per-call deadline for a knowledge search (shorter than the global gRPC default).",
    )
    default_limit: int = Field(
        default=10,
        ge=1,
        description="Documents returned when a caller asks for no particular number.",
    )


@lru_cache(maxsize=1)
def get_knowledge_settings() -> KnowledgeSettings:
    """Process-wide ``KnowledgeSettings`` singleton.

    Tests must call ``get_knowledge_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``KnowledgeSettings`` instance.
    """
    return KnowledgeSettings()
