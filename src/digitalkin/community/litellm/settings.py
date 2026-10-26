"""LiteLLM proxy settings."""

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class LiteLLMSettings(BaseSettings):
    """LiteLLM proxy endpoint and bearer token."""

    model_config = SettingsConfigDict(env_prefix="LITELLM_", case_sensitive=False)

    # No default: the boot check only reports empty values, so a URL default would let a module start
    # without one and send the bearer token to whatever host it names.
    base_url: str = Field(
        default="",
        description="LiteLLM proxy base URL.",
        json_schema_extra={"env_example": "https://litellm.staging.digitalkin.com/", "env_required": True},
    )
    api_key: SecretStr = Field(
        default=SecretStr(""),
        description="Bearer token for the LiteLLM proxy.",
        json_schema_extra={"env_example": "sk-xxxx", "env_required": True},
    )


@lru_cache(maxsize=1)
def get_litellm_settings() -> LiteLLMSettings:  # dk-allow-global (settings factory)
    """Process-wide ``LiteLLMSettings`` singleton.

    Returns:
        The shared ``LiteLLMSettings`` instance.
    """
    return LiteLLMSettings()
