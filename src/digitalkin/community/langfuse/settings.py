"""Langfuse tracing settings."""

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class LangfuseSettings(BaseSettings):
    """Langfuse credentials, endpoint and environment tag for OTEL tracing."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_LANGFUSE_", case_sensitive=False)

    public_key: str = Field(
        default="",
        description="Langfuse public key. Empty disables tracing.",
        json_schema_extra={"env_example": "pk-lf-xxxx"},
    )
    secret_key: SecretStr = Field(
        default=SecretStr(""),
        description="Langfuse secret key. Empty disables tracing.",
        json_schema_extra={"env_example": "sk-lf-xxxx"},
    )
    base_url: str = Field(
        default="https://cloud.langfuse.com",
        description="Langfuse instance URL.",
    )
    environment: str = Field(
        default="development",
        description="Deployment environment tagged on every trace (tag env:<value> and metadata env).",
    )

    @property
    def enabled(self) -> bool:
        """Whether both credentials are present."""
        return bool(self.public_key and self.secret_key.get_secret_value())


@lru_cache(maxsize=1)
def get_langfuse_settings() -> LangfuseSettings:  # dk-allow-global (settings factory)
    """Process-wide ``LangfuseSettings`` singleton.

    Returns:
        The shared ``LangfuseSettings`` instance.
    """
    return LangfuseSettings()
