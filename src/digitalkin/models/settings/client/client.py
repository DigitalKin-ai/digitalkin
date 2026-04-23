"""Top-level gRPC client settings."""

# utiliser channel
from typing import Any

from pydantic import Field, NonNegativeFloat, NonNegativeInt
from pydantic_settings import BaseSettings, SettingsConfigDict

from digitalkin.models.settings.client.channel import ClientChannelSettings
from digitalkin.models.settings.client.grpc_client import ClientGrpcSettings
from digitalkin.models.settings.client.retry_policy import ClientRetryPolicySettings


class ClientSettings(BaseSettings):
    """Top-level gRPC client settings."""

    model_config = SettingsConfigDict(env_prefix="CLIENT_", case_sensitive=False)

    # ── Options ───────────────────────────────────────────────────────────────────── #

    channel: ClientChannelSettings = Field(default_factory=ClientChannelSettings)
    grpc: ClientGrpcSettings = Field(default_factory=ClientGrpcSettings)
    retry_policy: ClientRetryPolicySettings = Field(default_factory=ClientRetryPolicySettings)
    query_max_retries: NonNegativeInt = Field(default=2, description="")
    query_backoff_base_ms: NonNegativeFloat = Field(default=50, description="")
    query_timeout: NonNegativeFloat = Field(default=30, description="")

    # ── Functions ─────────────────────────────────────────────────────────────────── #

    def __init__(self, **values: Any) -> None:
        """Initialize client settings."""
        super().__init__(**values)
