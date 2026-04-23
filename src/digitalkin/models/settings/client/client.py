"""Top-level gRPC client settings."""

# utiliser channel
from typing import Any

from pydantic import Field, NonNegativeFloat, NonNegativeInt
from pydantic_settings import BaseSettings, SettingsConfigDict

from digitalkin.models.settings.client.channel import ClientChannelSettings
from digitalkin.models.settings.client.grpc_client import ClientGrpcSettings
from digitalkin.models.settings.client.task import TaskSettings


class ClientSettings(BaseSettings):
    """Top-level gRPC client settings."""

    model_config = SettingsConfigDict(env_prefix="CLIENT_", case_sensitive=False)

    # ── Options ───────────────────────────────────────────────────────────────────── #

    channel: ClientChannelSettings = Field(default_factory=ClientChannelSettings)
    grpc: ClientGrpcSettings = Field(default_factory=ClientGrpcSettings)
    task: TaskSettings = Field(default_factory=TaskSettings)
    query_max_retries: NonNegativeInt = Field(default=2, description="Maximum number of retries for queries")
    query_backoff_base_ms: NonNegativeFloat = Field(
        default=50, description="Base backoff time in milliseconds for query retries"
    )
    query_timeout: NonNegativeFloat = Field(default=30, description="Timeout in seconds for queries")

    # ── Functions ─────────────────────────────────────────────────────────────────── #

    def __init__(self, **values: Any) -> None:
        """Initialize client settings."""
        super().__init__(**values)
