"""Client Channel settings."""

from typing import Any

from pydantic_settings import SettingsConfigDict

from digitalkin.models.settings.utils.channel import BaseChannelSettings


class ClientChannelSettings(BaseChannelSettings):
    """Client channel settings."""

    model_config = SettingsConfigDict(
        env_prefix="CLIENT_CHANNEL_",
        env_nested_delimiter="__",
        extra="forbid",
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

    # ── Options ───────────────────────────────────────────────────────────────────── #

    # ── Functions ─────────────────────────────────────────────────────────────────── #

    def __init__(self, **values: Any) -> None:
        """Default constructor."""
        super().__init__(**values)
