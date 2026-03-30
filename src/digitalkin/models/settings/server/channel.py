"""Server channel settings."""

from typing import Any

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from digitalkin.models.settings.utils.channel import BaseChannelSettings, Credentials


class ServerChannelSettings(BaseChannelSettings):
    """Settings for a server channel.

    Attributes:
        advertise_host (str | None): Public hostname/IP sent to registry for discovery. Falls back to host if not set.
        database_url (str | None): Database URL for registry data storage

    """

    model_config = SettingsConfigDict(
        env_prefix="SERVER_CHANNEL_",
        env_nested_delimiter="__",
        extra="forbid",
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

    advertise_host: str | None = Field(
        None, description="Public hostname/IP sent to registry for discovery. Falls back to host if not set."
    )

    database_url: str | None = Field(None, description="Database URL for registry data storage")

    def __init__(self, **values: Any) -> None:
        super().__init__(**values)
