"""Client channel settings."""

from functools import lru_cache

from pydantic import Field, NonNegativeInt, model_validator
from pydantic_settings import SettingsConfigDict

from digitalkin.models.settings.utils.channel import BaseChannelSettings


class ClientChannelSettings(BaseChannelSettings):
    """Settings for a client channel.

    Overrides the bind-oriented defaults of ``BaseChannelSettings`` with dial
    defaults: a client connects to a remote address instead of listening on
    every interface.

    Attributes:
        host (str): Host address the client dials.
        port (NonNegativeInt): Port the client dials.

    """

    model_config = SettingsConfigDict(
        env_prefix="CLIENT_CHANNEL_",
        env_nested_delimiter="__",
        extra="forbid",
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

    host: str = Field(
        default="[::]",
        description="Host address the client dials (the services provider)",
        json_schema_extra={"env_required": True},
    )

    port: NonNegativeInt = Field(
        default=50051,
        description="Port the client dials (the services provider)",
        json_schema_extra={"env_required": True},
    )

    @model_validator(mode="after")
    def validate_credentials(self) -> "ClientChannelSettings":
        """Accept secure mode without explicit credential paths.

        Unlike the server, a client falls back to the ``CERTIFICATE_`` volume:
        ``EnvManager.client_config`` resolves the pair there and raises
        ``SecurityError`` when the root CA is missing, so secure mode still
        fails closed.

        Returns:
            The validated settings.
        """
        return self


@lru_cache(maxsize=1)
def get_client_channel_settings() -> ClientChannelSettings:
    """Process-wide ``ClientChannelSettings`` singleton.

    Tests must call ``get_client_channel_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``ClientChannelSettings`` instance.
    """
    return ClientChannelSettings()
