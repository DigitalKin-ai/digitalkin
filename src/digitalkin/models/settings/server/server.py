"""Server settings for the DigitalKin application."""

import os
from functools import lru_cache
from typing import Any

from pydantic import Field, NonNegativeInt
from pydantic_settings import BaseSettings, SettingsConfigDict

from digitalkin.models.settings.profiling import ProfilingSettings
from digitalkin.models.settings.server.channel import ServerChannelSettings
from digitalkin.models.settings.server.grpc import GrpcServerSettings


class ServerSettings(BaseSettings):
    """Settings for the DigitalKin server.

    Attributes:
        channel (ServerChannelSettings): Settings for the server channel.
        grpc (GrpcServerSettings): Settings for the gRPC server.
        profiling (ProfilingSettings): Profiling and debugging configuration.
        health_check (bool): Whether to enable the health check service.
        reflection (bool): Whether to enable reflection for the server.
        max_concurrent_rpcs (NonNegativeInt): Maximum number of RPCs handled in parallel by the server.
        max_workers (NonNegativeInt): Maximum number of workers for sync mode.
        thread_pool_workers (NonNegativeInt): Number of workers in the server thread pool.

    """

    model_config = SettingsConfigDict(env_prefix="SERVER_", case_sensitive=False)

    channel: ServerChannelSettings = Field(default_factory=ServerChannelSettings)

    grpc: GrpcServerSettings = Field(default_factory=GrpcServerSettings)

    profiling: ProfilingSettings = Field(default_factory=ProfilingSettings)

    health_check: bool = Field(default=True, description="Enable health check service")
    reflection: bool = Field(default=True, description="Enable reflection for the server")
    max_concurrent_rpcs: NonNegativeInt = Field(
        (os.cpu_count() or 1) * 200,
        description="Maximum number of RPCs handled in parallel by the server.",
    )
    max_workers: NonNegativeInt = Field(10, description="Maximum number of workers for sync mode")
    thread_pool_workers: NonNegativeInt = Field(
        min(4, os.cpu_count() or 1),
        description="Number of workers in the server thread pool.",
    )

    def __init__(self, **values: Any) -> None:
        """Initialize the ServerSettings instance."""
        super().__init__(**values)


@lru_cache(maxsize=1)
def get_server_settings() -> ServerSettings:
    """Process-wide ``ServerSettings`` singleton.

    Nested settings accessed via composition: ``.channel``, ``.grpc``,
    ``.profiling``. Tests must call ``get_server_settings.cache_clear()`` after
    mutating env.

    Returns:
        The shared ``ServerSettings`` instance.
    """
    return ServerSettings()
