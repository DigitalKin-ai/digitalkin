"""Server settings for the DigitalKin application."""

import os
from typing import Any

from pydantic import Field, NonNegativeInt
from pydantic_settings import BaseSettings, SettingsConfigDict

from digitalkin.models.settings.server.channel import ServerChannelSettings
from digitalkin.models.settings.server.grpc_server import GrpcServerSettings


class ServerSettings(BaseSettings):
    """Settings for the DigitalKin server.

    Attributes:
        channel (ServerChannelSettings): Settings for the server channel.
        grpc (ServerGrpcSettings): Settings for the gRPC server.
        health_check (bool): Whether to enable the health check service.
        reflection (bool): Whether to enable reflection for the server.
        max_concurrent_rpcs (NonNegativeInt): Maximum number of RPCs handled in parallel by the server.
        max_workers (NonNegativeInt): Maximum number of workers for sync mode.
        thread_pool_workers (NonNegativeInt): Number of workers in the server thread pool.

    """

    model_config = SettingsConfigDict(env_prefix="SERVER_", case_sensitive=False)

    # ── Options ───────────────────────────────────────────────────────────────────── #

    channel: ServerChannelSettings = Field(default_factory=ServerChannelSettings)
    grpc: GrpcServerSettings = Field(default_factory=GrpcServerSettings)
    health_check: bool = Field(default=True, description="Enable health check service")
    reflection: bool = Field(default=True, description="Enable reflection for the server")
    max_concurrent_rpcs: NonNegativeInt = Field(
        default=(os.cpu_count() or 1) * 200,
        description="Maximum number of RPCs handled in parallel by the server.",
    )
    max_workers: NonNegativeInt = Field(default=10, description="Maximum number of workers for sync mode")
    thread_pool_workers: NonNegativeInt = Field(
        default=min(4, os.cpu_count() or 1),
        description="Number of workers in the server thread pool.",
    )

    # ── Functions ─────────────────────────────────────────────────────────────────── #

    def __init__(self, **values: Any) -> None:
        """Initialize the ServerSettings instance."""
        super().__init__(**values)
