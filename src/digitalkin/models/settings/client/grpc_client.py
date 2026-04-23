"""gRPC client settings for the SDK."""

from typing import Any

from pydantic import Field, NonNegativeInt
from pydantic_settings import SettingsConfigDict

from digitalkin.models.settings.client.retry_policy import ClientRetryPolicySettings
from digitalkin.models.settings.utils.grpc_base import BaseGrpcSettings


class ClientGrpcSettings(BaseGrpcSettings):
    """gRPC tuning settings for SDK clients."""

    model_config = SettingsConfigDict(
        env_prefix="CLIENT_GRPC_",
        env_nested_delimiter="__",
        extra="forbid",
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

    # ── Options ───────────────────────────────────────────────────────────────────── #

    dns_resolution_time: NonNegativeInt = Field(default=500, description="")
    initial_reconnect_time: NonNegativeInt = Field(default=1000, description="")
    max_reconnect_time: NonNegativeInt = Field(default=10000, description="")
    min_reconnect_time: NonNegativeInt = Field(default=500, description="")
    min_ping_interval_time: NonNegativeInt = Field(default=30000, description="")
    enable_retries: bool = Field(default=True, description="")
    retry_policy: ClientRetryPolicySettings = Field(default_factory=ClientRetryPolicySettings)

    @property
    def _specific_options(self) -> list[tuple[str, Any]]:
        """Return client specific gRPC options.

        Returns:
            List of tuples containing client specific gRPC options.
        """
        return [
            ("grpc.dns_min_time_between_resolutions_ms", self.dns_resolution_time),
            ("grpc.initial_reconnect_backoff_ms", self.initial_reconnect_time),
            ("grpc.max_reconnect_backoff_ms", self.max_reconnect_time),
            ("grpc.min_reconnect_backoff_ms", self.min_reconnect_time),
            ("grpc.http2.min_time_between_pings_ms", self.min_ping_interval_time),
            ("grpc.enable_retries", int(self.enable_retries)),
            self.retry_policy.to_grpc_option(),
        ]
