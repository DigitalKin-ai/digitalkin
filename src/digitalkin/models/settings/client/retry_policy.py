"""Retry policy settings for gRPC clients."""

import json
from typing import Any

from grpc import StatusCode
from pydantic import Field, NonNegativeFloat, NonNegativeInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class ClientRetryPolicySettings(BaseSettings):
    """Retry policy settings used to build gRPC service config."""

    model_config = SettingsConfigDict(
        env_prefix="CLIENT_RETRY_POLICY_",
        case_sensitive=False,
        frozen=True,
        extra="forbid",
    )

    # ── Options ───────────────────────────────────────────────────────────────────── #

    max_attempts: NonNegativeInt = Field(
        default=5,
        ge=1,
        le=10,
        description="Maximum retry attempts including the original call",
    )
    initial_backoff: NonNegativeFloat = Field(
        default=0.1,
        description="Initial backoff duration (e.g., '0.1s')",
    )
    max_backoff: NonNegativeInt = Field(
        default=10,
        description="Maximum backoff duration (e.g., '10s')",
    )
    backoff_multiplier: NonNegativeFloat = Field(
        default=2.0,
        ge=1.0,
        description="Multiplier for exponential backoff",
    )
    retryable_status_codes: list[StatusCode | str] = Field(
        default_factory=lambda: [
            StatusCode.UNAVAILABLE,
            StatusCode.RESOURCE_EXHAUSTED,
            StatusCode.DEADLINE_EXCEEDED,
        ],
        description="gRPC status codes that trigger retry",
    )

    # ── Functions ─────────────────────────────────────────────────────────────────── #

    def to_grpc_service_config(self) -> dict[str, Any]:
        """Build gRPC service config dictionary for retries.

        Returns:
            Service config dictionary compatible with ``grpc.service_config``.
        """
        return {
            "methodConfig": [
                {
                    "name": [{}],
                    "retryPolicy": {
                        "maxAttempts": self.max_attempts,
                        "initialBackoff": f"{self.initial_backoff:g}s",
                        "maxBackoff": f"{self.max_backoff}s",
                        "backoffMultiplier": self.backoff_multiplier,
                        "retryableStatusCodes": [
                            status.name if isinstance(status, grpc.StatusCode) else status
                            for status in self.retryable_status_codes
                        ],
                    },
                }
            ]
        }

    def to_grpc_service_config_json(self) -> str:
        """Serialize retry policy as ``grpc.service_config`` JSON string.

        Returns:
            Compact JSON string for gRPC channel option value.
        """
        return json.dumps(self.to_grpc_service_config(), separators=(",", ":"))

    def to_grpc_option(self) -> tuple[str, str]:
        """Build the gRPC channel option tuple for retry policy.

        Returns:
            Tuple usable directly in gRPC channel options.
        """
        return "grpc.service_config", self.to_grpc_service_config_json()

    def to_service_config_json(self) -> str:
        """Backward-compatible alias for legacy retry policy naming.

        Returns:
            JSON string for gRPC service config.
        """
        return self.to_grpc_service_config_json()
