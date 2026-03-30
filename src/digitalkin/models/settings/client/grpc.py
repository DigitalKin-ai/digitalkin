from datetime import timedelta
from typing import Any

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class GrpcClientSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CLIENT_GRPC_", extra="forbid", arbitrary_types_allowed=True, validate_assignment=True)


    timeout: timedelta = Field(timedelta(seconds=30),
                    description="Global timeout for gRPC calls. Must be in ISO 8601 duration format (e.g. PT30S for 30 seconds, PT1M for 1 minute).")
    poll_timeout: timedelta = Field(timedelta(seconds=1),
                    description="Timeout for individual GetSignals calls. Must be in ISO 8601 duration format (e.g. PT1S for 1 second).")
    poll_interval: timedelta = Field(timedelta(seconds=1),
                    description="Maximum interval between GetSignals polls. Must be in ISO 8601 duration format (e.g. PT1S for 1 second).")
    initial_poll_interval: timedelta = Field(timedelta(seconds=0.1),
                    description="Starting poll interval before exponential ramp-up. Must be in ISO 8601 duration format (e.g. PT0.1S for 100 milliseconds).")

    @property
    def options(self) -> list[tuple[str, Any]]:
        return [

        ]

    def __init__(self, **values: Any):
        super().__init__(**values)