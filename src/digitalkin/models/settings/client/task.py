"""Settings for task manager."""

from typing import Any

from pydantic import Field, NonNegativeInt, PositiveFloat, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class TaskSettings(BaseSettings):
    """Task settings."""

    model_config = SettingsConfigDict(env_prefix="TASK_", case_sensitive=False)

    # ── Options ───────────────────────────────────────────────────────────────────── #

    max_concurrent_tasks: PositiveInt = Field(default=100, description="Maximum number of concurrent tasks allowed.")
    wait_timeout: PositiveFloat = Field(
        default=30, description="Maximum time (in seconds) to wait for a task to complete before timing out."
    )
    stream_drain_timeout: PositiveInt = Field(
        default=60, description="Maximum time (in seconds) to wait for a stream to drain before forcing closure."
    )
    max_queued_tasks: NonNegativeInt = Field(
        default=50, description="Maximum number of tasks that can be queued before new tasks are rejected."
    )
    admission_timeout: PositiveInt = Field(
        default=5, description="Maximum time (in seconds) to wait for a task to be admitted before timing out."
    )
    queue_slot_timeout: PositiveInt = Field(
        default=600,
        description="Maximum time (in seconds) to wait for a queue slot to become available before timing out.",
    )

    signal_flush_interval: PositiveFloat = Field(default=0.1, description="Interval for flushing signals")
    signal_max_batch_size: PositiveInt = Field(default=50, description="Maximum batch size for signals")
    signal_max_retries: NonNegativeInt = Field(default=3, description="Number of retries for sending signals")
    signal_send_backoff_ms: PositiveFloat = Field(default=100.0, description="Backoff in ms for sending signals")
    signal_poll_interval: PositiveFloat = Field(default=1.0, description="Interval for polling signals")
    signal_initial_poll_interval: PositiveFloat = Field(default=0.1, description="Initial interval for polling signals")
    grpc_timeout: PositiveFloat = Field(default=30.0, description="gRPC timeout for task manager operations")
    poll_timeout: PositiveFloat = Field(default=1.0, description="Timeout for polling operations")

    # ── Functions ─────────────────────────────────────────────────────────────────── #

    def __init__(self, **values: Any) -> None:
        """Default constructor."""
        super().__init__(**values)
