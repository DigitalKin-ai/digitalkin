from typing import Any

from pydantic import Field, NonNegativeInt, PositiveFloat, PositiveInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class TaskSettings(BaseSettings):
    """Task settings."""

    model_config = SettingsConfigDict(env_prefix="TASK_", case_sensitive=False)

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

    def __init__(self, **values: Any) -> None:
        super().__init__(**values)
