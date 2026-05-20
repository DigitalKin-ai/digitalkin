"""Task and job manager settings."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from digitalkin.models.core.job_manager_models import BackpressureStrategy


class TaskManagerSettings(BaseSettings):
    """Concurrency and admission limits for BaseTaskManager."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_TASK_MANAGER_", case_sensitive=False)

    max_concurrent_tasks: int = Field(default=500, description="Max tasks executing concurrently.")
    task_wait_timeout: float = Field(default=30.0, description="Seconds a caller waits for an execution slot.")
    stream_drain_timeout: float = Field(default=2.0, description="Seconds to drain a stream on task teardown.")
    max_queued_tasks: int = Field(default=5000, description="Max tasks admitted and waiting for a slot.")
    admission_timeout: float = Field(default=5.0, description="Seconds a task waits for system admission.")
    queue_slot_timeout: float = Field(default=600.0, description="Max seconds an admitted task waits in the queue.")


class JobManagerSettings(BaseSettings):
    """Timeouts and backpressure for SingleJobManager."""

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_JOB_MANAGER_", case_sensitive=False)

    config_setup_timeout: float = Field(default=30.0, description="Max seconds for module config-setup.")
    backpressure_strategy: BackpressureStrategy = Field(
        default=BackpressureStrategy("block"), description="Output-queue backpressure strategy."
    )
    backpressure_timeout: float = Field(default=300.0, description="Max seconds to wait under backpressure.")
