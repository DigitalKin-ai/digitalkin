from pydantic_settings import BaseSettings, SettingsConfigDict


class TaskSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SERVER_TASK_", extra="forbid", arbitrary_types_allowed=True, validate_assignment=True)