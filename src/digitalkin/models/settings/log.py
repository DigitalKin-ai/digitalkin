"""Logging configuration settings."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class LoggingSettings(BaseSettings):
    """Logging configuration for the digitalkin logger.

    Env prefix ``DIGITALKIN_LOG_``; ``railway_service_name`` reads the
    unprefixed ``RAILWAY_SERVICE_NAME`` injected by the Railway platform.
    """

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_LOG_", case_sensitive=False)

    level: str = Field(default="INFO", description="Console log level for the digitalkin logger.")
    file_level: str = Field(default="DEBUG", description="Log level for the rotating file handler.")
    dir: str = Field(default="", description="Directory for rotating file logs. Empty disables file logging.")
    file: str = Field(default="", description="Explicit log file path. Empty derives '<dir>/<logger name>.log'.")
    railway_service_name: str | None = Field(
        default=None,
        validation_alias="RAILWAY_SERVICE_NAME",
        description="Railway platform service name. Presence flags a production environment.",
    )
