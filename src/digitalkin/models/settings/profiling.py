"""Profiling settings for task execution and asyncio inspection."""

from enum import Enum
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ProfilingSettings(BaseSettings):
    """Profiling and debugging configuration.

    Env vars: DIGITALKIN_PROFILER, DIGITALKIN_PROFILE_OUTPUT_DIR,
    DIGITALKIN_ASYNCIO_INSPECTOR, DIGITALKIN_ASYNCIO_INSPECTOR_PORT.
    """

    model_config = SettingsConfigDict(env_prefix="DIGITALKIN_", case_sensitive=False)

    profiler: str = Field(default="none", description="Profiler backend (none, pyinstrument, yappi, viztracer)")
    profile_output_dir: str = Field(default="./profiles", description="Directory for profile output files")
    uvloop: bool = Field(default=False, description="Enable uvloop event loop policy")
    profiler_keep_n: int = Field(default=100, description="Number of recent profile files to keep before rotation")


class ProfilerMode(str, Enum):
    """Profiler backend selection."""

    NONE = "none"
    VIZTRACER = "viztracer"
    YAPPI = "yappi"
    PYINSTRUMENT = "pyinstrument"


@lru_cache(maxsize=1)
def get_profiling_settings() -> ProfilingSettings:
    """Process-wide ``ProfilingSettings`` singleton.

    Tests must call ``get_profiling_settings.cache_clear()`` after mutating env.

    Returns:
        The shared ``ProfilingSettings`` instance.
    """
    return ProfilingSettings()
