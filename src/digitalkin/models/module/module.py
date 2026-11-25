"""Module model."""

from enum import Enum, auto
from typing import Any

from pydantic import BaseModel, Field


class ModuleCodeModel(BaseModel):
    """typed error/code model."""

    code: str = Field(...)
    message: str | None = Field(default=None)
    short_description: str | None = Field(default=None)
    retryable: bool | None = Field(default=None)

    @staticmethod
    def failure_text(frame: dict[str, Any]) -> str | None:
        """Agent-facing text of a module failure frame read from a stream.

        Args:
            frame: One stream frame as a dict.

        Returns:
            ``[CODE] message`` with the retry hint, or None when the frame is not a ``ModuleCodeModel``.
        """
        code = frame.get("code")
        if "root" in frame or not isinstance(code, str):
            return None
        retryable = frame.get("retryable")
        hint = " Retrying may succeed." if retryable is True else ""
        if retryable is False:
            hint = " Retrying the same call fails again."
        return f"[{code}] {frame.get('message') or frame.get('short_description') or 'module failed'}{hint}"


class ModuleStatus(Enum):
    """Possible module's state."""

    CREATED = auto()  # Module created but not started
    STARTING = auto()  # Module is starting
    RUNNING = auto()  # Module do run
    STOPPING = auto()  # Module is stopping
    STOPPED = auto()  # Module stop successfuly
    FAILED = auto()  # Module stopped due to internal error
    CANCELLED = auto()  # Module stopped due to internal error
    NOT_FOUND = auto()


class Module(BaseModel):
    """Module model."""

    name: str
    cost_schema: str
    input_schema: str
    output_schema: str
    setup_schema: str
    secret_schema: str
    type: str
    version: str
    description: str
