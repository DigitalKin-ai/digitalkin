"""Module model."""

from enum import Enum, auto

from pydantic import BaseModel


class ModuleStatus(Enum):
    """Possible module's state."""

    CREATED = auto()  # Module created but not started
    STARTING = auto()  # Module is starting
    RUNNING = auto()  # Module do run
    STOPPING = auto()  # Module is stopping
    STOPPED = auto()  # Module stop successfuly
    FAILED = auto()  # Module stopped due to internal error
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
