"""This module contains the models for the modules."""

from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.module_types import (
    DataModel,
    DataTrigger,
    SetupModel,
)
from digitalkin.models.module.utility import (
    EndOfStreamOutput,
    UtilityProtocol,
    UtilityRegistry,
)

__all__ = [
    # Core types (used by all SDK users)
    "DataModel",
    "DataTrigger",
    # Utility (commonly used)
    "EndOfStreamOutput",
    "ModuleContext",
    "SetupModel",
    "UtilityProtocol",
    "UtilityRegistry",
]
