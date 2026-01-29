"""Types for module models - backward compatibility re-exports.

This module re-exports types from their new locations for backward compatibility.
New code should import directly from the specific modules:
- digitalkin.models.module.base_types for DataTrigger, DataModel, TypeVars
- digitalkin.models.module.setup_types for SetupModel
"""

from digitalkin.models.module.base_types import (
    DataModel,
    DataTrigger,
    DataTriggerT,
    InputModelT,
    OutputModelT,
    SecretModelT,
    SetupModelT,
)
from digitalkin.models.module.setup_types import SetupModel

__all__ = [
    "DataModel",
    "DataTrigger",
    "DataTriggerT",
    "InputModelT",
    "OutputModelT",
    "SecretModelT",
    "SetupModel",
    "SetupModelT",
]
