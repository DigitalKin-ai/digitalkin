"""This module contains the models for the modules."""

# Import module_types first to avoid circular import with ag_ui
# Note: AgUiEventOutput and AgUiOutput are not imported here to avoid circular imports.
# Import them directly from digitalkin.models.module.ag_ui if needed.
from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.module_types import (
    DataModel,
    DataTrigger,
    SetupModel,
)
from digitalkin.models.module.request_metadata import RequestMetadata
from digitalkin.models.module.select_schema import SelectSchema
from digitalkin.models.module.tool_cache import (
    ToolCache,
    ToolDefinition,
    ToolModuleInfo,
    ToolParameter,
)
from digitalkin.models.module.tool_reference import (
    ToolReference,
    ToolSelection,
    tool_reference_input,
)
from digitalkin.models.module.utility import (
    EndOfStreamOutput,
    ModuleStartInfoOutput,
    UtilityProtocol,
    UtilityRegistry,
)

__all__ = [
    # Note: AgUiEventOutput and AgUiOutput removed to avoid circular imports
    # Import them directly from digitalkin.models.module.ag_ui if needed
    "DataModel",
    "DataTrigger",
    "EndOfStreamOutput",
    "ModuleContext",
    "ModuleStartInfoOutput",
    "RequestMetadata",
    "SelectSchema",
    "SetupModel",
    "ToolCache",
    "ToolDefinition",
    "ToolModuleInfo",
    "ToolParameter",
    "ToolReference",
    "ToolSelection",
    "UtilityProtocol",
    "UtilityRegistry",
    "tool_reference_input",
]
