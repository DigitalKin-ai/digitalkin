"""DigitalKin SDK!

This package implements the DigitalKin agentic mesh standards.
"""

from digitalkin.__version__ import __version__

# Import key components to make them available at the package level
from digitalkin.modules.archetype_module import ArchetypeModule
from digitalkin.modules.tool_module import ToolModule
from digitalkin.modules.trigger_module import TriggerModule

__all__ = [
    "ArchetypeModule",
    "ToolModule",
    "TriggerModule",
    "__version__",
]
