"""DigitalKin SDK!

This package implements the DigitalKin agentic mesh standards.
"""

from digitalkin.__version__ import __version__
from digitalkin.models import Module

# Import key components to make them available at the package level
from digitalkin.modules import ArchetypeModule, ToolModule, TriggerModule

__all__ = [
    "ArchetypeModule",
    "Module",
    "ToolModule",
    "TriggerModule",
    "__version__",
]
