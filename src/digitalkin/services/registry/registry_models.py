"""Registry data models.

This module contains Pydantic models for registry service data structures.
"""

from pydantic import BaseModel

from digitalkin.models.services.registry import RegistryModuleStatus


class ModuleStatusInfo(BaseModel):
    """Module status response."""

    module_id: str
    status: RegistryModuleStatus
