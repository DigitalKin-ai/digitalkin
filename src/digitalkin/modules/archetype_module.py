"""ArchetypeModule extends BaseModule to implement specific module types."""

from abc import ABC
from typing import ClassVar

from digitalkin.models.module.module_types import (
    InputModelT,
    OutputModelT,
    SecretModelT,
    SetupModelT,
)
from digitalkin.models.services.registry import RegistryModuleType
from digitalkin.modules._base_module import BaseModule


class ArchetypeModule(
    BaseModule[
        InputModelT,
        OutputModelT,
        SetupModelT,
        SecretModelT,
    ],
    ABC,
):
    """ArchetypeModule extends BaseModule to implement specific module types."""

    # Archetype modules compose tools — they resolve a tool cache. See BaseModule.
    _builds_tool_cache: ClassVar[bool] = True
    registry_type: ClassVar[RegistryModuleType] = RegistryModuleType.ARCHETYPE
