"""ToolModule extends BaseModule to implement specific module types."""

from abc import ABC
from typing import ClassVar

from digitalkin.models.module.module_types import (
    InputModelT,
    OutputModelT,
    SecretModelT,
    SetupModelT,
)
from digitalkin.models.services.registry import RegistryModuleType
from digitalkin.modules._base_module import BaseModule  # Private module import for SDK subclass


class ToolModule(
    BaseModule[
        InputModelT,
        OutputModelT,
        SetupModelT,
        SecretModelT,
    ],
    ABC,
):
    """ToolModule extends BaseModule to implement specific module types."""

    registry_type: ClassVar[RegistryModuleType] = RegistryModuleType.TOOL_MODULE
