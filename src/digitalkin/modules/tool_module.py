"""ToolModule extends BaseModule to implement specific module types."""

from abc import ABC
from typing import Any, ClassVar

from digitalkin.models.module.module_types import (
    InputModelT,
    OutputModelT,
    SecretModelT,
    SetupModelT,
)
from digitalkin.models.module.tool_cache import ToolModuleInfo
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

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Require a ``display_name`` on every tool trigger of the subclass's own ``input_format``.

        Raises:
            TypeError: When a tool trigger declares no ``display_name``.
        """
        super().__init_subclass__(**kwargs)
        input_format = cls.__dict__.get("input_format")
        if input_format is None:
            return
        missing = [
            tool.name
            for tool in ToolModuleInfo._extract_tools_from_schema(input_format.model_json_schema())  # ruff: ignore[private-member-access]
            if tool.display_name is None
        ]
        if missing:
            msg = (
                f"{cls.__name__}: trigger(s) without display_name: {', '.join(missing)}. "
                'Declare it as a plain class variable: display_name = "..." (no type annotation).'
            )
            raise TypeError(msg)
