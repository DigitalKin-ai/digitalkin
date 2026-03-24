"""ToolModule extends BaseModule to implement specific module types."""

from abc import ABC

from digitalkin.models.module.module_types import (
    InputModelT,
    OutputModelT,
    SecretModelT,
    SetupModelT,
)
from digitalkin.modules._base_module import BaseModule  # Private module import for SDK subclass # type: ignore


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

    tags = ["tool"]
