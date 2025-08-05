"""ToolModule extends BaseModule to implement specific module types."""

from abc import ABC

from digitalkin.models.module import InputModelT, OutputModelT, SecretModelT, SetupModelT
from digitalkin.modules._base_module import BaseModule # type: ignore


class ToolModule(BaseModule[InputModelT, OutputModelT, SetupModelT, SecretModelT,], ABC):
    """ToolModule extends BaseModule to implement specific module types."""
