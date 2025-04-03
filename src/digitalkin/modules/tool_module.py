"""ToolModule extends BaseModule to implement specific module types."""

from abc import ABC

from digitalkin.modules._base_module import BaseModule, InputModelT, OutputModelT, SecretModelT, SetupModelT


class ToolModule(BaseModule[InputModelT, OutputModelT, SetupModelT, SecretModelT], ABC):
    """ToolModule extends BaseModule to implement specific module types."""
