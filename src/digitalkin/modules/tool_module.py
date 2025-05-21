"""ToolModule extends BaseModule to implement specific module types."""

from abc import ABC

from digitalkin.models.module import InputModelT, OutputModelT, SecretModelT, SetupModelT
from digitalkin.models.module.module_types import ConfigSetupModelT
from digitalkin.modules._base_module import BaseModule # type: ignore


class ToolModule(BaseModule[InputModelT, OutputModelT, SetupModelT, SecretModelT,ConfigSetupModelT,], ABC):
    """ToolModule extends BaseModule to implement specific module types."""
