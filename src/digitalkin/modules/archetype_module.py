"""ArchetypeModule extends BaseModule to implement specific module types."""

from abc import ABC

from digitalkin.models.module import InputModelT, OutputModelT, SecretModelT, SetupModelT
from digitalkin.modules._base_module import BaseModule


class ArchetypeModule(BaseModule[InputModelT, OutputModelT, SetupModelT, SecretModelT], ABC):
    """ArchetypeModule extends BaseModule to implement specific module types."""
