"""TriggerModule extends BaseModule to implement specific module types."""

from abc import ABC

from ._base_module import BaseModule


class TriggerModule(BaseModule, ABC):
    """TriggerModule extends BaseModule to implement specific module types."""

    def __init__(self, metadata):
        """Initialize the module with the given metadata."""
        super().__init__(metadata)
        self.capabilities = ["trigger"]
