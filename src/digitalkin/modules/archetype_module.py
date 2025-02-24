"""ArchetypeModule extends BaseModule to implement specific module types."""

from abc import ABC

from ._base_module import BaseModule


class ArchetypeModule(BaseModule, ABC):
    """ArchetypeModule extends BaseModule to implement specific module types."""

    def __init__(self, metadata):
        """Initialize the module with the given metadata."""
        super().__init__(metadata)
        self.capabilities = ["archetype"]

    def execute(self, input_data):
        """Execute the module with the given input data."""
        raise NotImplementedError
