"""BaseModule is the abstract base for all modules in the DigitalKin SDK."""

from abc import ABC, abstractmethod


class BaseModule(ABC):
    """BaseModule is the abstract base for all modules in the DigitalKin SDK."""

    def __init__(self, metadata):
        """Initialize the module with the given metadata."""
        self.metadata = metadata
        self.capabilities = []

    @abstractmethod
    def execute(self, input_data):
        """Execute the module with the given input data."""
        raise NotImplementedError
