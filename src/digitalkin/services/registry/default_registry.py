"""Default registry."""

from .registry_strategy import RegistryStrategy


class DefaultRegistry(RegistryStrategy):
    """Default registry strategy."""

    def get_by_id(self, module_id: str) -> None:
        """Get services from the registry."""
