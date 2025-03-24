"""ArchetypeModule extends BaseModule to implement specific module types."""

from abc import ABC

from digitalkin.modules._base_module import BaseModule


class ArchetypeModule(BaseModule, ABC):
    """ArchetypeModule extends BaseModule to implement specific module types."""

    def __init__(self, name: str | None = None) -> None:
        """Initialize the module with the given metadata."""
        super().__init__(self.job_id, name=name)
        self.capabilities = ["archetype"]
