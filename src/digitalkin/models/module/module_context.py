"""Define the module context used in the triggers."""

from types import SimpleNamespace

from digitalkin.services.cost.cost_strategy import CostStrategy
from digitalkin.services.filesystem.filesystem_strategy import FilesystemStrategy
from digitalkin.services.storage.storage_strategy import StorageStrategy


class ModuleContext(SimpleNamespace):
    """ModuleContext provides a container for strategies and resources used by a module.

    Attributes:
        cost (CostStrategy): The strategy used to calculate or manage costs within the module.
        filesystem (FilesystemStrategy): The strategy for interacting with the filesystem.
        storage (StorageStrategy): The strategy for handling storage operations.

    This context object is designed to be passed to module components, providing them with
    access to shared strategies and resources. Additional attributes may be set dynamically.
    """

    cost: CostStrategy
    filesystem: FilesystemStrategy
    storage: StorageStrategy
