"""This module is responsible for handling the storage service."""

from .default_storage import DefaultStorage
from .storage_strategy import StorageStrategy

__all__ = ["DefaultStorage", "StorageStrategy"]
