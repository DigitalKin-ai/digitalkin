"""This module is responsible for handling the storage service."""

from digitalkin.services.storage.default_storage import DefaultStorage
from digitalkin.services.storage.grpc_storage import GrpcStorage
from digitalkin.services.storage.storage_strategy import StorageStrategy

__all__ = ["DefaultStorage", "GrpcStorage", "StorageStrategy"]
