"""This module is responsible for handling the storage service."""

from digitalkin.services.storage.storage_default import DefaultStorage
from digitalkin.services.storage.storage_grpc import GrpcStorage
from digitalkin.services.storage.storage_strategy import StorageStrategy

__all__ = ["DefaultStorage", "GrpcStorage", "StorageStrategy"]
