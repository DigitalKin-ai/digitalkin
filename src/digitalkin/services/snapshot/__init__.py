"""This module is responsible for handling the snapshot service."""

from .default_snapshot import DefaultSnapshot
from .snapshot_strategy import SnapshotStrategy

__all__ = ["DefaultSnapshot", "SnapshotStrategy"]
