"""Core models for Redis task-manager primitives."""

from enum import Enum


class ClaimResult(Enum):
    """Result of an idempotency claim attempt."""

    TAKEN = 0
    CLAIMED = 1
    RECLAIMED = 2
