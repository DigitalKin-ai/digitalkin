"""Circuit-breaker state model."""

from enum import Enum


class CBState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"
