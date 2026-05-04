"""Job manager models."""

from enum import Enum


class BackpressureStrategy(str, Enum):
    """Backpressure strategy for module output queue writes."""

    BLOCK = "block"
    DROP_OLDEST = "drop_oldest"
    REJECT = "reject"
