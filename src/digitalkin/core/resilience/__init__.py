"""Resilience patterns for fault tolerance.

- ``Bulkhead``: Per-service concurrency limiter.
"""

from digitalkin.core.exceptions import BulkheadFullError
from digitalkin.core.resilience.bulkhead import Bulkhead

__all__ = [
    "Bulkhead",
    "BulkheadFullError",
]
