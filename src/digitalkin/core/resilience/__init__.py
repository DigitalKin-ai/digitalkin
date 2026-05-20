"""Resilience patterns for fault tolerance.

- ``WatchdogThread``: OS thread detecting event loop stalls.
- ``Bulkhead``: Per-service concurrency limiter.
- ``SessionReaper``: Background task reaping leaked sessions.
- ``GracefulShutdownHandler``: SIGTERM sequenced shutdown with checkpoint.
- ``StartupRestorer``: Redis checkpoint restore on startup.
"""

from digitalkin.core.exceptions import BulkheadFullError
from digitalkin.core.resilience.bulkhead import Bulkhead
from digitalkin.core.resilience.graceful_shutdown import GracefulShutdownHandler, StartupRestorer
from digitalkin.core.resilience.session_reaper import SessionReaper
from digitalkin.core.resilience.watchdog import WatchdogThread

__all__ = [
    "Bulkhead",
    "BulkheadFullError",
    "GracefulShutdownHandler",
    "SessionReaper",
    "StartupRestorer",
    "WatchdogThread",
]
