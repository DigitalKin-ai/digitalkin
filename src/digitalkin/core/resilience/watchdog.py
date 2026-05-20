"""WatchdogThread — OS thread detecting asyncio event loop stalls.

The only non-asyncio component in the system. Runs as a daemon thread
that checks whether the event loop is making progress. If the loop stalls
for longer than ``stall_threshold`` seconds (default 5), the watchdog:

1. Logs all current asyncio tasks (stack dump).
2. Sends SIGTERM to allow graceful shutdown.
3. If still alive after 10s, sends SIGKILL.

The health counter is incremented by the event loop via ``call_soon``
on every watchdog tick. If the counter stops incrementing, the loop is stalled.
"""

from __future__ import annotations

import asyncio
import os
import signal
import threading
import time

from digitalkin.logger import logger
from digitalkin.models.settings.resilience import ResilienceSettings


class WatchdogThread:
    """Daemon thread monitoring event loop health.

    Usage::

        watchdog = WatchdogThread(loop)
        watchdog.start()
        # ... run event loop ...
        watchdog.stop()
    """

    _loop: asyncio.AbstractEventLoop
    _stall_threshold: float
    _check_interval: float
    _thread: threading.Thread | None
    _running: bool
    _counter: int
    _last_seen: int

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        stall_threshold: float | None = None,
        check_interval: float | None = None,
    ) -> None:
        """Initialize the watchdog.

        Args:
            loop: The asyncio event loop to monitor.
            stall_threshold: Seconds without progress before declaring stall.
                Defaults to ResilienceSettings.watchdog_stall_threshold.
            check_interval: Seconds between health checks.
                Defaults to ResilienceSettings.watchdog_check_interval.
        """
        settings = ResilienceSettings()
        self._loop = loop
        self._stall_threshold = stall_threshold if stall_threshold is not None else settings.watchdog_stall_threshold
        self._check_interval = check_interval if check_interval is not None else settings.watchdog_check_interval
        self._thread = None
        self._running = False
        self._counter = 0
        self._last_seen = 0

    def _increment(self) -> None:
        """Called from the event loop via call_soon — proves the loop is alive."""
        self._counter += 1

    def _check_loop(self) -> None:
        """Watchdog tick: schedule increment on the event loop, check progress."""
        while self._running:
            time.sleep(self._check_interval)

            if not self._running:
                break

            # Schedule health probe on the event loop (threadsafe)
            try:
                self._loop.call_soon_threadsafe(self._increment)
            except RuntimeError:
                # Loop closed — stop watching
                logger.info("Event loop closed, watchdog stopping")
                break

            # Check if counter advanced
            if self._counter == self._last_seen:
                stall_duration = self._check_interval
                # Counter didn't advance — loop might be stalled
                # Wait for full threshold before acting
                stall_start = time.monotonic()
                while self._running and self._counter == self._last_seen:
                    time.sleep(self._check_interval)
                    stall_duration = time.monotonic() - stall_start
                    if stall_duration >= self._stall_threshold:
                        self._on_stall(stall_duration)
                        return

            self._last_seen = self._counter

    def _on_stall(self, duration: float) -> None:
        """Handle a detected event loop stall.

        Args:
            duration: How long the loop has been stalled (seconds).
        """
        logger.critical(
            "Event loop stall detected: %.1fs without progress (threshold=%.1fs)",
            duration,
            self._stall_threshold,
        )

        # Dump all asyncio tasks for debugging
        try:
            tasks = asyncio.all_tasks(self._loop)
            logger.critical("Active tasks at stall (%d total):", len(tasks))
            for task in tasks:
                logger.critical("  %s: %s", task.get_name(), task.get_coro())
        except RuntimeError:
            logger.critical("Could not enumerate tasks (loop may be closing)")

        # SIGTERM — give the process a chance to shutdown gracefully
        pid = os.getpid()
        logger.critical("Sending SIGTERM to pid %d", pid)
        os.kill(pid, signal.SIGTERM)

        # Wait for graceful shutdown. Do NOT SIGKILL —
        # let the orchestrator (Docker, K8s) handle escalation to avoid
        # corrupting shared resources (Redis, files, locks).
        time.sleep(10)
        if self._running:
            logger.critical("Process still alive 10s after SIGTERM — orchestrator should escalate")
            self._running = False

    def start(self) -> None:
        """Start the watchdog daemon thread."""
        if self._thread is not None and self._thread.is_alive():
            return

        self._running = True
        self._counter = 0
        self._last_seen = 0
        self._thread = threading.Thread(
            target=self._check_loop,
            name="watchdog",
            daemon=True,
        )
        self._thread.start()
        logger.info(
            "WatchdogThread started (threshold=%.1fs, interval=%.1fs)",
            self._stall_threshold,
            self._check_interval,
        )

    def stop(self) -> None:
        """Stop the watchdog thread."""
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=self._check_interval * 2)
            self._thread = None
        logger.info("WatchdogThread stopped")

    @property
    def is_alive(self) -> bool:
        """Whether the watchdog thread is running."""
        return self._thread is not None and self._thread.is_alive()
