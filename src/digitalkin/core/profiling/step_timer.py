"""Zero-alloc step timer for latency audit.

Instrument the dispatch hot path with named ns-resolution marks. One call
emits one log line: ``prefix: step1=Xms step2=Yms ... total=Zms task_id=...``.

Usage:

    timer = StepTimer()
    timer.mark("validate")
    timer.mark("registry_lookup")
    ...
    timer.log("dispatch", task_id)
"""

from __future__ import annotations

import time

from digitalkin.logger import logger


class StepTimer:
    """Lightweight step timer. ``perf_counter_ns()`` resolution.

    Designed for the audit hot path — no allocations beyond a list of
    ``(name, ns)`` tuples. Idiomatic call:

        t = StepTimer()
        t.mark("a"); t.mark("b"); t.mark("c")
        t.log("dispatch", task_id="abc")
    """

    __slots__ = ("_last", "_steps", "_t0")

    def __init__(self) -> None:
        """Init start time."""
        now = time.perf_counter_ns()
        self._t0 = now
        self._last = now
        self._steps: list[tuple[str, int]] = []

    def mark(self, name: str) -> None:
        """Record a step with its delta from the previous mark."""
        now = time.perf_counter_ns()
        self._steps.append((name, now - self._last))
        self._last = now

    def log(self, prefix: str, task_id: str = "") -> None:
        """Emit one info line with all step deltas + total."""
        parts = [f"{name}={ns / 1e6:.2f}ms" for name, ns in self._steps]
        total = (self._last - self._t0) / 1e6
        parts.append(f"total={total:.2f}ms")
        if task_id:
            logger.info("[lat-audit] %s: %s task_id=%s", prefix, " ".join(parts), task_id)
        else:
            logger.info("[lat-audit] %s: %s", prefix, " ".join(parts))

    def total_ms(self) -> float:
        """Total elapsed time across all marks in milliseconds.

        Returns:
            float: time elapsed in ms
        """
        return (self._last - self._t0) / 1e6

    def elapsed_now_ms(self) -> float:
        """Elapsed ms since ``__init__``, independent of mark cadence.

        Returns:
            float: time elapsed in ms at call time.
        """
        return (time.perf_counter_ns() - self._t0) / 1e6

    def format_steps(self) -> str:
        """Render recorded marks as ``name=X.XXms ...`` (no total, no prefix).

        Returns:
            str: space-separated ``name=delta_ms`` pairs.
        """
        return " ".join(f"{name}={ns / 1e6:.2f}ms" for name, ns in self._steps)
