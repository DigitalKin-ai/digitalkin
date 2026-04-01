"""Flakiness quarantine plugin for pytest.

Tracks pass/fail history per test node over the last N runs.
Tests with flakiness_score > threshold are auto-quarantined (xfail).

Usage:
    # Register in conftest.py:
    pytest_plugins = ["tests.fixtures.flakiness"]

    # Mark known flaky tests:
    @pytest.mark.flaky(max_runs=3, min_passes=1)

    # View flakiness report:
    pytest --flakiness-report
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

HISTORY_FILE = Path(".pytest_flakiness_history.json")
HISTORY_WINDOW = 10
QUARANTINE_THRESHOLD = 0.2


class FlakinessTracker:
    """Tracks pass/fail per test node across runs."""

    _history: dict[str, list[bool]]

    def __init__(self) -> None:
        self._history = {}
        self._load()

    def _load(self) -> None:
        """Load history from disk."""
        if HISTORY_FILE.exists():
            try:
                data = json.loads(HISTORY_FILE.read_text())
                self._history = {k: v[-HISTORY_WINDOW:] for k, v in data.items()}
            except (json.JSONDecodeError, KeyError):
                self._history = {}

    def _save(self) -> None:
        """Persist history to disk."""
        trimmed = {k: v[-HISTORY_WINDOW:] for k, v in self._history.items()}
        HISTORY_FILE.write_text(json.dumps(trimmed, indent=2))

    def record(self, nodeid: str, passed: bool) -> None:
        """Record a test result."""
        if nodeid not in self._history:
            self._history[nodeid] = []
        self._history[nodeid].append(passed)
        self._history[nodeid] = self._history[nodeid][-HISTORY_WINDOW:]

    def flakiness_score(self, nodeid: str) -> float:
        """Compute flakiness score (0.0 = stable, 1.0 = always flaky).

        Score is the ratio of state transitions (pass→fail or fail→pass)
        to total results. A test that alternates every run scores 1.0.

        Args:
            nodeid: Test node ID.

        Returns:
            Flakiness score between 0.0 and 1.0.
        """
        results = self._history.get(nodeid, [])
        if len(results) < 2:
            return 0.0
        transitions = sum(1 for a, b in zip(results, results[1:]) if a != b)
        return transitions / (len(results) - 1)

    def is_quarantined(self, nodeid: str) -> bool:
        """Check if a test should be quarantined.

        Args:
            nodeid: Test node ID.

        Returns:
            True if flakiness score exceeds threshold.
        """
        return self.flakiness_score(nodeid) > QUARANTINE_THRESHOLD

    def save(self) -> None:
        """Persist current history."""
        self._save()

    def report(self) -> dict[str, dict[str, Any]]:
        """Generate flakiness report for all tracked tests.

        Returns:
            Dict of {nodeid: {score, runs, passes, fails, quarantined}}.
        """
        result = {}
        for nodeid, results in self._history.items():
            passes = sum(1 for r in results if r)
            fails = len(results) - passes
            score = self.flakiness_score(nodeid)
            result[nodeid] = {
                "score": round(score, 3),
                "runs": len(results),
                "passes": passes,
                "fails": fails,
                "quarantined": score > QUARANTINE_THRESHOLD,
            }
        return result


# Global tracker instance
_tracker = FlakinessTracker()


def pytest_runtest_makereport(item: Any, call: Any) -> None:
    """Record test results for flakiness tracking."""
    if call.when == "call":
        _tracker.record(item.nodeid, call.excinfo is None)


def pytest_sessionfinish(session: Any, exitstatus: int) -> None:
    """Save flakiness history at end of test session."""
    _tracker.save()


def pytest_collection_modifyitems(config: Any, items: list[Any]) -> None:
    """Auto-quarantine flaky tests by adding xfail marker."""
    for item in items:
        if _tracker.is_quarantined(item.nodeid):
            item.add_marker(pytest.mark.xfail(
                reason=f"Quarantined: flakiness score {_tracker.flakiness_score(item.nodeid):.2f}",
                strict=False,
            ))
