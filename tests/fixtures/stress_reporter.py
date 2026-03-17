"""Formatted live output for stress and performance tests.

Usage:
    report = StressReporter("Memory Scaling (5 -> 20 tasks)")
    report.metric("Baseline", StressReporter.mem(baseline))
    report.metric("Ratio", StressReporter.ratio(2.3))
    report.result(passed)
"""


class StressReporter:
    """Prints formatted metrics to stdout during test execution.

    Output is visible with ``pytest -s`` (live) or in captured stdout on failure.
    """

    def __init__(self, title: str, width: int = 56) -> None:
        """Print section header.

        Args:
            title: Section title.
            width: Bar width in characters.
        """
        self._width = width
        bar = "\u2500" * width
        print(f"\n  {bar}\n  {title}\n  {bar}", flush=True)  # noqa: T201

    def metric(self, label: str, value: str) -> None:
        """Print a single metric line.

        Args:
            label: Left-aligned label.
            value: Right-aligned value string.
        """
        print(f"  {label:<36} {value:>18}", flush=True)  # noqa: T201

    def spacer(self) -> None:
        """Print an empty line."""
        print(flush=True)  # noqa: T201

    def result(self, passed: bool) -> None:
        """Print closing bar and verdict.

        Args:
            passed: True for PASS, False for FAIL.
        """
        bar = "\u2500" * self._width
        verdict = "PASS" if passed else "FAIL"
        print(f"  {bar}\n  {verdict}\n", flush=True)  # noqa: T201

    # ------------------------------------------------------------------
    # Value formatters
    # ------------------------------------------------------------------

    @staticmethod
    def mem(value: int | float) -> str:
        """Format bytes as human-readable size."""
        if abs(value) >= 1024 * 1024:
            return f"{value / 1024 / 1024:.2f} MB"
        if abs(value) >= 1024:
            return f"{value / 1024:.2f} KB"
        return f"{value:.0f} B"

    @staticmethod
    def pct(value: float) -> str:
        """Format as percentage."""
        return f"{value:.1f}%"

    @staticmethod
    def ratio(value: float) -> str:
        """Format as multiplier."""
        return f"{value:.2f}x"

    @staticmethod
    def count(value: int) -> str:
        """Format integer with thousand separators."""
        return f"{value:,}"

    @staticmethod
    def duration(seconds: float) -> str:
        """Format seconds as human-readable duration."""
        if seconds >= 1.0:
            return f"{seconds:.2f}s"
        return f"{seconds * 1000:.1f}ms"

    @staticmethod
    def throughput(ops: int, seconds: float) -> str:
        """Format as operations per second."""
        if seconds <= 0:
            return "N/A"
        return f"{ops / seconds:,.0f} ops/s"
