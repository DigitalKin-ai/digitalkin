"""Core metrics collection for DigitalKin.

This module provides a thread-safe singleton MetricsCollector that tracks
various metrics about job execution, gRPC requests, and system performance.

No external dependencies required.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from typing import Any


@dataclass
class Histogram:
    """Simple histogram with configurable buckets."""

    buckets: tuple[float, ...] = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
    counts: dict[float, int] = field(default_factory=lambda: defaultdict(int))
    total_sum: float = 0.0
    count: int = 0

    def observe(self, value: float) -> None:
        """Record an observation in the histogram."""
        self.total_sum += value
        self.count += 1
        for bucket in self.buckets:
            if value <= bucket:
                self.counts[bucket] += 1

    def reset(self) -> None:
        """Reset histogram state."""
        self.counts = defaultdict(int)
        self.total_sum = 0.0
        self.count = 0


class MetricsCollector:
    """Thread-safe singleton metrics collector.

    Collects various metrics about job execution, gRPC requests,
    and system performance. Designed to be stateless per-request
    while maintaining aggregate counters.

    Usage:
        metrics = MetricsCollector()  # or get_metrics()
        metrics.inc_jobs_started("my_module")
        metrics.inc_jobs_completed("my_module", duration=1.5)
        print(metrics.snapshot())
    """

    _instance: ClassVar[MetricsCollector | None] = None
    _lock: ClassVar[Lock] = Lock()

    def __new__(cls) -> "MetricsCollector":
        """Create or return the singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    instance = super().__new__(cls)
                    instance._init_metrics()
                    cls._instance = instance
        return cls._instance

    def _init_metrics(self) -> None:
        """Initialize all metric storage."""
        # Counters
        self.jobs_started_total: int = 0
        self.jobs_completed_total: int = 0
        self.jobs_failed_total: int = 0
        self.jobs_cancelled_total: int = 0
        self.messages_sent_total: int = 0
        self.heartbeats_sent_total: int = 0
        self.errors_total: int = 0

        # Gauges
        self.active_jobs: int = 0
        self.active_connections: int = 0
        self.queue_depth: dict[str, int] = {}

        # Histograms
        self.job_duration_seconds = Histogram()
        self.message_latency_seconds = Histogram()
        self.grpc_request_duration_seconds = Histogram()

        # Labels for breakdown
        self._by_module: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._by_protocol: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

        # Instance lock for thread safety
        self._instance_lock = Lock()

    def inc_jobs_started(self, module_name: str) -> None:
        """Increment jobs started counter."""
        with self._instance_lock:
            self.jobs_started_total += 1
            self.active_jobs += 1
            self._by_module[module_name]["started"] += 1

    def inc_jobs_completed(self, module_name: str, duration: float) -> None:
        """Increment jobs completed counter and record duration."""
        with self._instance_lock:
            self.jobs_completed_total += 1
            self.active_jobs = max(0, self.active_jobs - 1)
            self._by_module[module_name]["completed"] += 1
            self.job_duration_seconds.observe(duration)

    def inc_jobs_failed(self, module_name: str) -> None:
        """Increment jobs failed counter."""
        with self._instance_lock:
            self.jobs_failed_total += 1
            self.active_jobs = max(0, self.active_jobs - 1)
            self._by_module[module_name]["failed"] += 1

    def inc_jobs_cancelled(self, module_name: str) -> None:
        """Increment jobs cancelled counter."""
        with self._instance_lock:
            self.jobs_cancelled_total += 1
            self.active_jobs = max(0, self.active_jobs - 1)
            self._by_module[module_name]["cancelled"] += 1

    def inc_messages_sent(self, protocol: str | None = None) -> None:
        """Increment messages sent counter."""
        with self._instance_lock:
            self.messages_sent_total += 1
            if protocol:
                self._by_protocol[protocol]["messages"] += 1

    def inc_heartbeats_sent(self) -> None:
        """Increment heartbeats sent counter."""
        with self._instance_lock:
            self.heartbeats_sent_total += 1

    def inc_errors(self) -> None:
        """Increment errors counter."""
        with self._instance_lock:
            self.errors_total += 1

    def set_queue_depth(self, job_id: str, depth: int) -> None:
        """Set the queue depth for a job."""
        with self._instance_lock:
            self.queue_depth[job_id] = depth

    def clear_queue_depth(self, job_id: str) -> None:
        """Clear queue depth tracking for a job."""
        with self._instance_lock:
            self.queue_depth.pop(job_id, None)

    def observe_grpc_duration(self, duration: float) -> None:
        """Record a gRPC request duration."""
        with self._instance_lock:
            self.grpc_request_duration_seconds.observe(duration)

    def observe_message_latency(self, latency: float) -> None:
        """Record a message latency."""
        with self._instance_lock:
            self.message_latency_seconds.observe(latency)

    def snapshot(self) -> dict[str, Any]:
        """Return current metrics as dict for export."""
        with self._instance_lock:
            return {
                "jobs_started_total": self.jobs_started_total,
                "jobs_completed_total": self.jobs_completed_total,
                "jobs_failed_total": self.jobs_failed_total,
                "jobs_cancelled_total": self.jobs_cancelled_total,
                "active_jobs": self.active_jobs,
                "messages_sent_total": self.messages_sent_total,
                "heartbeats_sent_total": self.heartbeats_sent_total,
                "errors_total": self.errors_total,
                "active_connections": self.active_connections,
                "total_queue_depth": sum(self.queue_depth.values()),
                "job_duration_seconds": {
                    "count": self.job_duration_seconds.count,
                    "sum": self.job_duration_seconds.total_sum,
                    "buckets": dict(self.job_duration_seconds.counts),
                },
                "grpc_request_duration_seconds": {
                    "count": self.grpc_request_duration_seconds.count,
                    "sum": self.grpc_request_duration_seconds.total_sum,
                    "buckets": dict(self.grpc_request_duration_seconds.counts),
                },
                "by_module": {k: dict(v) for k, v in self._by_module.items()},
                "by_protocol": {k: dict(v) for k, v in self._by_protocol.items()},
            }

    def reset(self) -> None:
        """Reset all metrics. Useful for testing."""
        with self._instance_lock:
            self._init_metrics()


def get_metrics() -> MetricsCollector:
    """Get the global MetricsCollector instance."""
    return MetricsCollector()
