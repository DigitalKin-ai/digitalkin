"""Tests for metrics collection.

Run with: python -m pytest tests/test_metrics.py
"""

import sys
from pathlib import Path

import pytest

# Add the parent directory to the path so we can import digitalkin_observability
sys.path.insert(0, str(Path(__file__).parent.parent))

from digitalkin_observability import MetricsCollector, PrometheusExporter, get_metrics


class TestMetricsCollector:
    """Tests for MetricsCollector singleton."""

    def setup_method(self) -> None:
        """Reset metrics before each test."""
        get_metrics().reset()

    def test_singleton_returns_same_instance(self) -> None:
        """Test that get_metrics returns the same instance."""
        m1 = get_metrics()
        m2 = get_metrics()
        assert m1 is m2

    def test_inc_jobs_started(self) -> None:
        """Test incrementing jobs started counter."""
        metrics = get_metrics()
        metrics.inc_jobs_started("TestModule")

        assert metrics.jobs_started_total == 1
        assert metrics.active_jobs == 1

    def test_inc_jobs_completed(self) -> None:
        """Test incrementing jobs completed counter."""
        metrics = get_metrics()
        metrics.inc_jobs_started("TestModule")
        metrics.inc_jobs_completed("TestModule", 1.5)

        assert metrics.jobs_completed_total == 1
        assert metrics.active_jobs == 0
        assert metrics.job_duration_seconds.count == 1
        assert metrics.job_duration_seconds.total_sum == 1.5

    def test_inc_jobs_failed(self) -> None:
        """Test incrementing jobs failed counter."""
        metrics = get_metrics()
        metrics.inc_jobs_started("TestModule")
        metrics.inc_jobs_failed("TestModule")

        assert metrics.jobs_failed_total == 1
        assert metrics.active_jobs == 0

    def test_inc_jobs_cancelled(self) -> None:
        """Test incrementing jobs cancelled counter."""
        metrics = get_metrics()
        metrics.inc_jobs_started("TestModule")
        metrics.inc_jobs_cancelled("TestModule")

        assert metrics.jobs_cancelled_total == 1
        assert metrics.active_jobs == 0

    def test_inc_messages_sent(self) -> None:
        """Test incrementing messages sent counter."""
        metrics = get_metrics()
        metrics.inc_messages_sent("message")
        metrics.inc_messages_sent("file")
        metrics.inc_messages_sent()

        assert metrics.messages_sent_total == 3

    def test_queue_depth_tracking(self) -> None:
        """Test queue depth tracking."""
        metrics = get_metrics()
        metrics.set_queue_depth("job1", 5)
        metrics.set_queue_depth("job2", 3)

        assert metrics.queue_depth["job1"] == 5
        assert metrics.queue_depth["job2"] == 3

        metrics.clear_queue_depth("job1")
        assert "job1" not in metrics.queue_depth

    def test_snapshot(self) -> None:
        """Test snapshot returns all metrics."""
        metrics = get_metrics()
        metrics.inc_jobs_started("TestModule")
        metrics.inc_jobs_completed("TestModule", 0.5)
        metrics.inc_messages_sent("message")

        snapshot = metrics.snapshot()

        assert snapshot["jobs_started_total"] == 1
        assert snapshot["jobs_completed_total"] == 1
        assert snapshot["messages_sent_total"] == 1
        assert "job_duration_seconds" in snapshot
        assert "by_module" in snapshot
        assert "TestModule" in snapshot["by_module"]

    def test_histogram_observe(self) -> None:
        """Test histogram observations."""
        metrics = get_metrics()
        metrics.observe_grpc_duration(0.05)
        metrics.observe_grpc_duration(0.15)

        assert metrics.grpc_request_duration_seconds.count == 2
        assert metrics.grpc_request_duration_seconds.total_sum == pytest.approx(0.2)

    def test_reset_clears_all_metrics(self) -> None:
        """Test reset clears all metrics."""
        metrics = get_metrics()
        metrics.inc_jobs_started("TestModule")
        metrics.inc_errors()

        metrics.reset()

        assert metrics.jobs_started_total == 0
        assert metrics.errors_total == 0
        assert metrics.active_jobs == 0


class TestPrometheusExporter:
    """Tests for Prometheus exporter."""

    def setup_method(self) -> None:
        """Reset metrics before each test."""
        get_metrics().reset()

    def test_export_returns_string(self) -> None:
        """Test that export returns a string."""
        output = PrometheusExporter.export()
        assert isinstance(output, str)

    def test_export_contains_job_counters(self) -> None:
        """Test export contains job counters."""
        metrics = get_metrics()
        metrics.inc_jobs_started("TestModule")

        output = PrometheusExporter.export()

        assert "digitalkin_jobs_started_total 1" in output
        assert "digitalkin_active_jobs 1" in output

    def test_export_contains_histogram(self) -> None:
        """Test export contains histogram data."""
        metrics = get_metrics()
        metrics.observe_grpc_duration(0.05)

        output = PrometheusExporter.export()

        assert "digitalkin_grpc_request_duration_seconds" in output
        assert "# TYPE digitalkin_grpc_request_duration_seconds histogram" in output

    def test_export_contains_module_breakdown(self) -> None:
        """Test export contains per-module breakdown."""
        metrics = get_metrics()
        metrics.inc_jobs_started("MyModule")

        output = PrometheusExporter.export()

        assert 'digitalkin_jobs_by_module{module="MyModule",status="started"} 1' in output

    def test_export_contains_help_and_type(self) -> None:
        """Test export contains HELP and TYPE comments."""
        output = PrometheusExporter.export()

        assert "# HELP digitalkin_jobs_started_total" in output
        assert "# TYPE digitalkin_jobs_started_total counter" in output
