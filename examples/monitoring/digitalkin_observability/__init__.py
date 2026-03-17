"""Standalone observability module for DigitalKin.

This module can be copied into your project and used independently.
It has no dependencies on the digitalkin package.

Usage:
    from digitalkin_observability import (
        MetricsCollector,
        MetricsServer,
        MetricsServerInterceptor,
        PrometheusExporter,
        get_metrics,
        start_metrics_server,
        stop_metrics_server,
    )

    # Start metrics HTTP server
    start_metrics_server(port=8081)

    # Track metrics
    metrics = get_metrics()
    metrics.inc_jobs_started("my_module")
    metrics.inc_jobs_completed("my_module", duration=1.5)

    # Export to Prometheus format
    print(PrometheusExporter.export())
"""

from digitalkin_observability.http_server import (
    MetricsServer,
    start_metrics_server,
    stop_metrics_server,
)
from digitalkin_observability.interceptors import MetricsServerInterceptor
from digitalkin_observability.metrics import MetricsCollector, get_metrics
from digitalkin_observability.prometheus import PrometheusExporter

__all__ = [
    "MetricsCollector",
    "MetricsServer",
    "MetricsServerInterceptor",
    "PrometheusExporter",
    "get_metrics",
    "start_metrics_server",
    "stop_metrics_server",
]
