"""Simple HTTP server for exposing Prometheus metrics.

This module provides an HTTP server that exposes metrics at /metrics endpoint.
No external dependencies required beyond Python standard library.
"""

from __future__ import annotations

import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from typing import Self

logger = logging.getLogger(__name__)


class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP request handler for metrics endpoint."""

    def do_GET(self) -> None:
        """Handle GET requests."""
        if self.path == "/metrics":
            self._serve_metrics()
        elif self.path == "/health":
            self._serve_health()
        else:
            self.send_error(404, "Not Found")

    def _serve_metrics(self) -> None:
        """Serve Prometheus metrics."""
        from digitalkin_observability.prometheus import PrometheusExporter

        content = PrometheusExporter.export()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content.encode("utf-8"))

    def _serve_health(self) -> None:
        """Serve health check."""
        content = '{"status": "ok"}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content.encode("utf-8"))

    def log_message(self, format: str, *args: object) -> None:
        """Suppress default logging."""


class MetricsServer:
    """HTTP server for exposing metrics to Prometheus.

    Usage:
        server = MetricsServer(port=8081)
        server.start()
        # ... run your application ...
        server.stop()

    Or as context manager:
        with MetricsServer(port=8081):
            # ... run your application ...

    Or as async context manager:
        async with MetricsServer(port=8081):
            # ... run your application ...
    """

    instance: ClassVar["MetricsServer | None"] = None

    def __init__(self, host: str = "0.0.0.0", port: int = 8081) -> None:
        """Initialize the metrics server.

        Args:
            host: Host to bind to (default: 0.0.0.0 for all interfaces).
            port: Port to listen on (default: 8081).
        """
        self.host = host
        self.port = port
        self._server: HTTPServer | None = None
        self._thread: Thread | None = None

    def start(self) -> None:
        """Start the metrics server in a background thread."""
        if self._server is not None:
            logger.warning("Metrics server already running")
            return

        self._server = HTTPServer((self.host, self.port), MetricsHandler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info(
            "Metrics server started on http://%s:%s/metrics",
            self.host,
            self.port,
        )

    def stop(self) -> None:
        """Stop the metrics server."""
        if self._server is not None:
            self._server.shutdown()
            self._server = None
            self._thread = None
            logger.info("Metrics server stopped")

    async def __aenter__(self) -> "Self":
        """Async context manager entry."""
        self.start()
        return self

    async def __aexit__(self, *args: object) -> None:
        """Async context manager exit."""
        self.stop()

    def __enter__(self) -> "Self":
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, *args: object) -> None:
        """Context manager exit."""
        self.stop()


def start_metrics_server(host: str = "0.0.0.0", port: int = 8081) -> MetricsServer:
    """Start a metrics server singleton.

    Args:
        host: Host to bind to.
        port: Port to listen on.

    Returns:
        The MetricsServer instance.
    """
    if MetricsServer.instance is None:
        MetricsServer.instance = MetricsServer(host, port)
        MetricsServer.instance.start()
    return MetricsServer.instance


def stop_metrics_server() -> None:
    """Stop the metrics server singleton."""
    if MetricsServer.instance is not None:
        MetricsServer.instance.stop()
        MetricsServer.instance = None
