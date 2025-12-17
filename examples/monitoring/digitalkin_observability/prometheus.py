"""Prometheus metrics exporter for DigitalKin.

This module exports metrics in Prometheus text exposition format.
No external dependencies required.
"""

from __future__ import annotations

from digitalkin_observability.metrics import get_metrics


class PrometheusExporter:
    """Exports metrics in Prometheus text format.

    Usage:
        output = PrometheusExporter.export()
        # Returns Prometheus-compatible text format
    """

    @staticmethod
    def export() -> str:
        """Generate Prometheus-compatible metrics output."""
        snapshot = get_metrics().snapshot()
        lines: list[str] = []

        # Counters
        lines.extend([
            "# HELP digitalkin_jobs_started_total Total jobs started",
            "# TYPE digitalkin_jobs_started_total counter",
            f"digitalkin_jobs_started_total {snapshot['jobs_started_total']}",
            "",
            "# HELP digitalkin_jobs_completed_total Total jobs completed successfully",
            "# TYPE digitalkin_jobs_completed_total counter",
            f"digitalkin_jobs_completed_total {snapshot['jobs_completed_total']}",
            "",
            "# HELP digitalkin_jobs_failed_total Total jobs failed",
            "# TYPE digitalkin_jobs_failed_total counter",
            f"digitalkin_jobs_failed_total {snapshot['jobs_failed_total']}",
            "",
            "# HELP digitalkin_jobs_cancelled_total Total jobs cancelled",
            "# TYPE digitalkin_jobs_cancelled_total counter",
            f"digitalkin_jobs_cancelled_total {snapshot['jobs_cancelled_total']}",
            "",
            "# HELP digitalkin_messages_sent_total Total messages sent",
            "# TYPE digitalkin_messages_sent_total counter",
            f"digitalkin_messages_sent_total {snapshot['messages_sent_total']}",
            "",
            "# HELP digitalkin_heartbeats_sent_total Total heartbeats sent",
            "# TYPE digitalkin_heartbeats_sent_total counter",
            f"digitalkin_heartbeats_sent_total {snapshot['heartbeats_sent_total']}",
            "",
            "# HELP digitalkin_errors_total Total errors",
            "# TYPE digitalkin_errors_total counter",
            f"digitalkin_errors_total {snapshot['errors_total']}",
            "",
        ])

        # Gauges
        lines.extend([
            "# HELP digitalkin_active_jobs Current number of active jobs",
            "# TYPE digitalkin_active_jobs gauge",
            f"digitalkin_active_jobs {snapshot['active_jobs']}",
            "",
            "# HELP digitalkin_active_connections Current number of active connections",
            "# TYPE digitalkin_active_connections gauge",
            f"digitalkin_active_connections {snapshot['active_connections']}",
            "",
            "# HELP digitalkin_total_queue_depth Total items in all job queues",
            "# TYPE digitalkin_total_queue_depth gauge",
            f"digitalkin_total_queue_depth {snapshot['total_queue_depth']}",
            "",
        ])

        # Job duration histogram
        lines.extend(PrometheusExporter._format_histogram(
            "digitalkin_job_duration_seconds",
            "Job execution duration in seconds",
            snapshot["job_duration_seconds"],
        ))

        # gRPC request duration histogram
        lines.extend(PrometheusExporter._format_histogram(
            "digitalkin_grpc_request_duration_seconds",
            "gRPC request duration in seconds",
            snapshot["grpc_request_duration_seconds"],
        ))

        # Per-module breakdown
        if snapshot["by_module"]:
            lines.extend([
                "",
                "# HELP digitalkin_jobs_by_module Jobs breakdown by module and status",
                "# TYPE digitalkin_jobs_by_module counter",
            ])
            for module_name, counts in snapshot["by_module"].items():
                for status, value in counts.items():
                    lines.append(
                        f'digitalkin_jobs_by_module{{module="{module_name}",status="{status}"}} {value}'
                    )

        # Per-protocol breakdown
        if snapshot["by_protocol"]:
            lines.extend([
                "",
                "# HELP digitalkin_messages_by_protocol Messages breakdown by protocol",
                "# TYPE digitalkin_messages_by_protocol counter",
            ])
            for protocol, counts in snapshot["by_protocol"].items():
                for metric, value in counts.items():
                    lines.append(
                        f'digitalkin_messages_by_protocol{{protocol="{protocol}",metric="{metric}"}} {value}'
                    )

        return "\n".join(lines)

    @staticmethod
    def _format_histogram(name: str, help_text: str, data: dict) -> list[str]:
        """Format a histogram for Prometheus output."""
        lines = [
            "",
            f"# HELP {name} {help_text}",
            f"# TYPE {name} histogram",
        ]

        # Sort buckets and output cumulative counts
        cumulative = 0
        for bucket in sorted(data.get("buckets", {}).keys()):
            cumulative += data["buckets"][bucket]
            lines.append(f'{name}_bucket{{le="{bucket}"}} {cumulative}')

        lines.extend([
            f'{name}_bucket{{le="+Inf"}} {data.get("count", 0)}',
            f'{name}_sum {data.get("sum", 0)}',
            f'{name}_count {data.get("count", 0)}',
        ])

        return lines
