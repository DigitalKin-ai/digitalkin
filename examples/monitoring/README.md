# DigitalKin Monitoring Stack

A standalone monitoring setup with metrics collection and Prometheus + Grafana visualization for DigitalKin modules.

This is an **optional add-on** that you can copy into your project. It is not bundled with the digitalkin package.

## Directory Structure

```
monitoring/
├── docker-compose.yml              # Prometheus + Grafana services
├── README.md                       # This file
├── digitalkin_observability/       # Python metrics module (copy this to your project)
│   ├── __init__.py
│   ├── metrics.py                  # Core MetricsCollector singleton
│   ├── prometheus.py               # Prometheus text format exporter
│   ├── http_server.py              # HTTP server for /metrics endpoint
│   └── interceptors.py             # gRPC interceptor for auto-instrumentation
├── tests/                          # Tests for the observability module
│   └── test_metrics.py
├── prometheus/
│   └── prometheus.yml              # Prometheus scrape configuration
└── grafana/
    ├── provisioning/
    │   ├── datasources/
    │   │   └── datasources.yml     # Prometheus datasource config
    │   └── dashboards/
    │       └── dashboards.yml      # Dashboard provider config
    └── dashboards/
        └── digitalkin-overview.json  # Pre-built dashboard
```

## Quick Start

### 1. Copy the monitoring module to your project

```bash
cp -r examples/monitoring /path/to/your/project/
```

### 2. Add the observability module to your Python path

Either copy `digitalkin_observability/` to your project's source directory, or add it to your path:

```python
import sys
sys.path.insert(0, "/path/to/monitoring")
```

### 3. Use metrics in your code

```python
from digitalkin_observability import (
    MetricsCollector,
    MetricsServer,
    PrometheusExporter,
    get_metrics,
    start_metrics_server,
)

# Start HTTP metrics server (exposes /metrics and /health endpoints)
start_metrics_server(port=8081)

# Track job metrics
metrics = get_metrics()
metrics.inc_jobs_started("my_module")
# ... do work ...
metrics.inc_jobs_completed("my_module", duration=1.5)

# Or manually export Prometheus format
print(PrometheusExporter.export())
```

### 4. Start the monitoring stack

```bash
cd monitoring
docker compose up -d
```

### 5. Access dashboards

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)

## Python API Reference

### MetricsCollector

Thread-safe singleton that collects metrics.

```python
from digitalkin_observability import get_metrics

metrics = get_metrics()

# Counters
metrics.inc_jobs_started("module_name")
metrics.inc_jobs_completed("module_name", duration=1.5)
metrics.inc_jobs_failed("module_name")
metrics.inc_jobs_cancelled("module_name")
metrics.inc_messages_sent("protocol_name")  # protocol is optional
metrics.inc_heartbeats_sent()
metrics.inc_errors()

# Gauges
metrics.set_queue_depth("job_id", 10)
metrics.clear_queue_depth("job_id")

# Histograms
metrics.observe_grpc_duration(0.05)
metrics.observe_message_latency(0.01)

# Get snapshot
data = metrics.snapshot()

# Reset (useful for testing)
metrics.reset()
```

### MetricsServer

HTTP server that exposes `/metrics` and `/health` endpoints.

```python
from digitalkin_observability import MetricsServer, start_metrics_server, stop_metrics_server

# Option 1: Singleton pattern
start_metrics_server(port=8081)
# ... your application ...
stop_metrics_server()

# Option 2: Context manager
with MetricsServer(port=8081):
    # ... your application ...

# Option 3: Async context manager
async with MetricsServer(port=8081):
    # ... your application ...
```

### MetricsServerInterceptor

gRPC interceptor for automatic request instrumentation.

```python
import grpc
from digitalkin_observability import MetricsServerInterceptor

interceptors = [MetricsServerInterceptor()]
server = grpc.aio.server(interceptors=interceptors)
```

### PrometheusExporter

Export metrics in Prometheus text format.

```python
from digitalkin_observability import PrometheusExporter

output = PrometheusExporter.export()
# Returns Prometheus text exposition format
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROMETHEUS_PORT` | 9090 | Prometheus web UI port |
| `GRAFANA_PORT` | 3000 | Grafana web UI port |
| `GRAFANA_ADMIN_USER` | admin | Grafana admin username |
| `GRAFANA_ADMIN_PASSWORD` | admin | Grafana admin password |

### Adding More Scrape Targets

Edit `prometheus/prometheus.yml` to add more module servers:

```yaml
scrape_configs:
  - job_name: 'digitalkin-modules'
    static_configs:
      - targets:
          - 'host.docker.internal:8081'  # Module 1
          - 'host.docker.internal:8082'  # Module 2
          - 'host.docker.internal:8083'  # Module 3
```

### Custom Dashboards

Add JSON dashboard files to `grafana/dashboards/` and they'll be automatically loaded.

## Available Metrics

### Counters
- `digitalkin_jobs_started_total` - Total jobs started
- `digitalkin_jobs_completed_total` - Total jobs completed successfully
- `digitalkin_jobs_failed_total` - Total jobs failed
- `digitalkin_jobs_cancelled_total` - Total jobs cancelled
- `digitalkin_messages_sent_total` - Total messages sent
- `digitalkin_heartbeats_sent_total` - Total heartbeats sent
- `digitalkin_errors_total` - Total errors

### Gauges
- `digitalkin_active_jobs` - Current number of active jobs
- `digitalkin_active_connections` - Current number of active connections
- `digitalkin_total_queue_depth` - Total items in all job queues

### Histograms
- `digitalkin_job_duration_seconds` - Job execution duration
- `digitalkin_grpc_request_duration_seconds` - gRPC request duration

### Labels
- `digitalkin_jobs_by_module{module="...",status="..."}` - Jobs breakdown by module
- `digitalkin_messages_by_protocol{protocol="...",metric="..."}` - Messages by protocol

## Pre-built Dashboard

The included "DigitalKin Overview" dashboard provides:

- Active jobs gauge
- Jobs started/completed/failed totals
- Jobs rate over time
- Job duration percentiles (P50, P90, P99)
- Messages sent rate
- Errors rate
- gRPC request duration percentiles
- Jobs by module pie chart
- Queue depth monitoring

## Running Tests

```bash
cd monitoring
python -m pytest tests/ -v
```

## Troubleshooting

### Prometheus can't reach the metrics endpoint

1. Ensure your module is running and exposing metrics:
   ```bash
   curl http://localhost:8081/metrics
   ```

2. Check Prometheus targets: http://localhost:9090/targets

3. If running on Linux, `host.docker.internal` might not work. Use your host IP instead:
   ```yaml
   # prometheus/prometheus.yml
   static_configs:
     - targets: ['172.17.0.1:8081']  # Docker bridge IP
   ```

### Grafana dashboard shows no data

1. Verify Prometheus is receiving metrics: http://localhost:9090/graph
2. Query `digitalkin_active_jobs` to check if metrics are being scraped
3. Check the time range in Grafana (top right corner)

### Module not exposing metrics

Ensure you've called `start_metrics_server()` in your module:

```python
from digitalkin_observability import start_metrics_server

start_metrics_server(port=8081)
```
