# gRPC & Concurrency Tuning Guide

## Architecture Overview

> **Deep dives:** [Retry & Fault Tolerance](architecture/resilience.md) · [Admission Queue](architecture/admission-queue.md) · [Concurrency Model](architecture/concurrency-model.md)

```
┌─────────────────────────────────────────────────┐
│  Layer 1: gRPC Server (accepts RPCs)            │
│  DIGITALKIN_MAX_CONCURRENT_RPCS                 │
│  DIGITALKIN_THREAD_POOL_WORKERS                 │
│  Server channel options (message size, pings)   │
├─────────────────────────────────────────────────┤
│  Layer 2: Task Manager (executes work)          │
│  DIGITALKIN_MAX_CONCURRENT_TASKS                │
│  DIGITALKIN_MAX_QUEUED_TASKS                    │
│  DIGITALKIN_ADMISSION_TIMEOUT                   │
│  DIGITALKIN_BACKPRESSURE_STRATEGY / _TIMEOUT    │
├─────────────────────────────────────────────────┤
│  Layer 3: Lifecycle (completion & cleanup)       │
│  DIGITALKIN_COMPLETION_TIMEOUT                  │
│  DIGITALKIN_STREAM_DRAIN_TIMEOUT                │
│  DIGITALKIN_SETUP_CACHE_MAX                     │
├─────────────────────────────────────────────────┤
│  Layer 4: Signal I/O (gRPC client calls out)    │
│  DIGITALKIN_GRPC_TIMEOUT                        │
│  DIGITALKIN_SIGNAL_* (batching, polling, retry) │
│  DIGITALKIN_GRPC_QUERY_MAX_RETRIES / _BACKOFF   │
│  DIGITALKIN_TOOL_RESOLVE_TIMEOUT                │
│  DIGITALKIN_CONFIG_SETUP_TIMEOUT                │
│  Client channel options (keepalive, retry, DNS) │
│  DIGITALKIN_GRPC_RETRY_* (channel retry policy) │
│  DIGITALKIN_GRPC_*_MS (keepalive, reconnect)    │
└─────────────────────────────────────────────────┘
```

Request flow:

```
Request arrives
  → MAX_CONCURRENT_RPCS gate (gRPC layer)
    → system_gate semaphore (MAX_CONCURRENT_TASKS + MAX_QUEUED_TASKS)
      → task_slot semaphore (MAX_CONCURRENT_TASKS)
        → actual execution
```

---

## Layer 1: gRPC Async Server

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DIGITALKIN_MAX_CONCURRENT_RPCS` | `cpu × 200` | How many RPCs the server accepts simultaneously. This is the front door. Each `StartModule` is a server-streaming RPC that stays open for the entire task duration (potentially minutes). Set this >= `MAX_CONCURRENT_TASKS + MAX_QUEUED_TASKS` so the server never rejects at the gRPC layer before the task manager can queue it. |
| `DIGITALKIN_THREAD_POOL_WORKERS` | `min(4, cpu)` | Migration thread pool for the async server. Used for running synchronous callbacks. For pure-async modules, 1-2 is enough. Only increase if you have sync blocking code. |

### Server Channel Options (hardcoded in `ServerConfig`)

| Option | Value | Notes |
|--------|-------|-------|
| `grpc.max_receive_message_length` | 100 MB | Increase only for huge payloads. |
| `grpc.max_send_message_length` | 100 MB | Same. |
| `grpc.keepalive_permit_without_calls` | `True` | Allows client pings even when idle. Leave on. |
| `grpc.http2.min_ping_interval_without_data_ms` | 10000 | Min 10s between client pings. Prevents GOAWAY. |

---

## Layer 2: Task Manager (Concurrency Control)

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DIGITALKIN_MAX_CONCURRENT_TASKS` | `100` | How many tasks actually execute simultaneously. Each task consumes event loop time, gRPC client connections, memory. Too high → event loop starvation. Too low → underutilized hardware. |
| `DIGITALKIN_MAX_QUEUED_TASKS` | `0` | How many tasks wait in line. When > 0, enables the admission queue: excess tasks queue patiently instead of being rejected. Set this to absorb your expected burst size. |
| `DIGITALKIN_ADMISSION_TIMEOUT` | `5.0` | Fast-fail timeout (seconds) when both running and queued slots are full. Keep short (3-5s) — if the queue is full, waiting longer won't help. |
| `DIGITALKIN_TASK_WAIT_TIMEOUT` | `30` | Legacy: timeout waiting for a slot when queue is disabled (`MAX_QUEUED_TASKS=0`). With queue enabled, this is ignored. |

### How the Admission Queue Works

> **Full documentation:** [architecture/admission-queue.md](architecture/admission-queue.md) — problem statement, two-phase flow diagrams, capacity planning, log analysis findings.

When `DIGITALKIN_MAX_QUEUED_TASKS > 0`, two-phase admission:

1. **Phase 1 — Enter system gate** (fast reject in `ADMISSION_TIMEOUT` seconds if `running + queued >= total capacity`)
2. **Phase 2 — Wait for execution slot** (patient wait, no timeout — freed by completing tasks)

When `DIGITALKIN_MAX_QUEUED_TASKS = 0` (default): legacy single-semaphore behavior with `TASK_WAIT_TIMEOUT`.

---

## Layer 3: Signal I/O (Client-Side gRPC)

### Outbound Signals (SendSignals Batching)

| Variable | Default | Description |
|----------|---------|-------------|
| `DIGITALKIN_GRPC_TIMEOUT` | `30` | Per-RPC timeout (seconds) for SendSignals. Under burst load, the services-provider slows down. Increase to 60s for safety under high concurrency. |
| `DIGITALKIN_SIGNAL_MAX_BATCH_SIZE` | `50` | Flush trigger. When this many signals accumulate, send immediately. Larger = fewer RPCs but bigger payloads and higher per-signal latency. |
| `DIGITALKIN_SIGNAL_FLUSH_INTERVAL` | `0.1` | Timer trigger (seconds). If batch doesn't fill in time, flush anyway. Lower = less latency. Higher = more batching efficiency. |
| `DIGITALKIN_SIGNAL_SEND_RETRIES` | `3` | Retry count for failed batch RPCs. With exponential backoff: 100ms → 200ms → 400ms. |
| `DIGITALKIN_SIGNAL_SEND_BACKOFF_MS` | `100` | Base backoff (ms) for retries. Doubles each attempt. |

### Inbound Signals (GetSignals Polling)

| Variable | Default | Description |
|----------|---------|-------------|
| `DIGITALKIN_POLL_TIMEOUT` | `1` | Per-RPC timeout (seconds) for GetSignals. Short because polling is frequent. |
| `DIGITALKIN_SIGNAL_POLL_INTERVAL` | `1.0` | Max interval (seconds) between polls (ceiling). Exponential backoff caps here. |
| `DIGITALKIN_SIGNAL_INITIAL_POLL_INTERVAL` | `0.1` | Starting interval. Doubles each empty poll until hitting ceiling. Resets when signals arrive. |
| `DIGITALKIN_SIGNAL_QUEUE_SIZE` | `512` | Per-task signal buffer. If a task is slow to consume, signals queue here. |

### Servicer & Lifecycle

| Variable | Default | Description |
|----------|---------|-------------|
| `DIGITALKIN_SETUP_CACHE_MAX` | `100` | Max cached setup configurations per module servicer. Avoids redundant GetSetup RPCs. |
| `DIGITALKIN_COMPLETION_TIMEOUT` | `300.0` | Timeout (seconds) waiting for a job to complete after streaming ends. If exceeded, the session is force-cleaned with `TIMEOUT` cancellation reason. |
| `DIGITALKIN_STREAM_DRAIN_TIMEOUT` | `300.0` | Timeout (seconds) waiting for a task's output stream to fully drain before cleanup. Prevents stale sessions when clients disconnect mid-stream. |
| `DIGITALKIN_BACKPRESSURE_STRATEGY` | `block` | What to do when all running slots are occupied: `block` (wait up to `BACKPRESSURE_TIMEOUT`) or `reject` (immediate failure). |
| `DIGITALKIN_BACKPRESSURE_TIMEOUT` | `300.0` | Max wait time (seconds) when `BACKPRESSURE_STRATEGY=block`. After this, the request is rejected. |

---

## Client Channel Options (env-configurable in `ClientConfig`)

### Keepalive

| Env Var | Option | Default | Why it matters |
|---------|--------|---------|---------------|
| `DIGITALKIN_GRPC_KEEPALIVE_TIME_MS` | `grpc.keepalive_time_ms` | 60000 | Ping interval to detect dead connections. Lower for faster detection. |
| `DIGITALKIN_GRPC_KEEPALIVE_TIMEOUT_MS` | `grpc.keepalive_timeout_ms` | 20000 | Pong wait time. No response → connection dead → reconnect. |
| — | `grpc.keepalive_permit_without_calls` | `True` | Keep pinging even when no RPCs in flight. Not configurable. |
| `DIGITALKIN_GRPC_MIN_PING_INTERVAL_MS` | `grpc.http2.min_time_between_pings_ms` | 30000 | Min interval between HTTP/2 pings. Must be >= server's 10s minimum. |

### Reconnection

| Env Var | Option | Default | Why it matters |
|---------|--------|---------|---------------|
| `DIGITALKIN_GRPC_DNS_RESOLUTION_MS` | `grpc.dns_min_time_between_resolutions_ms` | 500 | Critical for Railway/containers. Re-resolve DNS after this interval. |
| `DIGITALKIN_GRPC_INITIAL_RECONNECT_MS` | `grpc.initial_reconnect_backoff_ms` | 1000 | First reconnect delay. |
| `DIGITALKIN_GRPC_MAX_RECONNECT_MS` | `grpc.max_reconnect_backoff_ms` | 10000 | Cap between reconnect attempts. |
| `DIGITALKIN_GRPC_MIN_RECONNECT_MS` | `grpc.min_reconnect_backoff_ms` | 500 | Floor for reconnect backoff. |

### Retry (Channel-Level)

| Env Var | Option | Default | Description |
|---------|--------|---------|-------------|
| — | `grpc.enable_retries` | `1` | Enables gRPC-native retry via service config. Not configurable. |
| `DIGITALKIN_GRPC_RETRY_MAX_ATTEMPTS` | `RetryPolicy.max_attempts` | 5 | Channel-level retry attempts. |
| `DIGITALKIN_GRPC_RETRY_INITIAL_BACKOFF` | `RetryPolicy.initial_backoff` | 0.1s | Initial retry backoff. |
| `DIGITALKIN_GRPC_RETRY_MAX_BACKOFF` | `RetryPolicy.max_backoff` | 10s | Max retry backoff. |
| `DIGITALKIN_GRPC_RETRY_BACKOFF_MULTIPLIER` | `RetryPolicy.backoff_multiplier` | 2.0 | Backoff multiplier. |

### App-Level Retry (`exec_grpc_query`)

| Env Var | Default | Description |
|---------|---------|-------------|
| `DIGITALKIN_GRPC_QUERY_MAX_RETRIES` | 2 | App-level retry count for all gRPC client calls. |
| `DIGITALKIN_GRPC_QUERY_BACKOFF_BASE_MS` | 50 | Base backoff (ms), doubles per attempt. |

### Tool Resolution & Config Setup

| Env Var | Default | Description |
|---------|---------|-------------|
| `DIGITALKIN_TOOL_RESOLVE_TIMEOUT` | 10.0 | Per-tool resolution timeout (seconds). |
| `DIGITALKIN_CONFIG_SETUP_TIMEOUT` | 30.0 | Config setup response wait (seconds). |

---

## Retry Architecture (Three Independent Layers)

> **Full documentation:** [architecture/resilience.md](architecture/resilience.md) — problem statement, sequence diagrams, retryable vs non-retryable errors, before/after comparison.

```
RPC call
  → Layer A: gRPC service config retry (channel level, transparent)
      retryableStatusCodes: UNAVAILABLE, RESOURCE_EXHAUSTED, DEADLINE_EXCEEDED
      maxAttempts: DIGITALKIN_GRPC_RETRY_MAX_ATTEMPTS (default 5)
      backoff: DIGITALKIN_GRPC_RETRY_INITIAL_BACKOFF → DIGITALKIN_GRPC_RETRY_MAX_BACKOFF

  → Layer B: exec_grpc_query() app-level retry
      retryable: UNAVAILABLE, INTERNAL, DEADLINE_EXCEEDED
      max_retries: DIGITALKIN_GRPC_QUERY_MAX_RETRIES (default 2, 3 total)
      backoff: DIGITALKIN_GRPC_QUERY_BACKOFF_BASE_MS (default 50ms, doubles per attempt)

  → Layer C: SendSignals _flush() retry (batch-specific)
      retryable: DEADLINE_EXCEEDED, UNAVAILABLE, INTERNAL
      max_retries: 3 (4 total), backoff: 100ms → 800ms
```

Layer A retries transparently inside the channel. Layer B catches what A doesn't handle. Layer C is specific to the batched SendSignals path.

---

## Recommended Configurations

### Golden Rules

1. **`MAX_CONCURRENT_RPCS >= MAX_CONCURRENT_TASKS + MAX_QUEUED_TASKS`** — Otherwise gRPC rejects RPCs at the HTTP/2 layer before the task manager even sees them.

2. **`MAX_CONCURRENT_TASKS` should be 20-50x fewer than `MAX_CONCURRENT_RPCS`** — The server can hold thousands of open RPCs (cheap HTTP/2 streams). But each *executing* task consumes event loop cycles, gRPC client connections, DNS lookups, and LLM API calls.

3. **200 concurrent tasks with a 3000 queue will outperform 800 concurrent tasks with no queue every time** — Less concurrency = less event loop contention = faster individual tasks = higher sustained throughput.

### Small Instance (4 vCPU, 8 GB)

```env
# Server
DIGITALKIN_MAX_CONCURRENT_RPCS=1000
DIGITALKIN_THREAD_POOL_WORKERS=2

# Task management
DIGITALKIN_MAX_CONCURRENT_TASKS=100
DIGITALKIN_MAX_QUEUED_TASKS=1000
DIGITALKIN_ADMISSION_TIMEOUT=5.0

# Lifecycle
DIGITALKIN_COMPLETION_TIMEOUT=300.0
DIGITALKIN_STREAM_DRAIN_TIMEOUT=300.0

# Signals
DIGITALKIN_GRPC_TIMEOUT=30
DIGITALKIN_SIGNAL_MAX_BATCH_SIZE=50
DIGITALKIN_SIGNAL_FLUSH_INTERVAL=0.1
DIGITALKIN_SIGNAL_QUEUE_SIZE=512
DIGITALKIN_SETUP_CACHE_MAX=200
```

### Medium Instance (8 vCPU, 32 GB)

```env
# Server
DIGITALKIN_MAX_CONCURRENT_RPCS=2000
DIGITALKIN_THREAD_POOL_WORKERS=4

# Task management
DIGITALKIN_MAX_CONCURRENT_TASKS=200
DIGITALKIN_MAX_QUEUED_TASKS=3000
DIGITALKIN_ADMISSION_TIMEOUT=5.0

# Lifecycle
DIGITALKIN_COMPLETION_TIMEOUT=600.0
DIGITALKIN_STREAM_DRAIN_TIMEOUT=600.0

# Signals
DIGITALKIN_GRPC_TIMEOUT=60
DIGITALKIN_SIGNAL_MAX_BATCH_SIZE=100
DIGITALKIN_SIGNAL_FLUSH_INTERVAL=0.2
DIGITALKIN_SIGNAL_QUEUE_SIZE=1024
DIGITALKIN_SETUP_CACHE_MAX=500
```

### Large Instance (32 vCPU, 64 GB)

```env
# Server
DIGITALKIN_MAX_CONCURRENT_RPCS=5000
DIGITALKIN_THREAD_POOL_WORKERS=4

# Task management
DIGITALKIN_MAX_CONCURRENT_TASKS=400
DIGITALKIN_MAX_QUEUED_TASKS=5000
DIGITALKIN_ADMISSION_TIMEOUT=5.0

# Lifecycle
DIGITALKIN_COMPLETION_TIMEOUT=900.0
DIGITALKIN_STREAM_DRAIN_TIMEOUT=600.0

# Signals
DIGITALKIN_GRPC_TIMEOUT=60
DIGITALKIN_SIGNAL_MAX_BATCH_SIZE=200
DIGITALKIN_SIGNAL_FLUSH_INTERVAL=0.3
DIGITALKIN_SIGNAL_QUEUE_SIZE=2048
DIGITALKIN_SETUP_CACHE_MAX=1000
```

### Railway (Container PaaS)

Railway instances have ephemeral IPs and may restart under memory pressure. Optimize for fast recovery and moderate concurrency.

```env
# Server — Railway instances typically have 1-8 vCPU
DIGITALKIN_MAX_CONCURRENT_RPCS=1500
DIGITALKIN_THREAD_POOL_WORKERS=2

# Task management — conservative to avoid OOM kills
DIGITALKIN_MAX_CONCURRENT_TASKS=50
DIGITALKIN_MAX_QUEUED_TASKS=500
DIGITALKIN_ADMISSION_TIMEOUT=5.0
DIGITALKIN_BACKPRESSURE_STRATEGY=block
DIGITALKIN_BACKPRESSURE_TIMEOUT=120.0

# Lifecycle — shorter timeouts to release resources faster on restart
DIGITALKIN_COMPLETION_TIMEOUT=180.0
DIGITALKIN_STREAM_DRAIN_TIMEOUT=120.0

# Signals — tighter batching for lower memory footprint
DIGITALKIN_GRPC_TIMEOUT=30
DIGITALKIN_SIGNAL_MAX_BATCH_SIZE=50
DIGITALKIN_SIGNAL_FLUSH_INTERVAL=0.1
DIGITALKIN_SIGNAL_QUEUE_SIZE=256
DIGITALKIN_SIGNAL_SEND_RETRIES=5
DIGITALKIN_SIGNAL_SEND_BACKOFF_MS=200
DIGITALKIN_SETUP_CACHE_MAX=100

# I/O timing — fail fast on unreachable services
DIGITALKIN_GRPC_QUERY_MAX_RETRIES=1
DIGITALKIN_GRPC_QUERY_BACKOFF_BASE_MS=25
DIGITALKIN_TOOL_RESOLVE_TIMEOUT=3.0
DIGITALKIN_CONFIG_SETUP_TIMEOUT=15.0
DIGITALKIN_GRPC_RETRY_MAX_ATTEMPTS=3
DIGITALKIN_GRPC_RETRY_MAX_BACKOFF=3s
DIGITALKIN_GRPC_KEEPALIVE_TIME_MS=30000
DIGITALKIN_GRPC_MAX_RECONNECT_MS=5000

# Module
DIGITALKIN_CHAT_HISTORY_FLUSH_THRESHOLD=5
DIGITALKIN_LOG_DIR=/app/logs
DIGITALKIN_TIMEZONE=Europe/Paris
```

**Railway-specific notes:**

- **DNS re-resolution** is configurable via `DIGITALKIN_GRPC_DNS_RESOLUTION_MS` (default 500ms) — critical when services restart with new IPs.
- **Lower `SIGNAL_QUEUE_SIZE`** (256 vs 512) reduces per-task memory. Railway charges by memory usage.
- **Higher `SIGNAL_SEND_RETRIES`** (5 vs 3) with longer backoff absorbs brief connectivity gaps during Railway deploys.
- **Shorter lifecycle timeouts** prevent orphaned sessions from consuming memory after Railway restarts.
- Set `DIGITALKIN_MODULE_ID` per service if running multiple modules in the same Railway project.

---

## Complete Environment Variable Reference

| Variable | Type | Default | Layer | Purpose |
|----------|------|---------|-------|---------|
| `DIGITALKIN_MAX_CONCURRENT_RPCS` | int | cpu×200 | Server | Async server concurrent RPCs |
| `DIGITALKIN_THREAD_POOL_WORKERS` | int | min(4, cpu) | Server | Migration thread pool size |
| `DIGITALKIN_MAX_CONCURRENT_TASKS` | int | 100 | Task Mgr | Concurrent task execution limit |
| `DIGITALKIN_MAX_QUEUED_TASKS` | int | 0 | Task Mgr | Admission queue depth (0 = disabled) |
| `DIGITALKIN_ADMISSION_TIMEOUT` | float | 5.0s | Task Mgr | Fast-fail when queue full |
| `DIGITALKIN_TASK_WAIT_TIMEOUT` | float | 30s | Task Mgr | Legacy slot wait timeout (queue disabled) |
| `DIGITALKIN_BACKPRESSURE_STRATEGY` | str | block | Task Mgr | `block` or `reject` when slots full |
| `DIGITALKIN_BACKPRESSURE_TIMEOUT` | float | 300.0s | Task Mgr | Max wait when strategy=block |
| `DIGITALKIN_COMPLETION_TIMEOUT` | float | 300.0s | Lifecycle | Wait for job completion after stream ends |
| `DIGITALKIN_STREAM_DRAIN_TIMEOUT` | float | 300.0s | Lifecycle | Wait for output stream to drain before cleanup |
| `DIGITALKIN_GRPC_TIMEOUT` | float | 30s | Signal I/O | SendSignals RPC timeout |
| `DIGITALKIN_POLL_TIMEOUT` | float | 1s | Signal I/O | GetSignals RPC timeout |
| `DIGITALKIN_SIGNAL_POLL_INTERVAL` | float | 1.0s | Signal I/O | Max poll interval (ceiling) |
| `DIGITALKIN_SIGNAL_INITIAL_POLL_INTERVAL` | float | 0.1s | Signal I/O | Initial poll interval |
| `DIGITALKIN_SIGNAL_QUEUE_SIZE` | int | 512 | Signal I/O | Per-task signal buffer |
| `DIGITALKIN_SIGNAL_FLUSH_INTERVAL` | float | 0.1s | Signal I/O | Batch flush timer |
| `DIGITALKIN_SIGNAL_MAX_BATCH_SIZE` | int | 50 | Signal I/O | Batch flush trigger |
| `DIGITALKIN_SIGNAL_SEND_RETRIES` | int | 3 | Signal I/O | Batch send retry attempts |
| `DIGITALKIN_SIGNAL_SEND_BACKOFF_MS` | float | 100ms | Signal I/O | Retry backoff base |
| `DIGITALKIN_GRPC_QUERY_MAX_RETRIES` | int | 2 | App retry | App-level retry count for gRPC client calls |
| `DIGITALKIN_GRPC_QUERY_BACKOFF_BASE_MS` | float | 50 | App retry | Base backoff (ms), doubles per attempt |
| `DIGITALKIN_TOOL_RESOLVE_TIMEOUT` | float | 10.0s | Tool init | Per-tool resolution timeout |
| `DIGITALKIN_CONFIG_SETUP_TIMEOUT` | float | 30.0s | Job Mgr | Config setup response wait |
| `DIGITALKIN_GRPC_RETRY_MAX_ATTEMPTS` | int | 5 | Channel retry | Channel-level retry attempts |
| `DIGITALKIN_GRPC_RETRY_INITIAL_BACKOFF` | str | 0.1s | Channel retry | Channel retry initial backoff |
| `DIGITALKIN_GRPC_RETRY_MAX_BACKOFF` | str | 10s | Channel retry | Channel retry max backoff |
| `DIGITALKIN_GRPC_RETRY_BACKOFF_MULTIPLIER` | float | 2.0 | Channel retry | Backoff multiplier |
| `DIGITALKIN_GRPC_DNS_RESOLUTION_MS` | int | 500 | Channel opts | DNS re-resolve interval |
| `DIGITALKIN_GRPC_INITIAL_RECONNECT_MS` | int | 1000 | Channel opts | First reconnect delay |
| `DIGITALKIN_GRPC_MAX_RECONNECT_MS` | int | 10000 | Channel opts | Max reconnect backoff |
| `DIGITALKIN_GRPC_MIN_RECONNECT_MS` | int | 500 | Channel opts | Min reconnect backoff |
| `DIGITALKIN_GRPC_KEEPALIVE_TIME_MS` | int | 60000 | Channel opts | Keepalive ping interval |
| `DIGITALKIN_GRPC_KEEPALIVE_TIMEOUT_MS` | int | 20000 | Channel opts | Keepalive pong timeout |
| `DIGITALKIN_GRPC_MIN_PING_INTERVAL_MS` | int | 30000 | Channel opts | Min HTTP/2 ping interval |
| `DIGITALKIN_SETUP_CACHE_MAX` | int | 100 | Module | Setup config cache size |
| `DIGITALKIN_MODULE_ID` | str | metadata | Module | Override module identity at runtime |
| `DIGITALKIN_CHAT_HISTORY_FLUSH_THRESHOLD` | int | 10 | Module | Messages buffered before storage write |
| `DIGITALKIN_TIMEZONE` | str | Europe/Paris | Module | Default timezone (IANA zone name) |
| `DIGITALKIN_LOG_DIR` | str | /app/logs | Module | Rotating JSON log file directory |
| `DIGITALKIN_ASYNCIO_INSPECTOR` | bool | false | Debug | Enable asyncio event loop monitoring |
| `DIGITALKIN_ASYNCIO_INSPECTOR_PORT` | int | 8765 | Debug | Asyncio inspector port |
| `DIGITALKIN_PROFILER` | str | none | Debug | Profiler: none, pyinstrument, viztracer, yappi |
| `DIGITALKIN_PROFILE_OUTPUT_DIR` | str | ./profiles | Debug | Profiler output directory |
