# Operations

Deploy, size and tune a module process. Every variable below is a pydantic-settings field under `src/digitalkin/models/settings/`; [environment_variables.md](environment_variables.md) is the full reference and `.env.exemple` the generated template (`task env-template`). This page names only the variables each decision needs.

## Deployment checklist

| Item | Requirement |
|---|---|
| Minimum env | `.env.minimum`: `CLIENT_CHANNEL_HOST`, `CLIENT_CHANNEL_PORT` (services provider), `DIGITALKIN_MODULE_ID`, `DIGITALKIN_REDIS_URL`, `SERVER_CHANNEL_ADVERTISE_HOST`. |
| Redis | Mandatory. The gateway raises `RuntimeError` at startup when `DIGITALKIN_REDIS_URL` is empty. Every key carries a TTL; run HA Redis, it is a single point of failure. |
| Advertise host | `SERVER_CHANNEL_ADVERTISE_HOST` is the address sent to the registry. It falls back to `SERVER_CHANNEL_HOST` (`[::]` by default), which other services cannot dial. Always set it. |
| Bind | `SERVER_CHANNEL_HOST` / `SERVER_CHANNEL_PORT` (default `[::]:50055`). |
| Service mode | `SERVICE_MODE=remote` (default) uses the gRPC strategies against the services provider; `local` uses the in-process `Default*` stubs. Redis is required in both modes. |
| TLS | `SERVER_CHANNEL_SECURITY` / `CLIENT_CHANNEL_SECURITY` = `secure` or `insecure` (default). Enable mutual TLS with `*_CHANNEL_MTLS=true`. |
| Certificates | Explicit paths via `*_CHANNEL_CREDENTIALS__KEY_PATH` / `__CERT_PATH` / `__ROOT_CERT_PATH`, otherwise read from directories: server `server.key`, `server.crt`, `ca.crt` in `CERTIFICATE_CERT_VOLUME`; client `ca.crt` (+ `client.key`, `client.crt` under mTLS) in `CERTIFICATE_SERVICES_PROVIDER_CERT_VOLUME`. Both default to `/certificates`. A missing file raises `SecurityError` at startup. |
| Event loop | `DIGITALKIN_UVLOOP=true` installs the uvloop policy at import of the server module. |

## Capacity model

A request passes these gates in order. Defaults are per process.

| Limit | Variable | Default | When hit |
|---|---|---|---|
| Concurrent inbound RPCs | `SERVER_MAX_CONCURRENT_RPCS` | `cpu_count × 200` | gRPC rejects the RPC with `RESOURCE_EXHAUSTED` before SDK code runs. |
| Gateway sessions | `DIGITALKIN_GATEWAY_MAX_STREAMS` | `20000` | `StartStream` returns `accepted=false`; the `idem:{id}` claim is released so the same `task_id` can be retried. |
| Admission gate (running + queued) | `DIGITALKIN_TASK_MANAGER_MAX_CONCURRENT_TASKS` + `DIGITALKIN_TASK_MANAGER_MAX_QUEUED_TASKS` | `500` + `5000` | Waits `DIGITALKIN_TASK_MANAGER_ADMISSION_TIMEOUT` (`5.0` s), then fails the task: `stream.error MODULE_RUNTIME_ERROR`. |
| Execution slot (queued task) | `DIGITALKIN_TASK_MANAGER_QUEUE_SLOT_TIMEOUT` | `600.0` s | Admitted task gave up waiting for a slot: `MODULE_RUNTIME_ERROR`. |
| Execution slot (queue disabled) | `DIGITALKIN_TASK_MANAGER_TASK_WAIT_TIMEOUT` | `30.0` s | Used only when `MAX_QUEUED_TASKS=0`: single semaphore, fails after the timeout. |
| Signal listener registrations | `DIGITALKIN_SIGNAL_MAX_TASKS` | `10000` | Registering one more task raises `RuntimeError`. Keep ≥ running tasks. |
| Redis default pool (XADD, SET, …) | `DIGITALKIN_REDIS_POOL_SIZE_DEFAULT` | `0` → `POOL_SIZE // 2` = `1000` | redis-py raises a connection error: `REDIS_UNAVAILABLE`. |
| Redis blocking pool (XREAD) | `DIGITALKIN_REDIS_POOL_SIZE_BLOCKING` | `0` → `POOL_SIZE // 2` = `1000` | Same. Separate pool so blocked readers never starve writers. |
| Outbound M2M calls (`call_module`) | `DIGITALKIN_M2M_CALL_MAX_CONCURRENT` | `200` | Waits `DIGITALKIN_M2M_CALL_ACQUIRE_TIMEOUT_S` (`30.0`), then raises `M2MAtCapacityError`. |
| M2M per-call output queue | `DIGITALKIN_M2M_CALL_QUEUE_MAXSIZE` | `1024` | Producer output is held until the caller drains. |
| Per-service bulkhead | `DIGITALKIN_BULKHEAD_DEFAULT_MAX`, `DIGITALKIN_BULKHEAD_{SERVICE}_MAX` | `50` | Opt-in: only code that enters `Bulkhead.for_service(id)` is limited. Raises `BulkheadFullError` after `DIGITALKIN_BULKHEAD_TIMEOUT` (`2.0` s). The SDK's own clients do not use it. |
| Setup cache | `DIGITALKIN_MODULE_SERVICER_SETUP_CACHE_MAX` / `_TTL` | `100` / `600.0` s | Oldest entry evicted; next task for that setup re-fetches it. |

`DIGITALKIN_JOB_MANAGER_BACKPRESSURE_STRATEGY` (`block` / `drop_oldest` / `reject`) and `_TIMEOUT` (`300.0` s) govern the in-memory session queue, which only config-setup uses. Task output goes straight to Redis and is not affected.

## Sizing guidance

1. **Execution slots are bounded by one event loop.** Each process runs one asyncio loop; every running task competes for it. A moderate `MAX_CONCURRENT_TASKS` with a deep queue sustains more throughput than a high one with no queue, because individual tasks finish faster without scheduling contention. Scale out with more processes, not more slots.
2. **Queue = expected burst − running slots.** A queued task holds no CPU, only a session entry.
3. **`SERVER_MAX_CONCURRENT_RPCS` ≥ open `Stream` reads + headroom.** `StartStream` and `SendSignal` are unary, but a consumer that pulls with `Stream` holds an RPC for the whole task. With pull consumers, set it ≥ `MAX_CONCURRENT_TASKS + MAX_QUEUED_TASKS`. The default (`cpu × 200`, e.g. 800 on 4 vCPU) is below the default task capacity (5500).
4. **`DIGITALKIN_GATEWAY_MAX_STREAMS` ≥ running + queued**, or `StartStream` refuses work the task manager could have queued.
5. **Redis connections.** Pools grow lazily up to `DIGITALKIN_REDIS_POOL_SIZE` (`2000`) per process. Keep `replicas × POOL_SIZE` under Redis `maxclients`. Size the blocking pool ≥ concurrent `Stream` readers.
6. **Keep the timer chain ordered**: `DIGITALKIN_MODULE_HEARTBEAT_INTERVAL_S` < `DIGITALKIN_GATEWAY_STREAM_READ_IDLE_TIMEOUT_S` and `DIGITALKIN_GATEWAY_DIAL_BACK_IDLE_TIMEOUT_S`; `DIGITALKIN_GATEWAY_DIAL_BACK_RECONNECT_WINDOW_S` ≤ `DIGITALKIN_GATEWAY_STREAM_REDIS_STREAM_TTL`.
7. **Containers with ephemeral IPs**: keep `CLIENT_GRPC_OPTIONS_DNS_RESOLUTION_MS` low (default `500`) and lower `CLIENT_GRPC_OPTIONS_MAX_RECONNECT_MS` to recover faster after a peer restart.

Example for a burst of ~3,600 tasks on one process with pull consumers:

```env
DIGITALKIN_TASK_MANAGER_MAX_CONCURRENT_TASKS=200
DIGITALKIN_TASK_MANAGER_MAX_QUEUED_TASKS=3600
SERVER_MAX_CONCURRENT_RPCS=4000
DIGITALKIN_UVLOOP=true
```

## Timeouts and keepalives

| What | Variable | Default |
|---|---|---|
| Unary client call deadline | `CLIENT_TIMEOUT` | `30.0` s |
| Registry search deadline | `DIGITALKIN_REGISTRY_SEARCH_TIMEOUT_S` | `10.0` s |
| Per-tool resolution | `DIGITALKIN_MODULE_TOOL_RESOLVE_TIMEOUT` | `10.0` s |
| Config-setup | `DIGITALKIN_JOB_MANAGER_CONFIG_SETUP_TIMEOUT` | `30.0` s |
| Liveness frame on silence | `DIGITALKIN_MODULE_HEARTBEAT_INTERVAL_S` | `30.0` s |
| `Stream` read with no new entry | `DIGITALKIN_GATEWAY_STREAM_READ_IDLE_TIMEOUT_S` | `300.0` s |
| XREAD block | `DIGITALKIN_GATEWAY_STREAM_STREAM_READ_BLOCK_MS` | `50` ms |
| Dial-back idle (reset on each chunk) | `DIGITALKIN_GATEWAY_DIAL_BACK_IDLE_TIMEOUT_S` | `300.0` s |
| Dial-back absolute ceiling | `DIGITALKIN_GATEWAY_DIAL_BACK_MAX_LIFETIME_S` | `3600.0` s |
| Grace after `stream.end` on dial-back | `DIGITALKIN_GATEWAY_DIAL_BACK_CLOSE_GRACE_S` | `2.0` s |
| Dial-back re-dial window / backoff base / cap | `DIGITALKIN_GATEWAY_DIAL_BACK_RECONNECT_WINDOW_S` / `_BACKOFF_BASE_S` / `_BACKOFF_MAX_S` | `300.0` / `3.0` / `5.0` s |
| M2M per-output deadline | `DIGITALKIN_M2M_CALL_TIMEOUT_S` | `120.0` s |
| M2M AssociateTask mint | `DIGITALKIN_M2M_CALL_ASSOCIATE_TIMEOUT_S` | `5.0` s |
| Redis per-command read | `DIGITALKIN_REDIS_SOCKET_TIMEOUT` | `15.0` s |
| Redis connection PING interval | `DIGITALKIN_REDIS_HEALTH_CHECK_INTERVAL` | `15` s |
| Client keepalive ping / ack timeout | `CLIENT_GRPC_OPTIONS_KEEPALIVE_TIME` / `_KEEPALIVE_TIMEOUT` | `15000` / `5000` ms |
| Server keepalive ping / ack timeout | `SERVER_GRPC_OPTIONS_KEEPALIVE_TIME` / `_KEEPALIVE_TIMEOUT` | `120000` / `20000` ms |
| Min ping interval (client must be ≥ server) | `CLIENT_GRPC_OPTIONS_MIN_PING_INTERVAL` / `SERVER_GRPC_OPTIONS_MIN_PING_INTERVAL` | `10000` / `10000` ms |
| Message size (each direction, both sides) | `*_GRPC_OPTIONS_MAX_RECEIVE_MESSAGE_LENGTH` / `_MAX_SEND_MESSAGE_LENGTH` | `104857600` B |

`DIGITALKIN_REDIS_SOCKET_TIMEOUT` is a client-side asyncio timer: a stalled event loop trips it and logs "Timeout reading from …" while Redis is healthy. Keep it above the XREAD block time and any tolerable loop stall.

## Retries and circuit breaker

| Layer | Where | Retries | Default | Variables |
|---|---|---|---|---|
| A. Channel retry (gRPC C++) | service config on each client channel | `UNAVAILABLE`, `RESOURCE_EXHAUSTED`, `DEADLINE_EXCEEDED` | **off** | `CLIENT_GRPC_OPTIONS_ENABLE_RETRIES=false`; `CLIENT_GRPC_RETRY_MAX_ATTEMPTS` (`5`), `_INITIAL_BACKOFF` (`0.1s`), `_MAX_BACKOFF` (`10s`), `_BACKOFF_MULTIPLIER` (`2.0`) |
| B. `exec_grpc_query` loop | every SDK unary client call | `UNAVAILABLE`, `INTERNAL`, `DEADLINE_EXCEEDED` | on | `CLIENT_MAX_RETRIES` (`2`, so 3 attempts), `CLIENT_BACKOFF_BASE_MS` (`50`, doubles per attempt) |
| Circuit breaker | per service, around layer B | opens on `UNAVAILABLE`, `INTERNAL`, `DEADLINE_EXCEEDED`, `UNKNOWN`, `RESOURCE_EXHAUSTED` | on | `CLIENT_CIRCUIT_BREAKER_FAIL_MAX` (`5`), `CLIENT_CIRCUIT_BREAKER_RESET_TIMEOUT` (`30.0` s) |
| M2M breaker | per target `host:port` for `call_module` | — | on | `DIGITALKIN_M2M_CALL_BREAKER_FAIL_MAX` (`5`), `_RESET_TIMEOUT_S` (`30.0`) |
| XREAD retry | blocking Redis pool only | connection / timeout errors | on | `DIGITALKIN_REDIS_BLOCKING_READ_RETRIES` (`3`). The write pool never retries: a retried XADD duplicates a frame. |

An open breaker fails calls immediately with `ServerError`, without a network round-trip. Application codes (`NOT_FOUND`, `INVALID_ARGUMENT`, …) never trip it.

Leave layer A off. Enabling it stacks it under layer B (up to `5 × 3` attempts per call) and on a cold channel returning `UNAVAILABLE` produced multi-second stalls. Enable it only for a client whose calls bypass `exec_grpc_query`, and then lower `CLIENT_MAX_RETRIES`.

Signals have no retry layer: `SendSignal` is one Redis script (tombstone `SET cancel:{id}` + `PUBLISH signal_ch:{id}`). If Redis is down it returns `success=false` and the client retries.

## Stream retention

| Key | TTL / bound | Variable | Default |
|---|---|---|---|
| `task:{id}:stream` before EOS | sliding, re-armed on every output | `DIGITALKIN_GATEWAY_STREAM_REDIS_STREAM_INITIAL_TTL` | `600` s |
| `task:{id}:stream` after EOS | fixed | `DIGITALKIN_GATEWAY_STREAM_REDIS_STREAM_TTL` | `360` s |
| `task:{id}:stream` length | approximate `MAXLEN ~` trim | `DIGITALKIN_GATEWAY_STREAM_REDIS_STREAM_MAXLEN` | `1000` entries |
| `task:{id}:cursor` | fixed | `DIGITALKIN_GATEWAY_STREAM_REDIS_CURSOR_TTL` | `360` s |
| `idem:{id}` claim | until EOS, then the post-EOS stream TTL | `DIGITALKIN_REDIS_IDEM_TTL` | `3600` s |
| `cancel:{id}` tombstone | fixed | same as `REDIS_STREAM_INITIAL_TTL` | `600` s |
| Resume `seq` upper bound | `MAXLEN × multiplier` | `DIGITALKIN_GATEWAY_STREAM_FROM_SEQ_MULTIPLIER` | `10` |

A task emitting more than `MAXLEN` frames loses its oldest frames for late readers or resumes. Raise `MAXLEN` for chatty modules that must be fully replayable. Keep the post-EOS TTL ≥ the dial-back reconnect window so a finished stream survives a consumer reboot.

## Shutdown order

`ModuleServer.stop_async(grace)` (`src/digitalkin/grpc_servers/module_server.py`):

| Step | Action |
|---|---|
| 1 | Health service enters graceful shutdown (`NOT_SERVING`). |
| 2 | Deregister the module from the registry. |
| 3 | Stop the gRPC server; in-flight RPCs get `grace` seconds. |
| 4 | `job_manager.stop()`: cancel running tasks (reason `shutdown`), release the Redis signal listener. |
| 5 | Gateway servicer stop. |
| 6 | Module servicer resource shutdown. |
| 7 | Close the gateway Redis client. |
| 8 | Clear module shared state, close the registry client, close all cached gRPC channels. |

Each step logs and continues on failure. Give the orchestrator's termination grace period more than `grace` plus a few seconds for steps 4-8.

## Observability

| Signal | Source |
|---|---|
| Console level | `DIGITALKIN_LOG_LEVEL` (`INFO`) |
| Rotating file | `DIGITALKIN_LOG_DIR` (empty = off) or `DIGITALKIN_LOG_FILE`; level `DIGITALKIN_LOG_FILE_LEVEL` (`DEBUG`) |
| Production format | When `RAILWAY_SERVICE_NAME` is set, logs are compact single-line JSON; otherwise indented, coloured JSON. |
| Correlation IDs | Every SDK gRPC call carries `x-task-id`, `x-setup-id`, `x-mission-id` metadata (client interceptor); the server interceptor binds them to the context so every log record includes them. |
| Validation markers | `[VALIDATE <ID>]` log lines mark fixes awaiting production confirmation; see [validation_checklist.md](validation_checklist.md). |
| Profiling | `DIGITALKIN_PROFILER` (`none`, `pyinstrument`, `yappi`, `viztracer`), output in `DIGITALKIN_PROFILE_OUTPUT_DIR`. |

There is no built-in metrics or tracing in `src/digitalkin`. `examples/monitoring/` is an optional add-on to copy into a project: a metrics collector, a gRPC interceptor, a `/metrics` HTTP exporter, and a Prometheus + Grafana compose stack with a dashboard.

## Failure modes

| Symptom | `stream.error` code | Likely cause | Knob |
|---|---|---|---|
| `StartStream` → `accepted=false` | none | Gateway at `MAX_STREAMS`, invalid IDs, or `task_id` already claimed | `DIGITALKIN_GATEWAY_MAX_STREAMS` |
| RPC fails `RESOURCE_EXHAUSTED` | none | `SERVER_MAX_CONCURRENT_RPCS` reached | `SERVER_MAX_CONCURRENT_RPCS` |
| Task fails ~5 s after start under load | `MODULE_RUNTIME_ERROR` (admission rejected) | Running + queued at capacity | `DIGITALKIN_TASK_MANAGER_MAX_QUEUED_TASKS`, more replicas |
| Task fails after a long wait | `MODULE_RUNTIME_ERROR` (queue slot timeout) | Slots held by long tasks | `DIGITALKIN_TASK_MANAGER_MAX_CONCURRENT_TASKS`, `_QUEUE_SLOT_TIMEOUT` |
| Module raised | `MODULE_RUNTIME_ERROR` | Module code | — |
| Bad input / setup | `INPUT_VALIDATION_ERROR` / `SETUP_VALIDATION_ERROR` | Payload does not match the module models | — |
| Setup refused | `SETUP_ACCESS_DENIED` | Caller lacks access to the setup | — |
| Redis errors, "Too many connections" | `REDIS_UNAVAILABLE` | Redis down, pool exhausted, loop stall tripping the socket timer | `DIGITALKIN_REDIS_POOL_SIZE*`, `DIGITALKIN_REDIS_SOCKET_TIMEOUT` |
| Reader stops after silence | `STREAM_IDLE_TIMEOUT` | Producer died without EOS, or a module silent past the timeout | `DIGITALKIN_GATEWAY_STREAM_READ_IDLE_TIMEOUT_S`, heartbeat |
| Dial-back stops after silence | `DIAL_BACK_IDLE_TIMEOUT` | No output for the idle window | `DIGITALKIN_GATEWAY_DIAL_BACK_IDLE_TIMEOUT_S`, heartbeat |
| Consumer never reached | `DIAL_BACK_UNREACHABLE` | Bad callback address, firewall/NAT | — |
| Dial-back BiDi broke before output | `DIAL_BACK_RPC_ERROR` | Consumer server error or channel closed | keepalives |
| Consumer accepted but never replied | `DIAL_BACK_NO_QUERY` | Consumer did not send its first message | — |
| Unexpected gateway error | `DIAL_BACK_INTERNAL` | Bug; see gateway logs | — |
| Multi-second gap on first calls to a cold service | none | Channel retry (layer A) enabled | `CLIENT_GRPC_OPTIONS_ENABLE_RETRIES=false` |
| Calls fail instantly with `ServerError` | none | Circuit breaker open | `CLIENT_CIRCUIT_BREAKER_*` |

Codes are defined in `src/digitalkin/models/grpc_servers/stream_error_codes.py`; the wire format is in [gateway_protocol.md](gateway_protocol.md).

## Reference load test

2026-06-18, hosted gateway, 50 workers for 1200 s, dial-back over ngrok, agentic module (~27 events per call), uvloop, client gzip off, 180 s call timeout.

| Metric | p50 / p95 / p99 |
|---|---|
| `StartStream` → first message | 86 / 145 / 294 ms |
| Second message | 181 / 273 / 843 ms |
| → first model text | 13.8 / 21.0 / 25.4 s |
| → `stream.end` | 21.5 / 30.2 / 34.7 s |

2,734 calls, 0 failures, flat throughput. Call duration was dominated by the model, not transport.
