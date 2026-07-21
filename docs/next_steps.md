# Next Steps — Production Readiness Roadmap

## Current State

- SDK overhead: **14.8ms P50** at c=1 (host network, echo module, zero-delay)
- Throughput: **67 RPS** per instance (1 vCPU, 1GB RAM)
- Error rate: **0.0%** through 1000 concurrent connections (50k requests)
- Test suite: **1211 tests**, 0 failures, 0 warnings
- Lint: **0 new ruff errors**, 0 mypy errors

---

## 1. Observability

**Priority: Critical — can't operate in production without visibility.**

### OpenTelemetry Tracing
- Span per Gateway RPC (StartStream, ConsumeStream, ProduceStream, SendSignal)
- Span per Redis command (XADD, XREAD, EVAL) via InstrumentedRedisClient
- Span per module lifecycle (create → init → run → stop)
- Parent-child: Gateway span → loopback StartModule span → module.start span
- task_id as trace attribute on all spans
- Key values redacted (structural pattern only)

### Prometheus Metrics
- `gateway_request_duration_seconds` histogram (by RPC, status)
- `gateway_active_streams` gauge
- `redis_command_duration_seconds` histogram (by command)
- `redis_pool_connections` gauge (default + blocking)
- `module_lifecycle_duration_seconds` histogram (by phase: create, init, run, stop)
- `stream_registry_capacity` gauge (current / max)
- `circuit_breaker_state` gauge (0=closed, 1=open, 2=half_open)

### Structured Logging
- Already using digitalkin.logger (structlog-compatible)
- Add: request_id / trace_id correlation
- Add: per-request latency breakdown in log (start_ms, ttfr_ms, total_ms)

### Files
- Extend `src/digitalkin/core/task_manager/redis/instrumented.py` with OTEL + Prometheus
- New: `src/digitalkin/grpc_servers/interceptors/telemetry_interceptor.py`
- New: `src/digitalkin/core/metrics.py` (Prometheus registry)

---

## 2. Latency Optimization — Thread Pool for Module Lifecycle

**Priority: High — reduces P50 at high concurrency by parallelizing module work.**

### Problem
Module.start() runs on the main asyncio event loop. At c=100, the median request waits for ~50 module lifecycles (~15ms each = 750ms queueing delay). The event loop can only run one coroutine at a time.

### Solution
Run the CPU-bound parts of module lifecycle in a thread pool executor:
- `ModuleFactory.create_module_instance()` — 1.5ms of Python object creation
- `_init_strategies()` — 4ms of service strategy instantiation
- `build_tool_cache()` — 0.5ms of Pydantic model walking
- `module.initialize()` — user code, potentially CPU-bound

### Implementation
In `SingleJobManager.create_module_instance_job()`:
```python
module = await asyncio.get_event_loop().run_in_executor(
    self._thread_pool,
    ModuleFactory.create_module_instance, ...
)
```

### Constraints
- Module instances must be thread-safe during creation (no shared mutable state in __init__)
- The thread pool only runs the synchronous __init__, not the async start()
- Pool size = DIGITALKIN_MODULE_THREAD_POOL_SIZE (default: 4)

### Expected Impact
- At c=1: no change (single request, no queueing)
- At c=100: P50 from 1067ms to ~300ms (4 threads process 4 modules in parallel)
- Throughput: from 67 RPS to ~200 RPS (4x parallelism on CPU-bound work)

### Files
- `src/digitalkin/core/job_manager/single_job_manager.py` — add ThreadPoolExecutor
- `src/digitalkin/core/common/factories.py` — ensure create_module_instance is sync-safe

---

## 3. Latency Optimization — Pre-Warmed Module Pool

**Priority: Medium — eliminates per-request module creation cost.**

### Problem
Every request creates a new module instance: `__init__` (1.5ms) + `_init_strategies` (4ms) + `build_tool_cache` (0.5ms) + `initialize` (user code). At 67 RPS, that's 67 module instances created and destroyed per second.

### Solution
Pre-create a pool of initialized module instances at startup. Each request borrows one, runs it, returns it.

```python
class ModulePool:
    def __init__(self, module_class, pool_size=10):
        self._pool = asyncio.Queue(maxsize=pool_size)
        # Pre-create instances at startup
        for _ in range(pool_size):
            module = ModuleFactory.create_module_instance(module_class, ...)
            self._pool.put_nowait(module)

    async def acquire(self) -> BaseModule:
        return await self._pool.get()

    async def release(self, module: BaseModule):
        # Reset module state for reuse
        module._status = ModuleStatus.CREATED
        module.trigger_handlers = {}
        await self._pool.put(module)
```

### Constraints
- Module instances must be reusable (state reset between requests)
- Session-specific data (job_id, mission_id) must be re-bound per request
- Service strategies with per-request state (Cost, Storage) must be re-initialized
- Module.cleanup() must not destroy reusable resources
- Pool size trades memory for latency

### Expected Impact
- Per-request creation: 6ms → 0.5ms (just re-bind session IDs)
- P50 at c=1: 14.8ms → ~9ms
- Memory: +10 module instances × ~50KB each = 500KB constant

### Trade-offs
- (+) Eliminates 6ms of module creation per request
- (+) Reduces GC pressure (fewer object allocations)
- (-) Module state leakage risk if reset is incomplete
- (-) Pool exhaustion under burst (falls back to on-demand creation)
- (-) Requires audit of all module subclasses for reusability

### Files
- New: `src/digitalkin/core/job_manager/module_pool.py`
- `src/digitalkin/core/job_manager/single_job_manager.py` — use pool instead of factory
- `src/digitalkin/modules/_base_module.py` — add `reset()` method

---

## 4. Graceful Shutdown

**Priority: High — prevents data loss during deployments.**

- SIGTERM handler with configurable drain period (default: 30s)
- Stop accepting new StartStream RPCs immediately
- Wait for in-flight ConsumeStream to complete (up to drain timeout)
- Write EOS to all active streams
- Close Redis connections after all streams drained
- Health endpoint returns 503 during drain phase (LB stops routing)

### Files
- `src/digitalkin/grpc_servers/module_server.py` — signal handler + drain logic
- New: `src/digitalkin/grpc_servers/health_servicer.py` — gRPC health check

---

## 5. Configuration Validation

**Priority: Medium — prevents silent misconfig in production.**

- Validate all 47 env vars at startup (type, range, dependencies)
- Fail fast on invalid DIGITALKIN_REDIS_URL (verify at startup, not first request)
- Log config dump at INFO level on startup (with Redis URL masked)
- Warn on risky configs (pool_size < 100, max_concurrent_tasks > pool_size)

### Files
- New: `src/digitalkin/core/config.py` — centralized config validation

---

## 6. Structured Error Codes

**Priority: Medium — enables client retry logic.**

- Define error taxonomy: CAPACITY_EXCEEDED, SETUP_NOT_FOUND, MODULE_FAILED, REDIS_UNAVAILABLE
- Map to gRPC status codes consistently
- Include error_code in StreamError proto field
- Emit error_code in metrics (error rate by type)

### Files
- `src/digitalkin/grpc_servers/gateway_servicer.py` — consistent error mapping
- `gateway_constants.py` — error code enum

---

## 7. Rate Limiting

**Priority: Low — not needed until multi-tenant.**

- Per-client sliding window rate limit
- Token bucket with configurable rate and burst
- 429 RESOURCE_EXHAUSTED with Retry-After header
- Bypass for internal/service-to-service calls

---

## 8. Documentation

**Priority: Medium — needed for onboarding and operations.**

- API reference for 4 Gateway RPCs (proto + behavior)
- Deployment guide: Redis sizing, pool config, scaling formula
- Runbook: common failures, recovery procedures, Redis memory alerts
- Architecture diagram update with current data flow

---

## Execution Order

1. **Observability** — can't debug production without traces/metrics
2. **Graceful shutdown** — can't deploy safely without drain
3. **Thread pool** — biggest latency win at high concurrency
4. **Module pool** — gets P50 under 10ms at c=1
5. **Config validation** — prevents misconfig incidents
6. **Error codes** — enables smart client retry
7. **Documentation** — enables team onboarding
8. **Rate limiting** — needed when multi-tenant
