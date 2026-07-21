# Session Report — Gateway + Redis Architecture

## What was built

A BiDi streaming gateway for the DigitalKin SDK, replacing the direct `StartModule` RPC with a Redis-backed `StartStream` + `ConsumeStream` flow. Module output persists in Redis Streams, enabling crash recovery, late consumers, and horizontal scaling.

### Architecture

```
Client (Chainlit) → StartStream → Gateway (embedded in ModuleServer)
                                      ↓ loopback gRPC
                                  ModuleServer.StartModule → Module (Ada/template-tool)
                                      ↓ module output
                                  ProtoStreamWriter → Redis Stream
                  ← ConsumeStream ← ProtoStreamReader ← Redis Stream
```

### Files created/modified in dk-dev

| File | Status | Purpose |
|------|--------|---------|
| `src/digitalkin/grpc_servers/gateway_servicer.py` | Modified | 4 RPCs: StartStream, ConsumeStream, ProduceStream, SendSignal |
| `src/digitalkin/grpc_servers/gateway_server.py` | **Deleted** | Was standalone GatewayServer — replaced by embedded gateway in ModuleServer |
| `src/digitalkin/grpc_servers/gateway_constants.py` | **New** | All constants, Redis keys, env vars, validation |
| `src/digitalkin/grpc_servers/stream_registry.py` | Rewritten | Redis-backed session registry with Lua capacity check |
| `src/digitalkin/grpc_servers/stream_session.py` | Modified | Added tenant_id, removed dead _seq |
| `src/digitalkin/grpc_servers/module_server.py` | Modified | Auto-registers GatewayServicer when DIGITALKIN_REDIS_URL is set |
| `src/digitalkin/grpc_servers/interceptors/auth_interceptor.py` | **New** | Tenant auth, rate limiting, per-tenant caps |
| `src/digitalkin/grpc_servers/interceptors/__init__.py` | **New** | Package init |
| `src/digitalkin/core/task_manager/redis/proto_streams.py` | Modified | restore_seq, adaptive batch flush, backpressure, split pools |
| `src/digitalkin/core/task_manager/redis/redis_client.py` | Modified | Split pools (default + blocking), zadd/zrangebyscore/zrem/decr wrappers, pool_stats, info_memory, mask_redis_url |
| `src/digitalkin/grpc_servers/_base_server.py` | Modified | uvloop activation |

### Files created/modified in digitalkin-sandbox

| File | Status | Purpose |
|------|--------|---------|
| `scripts/chainlit_app/services/gateway_client.py` | **New** | GatewayClient for Chainlit (StartStream + ConsumeStream) |
| `scripts/chainlit_app/application.py` | Modified | Uses GatewayClient for streaming, ModuleClient for config setup |
| `scripts/chainlit_app/config.py` | Modified | GATEWAY_ADDRESS defaults to Ada |
| `scripts/chainlit_app/models/protocols.py` | Modified | Added EventOutputProtocol, made ModuleStartInfo fields optional |
| `scripts/chainlit_app/handlers/output_handler.py` | Modified | Handles EventOutputProtocol |
| `scripts/chainlit_app/services/setup.py` | Modified | Fixed missing awaits on async methods |
| `scripts/stress_test_grpc.py` | Modified | Gateway BiDi (StartStream+ConsumeStream), profiles, shared channel, cycling mission IDs |
| `modules/archetype-ada/src/archetype_ada/server.py` | Reverted to clean | Just uses ModuleServer (gateway auto-embeds) |
| `modules/archetype-ada/pyproject.toml` | Modified | digitalkin==1.0.0.dev0, agentic-mesh-protocol==1.0.0.dev0 |
| `modules/template-tool/pyproject.toml` | Modified | Same deps |
| `modules/template-tool/Dockerfile` | Modified | Copies wheel from packages/ |
| `docker-compose.yml` | Modified | Added Redis service, DIGITALKIN_REDIS_URL, pool size, template-tool uncommented |
| `.env` | Modified | All new gateway env vars |
| `fixtures/bundles_template_tool.surql` | **New** | SurrealDB fixture for template-tool |
| `examples/redis_demo/` | **New** | Demo server + client + echo module |

---

## Environment Variables

### Redis Gateway (NEW)

| Variable | Default | Purpose |
|----------|---------|---------|
| `DIGITALKIN_REDIS_URL` | `redis://localhost:6379/0` | Redis connection URL |
| `DIGITALKIN_REDIS_POOL_SIZE` | `2000` | Total pool connections |
| `DIGITALKIN_REDIS_POOL_SIZE_DEFAULT` | half of total | Pool for writes/commands |
| `DIGITALKIN_REDIS_POOL_SIZE_BLOCKING` | half of total | Pool for XREAD (blocking) |

### Gateway Capacity

| Variable | Default | Purpose |
|----------|---------|---------|
| `DIGITALKIN_GATEWAY_MAX_STREAMS` | `20000` | Cluster-wide session cap |
| `DIGITALKIN_GATEWAY_MAX_LOCAL_CACHE` | `5000` | Per-instance LRU cache |
| `DIGITALKIN_GATEWAY_HEARTBEAT_TTL` | `45` | Seconds before zombie detection |
| `DIGITALKIN_GATEWAY_REAPER_INTERVAL` | `30` | Reaper scan interval |

### Stream Lifecycle

| Variable | Default | Purpose |
|----------|---------|---------|
| `DIGITALKIN_REDIS_STREAM_TTL` | `60` | Stream TTL after EOS (seconds) |
| `DIGITALKIN_REDIS_STREAM_MAXLEN` | `1000` | Max entries per stream |
| `DIGITALKIN_REDIS_CURSOR_TTL` | `360` | Consumer cursor TTL |
| `DIGITALKIN_SESSION_STATE_TTL_S` | `3600` | Session metadata TTL |
| `DIGITALKIN_STREAM_READ_BLOCK_MS` | `1000` | XREAD max block time |

### Stream Batching

| Variable | Default | Purpose |
|----------|---------|---------|
| `DIGITALKIN_STREAM_BATCH_SIZE` | `20` | Entries per pipeline flush |
| `DIGITALKIN_STREAM_FLUSH_MS` | `50` | Adaptive flush threshold — writes spaced further apart go directly via XADD |

### Backpressure

| Variable | Default | Purpose |
|----------|---------|---------|
| `DIGITALKIN_STREAM_BACKPRESSURE_THRESHOLD` | `0.8` | Throttle at 80% of maxlen |
| `DIGITALKIN_STREAM_BACKPRESSURE_DELAY_MS` | `50` | Sleep duration when throttled |
| `DIGITALKIN_STREAM_BACKPRESSURE_CHECK_INTERVAL` | `100` | Check XLEN every N writes |
| `DIGITALKIN_STREAM_BACKPRESSURE_TIMEOUT_S` | `30` | Max wait before forcing write |

### Auth / Tenant

| Variable | Default | Purpose |
|----------|---------|---------|
| `DIGITALKIN_AUTH_REQUIRED` | `false` | Require tenant_id in metadata |
| `DIGITALKIN_TENANT_HEADER` | `x-tenant-id` | gRPC metadata header name |
| `DIGITALKIN_MAX_STREAMS_PER_TENANT` | `500` | Per-tenant stream cap |
| `DIGITALKIN_RATE_LIMIT_WINDOW_S` | `60` | Rate limit window |
| `DIGITALKIN_RATE_LIMIT_MAX_REQUESTS` | `100` | Max requests per window |
| `DIGITALKIN_MAX_STREAM_DURATION_S` | `3600` | Max stream lifetime |

### Performance

| Variable | Default | Purpose |
|----------|---------|---------|
| `DIGITALKIN_UVLOOP` | `true` | Enable uvloop event loop |

### gRPC Keepalive

| Variable | Default | Purpose |
|----------|---------|---------|
| `DIGITALKIN_GRPC_KEEPALIVE_TIME_MS` | `60000` | Client keepalive interval |
| `DIGITALKIN_GRPC_KEEPALIVE_TIMEOUT_MS` | `20000` | Keepalive timeout |
| `DIGITALKIN_GRPC_MIN_PING_INTERVAL_MS` | `30000` | Min time between pings |
| `DIGITALKIN_GRPC_SERVER_KEEPALIVE_TIME_MS` | `120000` | Server keepalive |
| `DIGITALKIN_GRPC_SERVER_MIN_PING_INTERVAL_MS` | `10000` | Server min ping interval |

---

## Bugs Fixed

| Bug | Root cause | Fix |
|-----|-----------|-----|
| Duplicate seq=1 in Redis stream | Two ProtoStreamWriter instances starting at _seq=0 | Added `restore_seq()` — reads last entry via XREVRANGE |
| Output went to queue, consumer read from Redis | `_start_module` used output_queue, ConsumeStream used Redis | Write to Redis via ProtoStreamWriter in _start_module |
| "empty stream" race at high concurrency | Session unregistered before ConsumeStream connected | Moved cleanup to ConsumeStream completion + late-consumer fallback |
| "Too many pings" GOAWAY | 50 gRPC channels each sending keepalive | Shared channel + increased keepalive interval |
| Redis MaxConnectionsError | Pool default 10, needed 1000+ | Pool auto-scales, split into read/write pools |
| Mission ID exhaustion | Hardcoded list of ~1000 IDs | Cycling iterator (get_mission_id) |
| `write_eos()` not called on early exit | Proto writer created after early-exit checks | Restructured: writer created first, try/finally always calls write_eos |
| `SendSignal` always returned success=True | No error handling | Try/except + Redis pub/sub fallback |
| `coroutine never awaited` in setup.py | Sync wrapper calling async SDK methods | Added async/await |
| `grpc.RpcError` construction crash | ABC can't be instantiated | register() returns bool, caller uses context.abort |
| Batch flush timer caused P50 regression | asyncio.Task creation + 50ms sleep per write | Adaptive flush: time-check on write, no background tasks |

## Security Hardening

| Fix | Impact |
|-----|--------|
| `validate_id()` regex on all user-provided IDs | Prevents Redis key injection |
| `tenant_id` on StreamSession + Redis hash | Enables tenant isolation |
| Auth interceptor mandatory when `AUTH_REQUIRED=true` | Prevents bypass |
| `mask_redis_url()` in all logs | No credential leakage |
| Error messages sanitized (no task_id echo, no config details) | No info leakage |
| `from_seq` bound to `STREAM_MAXLEN * 10` | Prevents DoS via seek |

## Code Quality

| Improvement | What |
|-------------|------|
| `gateway_constants.py` | All magic numbers → named constants with env var overrides |
| Redis key helpers | `session_key()`, `stream_key()`, `cursor_key()`, etc. — no hardcoded strings |
| No `hasattr()` | Explicit attribute initialization in `__init__` |
| No `._client` access | All Redis ops through RedisClient wrappers |
| Split Redis pools | Blocking XREAD can't starve non-blocking writes |

## Performance Results

Stress test: template-tool (no LLM, instant response), single Docker instance, batch+uvloop+split pools.

| Metric | Value |
|--------|-------|
| Max throughput (c=1) | ~85 req/s |
| P50 at c=1 | 6-15ms |
| P50 at c=50 | 366-530ms |
| P50 at c=200 | 2.1-2.5s |
| P50 at c=500 | 5-6.5s |
| Max sustained (500 concurrent, 10min) | 18,895 requests, 0 errors |
| Redis memory peak | 25 MB |
| Redis ops/request | ~25 |

### Single-instance limits

- **Sweet spot:** 25-50 concurrent (P50 < 500ms)
- **Throughput ceiling:** ~85 req/s at c=1, plateaus at ~60 req/s at c=100+
- **Scale trigger:** >50 concurrent for sub-500ms P50
- **Horizontal formula:** 1 instance per 50 concurrent users at 500ms SLA

## Tests

381 tests passing. Key test files:

| File | Tests | Covers |
|------|-------|--------|
| `tests/gateway/test_gateway_servicer.py` | 10 | All 4 RPCs, capacity, no-Redis error |
| `tests/gateway/test_gateway_servicer_extended.py` | 8 | Late consumer, EOS on all paths, SendSignal fallback |
| `tests/gateway/test_stream_registry.py` | 7 | Capacity, LRU eviction, shutdown |
| `tests/gateway/test_stream_session.py` | 7 | Init, enqueue, stop, teardown |
| `tests/core/redis/test_proto_streams.py` | 15 | Writer, reader, roundtrip, batch mode, zero-copy |
| `tests/core/redis/test_proto_streams_restore.py` | 9 | restore_seq, restore_cursor, xrevrange |

## What's next

1. **Horizontal scaling** — standalone gateway behind load balancer, multiple instances
2. **Consumer groups** — XREADGROUP for automatic rebalancing on crash
3. **Per-tenant Redis key scoping** — `task:{tenant_id}:{task_id}:stream`
4. **Observability** — Redis memory monitoring background task, structured latency logs
