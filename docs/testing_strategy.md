# Testing Strategy — Gateway + Redis Architecture (20K Concurrent)

## Scope

Tests for each phase of the scaling plan. Every new component must have tests before merge.

---

## 1. Unit Tests

### StreamRegistry (Redis-backed)

| Test | Category |
|------|----------|
| `register()` writes to Redis hash + increments counter | happy path |
| `register()` rejected when Lua cap reached | capacity |
| `unregister()` decrements counter, removes hash | cleanup |
| `get()` returns from LRU cache (no Redis hit) | cache |
| `get()` falls back to Redis on cache miss | cache miss |
| LRU eviction when cache exceeds bound | memory |
| Heartbeat sorted set updated on `touch_heartbeat()` | heartbeat |
| Reaper `ZRANGEBYSCORE` finds expired sessions | reaper |
| Reaper skips fresh sessions | reaper |
| Concurrent `register()` on same task_id (idempotent) | concurrency |

### Redis Pool (multi-pool)

| Test | Category |
|------|----------|
| Stream pool, session pool, pub/sub pool are separate | isolation |
| Pool exhaustion on stream pool doesn't block session ops | isolation |
| `pool_stats()` returns correct counts | monitoring |
| Pool auto-scales with `DIGITALKIN_REDIS_POOL_SIZE` env | config |

### ProtoStreamWriter/Reader (consumer groups)

| Test | Category |
|------|----------|
| `XREADGROUP` creates consumer group on first read | setup |
| `XACK` after successful delivery | ack |
| `XAUTOCLAIM` recovers pending messages after crash | recovery |
| Unacked messages redelivered to new consumer | failover |
| Multiple consumers in same group get different entries | fanout |
| `restore_seq()` works with consumer group streams | compat |
| Write-side backpressure: sleep at 80% maxlen | throttle |
| Write-side backpressure: block at 100% maxlen | block |

### StreamGC

| Test | Category |
|------|----------|
| Completed streams (EOS acked) deleted immediately | gc |
| Active streams not deleted | gc safety |
| Orphaned streams get short TTL | gc orphan |
| GC runs on configurable interval | config |

### Auth Interceptor

| Test | Category |
|------|----------|
| Extracts `tenant_id` from gRPC metadata | parse |
| Rejects request when `tenant_id` missing | auth |
| Per-tenant cap enforced via Redis counter | cap |
| Rate limit rejects burst above threshold | rate |
| Sliding window expires old entries | rate decay |
| Tenant-scoped Redis keys: `task:{tenant_id}:{task_id}:stream` | isolation |

### Routing Cache

| Test | Category |
|------|----------|
| Cache hit returns stored `(address, port)` | cache |
| Cache miss calls registry, stores result | miss |
| TTL expiration triggers re-fetch | expiry |
| Concurrent lookups for same setup_id don't duplicate calls | dedup |

---

## 2. Integration Tests

### Gateway End-to-End (single instance)

| Test | Category |
|------|----------|
| `StartStream` → `ConsumeStream` → receive all chunks → `COMPLETED` | happy path |
| Late consumer: module finishes before `ConsumeStream` connects | race |
| Fast module (~2ms): no empty stream errors | regression |
| `SendSignal` cancel stops module, consumer gets `COMPLETED` | signal |
| 50 concurrent `StartStream` + `ConsumeStream`, 0 errors | concurrency |
| Module crashes mid-stream: consumer gets EOS, no hang | error |
| Redis pool exhaustion under load: graceful degradation | resource |

### Gateway + Multiple Module Servers

| Test | Category |
|------|----------|
| Route to Ada (setup_id A) and template-tool (setup_id B) | routing |
| Module server restart mid-stream: consumer gets error, not hang | resilience |
| Registry returns updated address after module migration | discovery |

### Redis State

| Test | Category |
|------|----------|
| Session hash created on `StartStream`, removed after `ConsumeStream` | lifecycle |
| Global counter incremented on register, decremented on unregister | counter |
| Stream TTL applied after EOS | ttl |
| Tiered TTL: completed=60s, orphaned=30s | ttl tiers |
| `XLEN` stays below `maxlen` under sustained load | trimming |

---

## 3. Contract Tests

| Test | Category |
|------|----------|
| `StartStreamRequest` → `StartStreamResponse(accepted, task_id)` | proto |
| `ConsumeStreamInit(task_id, from_seq)` → `GatewayResponse(output\|status\|error\|heartbeat)` | proto |
| `ClientSignalRequest(task_id, action)` → `ClientSignalResponse(success)` | proto |
| `ModuleOutput` Pydantic model parses all protocol types from gateway response | compat |
| Proto `Struct` round-trip: write → Redis → read → identical content | serde |

---

## 4. Failure Scenarios

### Timeouts

| Test | Category |
|------|----------|
| `ConsumeStream` times out if no data after 300s | timeout |
| `_start_module` times out if module doesn't respond | timeout |
| Redis `xread` block timeout doesn't leak connections | resource |

### Retries

| Test | Category |
|------|----------|
| Client resends `StartStream` after gateway crash (stateless) | retry |
| `ConsumeStream` reconnect with `from_seq` resumes correctly | resume |

### Partial Outages

| Test | Category |
|------|----------|
| Redis down: `_start_module` falls back to in-memory queue | fallback |
| Redis recovers mid-stream: no data corruption | recovery |
| Module server down: `_start_module` returns EOS, not hang | module crash |
| One gateway instance dies: LB routes to surviving instance | failover |

### Network Partitions

| Test | Category |
|------|----------|
| Redis network partition: `write_struct` raises, caught in `_start_module` | partition |
| gRPC channel to module server drops: `call_module` raises, EOS written | partition |
| Consumer network drop: reaper cleans up after TTL | zombie |

---

## 5. Data Consistency

| Test | Category |
|------|----------|
| Seq monotonic: no gaps, no duplicates across writer lifecycle | ordering |
| `restore_seq()` after crash: next write continues correctly | ordering |
| Two writers on same task_id: detected/prevented (not silently corrupted) | idempotency |
| Consumer group: exactly-once delivery with `XACK` | delivery |
| Consumer group: at-least-once without `XACK` (redelivery on crash) | delivery |
| EOS always written on all exit paths (tested per exit path) | completeness |
| Tenant A's data never visible to tenant B | isolation |

---

## 6. Performance Tests

### Load

| Test | Concurrency | Duration | Target |
|------|-------------|----------|--------|
| `template-tool -c 200 -d 60` | 200 | 60s | 0 errors, <500ms p95 |
| `template-tool -c 500 -d 60` | 500 | 60s | 0 errors, <1s p95 |
| `ada -c 10 -d 120` | 10 | 120s | 0 errors |

### Stress

| Test | Concurrency | Duration | Target |
|------|-------------|----------|--------|
| `template-tool -c 1000 -d 60` | 1000 | 60s | <1% error rate |
| `template-tool -c 2000 -d 30` | 2000 | 30s | measure degradation curve |

### Spike

| Test | Pattern | Target |
|------|---------|--------|
| 0 → 500 → 0 in 10s burst | spike | recovery within 5s |
| 100 steady + 500 burst every 30s | mixed | no cascading failures |

### Soak

| Test | Concurrency | Duration | Target |
|------|-------------|----------|--------|
| `template-tool -c 100 -d 3600` | 100 | 1 hour | 0 errors, stable memory, no pool leak |
| `template-tool -c 500 -d 1800` | 500 | 30 min | Redis memory < 2GB |

---

## 7. Chaos / Fault Injection

| Test | Injection | Expected |
|------|-----------|----------|
| Kill Redis mid-stream | `docker kill redis` | EOS written (or fallback), consumer gets error, no hang |
| Kill gateway mid-stream | `docker kill gateway` | Client resends, new gateway handles from Redis |
| Kill module server mid-stream | `docker kill ada-server` | Gateway writes EOS, consumer gets COMPLETED |
| Network delay (100ms) on Redis | `tc qdisc add` | Latency increases, no errors |
| Network delay (500ms) on module | `tc qdisc add` | Latency increases, no timeouts |
| Redis OOM | `redis-cli CONFIG SET maxmemory 10mb` | Backpressure kicks in, graceful degradation |
| Packet loss 5% on gRPC | `tc qdisc add netem loss 5%` | Retries succeed, no data loss |

### Tooling
- `toxiproxy` for network fault injection (Python client: `toxiproxy-python`)
- `docker kill` / `docker pause` for process faults
- `tc qdisc` for network conditions (latency, loss, reorder)

---

## 8. Security Testing

| Test | Category |
|------|----------|
| Request without `tenant_id` metadata rejected | auth |
| Invalid `tenant_id` format rejected | auth |
| Tenant A can't consume tenant B's stream | isolation |
| Rate limit enforced: 429 equivalent after burst | rate |
| Large payload (>100MB) rejected at gRPC layer | dos |
| Malformed proto doesn't crash servicer | robustness |
| SQL/command injection in `setup_id` field has no effect | injection |

---

## 9. Deployment Validation

### Rolling Update

| Test | Category |
|------|----------|
| Old gateway instance drains active streams before shutdown | drain |
| New instance picks up new requests immediately | handoff |
| No 5xx during rolling restart with 2+ instances | zero-downtime |

### Canary

| Test | Category |
|------|----------|
| Route 10% traffic to new version via LB weight | canary |
| Compare error rate old vs new during canary period | validation |

### Rollback

| Test | Category |
|------|----------|
| Rollback to previous image: streams in Redis still readable | compat |
| Rollback doesn't corrupt Redis state | compat |

---

## 10. Observability Validation

| Test | Category |
|------|----------|
| `register()` logs `active_count` and latency | logging |
| `write_struct()` logs `stream_length` every 100 writes | logging |
| `read_structs()` logs `gap_count`, `read_latency` | logging |
| Pool exhaustion logged at ERROR level | logging |
| Redis memory > 70% logged at WARN | monitoring |
| Redis memory > 85% logged at ERROR | monitoring |
| All logs include `task_id` for correlation | tracing |

---

## 11. Disaster Recovery

| Test | Category |
|------|----------|
| Full Redis flush: system recovers, clients resend (stateless) | recovery |
| Gateway process crash: no orphaned resources after reaper TTL | cleanup |
| Redis failover (sentinel/cluster): gateway reconnects | failover |
| Full system restart: all services come up healthy | bootstrap |

---

## Tooling

| Tool | Purpose |
|------|---------|
| `pytest` + `pytest-asyncio` | Unit + integration tests |
| `fakeredis` | Redis mocking for unit tests |
| `pytest-timeout` | Prevent hanging tests |
| `hypothesis` | Property-based testing (seq ordering, data consistency) |
| `locust` or custom `stress_test_grpc.py` | Load/stress/soak tests |
| `toxiproxy` + `toxiproxy-python` | Network fault injection |
| `docker compose` | Integration environment |
| `grpcurl` | Contract/smoke tests |

## CI/CD Integration

```yaml
# Pipeline stages
stages:
  - unit:        pytest tests/core tests/gateway -q --timeout=15
  - integration: docker compose up -d && pytest tests/integration --timeout=60
  - contract:    grpcurl -plaintext localhost:50055 list  # verify services registered
  - load:        python scripts/stress_test_grpc.py template-tool -c 200 -d 60 --json
  - security:    pytest tests/security --timeout=30
```

- Unit tests run on every commit (fast, no external deps)
- Integration tests run on PR merge (requires Redis + module containers)
- Load tests run nightly or on release branch
- Chaos tests run weekly in staging
- Soak tests run before major releases
