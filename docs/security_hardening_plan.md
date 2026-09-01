# Security Hardening Plan — Gateway Architecture

## Audit Summary

4 CRITICAL, 7 HIGH, 5 MEDIUM, 3 LOW vulnerabilities identified. This plan addresses all CRITICAL and HIGH issues required for production deployment.

---

## CRITICAL — Must fix before any production traffic

### 1. Input Validation — Sanitize all user-provided IDs
- **`task_id`**: Regex `^[a-zA-Z0-9_-]{1,128}$`. Reject at `StartStream`, `ConsumeStream`, `ProduceStream`, `SendSignal`.
- **`tenant_id`**: Same regex. Reject at `TenantAuthInterceptor`.
- **`setup_id`**, **`mission_id`**: Same regex. Reject at `StartStream`.
- **Where:** New `_validate_id(value, field_name)` function in `gateway_constants.py`. Called at RPC entry points.
- **Files:** `gateway_servicer.py`, `auth_interceptor.py`, `gateway_constants.py`

### 2. Tenant Isolation — Bind task_id to tenant_id
- Add `tenant_id: str` to `StreamSession`.
- On `StartStream`: store `tenant_id` in session (from gRPC metadata).
- On `ConsumeStream`/`ProduceStream`/`SendSignal`: verify `session.tenant_id == current_tenant_id`. Reject with `PERMISSION_DENIED` if mismatch.
- Late consumer (no session): store `tenant_id` in Redis session hash (`gateway:session:{task_id}`). Verify on access.
- **Files:** `stream_session.py`, `gateway_servicer.py`, `stream_registry.py`

### 3. Auth Interceptor — Make tenant_id mandatory
- Change line 113: if `tenant_id` is missing, abort with `UNAUTHENTICATED` instead of passing through.
- Add env flag `DIGITALKIN_AUTH_REQUIRED=true` (default true). When false (dev/test), pass through without tenant_id.
- **File:** `auth_interceptor.py`

### 4. Redis Credential Safety
- Never log `redis_url` — mask password in log messages.
- Add `DIGITALKIN_REDIS_TLS_REQUIRED` env var (default false). When true, reject non-`rediss://` URLs.
- Document: production must use `rediss://` URLs with AUTH.
- **File:** `redis_client.py`

---

## HIGH — Fix before scaling beyond dev/staging

### 5. Per-Client Rate Limiting (independent of tenant)
- Add connection-level rate limiting via gRPC interceptor based on peer address (`context.peer()`).
- Limit: `DIGITALKIN_PER_IP_RATE_LIMIT` (default 50 req/s).
- Uses Redis sliding window (same Lua as tenant rate limit).
- **Files:** New method in `auth_interceptor.py`

### 6. Stream Idle Timeout
- In `ConsumeStream`: check `context.time_remaining()` every batch.
- Add server-side max stream duration: `DIGITALKIN_MAX_STREAM_DURATION_S` (default 3600 = 1h).
- If exceeded, yield `STREAM_STATE_COMPLETED` and close.
- **File:** `gateway_servicer.py`

### 7. gRPC Deadline Enforcement
- All BiDi RPCs (`ProduceStream`, `ConsumeStream`): check `context.cancelled()` in each loop iteration.
- Break on cancellation, clean up resources.
- **File:** `gateway_servicer.py`

### 8. Reduce gRPC Max Message Size
- `StartStream` (unary): reduce to 10MB (`grpc.max_receive_message_length`).
- BiDi streams: keep 100MB but add per-message size check before writing to Redis.
- Per-stream Redis memory cap: `STREAM_MAXLEN * max_message_bytes`. Log error if exceeded.
- **Files:** `models.py`, `proto_streams.py`

### 9. Error Message Sanitization
- Remove internal details from error responses sent to clients:
  - `"Gateway requires Redis — set DIGITALKIN_REDIS_URL"` → `"Service unavailable"`
  - `f"Task not found: {task_id}"` → `"Task not found"` (don't echo back)
  - `f"Setup not found: setup_id={...}"` → `"Invalid setup"`
- Keep detailed messages in server logs only.
- **File:** `gateway_servicer.py`

### 10. Task Enumeration Prevention
- Return identical error + timing for "session not found" and "stream not in Redis".
- Don't differentiate between "task never existed" and "task expired".
- **File:** `gateway_servicer.py`

### 11. Redis URL Masking in Logs
- `RedisClient` line 62: mask password in URL before logging.
- Pattern: `redis://user:****@host:port/db`
- **File:** `redis_client.py`

---

## MEDIUM — Backlog

### 12. `from_seq` bound to `STREAM_MAXLEN`
- Change `MAX_FROM_SEQ` from 100M to `STREAM_MAXLEN` (currently 1000).
- **File:** `gateway_constants.py`

### 13. Keepalive hardening
- Set `grpc.keepalive_permit_without_calls=False` on server side.
- Document that clients must have active RPCs to send keepalive.
- **File:** `models.py`

### 14. Session state TTL alignment
- Reduce `SESSION_STATE_TTL_S` to match `STREAM_TTL_S` (60s) or set to 300s.
- Currently 86400 (24h) — too long, leaks metadata.
- **File:** `gateway_constants.py`

---

## Files to modify

| File | Changes |
|------|---------|
| `gateway_constants.py` | `validate_id()`, `MAX_FROM_SEQ` bound, `SESSION_STATE_TTL_S` reduction |
| `gateway_servicer.py` | Input validation at all RPCs, tenant isolation checks, deadline enforcement, error sanitization |
| `stream_session.py` | Add `tenant_id` field |
| `stream_registry.py` | Store tenant_id in Redis session hash |
| `auth_interceptor.py` | Mandatory tenant_id, per-IP rate limit |
| `redis_client.py` | URL masking, TLS enforcement flag |
| `proto_streams.py` | Per-message size check |
| `models.py` | Reduce unary max message size, keepalive hardening |

## Tests

- Input validation: `task_id` with special chars (`*`, `\n`, `|`, `..`, 129+ chars) → rejected
- Tenant isolation: tenant A can't read tenant B's stream
- Auth bypass: missing header → `UNAUTHENTICATED`
- Idle timeout: stream closed after max duration
- Deadline: cancelled context stops streaming
- Error sanitization: no internal details in client-facing errors
- Redis URL masking: password not in logs
