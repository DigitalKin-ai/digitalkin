---
paths:
  - "src/digitalkin/grpc_servers/**/*.py"
  - "src/digitalkin/services/communication/**/*.py"
  - "**/*.proto"
---

# gRPC rules

## Error mapping
Servicer error paths convert exceptions to the correct `grpc.StatusCode`: `NOT_FOUND`, `INVALID_ARGUMENT`, `PERMISSION_DENIED`, `FAILED_PRECONDITION`, `RESOURCE_EXHAUSTED`, `DEADLINE_EXCEEDED`.

A bare exception surfacing as `UNKNOWN` is a defect. `INTERNAL` is not a catch-all.

## Streaming
Output goes through `context.callbacks.send_message()` and the asyncio.Queue path, never an ad-hoc return. No unbounded buffering before yield.

Lifecycle events travel in-band as `stream.*` sentinels in the data Struct's `protocol` field. Fatal paths emit `stream.error` then `stream.end`; never `context.abort` on `Stream`.

## Channels and interceptors
Cross-cutting client concerns — permission, authz, request-ID propagation — belong in a channel-level interceptor in `_init_channel`, not in per-service branches. Let the typed error pass through `exec_grpc_query`.

`x-task-id` / `x-setup-id` / `x-mission-id` ride on every call via ContextVar plus interceptors.

## Server mode
Secure vs insecure is a deliberate choice. Flag `insecure` outside local and test.

## Proto changes
Wire-breaking changes need a package version bump (`vN` → `vN+1`): tag reuse, tag removal without `reserved`, type change, enum value removal or reorder, cardinality change, service or RPC rename.

When a refactor spans both repos, land the proto phase first and stop. Never auto-roll the SDK phase.
