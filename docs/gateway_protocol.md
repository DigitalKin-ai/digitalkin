# Gateway Protocol

External clients (web UI, modules, any gRPC caller) talk to a producer module through the **Gateway** — a single small gRPC service. This document is language-agnostic; examples use Python and TypeScript pseudo-code interchangeably.

## TL;DR

Three client-facing RPCs (plus `AssociateTask` for nested runs), one BiDi data channel, in-band lifecycle:

```
  Client                      Gateway                       SDK module
   │                             │                              │
   │ ── StartStream ────────────►│                              │
   │ ◄────────── ack ────────────│                              │
   │                             │ ── dispatch (Redis) ────────►│
   │                             │                              │
   │ ── Stream(StreamRequest) ──►│                              │
   │                             │ ◄── output (Redis) ──────────│
   │ ◄── StreamResponse{stream.start}──                          │
   │ ◄── StreamResponse{<output>}──                             │
   │      ...                    │                              │
   │ ◄── StreamResponse{stream.end}──                            │
```

1. **`StartStream`** — unary. Reserves a task slot and dispatches the module. Returns `{ accepted, task_id }`.
2. **`Stream`** — BiDi. Client sends `StreamRequest` messages (the first carries the **query**, in `data`). Server
   yields `StreamResponse` messages until the stream closes cleanly.
3. **`SendSignal`** — unary. Out-of-band controls: cancel a task, or invalidate caches.

Lifecycle, errors, warnings travel **inside the data channel** as Struct sentinels (`stream.start`, `stream.end`, `stream.error`). gRPC status codes are not used to signal stream-level events on `Stream`.

---

## Service surface

```proto
service GatewayService {
  rpc AssociateTask(AssociateTaskRequest) returns (AssociateTaskResponse);
  rpc StartStream(StartStreamRequest) returns (StartStreamResponse);
  rpc Stream(stream StreamRequest) returns (stream StreamResponse);
  rpc SendSignal(SendSignalRequest) returns (SendSignalResponse);
}
```

Messages live in `agentic_mesh_protocol/gateway/v1/`: requests and responses of the unary RPCs in
`gateway_dto.proto`, the stream frames and signals in `gateway_messages.proto`, `CacheScope` in
`gateway_enums.proto`. Every field carries `buf.validate` rules; the SDK checks every unary request
against them and answers `INVALID_ARGUMENT` before the servicer runs. `Stream` frames are not
rejected that way — a bad frame surfaces in-band as `stream.error`.

```
```

### `StartStream`

| Field | Type | Notes |
|---|---|---|
| `task_id` | `string` | Required. Client-chosen unique ID (UUIDv4 recommended). |
| `setup_id` | `string` | Required. Must start with `setups:`. |
| `mission_id` | `string` | Required. Must start with `missions:`. |

Returns `{ accepted: bool, task_id: string }`. `accepted=false` means the gateway is at capacity; do not open `Stream`.
Malformed IDs are refused with `INVALID_ARGUMENT`.

### `Stream` — `StreamRequest` (client → gateway)

```
{ uint64 from_seq, string task_id, google.protobuf.Struct data }
```

- **First message** (mandatory): `task_id` set, `data` carries the **query** that becomes the SDK module's first input.
- **Subsequent messages**: `data` carries any additional upstream input (multi-turn, tool responses, …). `task_id` and `from_seq` are ignored after the first.

### `Stream` — `StreamResponse` (gateway → client)

```
{ uint64 seq, string task_id, google.protobuf.Struct data }
```

- `seq`: monotonic from 1, assigned by the gateway when persisting to Redis. **Stateful clients** save the highest seq received and pass it back as `from_seq` on reconnect; **stateless clients** ignore it.
- `data`: a Struct with a top-level `root` object whose `protocol` field disambiguates payload type.

### `SendSignal`

`SendSignalRequest` holds exactly one signal (a required `oneof signal`):

| Signal                            | Fields                                            | Effect                 |
|-----------------------------------|---------------------------------------------------|------------------------|
| `cancel` — `CancelSignal`         | `task_id` (required)                              | Per-task cancellation. |
| `invalidate` — `InvalidateSignal` | `scope: CacheScope` (required, not `UNSPECIFIED`) | Cache invalidation.    |

Returns `{ success: bool, task_id: string }` (`task_id` is empty for an invalidation).

```
enum CacheScope {
  CACHE_SCOPE_UNSPECIFIED = 0;
  ALL      = 1;  // wipe all caches
  CHANNELS = 2;  // gRPC channel pool, stubs, CB, bulkhead
  MODELS   = 3;  // Pydantic model class cache
  SETUP    = 4;  // setup JSON cache of one setup
  TOOLS    = 5;  // resolved tools of one setup
  SHARED   = 6;  // BaseModule._shared (litellm, toolkits, ...)
}
```

`cancel` publishes on the per-task Redis pub/sub channel. `invalidate` is a server-wide operation routed to the SDK's
cache handler — it does not affect any in-flight tasks. `SETUP` and `TOOLS` are scoped to one setup, read from the
caller's `x-setup-id` metadata header; without it they are skipped with a warning.

---

## The `stream.*` sentinel namespace

Every gateway-emitted control entry uses a Struct shaped:

```
data = { "root": { "protocol": "stream.<verb>", ... } }
```

Domain output from the module uses **non-prefixed** protocols (`text_chunk`, `tool_call`, `agui_event`, …) — they cannot collide with control sentinels.

| `protocol` | Fields | Meaning |
|---|---|---|
| `stream.start` | `task_id, mission_id, setup_id, started_at` | First entry on every stream. Seeded by the gateway. |
| `stream.end` | `task_id` | **Last** entry on every stream. Always present, no exceptions. |
| `stream.error` | `code, message, fatal, task_id` | Failure event. If `fatal=true`, immediately followed by `stream.end`. |
| `stream.warn` | `code, message` | Recoverable issue. Stream continues. |

**Invariant:** every stream ends with exactly one `stream.end`. Fatal errors are *two* writes — `stream.error(fatal=true)` then `stream.end` — because the diagnostic event and the structural terminator have separate jobs.

`code` values follow gRPC status names: `INVALID_ARGUMENT`, `NOT_FOUND`, `RESOURCE_EXHAUSTED`, `INTERNAL`, `UNAVAILABLE`, …

---

## Resume semantics — `from_seq`

Two profiles, no extra fields needed:

**Stateless** (web UIs, simple callers)
- Always send `from_seq = 0`.
- Server replays the full stream from `seq=1`.
- Ignore `seq` on `StreamResponse`.
- After a disconnect: reconnect with `from_seq=0`. Expect duplicate delivery; that's the cost of being stateless.

**Stateful** (durable consumers)
- Track `highest_seq` = max seq received.
- On reconnect: send `from_seq = highest_seq`. Server delivers `seq > from_seq` only.
- Optional gap detection: if `next.seq != prev.seq + 1`, an upstream truncation happened (Redis stream trim).

The resume window is bounded by Redis stream retention. If `from_seq` predates the oldest retained entry, the server delivers from the oldest available — the client sees a seq jump.

---

## Server-initiated dial-back (callback flow)

Two ways to consume a task's output:

1. **Client opens `Stream`** (default). Module-to-module callers fit this — they're already long-lived, can hold a BiDi connection, and prefer to pull.
2. **Server dials the client** (callback). External clients (web UI / chainlit) that prefer to be pushed to. The client runs its own `GatewayService` server (same proto, no extra service definition) and tells the gateway where to dial.

### How to opt in

Add the gRPC metadata header `x-client-address: host:port` to the `StartStream` call. The gateway acks the unary, then opens a BiDi to the address you advertised.

```python
metadata = (("x-client-address", "10.0.0.5:50080"),)
ack = await stub.StartStream(req, metadata=metadata)
```

### Init handshake

Exactly one extra round on the BiDi at startup; everything after is a normal `Stream` flow inverted in direction.

```
Gateway (gRPC client)                              Consumer (gRPC server)
        │                                                  │
        │ ── StreamRequest(data={protocol:"stream.init"}) ─►│
        │                                                   │
        │ ◄── StreamResponse(data=<query>) ─────────────────│  ← consumer sends the query
        │                                                   │
        │ ── StreamRequest(from_seq=N, data=<output_N>) ───►│  ← gateway pushes outputs
        │ ── StreamRequest(from_seq=N+1, data=<output_N+1>)►│
        │      ...                                          │
        │ ── StreamRequest(data={protocol:"stream.end"}) ──►│  ← terminator
```

The query payload from the consumer is delivered to the SDK module exactly like the first message in the client-initiated `Stream` flow. The dispatcher unblocks on `session.input_queue` once the query lands. From there, the producer's lifecycle is identical to the standard path.

### Field semantics in the dial-back direction

The gateway is the gRPC client here, so it sends `StreamRequest` and reads `StreamResponse` — the same messages, no
extra service definition. The semantics on the **gateway → consumer** push direction:

- `StreamRequest.task_id` — repeated on every message; the consumer can multiplex many concurrent pushes by task.
- `StreamRequest.from_seq` — repurposed as the **per-message seq** (the originating `seq` from `_consume_from_redis`).
  On the client-initiated flow `from_seq` is the resume point; here it's the per-frame counter.
- `StreamRequest.data` — the actual output payload, identical to what `Stream`'s `StreamResponse.data` would carry.

On the **consumer → gateway** upstream direction:

- `StreamResponse.data` — first message is the query; subsequent messages are additional upstream input (multi-turn
  turns, tool replies). Both feed the module's `session.input_queue`.

### Buffer and recovery

Outputs are persisted to the Redis stream `task:<task_id>:stream` with retention = `STREAM_TTL_S`. If the dial-back BiDi drops mid-stream:

- The producer keeps writing to Redis (no back-pressure on the producer from a flaky consumer).
- The consumer can recover by re-issuing `StartStream` (server dedups on existing `task_id`) and either re-advertising `x-client-address` for another push attempt, or opening `Stream` BiDi directly with `from_seq=<last_seq>` to pull the rest.
- Data remains available for the configured retention window.

### Consumer-side skeleton (Python)

```python
class ConsumerCallback(gateway_service_pb2_grpc.GatewayServiceServicer):
    async def Stream(self, request_iterator, context):
        # 1. First incoming StreamRequest should be stream.init.
        first = await anext(request_iterator)
        # (sanity-check: first.data.fields["root"].struct_value.fields["protocol"] == "stream.init")

        # 2. Send the query as the first StreamResponse reply.
        query = struct_pb2.Struct()
        query.update({"protocol": "agui_stream", "messages": [...]})
        yield gateway_messages_pb2.StreamResponse(seq=0, task_id=first.task_id, data=query)

        # 3. Read pushed outputs.
        async for msg in request_iterator:
            proto = msg.data.fields.get("root")
            if proto and proto.struct_value.fields["protocol"].string_value == "stream.end":
                return
            handle(msg.data)
            # Optional: yield more StreamResponse messages for additional upstream input
```

The consumer does not implement `StartStream` or `SendSignal` (those are gateway-only); only `Stream` is needed. Most gRPC servers let you implement just one method of a service.

### When to use which flow

- **Use `Stream` BiDi (client-initiated)** if your consumer is a module or any long-lived process that can hold an outbound gRPC stream open.
- **Use callback dial-back (server-initiated)** if your consumer is a web UI/edge service that prefers receiving pushes, can run a small gRPC server, and is reachable from the gateway (no NAT/firewall blocks the gateway → consumer direction).

The two flows coexist on the same gateway. A consumer that doesn't pass `x-client-address` is not affected by the callback path.

---

## Quick start — Python

```python
import uuid
import grpc
from google.protobuf import struct_pb2
from agentic_mesh_protocol.gateway.v1 import gateway_dto_pb2, gateway_messages_pb2, gateway_service_pb2_grpc

async def call(host, setup_id, mission_id, query):
    channel = grpc.aio.insecure_channel(host)
    stub = gateway_service_pb2_grpc.GatewayServiceStub(channel)

    task_id = str(uuid.uuid4())

    # 1. StartStream — get the ack.
    ack = await stub.StartStream(gateway_dto_pb2.StartStreamRequest(
        task_id=task_id, setup_id=setup_id, mission_id=mission_id,
    ))
    if not ack.accepted:
        raise RuntimeError("rejected")

    # 2. Build the query Struct.
    data = struct_pb2.Struct()
    data.update({"root": {"protocol": "agui_stream", "messages": [query]}})

    # 3. Open Stream BiDi — first message carries the query.
    async def client_stream():
        yield gateway_messages_pb2.StreamRequest(task_id=task_id, from_seq=0, data=data)

    async for msg in stub.Stream(client_stream()):
        proto = msg.data.fields["root"].struct_value.fields["protocol"].string_value
        if proto == "stream.start":
            continue
        if proto == "stream.error":
            err = msg.data.fields["root"].struct_value.fields
            print(f"ERROR {err['code'].string_value}: {err['message'].string_value}")
            # If fatal, stream.end follows; we just keep iterating.
            continue
        if proto == "stream.end":
            break
        # Domain output — handle as needed
        handle(msg.data)

    await channel.close()
```

## Quick start — TypeScript / Node

```ts
import { v4 as uuid } from "uuid";
import { Struct } from "google-protobuf/google/protobuf/struct_pb";
import { GatewayServiceClient } from "./gen/gateway_service_grpc_pb";
import { StartStreamRequest } from "./gen/gateway_dto_pb";
import { StreamRequest } from "./gen/gateway_messages_pb";

async function call(host: string, setupId: string, missionId: string, query: any) {
  const client = new GatewayServiceClient(host, /* credentials */);
  const taskId = uuid();

  // 1. StartStream
  const ack = await new Promise<any>((resolve, reject) => {
    const req = new StartStreamRequest()
      .setTaskId(taskId).setSetupId(setupId).setMissionId(missionId);
    client.startStream(req, (err, resp) => err ? reject(err) : resolve(resp));
  });
  if (!ack.getAccepted()) throw new Error("rejected");

  // 2. Build query Struct
  const data = Struct.fromJavaScript({ root: { protocol: "agui_stream", messages: [query] } });

  // 3. Open Stream BiDi — first message carries the query
  const call = client.stream();
  const first = new StreamRequest().setTaskId(taskId).setFromSeq(0).setData(data);
  call.write(first);

  for await (const msg of call) {
    const proto = msg.getData().getFieldsMap().get("root")
                     .getStructValue().getFieldsMap().get("protocol").getStringValue();
    switch (proto) {
      case "stream.start": continue;
      case "stream.error":
        const err = msg.getData().getFieldsMap().get("root").getStructValue().getFieldsMap();
        console.error(`ERROR ${err.get("code").getStringValue()}: ${err.get("message").getStringValue()}`);
        continue;
      case "stream.end":
        call.end();
        return;
      default:
        handle(msg.getData());
    }
  }
}
```

---

## Patterns & gotchas

### One client per task

Each task is its own gRPC stream. Don't multiplex multiple tasks onto one `Stream` call — `task_id` is bound on the first message.

### The first `StreamRequest` carries the query

There is no separate "init" message. The first frame is **both** the registration (task_id + from_seq) **and** the query (data). The server delivers `data` to the SDK module as its first input.

### Sending more upstream input

After the first message you may keep sending `StreamRequest` frames; only `data` is read. Use this for multi-turn
conversation, tool responses streamed back, etc.

### Cancelling

`SendSignal(cancel=CancelSignal(task_id=<tid>))`. The gateway publishes on `signal_ch:<tid>`; the SDK module receives
the signal and shuts down. Your `Stream` call ends with the usual `stream.end`.

### Cache invalidation

`SendSignal(invalidate=InvalidateSignal(scope=<CacheScope>))` is server-wide (`SETUP` / `TOOLS` need the `x-setup-id`
header). Running tasks are not affected — a dict-swap pattern preserves their references. Useful for forcing a re-fetch
of setups/tools after a configuration change.

### Errors are NOT `aio.AioRpcError`

A misbehaving `Stream` call **does not** raise `aio.AioRpcError` from the call iterator on common failures — it yields a `stream.error` Struct, then `stream.end`, then closes cleanly. Treat your RPC iteration as data-only; reserve gRPC-level exception handling for transport faults (channel down, deadline exceeded).

### Recoverable warnings

`stream.error(fatal=false)` and `stream.warn` keep the stream open. Log them; don't tear down on every error event.

### Timing

Timestamps are not in `StreamResponse` (intentionally minimal). Stamp on receive if you need them. The gateway logs
end-to-end latency server-side.

---

## Wire-shape cheat-sheet

```
StartStreamRequest  := { task_id, setup_id, mission_id }
StartStreamResponse := { accepted, task_id }

StreamRequest   := { from_seq, task_id, data: Struct }   // client → server
StreamResponse  := { seq, task_id, data: Struct }        // server → client

SendSignalRequest  := { oneof signal { cancel: { task_id } | invalidate: { scope: CacheScope } } }
SendSignalResponse := { success, task_id }
```

`data.root.protocol` discriminates payload type. `stream.*` is reserved for gateway-emitted control sentinels. Module-defined protocols (your domain output) use any other string.

---

## What changed from earlier protocol versions

For repos migrating from the previous shape:

- `ProduceStream` RPC and all `ProduceStream*` messages → **deleted**. Producer modules write directly to Redis; no gRPC connection from module to gateway.
- `ConsumeStream` → renamed `Stream`.
- `GatewayResponse` envelope, `StreamStatus`, `StreamError`, `ServerHeartbeat`, `Checkpoint` → **deleted**. Use `stream.*` sentinels instead.
- `StartStreamRequest.input` field → **deleted**. The query lives on the first `StreamRequest.data`.
- `feat/validation-proto`: stream frames renamed after their direction — `StreamClient` → `StreamRequest`,
  `StreamServer` → `StreamResponse` (same field numbers, wire-compatible).
  `ClientSignalRequest{task_id, action: SignalAction}` → `SendSignalRequest{oneof cancel | invalidate}`;
  `SignalAction.INVALIDATE_*` → `CacheScope`. Python imports move from `gateway_pb2` to `gateway_dto_pb2` /
  `gateway_messages_pb2` / `gateway_enums_pb2`.
- Sentinel rename: `module_start_info` → `stream.start`; `end_of_stream` → `stream.end`.
