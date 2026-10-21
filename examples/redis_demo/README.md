# Redis Gateway Demo — EchoModule

Same architecture as template-tool: `ToolModule` + `TriggerHandler` + `ModuleServer` with embedded `GatewayServicer`.

## Structure

```
examples/redis_demo/
├── echo_module.py          # EchoToolModule (ToolModule subclass)
├── models/
│   ├── input.py            # MessageInputPayload + EchoInput
│   ├── output.py           # MessageOutputPayload + EchoOutput
│   ├── setup.py            # EchoSetup (uppercase, repeat, delay, prefix, reverse)
│   └── secret.py           # EchoSecret (empty)
├── triggers/
│   └── message_trigger.py  # MessageTrigger (processes input, streams chunks)
├── server.py               # ModuleServer entry point
└── docker-compose.yml      # Redis container
```

## Run

```bash
docker compose -f examples/redis_demo/docker-compose.yml up -d
SERVICE_MODE=local DIGITALKIN_REDIS_URL=redis://localhost:6379/0 python examples/redis_demo/server.py
```

The server exposes `ModuleService` and the embedded `GatewayService` on one port (`SERVER_CHANNEL_PORT`).

## Calling it

There is no bundled client: a consumer calls `StartStream` with an `x-client-address` pointing at its own
`GatewayService.Stream` endpoint, the module dials back, receives the query as the consumer's first reply,
and streams `MessageOutputPayload` chunks followed by `stream.end`. The full contract is in
[`docs/gateway_protocol.md`](../../docs/gateway_protocol.md).
