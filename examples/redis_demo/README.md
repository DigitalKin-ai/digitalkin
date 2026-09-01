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
├── client.py               # CLI client (StartStream + ConsumeStream)
└── docker-compose.yml      # Redis container
```

## Setup

```bash
# 1. Start Redis
docker compose -f examples/redis_demo/docker-compose.yml up -d

# 2. Start the server
DIGITALKIN_REDIS_URL=redis://localhost:6379/0 python examples/redis_demo/server.py

# 3. Test with the client
python examples/redis_demo/client.py full --prompt "Hello world"
python examples/redis_demo/client.py full --prompt "Test" --setup '{"uppercase": true, "repeat": 5}'
```

## How it works

1. `ModuleServer` starts with `EchoToolModule`
2. Because `DIGITALKIN_REDIS_URL` is set, `ModuleServer._register_gateway_servicer()` auto-registers the `GatewayServicer` on the same port
3. The server exposes both `ModuleService` and `GatewayService` on port 50051
4. Client calls `StartStream` → gateway registers session, calls `StartModule` via loopback → `MessageTrigger.handle()` runs → output goes through Redis stream → client reads via `ConsumeStream`
