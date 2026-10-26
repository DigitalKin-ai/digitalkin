<!-- --8<-- [start:intro] -->
# DigitalKin Python SDK

`digitalkin` is the Python SDK for writing modules (tools and archetypes) of the DigitalKin agentic mesh.
You declare typed Pydantic contracts and trigger handlers; the SDK serves them over gRPC.
Each module process embeds a gateway that persists outputs to Redis Streams and pushes them to the consumer.

**What it is not**

- Not a standalone gateway process: the gateway runs inside each module server.
- Not a registry server: modules register with an external registry in remote mode.
- Not a client library: the consumer calls `StartStream` and hosts the dial-back `Stream` endpoint itself.
- Not the wire contract: protos come from the `agentic-mesh-protocol` package.

## Install

`uv add digitalkin` or `pip install digitalkin` (Python 3.10+); add `[agno]` for the Agno integration.
Extras: `agno` (Agno integration), `performance` (uvloop), `profiling`.

## Quick start

Files: `echo_module.py`, `server.py`, `triggers/__init__.py`, `triggers/echo_trigger.py` (discovery imports `*_trigger.py`).

```python
# echo_module.py
from typing import Any, ClassVar, Literal
from pydantic import BaseModel
from digitalkin import ModuleContext, ToolModule
from digitalkin.models.module import DataModel, DataTrigger, SetupModel
from digitalkin.utils.package_discover import ModuleDiscoverer

class EchoIn(DataTrigger):
    protocol: Literal["message"] = "message"
    text: str

class EchoOut(DataTrigger):
    protocol: Literal["message"] = "message"
    reply: str

class EchoInput(DataModel[EchoIn]):
    root: EchoIn

class EchoOutput(DataModel[EchoOut]):
    root: EchoOut

class EchoSetup(SetupModel):
    prefix: str = "echo: "

class EchoModule(ToolModule[EchoInput, EchoOutput, EchoSetup, BaseModel]):
    name = "EchoModule"
    description = "Echoes the input text."
    input_format = EchoInput
    output_format = EchoOutput
    setup_format = EchoSetup
    secret_format = BaseModel  # no secrets
    metadata: ClassVar[dict[str, Any]] = {"module_id": "modules:echo", "version": "0.1.0"}
    services_config_params: ClassVar[dict[str, Any]] = {"storage": {"config": {}}, "cost": {"config": {}}}
    triggers_discoverer = ModuleDiscoverer(packages=["triggers"])

    async def initialize(self, context: ModuleContext, setup_data: EchoSetup) -> None:
        pass  # open clients, build agents

    async def cleanup(self) -> None:
        pass
```

```python
# triggers/echo_trigger.py
from digitalkin import ModuleContext, TriggerHandler
from echo_module import EchoIn, EchoModule, EchoOut, EchoOutput, EchoSetup

@EchoModule.register
class EchoTrigger(TriggerHandler):
    protocol = "message"
    input_format = EchoIn
    output_format = EchoOut

    async def handle(self, input_data: EchoIn, setup_data: EchoSetup, context: ModuleContext) -> None:
        reply = EchoOut(reply=setup_data.prefix + input_data.text)
        await context.callbacks.send_message(EchoOutput(root=reply))
```

```python
# server.py
import asyncio
from digitalkin.grpc_servers.module_server import ModuleServer
from echo_module import EchoModule

async def main() -> None:
    server = ModuleServer(EchoModule)
    await server.start_async()
    try:
        await server.await_termination()
    finally:
        await server.stop_async()

asyncio.run(main())
```

```bash
docker compose -f examples/redis_demo/docker-compose.yml up -d     # Redis on localhost:6379
SERVICE_MODE=local DIGITALKIN_REDIS_URL=redis://localhost:6379/0 python server.py
```

The server listens on `SERVER_CHANNEL_HOST:SERVER_CHANNEL_PORT` (default `[::]:50055`).
`SERVICE_MODE=local` (or `-d LOCAL`) uses in-process services; remote mode (default) needs the variables in `.env.minimum`.
Consumers drive the module with the gateway RPCs described in the gateway protocol page.

<!-- --8<-- [end:intro] -->

## Documentation

| Topic | Page |
| --- | --- |
| Architecture, data and signal flow | [docs/architecture.md](docs/architecture.md) |
| Writing modules | [docs/module_authoring.md](docs/module_authoring.md) |
| Wire contracts | [docs/gateway_protocol.md](docs/gateway_protocol.md), [docs/agui_events.md](docs/agui_events.md) (AG-UI) |
| Deploy and tune | [docs/operations.md](docs/operations.md) |
| Environment variables | [docs/environment_variables.md](docs/environment_variables.md) |
| Testing | [docs/testing.md](docs/testing.md) |
| Changelog | [docs/changelog/index.md](docs/changelog/index.md) |
| API reference | [docs/api/api.md](docs/api/api.md) |

## Development

```bash
task setup-dev     # venv, dev/docs/tests deps, pre-commit hooks
task check         # ruff format + lint, mypy, tests
task run-tests     # full suite in docker compose (needs Redis)
task docs-serve    # mkdocs serve
```

License: [CC BY-NC-SA 4.0](LICENSE).
