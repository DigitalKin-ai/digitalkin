# DigitalKin Python SDK

The DigitalKin Python SDK for building and managing modules within the DigitalKin agentic mesh. Create custom **Tools** and **Archetypes** that communicate over gRPC, register with a service mesh, and scale independently.

## Features

- **Async-native gRPC module system** — every module is a gRPC server built on `grpcio` with full async support
- **Typed module contracts** — Pydantic models for Input, Output, Setup, and Secret schemas with protocol-based trigger dispatch
- **Module-to-module communication** — tools and archetypes discover each other via the registry and exchange requests over gRPC
- **Tool resolution** — archetypes dynamically resolve and invoke tool modules at runtime
- **Admission queue & backpressure** — built-in request admission with configurable concurrency limits
- **Healthcheck protocols** — automatic ping, services, and status healthcheck triggers registered on every module
- **Profiling** — optional `[profiling]` extra with asyncio-inspector, pyinstrument, viztracer, and yappi
- **Batched history writes** — efficient storage writes for conversation history
- **Redis Streams** — durable message passing, crash recovery, and reconnection

## Installation

```bash
# With uv (recommended)
uv add digitalkin

# With pip
pip install digitalkin
```

**Optional extras:**

```bash
# Async profiling tools
uv add "digitalkin[profiling]"

# uvloop for faster event loop
uv add "digitalkin[performance]"
```

## Quick Start

### 1. Define your models

```python
from pydantic import BaseModel
from digitalkin.models.module.base_types import DataModel, DataTrigger


class MessageInput(DataTrigger):
    protocol: str = "message"
    content: str


class InputModel(DataModel[MessageInput]):
    root: MessageInput


class MessageOutput(DataTrigger):
    protocol: str = "message"
    reply: str


class OutputModel(DataModel[MessageOutput]):
    root: MessageOutput
```

### 2. Create a module and trigger

```python
from digitalkin import ArchetypeModule, ModuleContext, TriggerHandler
from digitalkin.models.module.setup_types import SetupModel


class MyArchetype(ArchetypeModule[InputModel, OutputModel, SetupModel, BaseModel]):
    async def initialize(self, context: ModuleContext, setup_data: SetupModel) -> None:
        pass

    async def cleanup(self, context: ModuleContext) -> None:
        pass


@MyArchetype.register
class MessageTrigger(TriggerHandler[InputModel, SetupModel, OutputModel]):
    protocol = "message"
    input_format = InputModel
    output_format = OutputModel

    def __init__(self, context: ModuleContext) -> None:
        super().__init__(context)

    async def handle(
        self,
        input_data: InputModel,
        setup_data: SetupModel,
        context: ModuleContext,
    ) -> None:
        output = OutputModel(root=MessageOutput(reply=f"Echo: {input_data.root.content}"))
        await self.send_message(context, output)
```

### 3. Run the server

```python
import asyncio
from digitalkin.grpc_servers.module_server import ModuleServer

async def main() -> None:
    server = ModuleServer(MyArchetype)
    await server.start_async()
    await server.await_termination()

asyncio.run(main())
```

## Redis Gateway

The embedded gateway enables real-time bidirectional communication between modules via Redis Streams, with crash recovery and horizontal scaling.

- **Durable Streaming**: Output persisted to Redis Streams — reconnection via `from_seq`.
- **Zero-Copy Proto**: Binary proto serialization to Redis — no JSON intermediary.
- **Horizontal Scaling**: Each module instance embeds its own gateway. Scale by adding replicas behind a load balancer.

## Development

### Prerequisites

- Python 3.10+
- [uv](https://astral.sh/uv) — modern Python package management
- [Task](https://taskfile.dev/) — task runner

### Setup

```bash
git clone --recurse-submodules https://github.com/DigitalKin-ai/digitalkin.git
cd digitalkin

task setup-dev
source .venv/bin/activate
```

### Common Tasks

```bash
task linter               # Format + lint (ruff) + type check (mypy)
task check                # Linter + mypy + tests
task run-tests            # Run pytest via Docker
task build-package        # Build distribution
task bump-version -- patch|minor|major

task docs-serve           # Serve docs locally (mkdocs)
task docs-build           # Build docs

task generate-certificates  # Generate mTLS certs for gRPC
task clean                # Remove build artifacts + __pycache__
task clean-all            # Above + remove .venv
```

### Publishing Process

1. Update code and commit changes (following conventional branch/commit standard).
1. Use `task bump-version -- major|minor|patch` to commit the new version.
1. Use GitHub "Create Release" workflow to publish the new version.
1. Workflow automatically publishes to Test PyPI and PyPI.

## License

This project is licensed under the terms specified in the LICENSE file.

______________________________________________________________________

For more information, visit our [Documentation](https://digitalkin.com) or report issues on our [Issues page](https://github.com/DigitalKin-ai/digitalkin/issues).
