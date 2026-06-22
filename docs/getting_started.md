# Getting Started

This guide walks you through installing the DigitalKin SDK and creating your first module.

## Installation

Install the SDK using pip:

```bash
pip install digitalkin
```

Or using [uv](https://astral.sh/uv):

```bash
uv add digitalkin
```

**Requirements**: Python 3.10+

## Quick Start: Creating Your First Module

A DigitalKin module consists of four Pydantic models (input, output, setup, secret) and a `TriggerHandler` that processes incoming requests.

### 1. Define Your Models

```python
from pydantic import BaseModel, Field
from digitalkin.models.module.module_types import DataModel, DataTrigger


class MessageInput(DataTrigger):
    """Input trigger for message processing."""

    protocol: str = "message"
    content: str = Field(default="", description="The user message.")


class ModuleInput(DataModel[MessageInput]):
    """Module input wrapping the message trigger."""

    root: MessageInput = Field(default_factory=MessageInput)


class MessageOutput(DataTrigger):
    """Output trigger for message responses."""

    protocol: str = "message"
    content: str = Field(default="", description="The response content.")


class ModuleOutput(DataModel[MessageOutput]):
    """Module output wrapping the message trigger."""

    root: MessageOutput = Field(default_factory=MessageOutput)


class ModuleSetup(BaseModel):
    """Module configuration."""

    greeting_prefix: str = Field(
        default="Hello",
        description="Prefix for the greeting response.",
    )


class ModuleSecret(BaseModel):
    """Module secrets (empty for this example)."""
```

### 2. Create a Trigger Handler

```python
from digitalkin.modules.trigger_handler import TriggerHandler
from digitalkin.models.module.module_context import ModuleContext


class MessageHandler(TriggerHandler[ModuleInput, ModuleSetup, ModuleOutput]):
    """Handles incoming message triggers."""

    protocol = "message"
    description = "Processes text messages and returns a greeting."
    input_format = ModuleInput
    output_format = ModuleOutput

    async def handle(
        self,
        input_data: ModuleInput,
        setup_data: ModuleSetup,
        context: ModuleContext,
    ) -> None:
        """Process the input message and stream a response."""
        user_message = input_data.root.content
        response = MessageOutput(
            content=f"{setup_data.greeting_prefix}, you said: {user_message}",
        )
        await self.send_message(ModuleOutput(root=response))
```

### 3. Define the Module

```python
from typing import Any, ClassVar
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_models import ServicesStrategy


class GreetingModule(
    BaseModule[ModuleInput, ModuleOutput, ModuleSetup, ModuleSecret]
):
    """A simple greeting module."""

    name = "GreetingModule"
    description = "Echoes back a greeting with a configurable prefix."

    input_format = ModuleInput
    output_format = ModuleOutput
    setup_format = ModuleSetup
    secret_format = ModuleSecret

    metadata: ClassVar[dict[str, Any]] = {
        "name": "GreetingModule",
        "version": "1.0.0",
        "tags": ["example", "greeting"],
    }

    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
    services_config_params: ClassVar[dict[str, dict[str, Any | None] | None]] = {}

    async def cleanup(self) -> None:
        """Clean up resources."""
```

### 4. Start the gRPC Server

```python
import asyncio
from digitalkin.grpc_servers.module_server import ModuleServer
from digitalkin.grpc_servers.utils.models import (
    ModuleServerConfig,
    SecurityMode,
    ServerMode,
)


async def main() -> None:
    server_config = ModuleServerConfig(
        host="[::]",
        port=50055,
        mode=ServerMode.ASYNC,
        security=SecurityMode.INSECURE,
        max_workers=10,
        credentials=None,
        registry_address="[::]:50052",
    )

    module_server = ModuleServer(
        GreetingModule,
        server_config=server_config,
    )
    await module_server.start_async()
    await module_server.await_termination()


if __name__ == "__main__":
    asyncio.run(main())
```

## Next Steps

- [Guides](guides.md) -- Learn about trigger handlers, service strategies, and module lifecycle
- [Architecture](architecture/sdk-flow.md) -- Understand the full SDK execution flow
- [API Reference](api/api.md) -- Detailed API documentation
- [Examples](https://github.com/DigitalKin-ai/digitalkin/tree/main/examples) -- Browse complete example modules in the repository
