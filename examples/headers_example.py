"""DigitalKin Headers / Metadata Example.

This example demonstrates how to use gRPC metadata (headers) with DigitalKin modules:

1. **Server side** — Reading headers inside a trigger's `handle()` method via
   `context.request_metadata`.
2. **Client side** — Sending custom headers when calling a module via gRPC metadata.
3. **Propagation** — When a module calls another module (tool function), the
   original request headers are automatically forwarded.

Key concepts:
- `context.request_metadata` is a `RequestMetadata` wrapper with typed accessors
  and generic `get()` / `__contains__` / `__getitem__` support.
- Headers are extracted from `context.invocation_metadata()` on the server side
  and injected transparently through the entire chain:
  ModuleServicer → JobManager → ModuleFactory → BaseModule → ModuleContext → trigger.
- When `ModuleContext.create_tool_functions()` builds callable wrappers for
  remote tools, each wrapper forwards `request_metadata` as gRPC metadata in the
  outgoing `call_module` request.

To run:
    uv run examples/headers_example.py

Requirements:
    - DigitalKin SDK installed
    - No external services required (example is illustrative)
"""

import asyncio
import json
import logging
import sys

import grpc
from agentic_mesh_protocol.module.v1 import lifecycle_pb2, module_service_pb2_grpc
from google.protobuf import struct_pb2

from digitalkin.grpc_servers.module_server import ModuleServer
from digitalkin.models.grpc_servers.models import (
    ModuleServerConfig,
)
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# 1. Server: start a module that reads headers in its trigger
# ---------------------------------------------------------------------------

async def start_server() -> ModuleServer:
    """Start a module server (no auth interceptor — plain header reading)."""
    # Import your module class here. For illustration we use a placeholder:
    # from my_project.module import MyModule
    #
    # The module's trigger can access headers like this:
    #
    #   class MessageTrigger(BaseTrigger):
    #       async def handle(self, input_data, setup, context, callback, ...):
    #           tenant = context.request_metadata.get("x-tenant-id")
    #           trace  = context.request_metadata.get("x-trace-id")
    #           logger.info("Tenant: %s | Trace: %s", tenant, trace)
    #           ...
    #
    # Any tool function created by context.create_tool_functions() will
    # automatically forward context.request_metadata as gRPC metadata when
    # calling the remote module.

    config = ModuleServerConfig(
        host="[::]",
        port=50055,
        mode=ControlFlow.ASYNC,
        security=SecurityMode.INSECURE,
        max_workers=10,
        credentials=None,
    )

    # Replace with your real module class
    # module_server = ModuleServer(MyModule, server_config=config)
    # await module_server.start_async()
    # return module_server
    logger.info("Server config ready (replace MyModule with your module class).")
    logger.info("ModuleServerConfig: %s", config)
    raise SystemExit(0)


# ---------------------------------------------------------------------------
# 2. Client: send custom headers when calling a module
# ---------------------------------------------------------------------------

async def call_module_with_headers() -> None:
    """Call a running module with custom gRPC metadata (headers).

    The headers will be available in the module's trigger via
    ``context.request_metadata``.
    """
    # Custom headers to send
    metadata = [
        ("x-tenant-id", "tenant-42"),
        ("x-trace-id", "abc-123-def"),
        ("x-custom-header", "hello-world"),
    ]

    async with grpc.aio.insecure_channel("localhost:50066") as channel:
        stub = module_service_pb2_grpc.ModuleServiceStub(channel)

        # Build input data
        input_struct = struct_pb2.Struct()
        input_struct.update({"root": {"protocol": "search", "query": "Hello from client with headers!"}})

        request = lifecycle_pb2.StartModuleRequest(
            input=input_struct,
            setup_id="setups:0",
            mission_id="missions:0",
        )

        logger.info("Sending request with metadata: %s", metadata)

        # Pass metadata as the second positional or keyword argument
        responses = stub.StartModule(request, metadata=metadata)

        async for response in responses:
            if response.HasField("output"):
                output_dict = dict(response.output)
                logger.info("Response: %s", json.dumps(output_dict, indent=2))

    logger.info("Done.")


# ---------------------------------------------------------------------------
# 3. Inter-module propagation (how it works internally)
# ---------------------------------------------------------------------------

def explain_propagation() -> None:
    """Print a summary of how header propagation works."""
    explanation = """
    Header Propagation Chain
    ========================

    1. Client sends gRPC request with metadata:
         stub.StartModule(request, metadata=[("x-tenant-id", "t-42")])

    2. ModuleServicer extracts metadata:
         request_metadata = dict(context.invocation_metadata())

    3. Metadata is passed through the job manager to ModuleFactory:
         ModuleFactory.create_module_instance(..., request_metadata=request_metadata)

    4. BaseModule stores it in ModuleContext:
         self.context.request_metadata = RequestMetadata(request_metadata)

    5. Trigger reads headers:
         tenant = context.request_metadata.get("x-tenant-id")  # "t-42"
         tenant = context.request_metadata["x-tenant-id"]       # "t-42"
         has_it = "x-tenant-id" in context.request_metadata     # True

    6. When the trigger calls a tool (remote module), headers are forwarded:
         # Automatic — create_tool_functions() injects metadata
         result = await tool_fn(input_data)
         # Under the hood: call_module(..., metadata=context.request_metadata.to_dict())
    """
    print(explanation)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> int:
    """Run the example."""
    if len(sys.argv) > 1 and sys.argv[1] == "client":
        await call_module_with_headers()
    elif len(sys.argv) > 1 and sys.argv[1] == "server":
        await start_server()
    else:
        explain_propagation()
        logger.info("Usage: python headers_example.py [server|client]")
        logger.info("  server  — start a module server (requires MyModule)")
        logger.info("  client  — call a running module with custom headers")
        logger.info("  (none)  — print propagation explanation")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
