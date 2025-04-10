#!/usr/bin/env python3
"""Example of an asynchronous insecure gRPC server using BaseServer."""

import asyncio
import logging
import sys
from pathlib import Path

# Add parent directory to path to enable imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from digitalkin.grpc_servers._base_server import BaseServer
from digitalkin.grpc_servers.utils.models import SecurityMode, ServerConfig, ServerMode
from examples.base_server.mock.mock_pb2 import DESCRIPTOR, HelloReply  # type: ignore
from examples.base_server.mock.mock_pb2_grpc import (
    Greeter,
    add_GreeterServicer_to_server,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class AsyncGreeterImpl(Greeter):
    """Asynchronous implementation of Greeter service."""

    async def SayHello(self, request, context):  # noqa: N802
        """Asynchronous implementation of SayHello method."""
        logger.info(f"Received request object: {request}")
        logger.info(f"Request attributes: {vars(request)}")
        logger.info(f"Received request with name: {request.name}")

        # If the name is still empty, try to get metadata from the context
        name = request.name
        if not name:
            name = "unknown"
            # Check context metadata
            for key, value in context.invocation_metadata():
                logger.info(f"Metadata: {key}={value}")
                if key.lower() == "name":
                    name = value

        # Simulate some async work
        await asyncio.sleep(0.1)

        return HelloReply(message=f"Hello async, {name}!")


class AsyncInsecureServer(BaseServer):
    """Asynchronous insecure gRPC server implementation."""

    def _register_servicers(self) -> None:
        """Register servicers with the gRPC server."""
        if self.server is None:
            msg = "Server must be created before registering servicers"
            raise RuntimeError(msg)

        # Create and register the servicer
        servicer = AsyncGreeterImpl()
        self.register_servicer(
            servicer,
            add_GreeterServicer_to_server,
            service_descriptor=DESCRIPTOR,
        )
        logger.info("Registered Async Greeter servicer")


async def main_async() -> int:
    """Run the asynchronous insecure server."""
    server = None
    try:
        # Create server configuration
        config = ServerConfig(
            host="localhost",
            port=50051,
            mode=ServerMode.ASYNC,
            security=SecurityMode.INSECURE,
        )

        # Create the server
        server = AsyncInsecureServer(config)

        # Use the async-specific start method
        await server.start_async()

        logger.info("Server started. Press Ctrl+C to stop.")

        # Keep the server running until interrupted
        await server.await_termination()

    except KeyboardInterrupt:
        # This inner handler will rarely be reached,
        # as the KeyboardInterrupt usually breaks out of asyncio.run()
        logger.info("Server stopping due to keyboard interrupt...")
    except Exception as e:
        logger.exception(f"Error running server: {e}")
        return 1
    finally:
        # Clean up resources if server was started
        if server is not None and server.server is not None:
            await server.stop_async()

    return 0


def main():
    """Run the async main function."""
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        # This is the primary KeyboardInterrupt handler
        logger.info("Server stopped by keyboard interrupt")
        return 0  # Clean exit
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
