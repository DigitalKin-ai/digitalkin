#!/usr/bin/env python3
"""Example of a synchronous insecure gRPC server using BaseServer."""

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


class SyncGreeterServicer(Greeter):
    """Synchronous implementation of Greeter service."""

    def SayHello(self, request, context):  # noqa: N802
        """Implementation of SayHello method."""
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

        return HelloReply(message=f"Hello, {name}!")


class SyncInsecureServer(BaseServer):
    """Synchronous insecure gRPC server implementation."""

    def _register_servicers(self) -> None:
        """Register servicers with the gRPC server."""
        if self.server is None:
            msg = "Server must be created before registering servicers"
            raise RuntimeError(msg)

        # Create and register the servicer
        servicer = SyncGreeterServicer()
        self.register_servicer(
            servicer,
            add_GreeterServicer_to_server,
            service_descriptor=DESCRIPTOR,
        )
        logger.info("Registered Greeter servicer")


def main() -> int:
    """Run the synchronous insecure server."""
    try:
        # Create server configuration
        config = ServerConfig(
            host="localhost",
            port=50051,
            mode=ServerMode.SYNC,
            security=SecurityMode.INSECURE,
            max_workers=10,
        )

        # Create and start the server
        server = SyncInsecureServer(config)
        server.start()

        logger.info("Server started. Press Ctrl+C to stop.")

        # Keep the server running until interrupted
        try:
            server.wait_for_termination()
        except KeyboardInterrupt:
            logger.info("Server stopping due to keyboard interrupt...")
        finally:
            server.stop()

    except Exception as e:
        logger.exception(f"Error running server: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
