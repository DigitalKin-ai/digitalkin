#!/usr/bin/env python3
"""Example of a synchronous secure gRPC server using BaseServer with TLS."""

import logging
import sys
from pathlib import Path

# Add parent directory to path to enable imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from digitalkin.grpc_servers._base_server import BaseServer
from digitalkin.grpc_servers.utils.models import (
    SecurityMode,
    ServerConfig,
    ServerCredentials,
    ServerMode,
)
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

        return HelloReply(message=f"Hello secure, {name}!")


class SyncSecureServer(BaseServer):
    """Synchronous secure gRPC server implementation."""

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
    """Run the synchronous secure server."""
    try:
        # Path to certificate files
        cert_dir = Path(__file__).parent.parent.parent / "certs"
        # Check if certificates exist
        if not cert_dir.exists() or not (cert_dir / "server.key").exists():
            logger.error("Certificate files not found. Please generate them first.")
            logger.info("Run the generate_certificates.py script to create certificates.")
            return 1

        # Create server configuration with security credentials
        config = ServerConfig(
            host="localhost",
            port=50051,
            mode=ServerMode.SYNC,
            security=SecurityMode.SECURE,
            credentials=ServerCredentials(
                server_key_path=cert_dir / "server.key",
                server_cert_path=cert_dir / "server.crt",
                # For mTLS (mutual TLS with client authentication), uncomment:
                # root_cert_path=cert_dir / "ca.crt",  # noqa: ERA001
            ),
            max_workers=10,
        )

        # Create and start the server
        server = SyncSecureServer(config)
        server.start()

        logger.info("Secure server started. Press Ctrl+C to stop.")

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
