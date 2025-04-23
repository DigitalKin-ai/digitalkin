"""DigitalKin Module Server Example.

This example demonstrates how to create a custom module using the DigitalKin SDK.
It shows:
1. Defining input/output/setup schema using Pydantic models.
2. Implementing module business logic with streaming capabilities.
3. Starting a gRPC server to host the module.
4. Registering the module with a registry server.

To run: uv run module_server_example.py

Requirements:
- DigitalKin SDK and proto files installed.
- Registry server running (default: localhost:50052).

Raises:
    Exception: If server configuration or runtime operations fail.
"""

import asyncio
import logging
import sys

from modules.cpu_intensive_module import CPUIntensiveModule
from modules.minimal_llm_module import OpenAIToolModule

from digitalkin.grpc_servers.module_server import ModuleServer
from digitalkin.grpc_servers.utils.models import (
    ClientConfig,
    ModuleServerConfig,
    SecurityMode,
    ServerMode,
)

# Configure logging with clear formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def serve_module() -> int:
    """Initialize and start the module server.

    Returns:
        int: Exit code (0 for success, non-zero for errors).

    Raises:
        Exception: If server startup or runtime errors occur.
    """
    module_server = None
    try:
        module_config = ModuleServerConfig(
            host="[::]",
            port=50055,
            mode=ServerMode.ASYNC,
            security=SecurityMode.INSECURE,
            max_workers=10,
            credentials=None,
            registry_address="[::]:50052",
        )

        client_config = ClientConfig(
            host="[::]",
            port=50051,
            mode=ServerMode.ASYNC,
            security=SecurityMode.INSECURE,
            credentials=None,
        )

        if len(sys.argv) > 1 and "llm" in sys.argv:
            module_server = ModuleServer(OpenAIToolModule, server_config=module_config, client_config=client_config)
            await module_server.start_async()

        else:
            module_server = ModuleServer(CPUIntensiveModule, server_config=module_config, client_config=client_config)
            await module_server.start_async()

        logger.info("Module server started on port 50055. Press Ctrl+C to stop.")
        await module_server.await_termination()
    except KeyboardInterrupt:
        logger.info("Server stopping due to keyboard interrupt...")
        return 0
    except Exception:
        logger.exception("Error running server:")
        return 1
    finally:
        if module_server is not None and module_server.server is not None:
            logger.info("Stopping module server...")
            await module_server.stop_async()
            logger.info("Module server stopped.")

    return 0


def main() -> int:
    """Application entry point.

    Returns:
        int: Exit code (0 for success, non-zero for errors).

    Raises:
        Exception: If the server fails to run.
    """
    try:
        return asyncio.run(serve_module())
    except KeyboardInterrupt:
        logger.info("Server stopped by keyboard interrupt")
        return 0
    except Exception:
        logger.exception("Fatal error:")
        return 1


if __name__ == "__main__":
    sys.exit(main())
