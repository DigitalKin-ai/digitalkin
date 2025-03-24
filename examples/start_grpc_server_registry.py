"""Usage examples for DigitalKin gRPC servers."""

import asyncio
import logging
import sys

from digitalkin.grpc.registry_server import RegistryServer
from digitalkin.grpc.utils.models import RegistryServerConfig, SecurityMode, ServerMode

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def serve_registry() -> int:
    """Run registry server.

    Returns:
        error code
    """
    registry_server = None
    try:
        # Create server configuration
        registry_config = RegistryServerConfig(
            host="[::]",
            port=50052,
            mode=ServerMode.ASYNC,
            security=SecurityMode.INSECURE,
            max_workers=10,
            credentials=None,
            database_url="",
        )

        # Create the registry server
        registry_server = RegistryServer(config=registry_config)

        # Use the async-specific start method
        await registry_server.start_async()
        await registry_server.await_termination()

    except KeyboardInterrupt:
        # This inner handler will rarely be reached,
        # as the KeyboardInterrupt usually breaks out of asyncio.run()
        logger.info("Server stopping due to keyboard interrupt...")
    except Exception:
        logger.exception("Error running server")
        return 1
    finally:
        # Clean up resources if server was started
        if registry_server is not None and registry_server.server is not None:
            await registry_server.stop_async()
    return 0


def main() -> int:
    """Run the async main function.

    Raises:
        Exception: ?

    Returns:
        error code
    """
    try:
        return asyncio.run(serve_registry())
    except KeyboardInterrupt:
        # This is the primary KeyboardInterrupt handler
        logger.info("Server stopped by keyboard interrupt")
        return 0  # Clean exit
    except Exception:
        logger.exception("Fatal error")
        return 1


if __name__ == "__main__":
    sys.exit(main())
