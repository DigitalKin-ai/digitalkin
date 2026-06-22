"""EchoModule server — same pattern as template-tool.

Uses ModuleServer with auto-embedded GatewayServicer (via DIGITALKIN_REDIS_URL).
Server config via env vars (ServerSettings from pydantic-settings):
  SERVER_CHANNEL_HOST, SERVER_CHANNEL_PORT, SERVER_CHANNEL_SECURITY, etc.

Usage:
    # 1. Start Redis:
    docker compose -f examples/redis_demo/docker-compose.yml up -d

    # 2. Set env and start:
    DIGITALKIN_REDIS_URL=redis://localhost:6379/0 python examples/redis_demo/server.py

    # 3. Test with the client:
    python examples/redis_demo/client.py full --prompt "Hello world"
"""

import asyncio
import logging
import sys

from digitalkin.grpc_servers.module_server import ModuleServer

from echo_module import EchoToolModule

logger = logging.getLogger(__name__)


async def main_async() -> int:
    """Run the EchoModule server.

    Returns:
        Exit code (0 for success, non-zero for errors).
    """
    module_server = None
    try:
        module_server = ModuleServer(EchoToolModule)

        await module_server.start_async()
        logger.info("EchoModule server started")
        await module_server.await_termination()
    except KeyboardInterrupt:
        logger.info("Server stopping due to keyboard interrupt...")
    except Exception:
        logger.exception("Error running server")
        return 1
    finally:
        if module_server is not None and module_server.server is not None:
            await module_server.stop_async()
    return 0


def main() -> int:
    """Run the async main function.

    Returns:
        Exit code.
    """
    try:
        return asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Server stopped by keyboard interrupt")
        return 0
    except Exception:
        logger.exception("Fatal error")
        return 1


if __name__ == "__main__":
    sys.exit(main())
