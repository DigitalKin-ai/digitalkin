"""DigitalKin Module Server Example.

This example demonstrates how to create a custom module using the DigitalKin SDK.
It shows:
1. Defining input/output/setup schema using Pydantic models
2. Implementing module business logic with streaming capabilities
3. Starting a gRPC server to host the module
4. Registering the module with a registry server

To run: uv run module_server_example.py

Requirements:
- DigitalKin SDK and proto files installed
- Registry server running (default: localhost:50052)
"""

import asyncio
import datetime
import logging
import sys
from os.path import dirname

from digitalkin.grpc.module_server import ModuleServer
from digitalkin.grpc.utils.models import ModuleServerConfig, SecurityMode, ServerConfig, ServerMode

sys.path.append(dirname(__file__))
from modules.minimal_llm_module import OpenAIToolModule, OpenAIToolSetup
from modules.text_transform_module import TextTransformModule, TextTransformSetup

from digitalkin.services.storage.storage_strategy import DataType, StorageData

# Configure logging with clear formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def serve_module() -> int:
    """Initialize and start the module server.

    Returns:
        int: Exit code (0 for success, non-zero for errors)
    """
    module_server = None
    try:
        # Configure the module server
        module_config = ModuleServerConfig(
            host="[::]",  # Listen on all interfaces
            port=50051,  # Port for the module server
            mode=ServerMode.ASYNC,  # Use async mode for performance
            security=SecurityMode.INSECURE,  # Use insecure mode for development
            max_workers=10,  # Number of concurrent requests
            credentials=None,  # No credentials for insecure mode
            registry_address="[::]:50052",  # Address of the registry server
        )

        if len(sys.argv) > 1 and "llm" in sys.argv:
            module_server = ModuleServer(OpenAIToolModule, config=module_config)
            await module_server.start_async()

            module_server.module_class.storage.__post_init__(  # type: ignore
                ServerConfig(
                    host="[::]",
                    port=50151,
                    mode=ServerMode.ASYNC,
                    security=SecurityMode.INSECURE,
                    max_workers=10,
                    credentials=None,
                )
            )

            module_server.module_class.storage.create(
                storage_dict={
                    "table": "setups",
                    "data": StorageData(
                        mission_id="missions:1",
                        name="setups",
                        timestamp=datetime.datetime.now(datetime.timezone.utc),
                        type=DataType.VIEW,
                        data=dict(
                            OpenAIToolSetup(
                                openai_key="XXX",
                                model_name="gpt-4o-mini",
                                prepa_prompt="You are an python specialist focused "
                                "on the aync module and process optimization.",
                            )
                        ),
                    ),
                },
            )
        else:
            # Create the module server with our custom module
            module_server = ModuleServer(TextTransformModule, config=module_config)
            await module_server.start_async()

            module_server.module_class.storage.__post_init__(  # type: ignore
                ServerConfig(
                    host="[::]",
                    port=50151,
                    mode=ServerMode.ASYNC,
                    security=SecurityMode.INSECURE,
                    max_workers=10,
                    credentials=None,
                )
            )

            module_server.module_class.storage.create(
                storage_dict={
                    "table": "setups",
                    "data": StorageData(
                        mission_id="missions:1",
                        name="setups",
                        timestamp=datetime.datetime.now(datetime.timezone.utc),
                        type=DataType.VIEW,
                        data=dict(TextTransformSetup(shift_amount=2, uppercase=True)),
                    ),
                },
            )

        # Start the server asynchronously
        logger.info("Module server started on port 50051. Press Ctrl+C to stop.")

        # Keep the server running until interrupted
        await module_server.await_termination()
    except KeyboardInterrupt:
        logger.info("Server stopping due to keyboard interrupt...")
        return 0
    except Exception:
        logger.exception("Error running server:")
        return 1
    finally:
        # Clean up server resources
        if module_server is not None and module_server.server is not None:
            logger.info("Stopping module server...")
            await module_server.stop_async()
            logger.info("Module server stopped.")

    return 0


def main() -> int:
    """Application entry point.

    Returns:
        int: Exit code (0 for success, non-zero for errors)
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
