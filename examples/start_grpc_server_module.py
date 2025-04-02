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
import datetime
import logging
import sys

from modules.minimal_llm_module import OpenAIToolModule, OpenAIToolSetup
from modules.text_transform_module import TextTransformModule, TextTransformSetup

from digitalkin.grpc_servers.module_server import ModuleServer
from digitalkin.grpc_servers.utils.models import (
    ModuleServerConfig,
    SecurityMode,
    ServerConfig,
    ServerMode,
)
from digitalkin.services.setup.setup_strategy import SetupData, SetupVersionData

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
            port=50051,
            mode=ServerMode.ASYNC,
            security=SecurityMode.INSECURE,
            max_workers=10,
            credentials=None,
            registry_address="[::]:50052",
        )

        if len(sys.argv) > 1 and "llm" in sys.argv:
            module_server = ModuleServer(OpenAIToolModule, config=module_config)
            await module_server.start_async()

            module_server.module_servicer.setup.__post_init__(  # type: ignore
                ServerConfig(
                    host="[::]",
                    port=50151,
                    mode=ServerMode.ASYNC,
                    security=SecurityMode.INSECURE,
                    max_workers=10,
                    credentials=None,
                )
            )

            setup_id = "setups:0"
            setup_version_data = SetupVersionData(
                name="gpt4o-mini",
                version="v1",
                creation_date=datetime.datetime.now(datetime.timezone.utc),
                content={
                    **OpenAIToolSetup(
                        openai_key="XXX",
                        model_name="gpt-4o-mini",
                        dev_prompt=(
                            "You are a python specialist focused on the async module and process optimization."
                        ),
                    ).model_dump()
                },
            )

            module_server.module_servicer.setup.create_setup_version(
                setup_version_dict={
                    "setup_id": setup_id,
                    "data": setup_version_data,
                }
            )

            module_server.module_servicer.setup.create_setup(
                setup_dict={
                    "setup_id": setup_id,
                    "data": SetupData(
                        id="1",
                        name="module_openai",
                        organisation_id="organisations:1",
                        owner="owner:1",
                        module_id="modules:1",
                        current_setup_version=setup_version_data,
                    ),
                }
            )
        else:
            module_server = ModuleServer(TextTransformModule, config=module_config)
            await module_server.start_async()

            module_server.module_servicer.setup.__post_init__(  # type: ignore
                ServerConfig(
                    host="[::]",
                    port=50151,
                    mode=ServerMode.ASYNC,
                    security=SecurityMode.INSECURE,
                    max_workers=10,
                    credentials=None,
                )
            )

            setup_id = "setups:0"
            setup_version_data = SetupVersionData(
                name="text_transform_5",
                version="v1",
                creation_date=datetime.datetime.now(datetime.timezone.utc),
                content={**TextTransformSetup(shift_amount=2, uppercase=True).model_dump()},
            )
            module_server.module_servicer.setup.create_setup_version(
                setup_version_dict={
                    "setup_id": setup_id,
                    "data": setup_version_data,
                }
            )

            module_server.module_servicer.setup.create_setup(
                setup_dict={
                    "setup_id": setup_id,
                    "data": SetupData(
                        id="1",
                        name="module_test_Transform",
                        organisation_id="organisations:1",
                        owner="owner:1",
                        module_id="modules:1",
                        current_setup_version=setup_version_data,
                    ),
                }
            )

        logger.info("Module server started on port 50051. Press Ctrl+C to stop.")
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
