"""Usage examples for DigitalKin gRPC servers."""

import asyncio
import logging
from typing import Any

from digitalkin.grpc.utils.factory import create_module_server, create_registry_server
from digitalkin.modules._base_module import BaseModule

# Configure logging
logging.basicConfig(level=logging.INFO)


class SampleModule(BaseModule):
    """A sample module for demonstration."""

    def __init__(self):
        """Initialize the sample module."""
        metadata = {
            "module_id": "sample-module-1",
            "name": "Sample Module",
            "description": "A sample module for demonstration",
            "version": "1.0.0",
            "tags": ["sample", "demo"],
        }
        super().__init__(metadata)
        self.capabilities = ["text-processing", "sample-capability"]

    def execute(self, input_data: dict[str, Any]) -> dict[str, Any]:
        """Execute the module with the given input.

        Args:
            input_data: The input data.

        Returns:
            The output data.
        """
        # Process input and return output
        return {"result": f"Processed: {input_data.get('text', 'No input')}", "status": "success"}


def run_sync_example():
    """Run a synchronous server example."""
    # Create a registry server
    registry_server = create_registry_server(
        host="localhost",
        port=50052,
        mode="sync",
        security="insecure",
    )

    # Start the registry server
    registry_server.start()

    # Create a module
    module = SampleModule()

    # Create a module server that registers with the registry
    module_server = create_module_server(
        module=module,
        host="localhost",
        port=50051,
        mode="sync",
        security="insecure",
        registry_address="localhost:50052",
    )

    # Start the module server
    module_server.start()

    try:
        # Wait for the registry server to terminate
        registry_server.wait_for_termination()
    except KeyboardInterrupt:
        # Stop the servers on keyboard interrupt
        module_server.stop()
        registry_server.stop()


async def run_async_example():
    """Run an asynchronous server example."""
    # Create a registry server
    registry_server = create_registry_server(
        host="localhost",
        port=50052,
        mode="async",
        security="insecure",
    )

    # Start the registry server
    registry_server.start()

    # Create a module
    module = SampleModule()

    # Create a module server that registers with the registry
    module_server = create_module_server(
        module=module,
        host="localhost",
        port=50051,
        mode="async",
        security="insecure",
        registry_address="localhost:50052",
    )

    # Start the module server
    module_server.start()

    try:
        # Wait for the registry server to terminate
        await registry_server.await_termination()
    except KeyboardInterrupt:
        # Stop the servers on keyboard interrupt
        module_server.stop()
        registry_server.stop()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "async":
        # Run the async example
        asyncio.run(run_async_example())
    else:
        # Run the sync example
        run_sync_example()
