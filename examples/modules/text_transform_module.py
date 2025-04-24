"""Simple module example transforming a text."""

import logging
from collections.abc import Callable
from typing import Any, ClassVar

from pydantic import BaseModel

from digitalkin.grpc_servers.utils.models import SecurityMode, ClientConfig, ServerMode
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.setup.setup_strategy import SetupData
from digitalkin.services.storage.storage_strategy import DataType, StorageRecord

# Configure logging with clear formatting
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Define schema models using Pydantic
class TextTransformInput(BaseModel):
    """Input model defining what data the module expects."""

    text: str
    transform_count: int = 1  # Default to 1 transformation


class TextTransformOutput(BaseModel):
    """Output model defining what data the module produces."""

    transformed_text: str
    iteration: int  # Tracks which transformation this is


class TextTransformSetup(BaseModel):
    """Setup model defining module configuration parameters."""

    shift_amount: int = 1  # Default Caesar shift by 1
    uppercase: bool = False  # Whether to convert to uppercase


class TextTransformSecret(BaseModel):
    """Secret model defining module configuration parameters."""


class TextTransformStorage(BaseModel):
    """Secret model defining module configuration parameters."""

    module: str = "Text_Transform_Module"
    user: str = "user"
    consumption: int = 0
    ended: bool = False


client_config = ClientConfig(
    host="[::]",
    port=50151,
    mode=ServerMode.ASYNC,
    security=SecurityMode.INSECURE,
    credentials=None,
)


class TextTransformModule(BaseModule[TextTransformInput, TextTransformOutput, TextTransformSetup, TextTransformSecret]):
    """A text transformation module that demonstrates streaming capabilities.

    This module takes text input and performs multiple transformations on it,
    sending back each transformation as a separate output message.
    """

    # Define the schema formats for the module
    name = "Text_Transform_Module"
    input_format = TextTransformInput
    output_format = TextTransformOutput
    setup_format = TextTransformSetup
    secret_format = TextTransformSecret

    # Define module metadata for discovery
    metadata: ClassVar[dict[str, Any]] = {
        "name": "Text_Transform_Module",
        "description": "Transforms input text using Caesar cipher with streaming output",
        "version": "1.0.0",
        "tags": ["text", "transformation", "encryption", "streaming"],
    }

    # Define services_config_params with default values
    services_config_strategies = {}
    services_config_params = {
        "storage": {
            "config": {"monitor": TextTransformStorage, "setups": TextTransformStorage},
            "client_config": client_config,
        },
        "filesystem": {
            "config": {},
            "client_config": client_config,
        },
    }

    async def initialize(self, setup_data: SetupData) -> None:
        """Initialize the module capabilities.

        This method is called when the module is loaded by the server.
        Use it to set up module-specific resources or configurations.

        Raises:
            Exception: If initialization fails.
        """
        # Define what capabilities this module provides
        self.capabilities = ["text-processing", "streaming", "transformation"]
        logger.info(
            "Module %s initialized with capabilities: %s",
            self.metadata["name"],
            self.capabilities,
        )

        self.db_id = self.storage.store(
            "monitor",
            {
                "module": self.metadata["name"],
                "user": f"xxxx+{self.job_id}",
                "consumption": 0,
                "ended": False,
            },
            data_type=DataType.VIEW,
        )

    async def run(
        self,
        input_data: dict[str, Any],
        setup_data: SetupData,
        callback: Callable,
    ) -> None:
        """Process input text and stream transformation results.

        This method implements a Caesar cipher transformation on input text.
        It demonstrates streaming capability by sending multiple outputs through
        the callback for each transformation iteration.

        Args:
            input_data: Contains the text to transform and number of iterations.
            setup_data: Contains shift amount and uppercase flags.
            callback: Function to send output data back to the client.
        """
        text = input_data["text"]
        transform_count = int(input_data["transform_count"])
        logger.info("%s  | %s", setup_data, type(setup_data))
        shift_amount = int(setup_data.current_setup_version.content["shift_amount"])
        uppercase = setup_data.current_setup_version.content["uppercase"]

        logger.info(
            "Running job %s with text: '%s', iterations: %s",
            self.job_id,
            text,
            transform_count,
        )

        # Process the text for each iteration
        for i in range(transform_count):
            # Apply Caesar cipher (shift each character by specified amount)
            transformed = "".join([chr(ord(char) + shift_amount) if char.isalpha() else char for char in text])

            # Apply uppercase transformation if configured
            if uppercase:
                transformed = transformed.upper()

            output_data = TextTransformOutput(transformed_text=transformed, iteration=i + 1)

            logger.info(
                "Sending transformation %s/%s: '%s'",
                i + 1,
                transform_count,
                transformed,
            )

            monitor_obj: StorageRecord | None = self.storage.read("monitor")
            if monitor_obj is None:
                logger.error("Monitor object not found in storage.")
                break
            monitor_obj.data.consumption += 1
            updated_monitor_obj: StorageRecord | None = self.storage.modify("monitor", monitor_obj.data.model_dump())
            self.db_id = updated_monitor_obj.name if updated_monitor_obj else "monitor"

            # Send results through callback and wait for acknowledgment
            await callback(job_id=self.job_id, output_data=output_data.model_dump())
            text = transformed

        logger.info("Job %s completed with %s transformations", self.job_id, transform_count)

    async def cleanup(self) -> None:
        """Clean up any resources when the module is stopped.

        This method is called when the module is being shut down.
        Use it to close connections, free resources, etc.
        """
        logger.info(f"Cleaning up module {self.metadata['name']}")
        monitor_obj = self.storage.read("monitor")
        if monitor_obj is None:
            logger.error("Monitor object not found in storage.")
            return
        monitor_obj.data.ended = True
        updated_monitor_obj: StorageRecord | None = self.storage.modify("monitor", monitor_obj.data.model_dump())
        self.db_id = updated_monitor_obj.name if updated_monitor_obj else "monitor"
