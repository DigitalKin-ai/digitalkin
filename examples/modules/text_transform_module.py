"""Simple module example transforming a text."""

import logging
from collections.abc import Callable
from typing import Any, ClassVar

from pydantic import BaseModel

from digitalkin.modules._base_module import BaseModule
from digitalkin.services.default_service import DefaultServiceProvider
from digitalkin.services.development_service import DevelopmentServiceProvider

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


class TextTransformModule(BaseModule[TextTransformInput, TextTransformOutput, TextTransformSetup]):
    """A text transformation module that demonstrates streaming capabilities.

    This module takes text input and performs multiple transformations on it,
    sending back each transformation as a separate output message.
    """

    # Define the schema formats for the module
    input_format = TextTransformInput
    output_format = TextTransformOutput
    setup_format = TextTransformSetup

    local_services = DefaultServiceProvider
    dev_services = DevelopmentServiceProvider

    # Define module metadata for discovery
    metadata: ClassVar[dict[str, Any]] = {
        "name": "Text_Transform_Module",
        "description": "Transforms input text using Caesar cipher with streaming output",
        "version": "1.0.0",
        "tags": ["text", "transformation", "encryption", "streaming"],
    }

    async def initialize(self, setup_data: dict[str, Any]) -> None:
        """Initialize the module capabilities.

        This method is called when the module is loaded by the server.
        Use it to set up module-specific resources or configurations.
        """
        # Define what capabilities this module provides
        self.capabilities = ["text-processing", "streaming", "transformation"]
        logger.info(f"Module {self.metadata['name']} initialized with capabilities: {self.capabilities}")

        self.db_id = int(
            self.storage.create(
                table="monitor",
                data={
                    "mission_id": "mission_id:test_mission_id",
                    "name": "monitor",
                    "data": {
                        "module": self.metadata["name"],
                        "user": f"xxxx+{self.job_id}",
                        "consumption": 0,
                        "ended": False,
                    },
                },
            )
        )

    async def run(
        self,
        input_data: dict[str, Any],
        setup_data: dict[str, Any],
        callback: Callable,
    ) -> None:
        """Process input text and stream transformation results.

        This method implements a Caesar cipher transformation on input text.
        It demonstrates streaming capability by sending multiple outputs through
        the callback for each transformation iteration.

        Args:
            input_data: Contains the text to transform and number of iterations
            setup_data: Contains shift amount and uppercase flags
            callback: Function to send output data back to the client
        """
        # Extract parameters from input and setup
        text = input_data["text"]
        transform_count = int(input_data["transform_count"])
        shift_amount = int(setup_data["shift_amount"])
        uppercase = setup_data["uppercase"]

        logger.info(f"Running job {self.job_id} with text: '{text}', iterations: {transform_count}")

        # Process the text for each iteration
        for i in range(transform_count):
            # Apply Caesar cipher (shift each character by specified amount)
            transformed = "".join([chr(ord(char) + shift_amount) if char.isalpha() else char for char in text])

            # Apply uppercase transformation if configured
            if uppercase:
                transformed = transformed.upper()

            # Create output model with results
            output_data = TextTransformOutput(transformed_text=transformed, iteration=i + 1)

            logger.info(f"Sending transformation {i + 1}/{transform_count}: '{transformed}'")

            monitor_obj = self.storage.get(
                table="monitor",
                data={"keys": [self.db_id]},
            )[0]
            monitor_obj["consumption"] += 1
            self.storage.update(
                table="monitor",
                data={
                    "update_id": self.db_id,
                    "update_value": monitor_obj,
                },
            )
            # Send results through callback and wait for acknowledgment
            await callback(job_id=self.job_id, output_data=output_data.model_dump())

            # Update the text for the next iteration (each transformation builds on the previous)
            text = transformed

        logger.info(f"Job {self.job_id} completed with {transform_count} transformations")

    async def cleanup(self) -> None:
        """Clean up any resources when the module is stopped.

        This method is called when the module is being shut down.
        Use it to close connections, free resources, etc.
        """
        logger.info(f"Cleaning up module {self.metadata['name']}")
        monitor_obj = self.storage.get(table="monitor", data={"keys": [self.db_id]})[0]
        monitor_obj["ended"] = True
        self.storage.update(
            table="monitor",
            data={
                "update_id": self.db_id,
                "update_value": monitor_obj,
            },
        )
