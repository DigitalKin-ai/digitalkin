"""Example module implementation to test ArchetypeModule functionality."""

import asyncio
import datetime
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, Field

from digitalkin.logger import logger
from digitalkin.models.module import ModuleStatus
from digitalkin.modules.archetype_module import ArchetypeModule
from digitalkin.services.services_config import ServicesConfig
from digitalkin.services.services_models import ServicesMode
from digitalkin.services.storage.storage_strategy import StorageRecord


class ExampleInput(BaseModel):
    """Input model for example module."""

    message: str = Field(description="Message to process")
    number: int = Field(description="Number to process")


class ExampleOutput(BaseModel):
    """Output model for example module."""

    processed_message: str = Field(description="The processed message")
    processed_number: int = Field(description="The processed number")
    timestamp: datetime.datetime = Field(description="When the processing was done")


class ExampleSetup(BaseModel):
    """Setup model for example module."""

    processing_mode: str = Field(description="Mode to process data in", default="default")
    multiply_factor: int = Field(description="Factor to multiply number by", default=1)


class ExampleSecret(BaseModel):
    """Secret model for example module."""

    api_key: str = Field(description="API key for external service")


class ExampleStorage(BaseModel):
    """Secret model for example module."""

    test_key: str = Field(description="Test value for storage")


class ExampleModule(ArchetypeModule[ExampleInput, ExampleOutput, ExampleSetup, ExampleSecret]):
    """Example module that demonstrates ArchetypeModule functionality."""

    name = "ExampleModule"
    description = "An example module for testing purposes"
    input_format = ExampleInput
    output_format = ExampleOutput
    setup_format = ExampleSetup
    secret_format = ExampleSecret
    metadata = {"name": "ExampleModule", "description": "A module for testing ArchetypeModule functionality"}

    # Define services_config_params with default values
    services_config_strategies = {}
    services_config_params = {"storage": {"config": {"example": ExampleOutput}}, "filesystem": {"config": {}}}

    def __init__(self, job_id: str, mission_id: str) -> None:
        """Initialize the example module.

        Args:
            job_id: Unique identifier for the job
            name: Optional name for the module
        """
        # Initialize services configuration using the class attribute before the instance is created
        self.services_config = ServicesConfig(
            services_config_strategies=self.services_config_strategies,
            services_config_params=self.services_config_params,
            mode=ServicesMode.LOCAL,
        )

        super().__init__(job_id, mission_id)

    async def initialize(self, setup_data: ExampleSetup) -> None:
        """Initialize the module.

        Args:
            setup_data: Setup data for the module
        """
        logger.info("Initializing ExampleModule with setup data: %s", setup_data)
        self.setup = self.setup_format.model_validate(setup_data)
        logger.info("Initialization complete, using processing mode: [%s]", self.setup.processing_mode)

    async def run(
        self,
        input_data: dict[str, Any],
        setup_data: ExampleSetup,
        callback: Callable,
    ) -> None:
        """Run the module.

        Args:
            input_data: Input data for the module
            setup_data: Setup data for the module
            callback: Callback function to report progress
        """
        # Validate the input data
        input_model = self.input_format.model_validate(input_data)
        logger.info("Running with input data: %s", input_model)

        # Process the data
        processed_message = f"Processed: {input_model.message}"
        processed_number = input_model.number * self.setup.multiply_factor

        # Create output model
        output_data = self.output_format(
            processed_message=processed_message,
            processed_number=processed_number,
            timestamp=datetime.datetime.now(),
        )

        # Store the output data in storage
        storage_id = self.storage.store(
            collection="example",
            record_id=f"example_outputs",
            data=output_data.model_dump(),
            data_type="OUTPUT"
        )

        logger.info("Stored output data with ID: %s", storage_id)

        # Call the callback with the output data
        callback(output_data.model_dump())

        # Wait a bit to simulate processing time
        await asyncio.sleep(1)

    async def cleanup(self) -> None:
        """Clean up the module."""
        logger.info("Cleaning up ExampleModule")
        # Nothing to clean up in this example


async def test_module() -> None:
    """Test the example module."""
    # Create the module
    module = ExampleModule(job_id="test-job-123", mission_id="test-mission-123")

    # Define input and setup data
    input_data = ExampleInput(message="Hello, world!", number=42)

    setup_data = ExampleSetup(processing_mode="test", multiply_factor=10)

    # Define a callback function
    def callback(result) -> None:
        for key, value in result.items():
            pass

    # Start the module
    await module.start(input_data, setup_data, callback)

    # Wait for the module to complete
    while module.status not in {ModuleStatus.STOPPED, ModuleStatus.FAILED}:
        await asyncio.sleep(0.5)

    # Check the storage
    if module.status == ModuleStatus.STOPPED:
        result: StorageRecord = module.storage.read("example", "example_outputs")
        if result:
            pass


def test_storage_directly() -> None:
    """Test the storage service directly."""
    # Initialize storage service
    storage = ServicesConfig().storage(mission_id="test-mission", config={"example": ExampleStorage})

    # Create a test record
    storage.store("example", "test_table", {"test_key": "test_value"}, "OUTPUT")

    # Retrieve the record
    retrieved = storage.read("example", "test_table")

    if retrieved:
        pass


if __name__ == "__main__":
    # Run the module test
    asyncio.run(test_module())

    # Test storage directly
    test_storage_directly()
