"""Simple module calling an LLM."""

import logging
from collections.abc import Callable
from typing import Any, ClassVar

import grpc
import openai
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
class OpenAIToolInput(BaseModel):
    """Input model defining what data the module expects."""

    prompt: str


class OpenAIToolOutput(BaseModel):
    """Output model defining what data the module produces."""

    response: str


class OpenAIToolSetup(BaseModel):
    """Setup model defining module configuration parameters."""

    openai_key: str
    model_name: str
    prepa_prompt: str


class OpenAIToolModule(BaseModule[OpenAIToolInput, OpenAIToolOutput, OpenAIToolSetup]):
    """A openAI endpoint tool module module."""

    # Define the schema formats for the module
    input_format = OpenAIToolInput
    output_format = OpenAIToolOutput
    setup_format = OpenAIToolSetup

    openai_client: openai.OpenAI

    # should be pre-defined in the Tool/Kin/Trigger Module
    # It can be then custom here
    local_services = DefaultServiceProvider
    dev_services = DevelopmentServiceProvider

    # Define module metadata for discovery
    metadata: ClassVar[dict[str, Any]] = {
        "name": "Minimal_LLM_Tool",
        "description": "Transforms input text using Caesar cipher with streaming output",
        "version": "1.0.0",
        "tags": ["text", "transformation", "encryption", "streaming"],
    }

    async def initialize(self, setup_data: dict[str, Any]) -> None:
        """Initialize the module capabilities.

        This method is called when the module is loaded by the server.
        Use it to set up module-specific resources or configurations.
        """
        self.openai_client = openai.OpenAI(api_key=setup_data["openai_key"])
        # Define what capabilities this module provides
        self.capabilities = ["text-processing", "streaming", "transformation"]
        logger.info(f"Module {self.metadata['name']} initialized with capabilities: {self.capabilities}")

    async def run(
        self,
        input_data: dict[str, Any],
        setup_data: dict[str, Any],
        callback: Callable,
    ) -> None:
        """Process input text and stream LLM responses.

        Args:
            input_data: Contains the text to transform and number of iterations
            setup_data: Contains shift amount and uppercase flags
            callback: Function to send output data back to the client
        """
        logger.info(
            f"Running job {self.job_id} with prompt: '{input_data['prompt']}' on model: {setup_data['model_name']}"
        )
        # tract parameters from input and setup
        try:
            response = self.openai_client.responses.create(
                model=setup_data["model_name"],
                tools=[{"type": "web_search_preview"}],
                instructions=setup_data["prepa_prompt"],
                input=input_data["prompt"],
            )
            if not response.output_text:
                raise openai.APIConnectionError

            # Create output model with results
            output_data = OpenAIToolOutput(response=response.output_text).model_dump()

        except openai.AuthenticationError as _:
            message = "Authentication Error, OPENAI auth token was never set."
            logging.exception(message)
            output_data = {"error": {"code": grpc.StatusCode.UNAUTHENTICATED, "error_message": message}}
        except openai.APIConnectionError as _:
            message = "API Error, please try again."
            logging.exception(message)
            output_data = {"error": {"code": grpc.StatusCode.UNAVAILABLE, "error_message": message}}

        # Send results through callback and wait for acknowledgment
        await callback(job_id=self.job_id, output_data=output_data)
        logger.info(f"Job {self.job_id} completed")

    async def cleanup(self) -> None:
        """Clean up any resources when the module is stopped.

        This method is called when the module is being shut down.
        Use it to close connections, free resources, etc.
        """
        logger.info(f"Cleaning up module {self.metadata['name']}")
        # Release any resources here if needed
