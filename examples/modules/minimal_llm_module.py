"""Simple module calling an LLM."""

import logging
from collections.abc import Callable
from typing import Any, ClassVar

import grpc
import openai
from pydantic import BaseModel

from digitalkin.grpc_servers.utils.models import SecurityMode, ClientConfig, ServerMode
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.setup.setup_strategy import SetupData

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
    dev_prompt: str


class OpenAIToolSecret(BaseModel):
    """Secret model defining module configuration parameters."""


client_config = ClientConfig(
    host="[::]",
    port=50151,
    mode=ServerMode.ASYNC,
    security=SecurityMode.INSECURE,
    credentials=None,
)


class OpenAIToolModule(BaseModule[OpenAIToolInput, OpenAIToolOutput, OpenAIToolSetup, OpenAIToolSecret]):
    """A openAI endpoint tool module module."""

    name = "OpenAIToolModule"
    description = "A module that interacts with OpenAI API to process text"

    # Define the schema formats for the module
    input_format = OpenAIToolInput
    output_format = OpenAIToolOutput
    setup_format = OpenAIToolSetup
    secret_format = OpenAIToolSecret

    openai_client: openai.OpenAI

    # Define module metadata for discovery
    metadata: ClassVar[dict[str, Any]] = {
        "name": "Minimal_LLM_Tool",
        "description": "Transforms input text using a streaming LLM response.",
        "version": "1.0.0",
        "tags": ["text", "transformation", "encryption", "streaming"],
    }
    # Define services_config_params with default values
    services_config_strategies = {}
    services_config_params = {
        "storage": {
            "config": {"setups": OpenAIToolSetup},
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
        """
        self.openai_client = openai.OpenAI(api_key=setup_data.current_setup_version.content["openai_key"])
        # Define what capabilities this module provides
        self.capabilities = ["text-processing", "streaming", "transformation"]
        logger.info(
            "Module %s initialized with capabilities: %s",
            self.metadata["name"],
            self.capabilities,
        )

    async def run(
        self,
        input_data: dict[str, Any],
        setup_data: SetupData,
        callback: Callable,
    ) -> None:
        """Process input text and stream LLM responses.

        Args:
            input_data: Contains the text to process.
            setup_data: Contains model configuration and development prompt.
            callback: Function to send output data back to the client.

        Raises:
            grpc.RpcError: If gRPC communication fails.
            openai.AuthenticationError: If authentication with OpenAI fails.
            openai.APIConnectionError: If an API connection error occurs.
            Exception: For any unexpected runtime errors.
        """
        logger.info(
            "Running job %s with prompt: '%s' on model: %s",
            self.job_id,
            input_data["prompt"],
            setup_data.current_setup_version.content["model_name"],
        )
        try:
            response = self.openai_client.responses.create(
                model=setup_data.current_setup_version.content["model_name"],
                tools=[{"type": "web_search_preview"}],
                instructions=setup_data.current_setup_version.content["dev_prompt"],
                input=input_data["prompt"],
            )
            if not response.output_text:
                raise openai.APIConnectionError
            output_data = OpenAIToolOutput(response=response.output_text).model_dump()

        except openai.AuthenticationError as _:
            message = "Authentication Error, OPENAI auth token was never set."
            logger.exception(message)
            output_data = {
                "error": {
                    "code": grpc.StatusCode.UNAUTHENTICATED,
                    "error_message": message,
                }
            }
        except openai.APIConnectionError as _:
            message = "API Error, please try again."
            logger.exception(message)
            output_data = {"error": {"code": grpc.StatusCode.UNAVAILABLE, "error_message": message}}
        await callback(job_id=self.job_id, output_data=output_data)
        logger.info("Job %s completed", self.job_id)

    async def cleanup(self) -> None:
        """Clean up any resources when the module is stopped.

        This method is called when the module is being shut down.
        Use it to close connections, free resources, etc.
        """
        logger.info("Cleaning up module %s", self.metadata["name"])
        # Release any resources here if needed.
