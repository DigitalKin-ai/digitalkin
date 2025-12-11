"""Example module demonstrating dynamic schema fields in SetupModel.

This example shows how to use the Dynamic metadata class with async fetchers
to populate field schemas (like enums) at runtime. This is useful when the
available options come from external sources like databases or APIs.

Usage:
    # Start the module server
    python examples/modules/dynamic_setup_module.py

    # Or import and use in your own code
    from examples.modules.dynamic_setup_module import DynamicSetupModule
"""

import asyncio
import logging
from typing import Annotated, Any, ClassVar

from pydantic import BaseModel, Field

from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.module_types import DataModel, DataTrigger, SetupModel
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_models import ServicesStrategy
from digitalkin.utils import Dynamic

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# =============================================================================
# Simulated External Services (replace with real implementations)
# =============================================================================


class MockModelRegistry:
    """Simulates an external model registry service.

    In a real application, this would be a connection to a database,
    API service, or configuration management system.
    """

    _models: ClassVar[list[str]] = ["gpt-4", "gpt-4-turbo", "gpt-3.5-turbo"]
    _languages: ClassVar[list[str]] = ["en", "fr", "de", "es", "it", "pt"]

    @classmethod
    async def fetch_available_models(cls) -> list[str]:
        """Fetch available models from the registry.

        Simulates an async API call with a small delay.
        """
        await asyncio.sleep(0.1)  # Simulate network latency
        logger.info("Fetched %d models from registry", len(cls._models))
        return cls._models.copy()

    @classmethod
    async def fetch_supported_languages(cls) -> list[str]:
        """Fetch supported languages from the registry."""
        await asyncio.sleep(0.05)  # Simulate network latency
        logger.info("Fetched %d languages from registry", len(cls._languages))
        return cls._languages.copy()

    @classmethod
    def get_default_model(cls) -> str:
        """Get the default model (sync fetcher example)."""
        return cls._models[0] if cls._models else "gpt-4"


# =============================================================================
# Dynamic Fetcher Functions
# =============================================================================


async def fetch_models() -> list[str]:
    """Async fetcher for available model names.

    This function is called when SetupModel.get_clean_model(force=True)
    is invoked, typically during module initialization or schema refresh.
    """
    return await MockModelRegistry.fetch_available_models()


async def fetch_languages() -> list[str]:
    """Async fetcher for supported languages."""
    return await MockModelRegistry.fetch_supported_languages()


def get_temperature_range() -> dict[str, float]:
    """Sync fetcher example returning min/max for temperature.

    Demonstrates that fetchers can return any JSON-serializable value,
    not just lists for enums.
    """
    return {"minimum": 0.0, "maximum": 2.0}


# =============================================================================
# Setup Model with Dynamic Fields
# =============================================================================


class DynamicAgentSetup(SetupModel):
    """Setup model demonstrating dynamic schema fields.

    Fields marked with Dynamic(...) will have their schema values
    refreshed at runtime when get_clean_model(force=True) is called.

    Attributes:
        model_name: The LLM model to use. Enum values fetched from registry.
        language: Output language. Enum values fetched dynamically.
        temperature: Sampling temperature. Static field for comparison.
        max_tokens: Maximum tokens to generate.
        system_prompt: The system prompt for the model.
    """

    # Dynamic field: enum values fetched asynchronously from model registry
    model_name: Annotated[str, Dynamic(enum=fetch_models)] = Field(
        default="gpt-4",
        title="Model Name",
        description="The LLM model to use for generation.",
        json_schema_extra={
            "config": True,  # Shown in initial configuration
            "ui:widget": "select",
        },
    )

    # Dynamic field: language options fetched asynchronously
    language: Annotated[str, Dynamic(enum=fetch_languages)] = Field(
        default="en",
        title="Output Language",
        description="The language for generated responses.",
        json_schema_extra={
            "config": True,
            "ui:widget": "select",
        },
    )

    # Static field: no dynamic fetcher, values defined at class time
    temperature: float = Field(
        default=0.7,
        ge=0.0,
        le=2.0,
        title="Temperature",
        description="Controls randomness. Higher values = more creative.",
        json_schema_extra={"config": True},
    )

    # Static field with hidden flag (runtime-only, not in initial config)
    max_tokens: int = Field(
        default=1024,
        ge=1,
        le=4096,
        title="Max Tokens",
        description="Maximum tokens in the response.",
        json_schema_extra={"hidden": True},
    )

    # Static field without any special flags
    system_prompt: str = Field(
        default="You are a helpful assistant.",
        title="System Prompt",
        description="The system prompt defining assistant behavior.",
    )


# =============================================================================
# Input/Output Models (Using DataModel/DataTrigger pattern)
# =============================================================================


class MessageInputTrigger(DataTrigger):
    """Message input trigger following DigitalKin DataTrigger pattern.

    The protocol field determines which trigger handler processes this input.
    """

    protocol: str = "message"
    content: str = Field(default="", description="The user message content.")


class DynamicModuleInput(DataModel[MessageInputTrigger]):
    """Input model following DigitalKin DataModel pattern.

    Wraps the trigger in a root field with optional annotations.
    """

    root: MessageInputTrigger = Field(default_factory=MessageInputTrigger)


class MessageOutputTrigger(DataTrigger):
    """Message output trigger following DigitalKin DataTrigger pattern."""

    protocol: str = "message"
    content: str = Field(default="", description="The generated response.")
    model_used: str = Field(default="", description="The model that generated this response.")
    language: str = Field(default="", description="The output language.")


class DynamicModuleOutput(DataModel[MessageOutputTrigger]):
    """Output model following DigitalKin DataModel pattern."""

    root: MessageOutputTrigger = Field(default_factory=MessageOutputTrigger)


class DynamicModuleSecret(BaseModel):
    """Secret model (empty for this example)."""


# =============================================================================
# Module Implementation
# =============================================================================


class DynamicSetupModule(
    BaseModule[
        DynamicModuleInput,
        DynamicModuleOutput,
        DynamicAgentSetup,
        DynamicModuleSecret,
    ]
):
    """Example module demonstrating dynamic schema in SetupModel.

    This module shows how to:
    1. Define setup fields with Dynamic() metadata for runtime enum fetching
    2. Mix static and dynamic fields in the same SetupModel
    3. Use async fetchers that simulate external service calls
    4. Follow DigitalKin's DataModel/DataTrigger pattern for I/O

    The key integration point is in the gRPC servicer, which calls
    SetupModel.get_clean_model(force=True) to refresh dynamic values
    before returning schema information to clients.
    """

    name = "DynamicSetupModule"
    description = "Demonstrates dynamic schema fields in module setup"

    # Schema format definitions
    input_format = DynamicModuleInput
    output_format = DynamicModuleOutput
    setup_format = DynamicAgentSetup
    secret_format = DynamicModuleSecret

    # Module metadata
    metadata: ClassVar[dict[str, Any]] = {
        "name": "DynamicSetupModule",
        "description": "Example module with dynamic setup schema",
        "version": "1.0.0",
        "tags": ["example", "dynamic-schema"],
    }

    # Services configuration (empty for this example)
    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
    services_config_params: ClassVar[dict[str, dict[str, Any | None] | None]] = {}

    async def initialize(self, context: ModuleContext, setup_data: DynamicAgentSetup) -> None:
        """Initialize the module with setup data.

        Args:
            context: The module context with services and session info.
            setup_data: The validated setup configuration.
        """
        logger.info(
            "Initializing DynamicSetupModule with model=%s, language=%s",
            setup_data.model_name,
            setup_data.language,
        )
        self.setup = setup_data

    async def cleanup(self) -> None:
        """Clean up resources."""
        logger.info("Cleaning up DynamicSetupModule")


# =============================================================================
# Demonstration Script
# =============================================================================


async def demonstrate_dynamic_schema() -> None:
    """Demonstrate the dynamic schema functionality."""
    # 1. Show schema WITHOUT force (dynamic fields not resolved)

    model_no_force = await DynamicAgentSetup.get_clean_model(
        config_fields=True,
        hidden_fields=False,
        force=False,
    )
    schema_no_force = model_no_force.model_json_schema()

    # Check if enum is present
    model_name_schema = schema_no_force.get("properties", {}).get("model_name", {})
    if "enum" in model_name_schema:
        pass

    # 2. Show schema WITH force (dynamic fields resolved)

    model_with_force = await DynamicAgentSetup.get_clean_model(
        config_fields=True,
        hidden_fields=False,
        force=True,
    )
    schema_with_force = model_with_force.model_json_schema()

    # Check enum values after force
    model_name_schema = schema_with_force.get("properties", {}).get("model_name", {})
    if "enum" in model_name_schema:
        pass

    language_schema = schema_with_force.get("properties", {}).get("language", {})
    if "enum" in language_schema:
        pass

    # 3. Show that static json_schema_extra is preserved

    # 4. Show field filtering

    # Config fields only (hidden excluded)
    await DynamicAgentSetup.get_clean_model(
        config_fields=True,
        hidden_fields=False,
        force=False,
    )

    # All fields including hidden
    await DynamicAgentSetup.get_clean_model(
        config_fields=True,
        hidden_fields=True,
        force=False,
    )


if __name__ == "__main__":
    asyncio.run(demonstrate_dynamic_schema())
