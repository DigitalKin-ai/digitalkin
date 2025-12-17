"""Example archetype module with tool cache integration."""

import logging
from typing import Any, ClassVar, Literal

from pydantic import BaseModel, Field

from digitalkin.models.grpc_servers.models import ClientConfig, SecurityMode, ServerMode
from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.setup_types import SetupModel
from digitalkin.models.module.tool_reference import (
    ToolReference,
    ToolReferenceConfig,
    ToolSelectionMode,
)
from digitalkin.modules._base_module import BaseModule  # noqa: PLC2701
from digitalkin.services.services_models import ServicesStrategy

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class MessageInputPayload(BaseModel):
    """Message input payload."""

    payload_type: Literal["message"] = "message"
    user_prompt: str


class ArchetypeInput(BaseModel):
    """Archetype input."""

    payload: MessageInputPayload = Field(discriminator="payload_type")


class MessageOutputPayload(BaseModel):
    """Message output payload."""

    payload_type: Literal["message"] = "message"
    response: str
    tools_used: list[str] = Field(default_factory=list)


class ArchetypeOutput(BaseModel):
    """Archetype output."""

    payload: MessageOutputPayload = Field(discriminator="payload_type")


class ArchetypeSetup(SetupModel):
    """Setup with tool references resolved during config setup."""

    model_name: str = Field(
        default="gpt-4",
        json_schema_extra={"config": True},
    )
    temperature: float = Field(
        default=0.7,
        json_schema_extra={"config": True},
    )

    search_tool: ToolReference = Field(
        default_factory=lambda: ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.FIXED,
                module_id="search-tool-v1",
            )
        ),
        json_schema_extra={"config": True},
    )

    calculator_tool: ToolReference = Field(
        default_factory=lambda: ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.TAG,
                tag="math-calculator",
            )
        ),
        json_schema_extra={"config": True},
    )

    dynamic_tool: ToolReference = Field(
        default_factory=lambda: ToolReference(
            config=ToolReferenceConfig(
                mode=ToolSelectionMode.DISCOVERABLE,
            )
        ),
        json_schema_extra={"config": True},
    )

    system_prompt: str = Field(
        default="You are a helpful assistant with access to tools.",
        json_schema_extra={"hidden": True},
    )


class ArchetypeConfigSetup(BaseModel):
    """Config setup model."""

    additional_instructions: str | None = None


class ArchetypeSecret(BaseModel):
    """Secrets model."""


client_config = ClientConfig(
    host="[::]",
    port=50152,
    mode=ServerMode.ASYNC,
    security=SecurityMode.INSECURE,
    credentials=None,
)


class ArchetypeWithToolsModule(
    BaseModule[
        ArchetypeInput,
        ArchetypeOutput,
        ArchetypeSetup,
        ArchetypeSecret,
    ]
):
    """Archetype module demonstrating tool cache usage."""

    name = "ArchetypeWithToolsModule"
    description = "Archetype with tool cache integration"

    config_setup_format = ArchetypeConfigSetup
    input_format = ArchetypeInput
    output_format = ArchetypeOutput
    setup_format = ArchetypeSetup
    secret_format = ArchetypeSecret

    metadata: ClassVar[dict[str, Any]] = {
        "name": "ArchetypeWithToolsModule",
        "version": "1.0.0",
        "tags": ["archetype", "tools"],
    }

    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
    services_config_params: ClassVar[dict[str, dict[str, Any | None] | None]] = {
        "registry": {
            "config": {},
            "client_config": client_config,
        },
    }

    async def run_config_setup(
        self,
        context: ModuleContext,  # noqa: ARG002
        config_setup_data: ArchetypeSetup,
    ) -> ArchetypeSetup:
        """Custom config setup logic, runs in parallel with tool resolution.

        Args:
            context: Module context with services.
            config_setup_data: Setup data being configured.

        Returns:
            Configured setup data.
        """
        logger.info("Running config setup for %s", self.name)
        return config_setup_data

    async def initialize(self, context: ModuleContext, setup_data: ArchetypeSetup) -> None:  # noqa: ARG002
        """Initialize module.

        Args:
            context: Module context with services and tool cache.
            setup_data: Setup data for the module.
        """
        logger.info("Initializing %s", self.name)
        if context.tool_cache:
            logger.info("Available tools: %s", context.tool_cache.list_tools())

    async def run(
        self,
        input_data: ArchetypeInput,
        setup_data: ArchetypeSetup,  # noqa: ARG002
    ) -> None:
        """Run module with tool cache lookups and call_module_by_id.

        Args:
            input_data: Input data to process.
            setup_data: Setup configuration.
        """
        logger.info("Running %s", self.name)

        tools_used: list[str] = []
        tool_results: list[str] = []

        # Get search tool from cache and call via call_module_by_id
        search_info = self.context.tool_cache.get("search_tool")
        if search_info:
            tools_used.append(f"search:{search_info.module_id}")
            async for response in self.context.call_module_by_id(
                module_id=search_info.module_id,
                input_data={"query": input_data.payload.user_prompt},
                setup_id=self.context.session.setup_id,
                mission_id=self.context.session.mission_id,
            ):
                tool_results.append(f"search_result: {response}")

        # Get calculator tool from cache
        calc_info = self.context.tool_cache.get("calculator_tool")
        if calc_info:
            tools_used.append(f"calculator:{calc_info.module_id}")
            async for response in self.context.call_module_by_id(
                module_id=calc_info.module_id,
                input_data={"expression": "2 + 2"},
                setup_id=self.context.session.setup_id,
                mission_id=self.context.session.mission_id,
            ):
                tool_results.append(f"calc_result: {response}")

        # Dynamic discovery via registry fallback for tools not in cache
        dynamic_info = self.context.tool_cache.get(
            "some_dynamic_tool",
            registry=self.context.registry,
        )
        if dynamic_info:
            tools_used.append(f"dynamic:{dynamic_info.module_id}")
            async for response in self.context.call_module_by_id(
                module_id=dynamic_info.module_id,
                input_data={"prompt": input_data.payload.user_prompt},
                setup_id=self.context.session.setup_id,
                mission_id=self.context.session.mission_id,
            ):
                tool_results.append(f"dynamic_result: {response}")

        response = MessageOutputPayload(
            response=f"Processed: {input_data.payload.user_prompt} | Results: {len(tool_results)}",
            tools_used=tools_used,
        )

        await self.context.callbacks.send_message(ArchetypeOutput(payload=response))

    async def cleanup(self) -> None:
        """Clean up resources."""
        logger.info("Cleaning up %s", self.name)
