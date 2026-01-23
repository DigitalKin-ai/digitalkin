"""Tool reference types for module configuration."""

from typing import Annotated

from pydantic import AfterValidator, BaseModel, BeforeValidator, Field
from pydantic.annotated_handlers import GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema

from digitalkin.models.module.tool_cache import (
    SelectedTool,
    ToolModuleInfo,
    module_info_to_tool_module_info,
)
from digitalkin.services.communication.communication_strategy import CommunicationStrategy
from digitalkin.services.registry import RegistryStrategy


class ToolReference(BaseModel):
    """Tool selection configuration and reference."""

    selected_tools: list[SelectedTool] = Field(default=[], description="Tools selected by the user.")
    setup_ids: list[str] = Field(default=[], description="Setup IDs for the user to choose from.")
    module_ids: list[str] = Field(default=[], description="Module IDs for the user to choose from.")
    tags: list[str] = Field(default=[], description="Tags for the user to choose from.")
    max_tools: int = Field(default=0, description="Maximum tools to select. 0 for unlimited.")
    min_tools: int = Field(default=0, description="Minimum tools to select. 0 for no minimum.")

    async def resolve(self, registry: RegistryStrategy, communication: CommunicationStrategy) -> list[ToolModuleInfo]:
        """Resolve this reference using the registry.

        Args:
            registry: Registry service for module discovery.
            communication: Communication service for module schemas.

        Returns:
            List of ToolModuleInfo if resolved.
        """
        resolved: list[ToolModuleInfo] = []
        for tool in self.selected_tools:
            setup = registry.get_setup(tool.setup_id)
            if setup and setup.module_id:
                info = registry.discover_by_id(setup.module_id)
                tool.slug = tool.setup_id
                tool.module_id = setup.module_id
                tool.name = setup.name
                if info:
                    resolved.append(await module_info_to_tool_module_info(info, tool, communication))

        return resolved


class _ToolReferenceInputSchema:
    """Custom JSON schema generator with configurable maxItems and ui:options."""

    def __init__(
        self,
        setup_ids: list[str],
        module_ids: list[str],
        tags: list[str],
        max_tools: int = 0,
        min_tools: int = 0,
    ) -> None:
        self.setup_ids = setup_ids
        self.module_ids = module_ids
        self.tags = tags
        self.max_tools = max_tools
        self.min_tools = min_tools

    def __get_pydantic_json_schema__(  # noqa: PLW3201
        self,
        schema: CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        """Generate JSON schema accepting both list[str] and ToolReference.

        Args:
            schema: The core schema from Pydantic.
            handler: Handler to generate JSON schema from core schema.

        Returns:
            JSON schema with anyOf accepting array or ToolReference, plus ui:options.
        """
        json_schema = handler(schema)

        array_option: dict[str, object] = {"type": "array", "items": {"type": "string"}}
        if self.max_tools > 0 and self.max_tools >= self.min_tools:
            array_option["maxItems"] = self.max_tools
        if self.min_tools > 0 and self.min_tools <= self.max_tools:
            array_option["minItems"] = self.min_tools

        return {
            "anyOf": [
                array_option,
                json_schema,
            ],
            "ui:options": {
                "setup_ids": self.setup_ids,
                "module_ids": self.module_ids,
                "tags": self.tags,
                "max_tools": self.max_tools,
                "min_tools": self.min_tools,
            },
        }


def tool_reference_input(
    setup_ids: list[str] = [],
    module_ids: list[str] = [],
    tags: list[str] = [],
    max_tools: int = 0,
    min_tools: int = 0,
) -> type[ToolReference]:
    """Create ToolReferenceInput type with schema options and validation.

    Args:
        setup_ids: Setup IDs for the user to choose from.
        module_ids: Module IDs for the user to choose from.
        tags: Tags for the user to choose from.
        max_tools: Maximum tools allowed. 0 for unlimited.
        min_tools: Minimum tools required. 0 for no minimum.

    Returns:
        Annotated type for use in Pydantic models.
    """

    def convert_to_tool_reference(v: object) -> ToolReference | object:
        """Convert list of setup IDs to ToolReference with config preserved.

        Returns:
            ToolReference if input is list, otherwise original value.
        """
        if isinstance(v, list):
            return ToolReference(
                selected_tools=[SelectedTool(setup_id=sid, slug=sid) for sid in v],
                setup_ids=setup_ids,
                module_ids=module_ids,
                tags=tags,
                max_tools=max_tools,
                min_tools=min_tools,
            )
        return v

    def validate_tools_count(v: ToolReference) -> ToolReference:
        """Validate selected_tools count against min/max constraints.

        Returns:
            The validated ToolReference.

        Raises:
            ValueError: If count is below min_tools or above max_tools.
        """
        count = len(v.selected_tools)
        if min_tools > 0 and count < min_tools:
            msg = f"At least {min_tools} tools required, got {count}"
            raise ValueError(msg)
        if max_tools > 0 and count > max_tools:
            msg = f"At most {max_tools} tools allowed, got {count}"
            raise ValueError(msg)
        return v

    return Annotated[  # type: ignore[return-value]
        ToolReference,
        BeforeValidator(convert_to_tool_reference),
        AfterValidator(validate_tools_count),
        _ToolReferenceInputSchema(
            setup_ids=setup_ids,
            module_ids=module_ids,
            tags=tags,
            max_tools=max_tools,
            min_tools=min_tools,
        ),
    ]
