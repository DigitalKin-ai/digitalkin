"""Tool reference types for module configuration."""

from typing import Annotated

from pydantic import BaseModel, BeforeValidator, Field
from pydantic.json_schema import GetJsonSchemaHandler, JsonSchemaValue
from pydantic_core import CoreSchema

from digitalkin.models.module.tool_cache import (
    SelectedTool,
    ToolModuleInfo,
    module_info_to_tool_module_info,
)
from digitalkin.services.communication.communication_strategy import CommunicationStrategy
from digitalkin.services.registry import RegistryStrategy


class ToolReference(BaseModel):
    """Tool selection configuration and reference.

    The mode determines validation requirements and resolution behavior:
    - FIXED: Requires setup_id, resolves to exact tool
    - MODULE: Requires module_id, returns constraint for frontend selection
    - TAG: Requires tag, returns constraint for frontend selection
    - DISCOVERABLE: Optional module_id/tag constraints, returns constraint info
    """

    selected_tools: list[SelectedTool] = Field(default=[], description="Tools selected by the user.")
    setup_ids: list[str] = Field(default=[], description="Setup IDs for the user to choose from.")
    module_ids: list[str] = Field(default=[], description="Module IDs for the user to choose from.")
    tags: list[str] = Field(default=[], description="Tags for the user to choose from.")
    max_tools: int = Field(default=0, description="Maximum tools to select. 0 for unlimited.")

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


def _convert_to_tool_reference(v: object) -> "ToolReference | object":
    """Convert list of setup IDs to ToolReference.

    Args:
        v: Input value, either a list of setup IDs or passthrough.

    Returns:
        ToolReference if input is list, otherwise original value.
    """
    if isinstance(v, list):
        return ToolReference(selected_tools=[SelectedTool(setup_id=sid, slug=sid) for sid in v])
    return v


class _ToolReferenceInputSchema:
    """Custom JSON schema generator that wraps ToolReference in anyOf with array option."""

    @staticmethod
    def __get_pydantic_json_schema__(  # noqa: PLW3201
        schema: CoreSchema,
        handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        """Generate JSON schema accepting both list[str] and ToolReference.

        Args:
            schema: The core schema from Pydantic.
            handler: Handler to generate JSON schema from core schema.

        Returns:
            JSON schema with anyOf accepting array or ToolReference.
        """
        json_schema = handler(schema)
        return {
            "anyOf": [
                {"type": "array", "items": {"type": "string"}},
                json_schema,
            ]
        }


ToolReferenceInput = Annotated[
    ToolReference,
    BeforeValidator(_convert_to_tool_reference),
    _ToolReferenceInputSchema,
]
"""Type alias for ToolReference fields that accept list[str] input from frontend."""
