"""Tool reference types for module configuration."""

import asyncio
import os
from typing import Annotated, ClassVar

from pydantic import AfterValidator, BaseModel, BeforeValidator, Field, PlainSerializer
from pydantic.annotated_handlers import GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema

from digitalkin.logger import logger
from digitalkin.models.module.tool_cache import ToolModuleInfo, module_info_to_tool_module_info
from digitalkin.services.communication.communication_strategy import CommunicationStrategy
from digitalkin.services.registry import RegistryStrategy


class ToolSelection(BaseModel):
    """Single tool selection with trigger filtering."""

    setup_id: str = Field(description="Setup ID of the selected tool.")
    triggers: dict[str, bool] = Field(min_length=1, max_length=100, description="Trigger protocols with enabled state.")


class ToolReference(BaseModel):
    """Tool selection containing setup IDs and trigger filters."""

    _TOOL_RESOLVE_TIMEOUT: ClassVar[float] = float(os.environ.get("DIGITALKIN_TOOL_RESOLVE_TIMEOUT", "10.0"))

    selected_tools: list[ToolSelection] = Field(
        default_factory=list, description="Selected tools with trigger filters."
    )

    async def resolve(self, registry: RegistryStrategy, communication: CommunicationStrategy) -> list[ToolModuleInfo]:
        """Resolve selected tools using the registry.

        Each tool resolution is bounded by DIGITALKIN_TOOL_RESOLVE_TIMEOUT (default 10s).

        Args:
            registry: Registry service for module discovery.
            communication: Communication service for module schemas.

        Returns:
            List of ToolModuleInfo for resolved tools, filtered by enabled triggers.
        """
        timeout = self._TOOL_RESOLVE_TIMEOUT

        async def _resolve_with_timeout(entry: ToolSelection) -> ToolModuleInfo | None:
            return await asyncio.wait_for(
                ToolReference._resolve_single(entry, registry, communication),
                timeout=timeout,
            )

        results = await asyncio.gather(
            *(_resolve_with_timeout(entry) for entry in self.selected_tools),
            return_exceptions=True,
        )
        resolved: list[ToolModuleInfo] = []
        for entry, result in zip(self.selected_tools, results):
            if isinstance(result, BaseException):
                logger.warning("Failed to resolve tool (setup_id=%s): %s", entry.setup_id, result)
            elif isinstance(result, ToolModuleInfo):
                resolved.append(result)
        return resolved

    @staticmethod
    async def _resolve_single(
        entry: "ToolSelection",
        registry: RegistryStrategy,
        communication: CommunicationStrategy,
    ) -> ToolModuleInfo | None:
        """Resolve a single tool selection.

        Args:
            entry: Tool selection to resolve.
            registry: Registry service for module discovery.
            communication: Communication service for module schemas.

        Returns:
            ToolModuleInfo if resolved, None otherwise.
        """
        setup = await registry.get_setup(entry.setup_id)
        if not setup or not setup.module_id:
            return None
        info = await registry.discover_by_id(setup.module_id)
        if not info:
            return None
        tool_info = await module_info_to_tool_module_info(info, entry.setup_id, setup.name, communication)
        if enabled_triggers := {name for name, enabled in entry.triggers.items() if enabled}:
            tool_info.tools = [t for t in tool_info.tools if t.name in enabled_triggers]
        return tool_info


class _ToolReferenceInputSchema:
    """Custom JSON schema generator with configurable maxItems and ui:options."""

    def __init__(
        self,
        setup_ids: list[str] | None,
        module_ids: list[str] | None,
        tag_ids: list[str] | None,
        categories: list[str] | None,
        max_tools: int = 0,
        min_tools: int = 0,
    ) -> None:
        self.setup_ids = setup_ids or []
        self.module_ids = module_ids
        self.tag_ids = tag_ids or []
        self.max_tools = max_tools
        self.min_tools = min_tools
        self.categories = categories or []

    def __get_pydantic_json_schema__(
        self,
        _schema: CoreSchema,
        _handler: GetJsonSchemaHandler,
    ) -> JsonSchemaValue:
        """Generate JSON schema as array for UI, hiding ToolReference complexity.

        Returns:
            JSON schema as array with ui:widget toolSelect.
        """
        json_schema: dict[str, object] = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "setupId": {"type": "string"},
                    "triggers": {
                        "type": "object",
                        "additionalProperties": {"type": "boolean"},
                        "minProperties": 1,
                        "maxProperties": 100,
                    },
                },
                "required": ["setupId", "triggers"],
            },
        }
        if self.max_tools > 0:
            json_schema["maxItems"] = self.max_tools
        if self.min_tools > 0:
            json_schema["minItems"] = self.min_tools
        json_schema["ui:widget"] = "toolSelect"
        json_schema["ui:options"] = {
            "setupIds": self.setup_ids or [],
            "tagIds": self.tag_ids or [],
            "categories": self.categories or [],
            "moduleIds": self.module_ids or [],
            "showModules": self.module_ids is not None,
        }
        return json_schema


def tool_reference_input(
    setup_ids: list[str] | None = None,
    module_ids: list[str] | None = None,
    tag_ids: list[str] | None = None,
    categories: list[str] | None = None,
    max_tools: int = 0,
    min_tools: int = 0,
) -> type[ToolReference]:
    """Create ToolReferenceInput type with schema options and validation.

    Args:
        setup_ids: Setup IDs for the user to choose from.
        module_ids: Module IDs for the user to choose from.
        tag_ids: Tag IDs for the user to choose from.
        categories: Categories for the user to choose from.
        max_tools: Maximum tools allowed. 0 for unlimited.
        min_tools: Minimum tools required. 0 for no minimum.

    Returns:
        Annotated type for use in Pydantic models.
    """

    def convert_to_tool_reference(v: object) -> ToolReference | object:
        """Convert list of tool selection dicts to ToolReference.

        Returns:
            ToolReference if input is list, otherwise original value.
        """
        if isinstance(v, list):
            return ToolReference(
                selected_tools=[
                    ToolSelection(
                        setup_id=e.get("setup_id", e.get("setupId", "")),  # type: ignore[arg-type]
                        triggers=e.get("triggers", {}),
                    )
                    if isinstance(e, dict)
                    else e
                    for e in v
                ]
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

    def serialize_to_list(v: ToolReference) -> list[dict[str, object]]:
        """Serialize ToolReference as list of dicts for frontend compatibility.

        Returns:
            List of tool selection dicts with id and subtools.
        """
        return [{"setupId": t.setup_id, "triggers": t.triggers} for t in v.selected_tools]

    return Annotated[  # type: ignore[return-value]  # Returns Annotated type, not ToolReference directly
        ToolReference,
        BeforeValidator(convert_to_tool_reference),
        AfterValidator(validate_tools_count),
        PlainSerializer(serialize_to_list, return_type=list[dict[str, object]]),
        _ToolReferenceInputSchema(
            setup_ids=setup_ids or [],
            module_ids=module_ids,
            tag_ids=tag_ids or [],
            categories=categories or [],
            max_tools=max_tools,
            min_tools=min_tools,
        ),
        Field(default_factory=ToolReference),
    ]
