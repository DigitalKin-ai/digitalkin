"""Tool reference types for module configuration."""

from typing import Annotated

from pydantic import AfterValidator, BaseModel, BeforeValidator, Field, PlainSerializer
from pydantic.annotated_handlers import GetJsonSchemaHandler
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import CoreSchema

from digitalkin.models.module.tool_cache import ToolModuleInfo, module_info_to_tool_module_info
from digitalkin.services.communication.communication_strategy import CommunicationStrategy
from digitalkin.services.registry import RegistryStrategy


class ToolReference(BaseModel):
    """Tool selection containing setup IDs."""

    selected_tools: list[str] = Field(default=[], description="Setup IDs of selected tools.")

    async def resolve(self, registry: RegistryStrategy, communication: CommunicationStrategy) -> list[ToolModuleInfo]:
        """Resolve selected tools using the registry.

        Args:
            registry: Registry service for module discovery.
            communication: Communication service for module schemas.

        Returns:
            List of ToolModuleInfo for resolved tools.
        """
        resolved: list[ToolModuleInfo] = []
        for setup_id in self.selected_tools:
            setup = registry.get_setup(setup_id)
            if setup and setup.module_id:
                info = registry.discover_by_id(setup.module_id)
                if info:
                    resolved.append(await module_info_to_tool_module_info(info, setup_id, setup.name, communication))
        return resolved


class _ToolReferenceInputSchema:
    """Custom JSON schema generator with configurable maxItems and ui:options."""

    def __init__(
        self,
        setup_ids: list[str],
        module_ids: list[str] | None,
        tag_ids: list[str],
        categories: list[str],
        max_tools: int = 0,
        min_tools: int = 0,
    ) -> None:
        self.setup_ids = setup_ids
        self.module_ids = module_ids
        self.tag_ids = tag_ids
        self.max_tools = max_tools
        self.min_tools = min_tools
        self.categories = categories

    def __get_pydantic_json_schema__(  # noqa: PLW3201
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
            "items": {"type": "string"},
        }
        if self.max_tools > 0:
            json_schema["maxItems"] = self.max_tools
        if self.min_tools > 0:
            json_schema["minItems"] = self.min_tools
        json_schema["ui:widget"] = "toolSelect"
        json_schema["ui:options"] = {
            "setupIds": self.setup_ids,
            "tagIds": self.tag_ids,
            "categories": self.categories,
            "moduleIds": self.module_ids or [],
            "showModules": self.module_ids is not None,
        }
        return json_schema


def tool_reference_input(
    setup_ids: list[str] = [],
    module_ids: list[str] | None = [],
    tag_ids: list[str] = [],
    categories: list[str] = [],
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
        """Convert list of setup IDs to ToolReference.

        Returns:
            ToolReference if input is list, otherwise original value.
        """
        if isinstance(v, list):
            return ToolReference(selected_tools=v)
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

    def serialize_to_list(v: ToolReference) -> list[str]:
        """Serialize ToolReference as plain list for frontend compatibility.

        Returns:
            List of setup IDs.
        """
        return v.selected_tools

    return Annotated[  # type: ignore[return-value]
        ToolReference,
        BeforeValidator(convert_to_tool_reference),
        AfterValidator(validate_tools_count),
        PlainSerializer(serialize_to_list, return_type=list[str]),
        _ToolReferenceInputSchema(
            setup_ids=setup_ids,
            module_ids=module_ids,
            tag_ids=tag_ids,
            categories=categories,
            max_tools=max_tools,
            min_tools=min_tools,
        ),
    ]
