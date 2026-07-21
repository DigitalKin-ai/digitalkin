"""Tool cache for resolved tool references."""

import re
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from digitalkin.logger import logger
from digitalkin.models.services.registry import ModuleInfo
from digitalkin.utils.llm_ready_schema import LlmReadySchema


class SelectedTool(BaseModel):
    """Selected tool information."""

    setup_id: str = ""
    module_id: str = ""
    slug: str = ""
    name: str = ""


if TYPE_CHECKING:
    from digitalkin.services.communication import CommunicationStrategy


class ToolDefinition(BaseModel):
    """Complete definition of an LLM tool with resolved JSON Schema parameters.

    Attributes:
        name: Tool name (from protocol const or trigger class name).
        description: Tool description (from trigger docstring).
        parameters_schema: JSON Schema object describing the tool's parameters.
    """

    name: str
    description: str
    parameters_schema: dict[str, Any] = Field(
        default_factory=lambda: {"type": "object", "properties": {}, "required": []}
    )

    @property
    def parameter_names(self) -> set[str]:
        """The set of parameter names from the schema."""
        return set[str](self.parameters_schema.get("properties", {}).keys())

    @property
    def parameter_count(self) -> int:
        """The number of parameters in the schema."""
        return len(self.parameters_schema.get("properties", {}))


class ToolModuleInfo(ModuleInfo):
    """Module info for tool modules."""

    setup_id: str = ""
    tool_name: str = ""
    cost_config: dict[str, Any] = Field(default_factory=dict)
    tools: list[ToolDefinition] = Field(default_factory=list)

    @property
    def slug(self) -> str:
        """Slugified tool name for cache keys and function naming."""
        return ToolModuleInfo._slugify(self.tool_name)

    @staticmethod
    def _slugify(name: str) -> str:
        """Convert a name to a valid lowercase identifier.

        Args:
            name: Human-readable name (e.g., "Google Search").

        Returns:
            Slugified name (e.g., "google_search").
        """
        slug = name.lower()
        slug = re.sub(r"[^a-z0-9]+", "_", slug)
        return slug.strip("_")

    @classmethod
    async def from_module_info(
        cls,
        module_info: ModuleInfo,
        setup_id: str,
        tool_name: str,
        communication: "CommunicationStrategy",
        *,
        llm_format: bool = True,
    ) -> "ToolModuleInfo":
        """Convert ModuleInfo to ToolModuleInfo by fetching schemas via gRPC.

        Args:
            module_info: Module info from registry.
            setup_id: Setup ID of the selected tool.
            tool_name: Name of the tool.
            communication: Communication strategy for gRPC calls.
            llm_format: Use LLM-friendly schema format.

        Returns:
            ToolModuleInfo with tools extracted from input schema.
        """
        schemas = await communication.get_module_schemas(
            module_info.address,
            module_info.port,
            llm_format=llm_format,
        )

        input_schema = schemas.get("input", {})
        if llm_format:
            input_schema = input_schema.get("json_schema", input_schema)

        return cls(
            module_id=module_info.module_id,
            module_type=module_info.module_type,
            address=module_info.address,
            port=module_info.port,
            version=module_info.version,
            module_name=module_info.module_name,
            documentation=module_info.documentation,
            status=module_info.status,
            tools=cls._extract_tools_from_schema(input_schema),
            setup_id=setup_id,
            tool_name=tool_name,
            cost_config=schemas.get("cost", {}),
        )

    @staticmethod
    def _build_parameters_from_schema(def_schema: dict[str, Any]) -> dict[str, Any]:
        """Build parameters_schema directly from an inlined JSON Schema.

        Skips internal fields (``protocol``, ``created_at``).

        Args:
            def_schema: JSON Schema for the trigger with all ``$ref`` already inlined.

        Returns:
            JSON Schema dict with properties and required fields.
        """
        properties = def_schema.get("properties", {})
        required_fields = set[Any](def_schema.get("required", []))
        param_properties: dict[str, Any] = {}
        required_list: list[str] = []

        for prop_name, prop_info in properties.items():
            if prop_name in {"protocol", "created_at"}:
                continue
            param_properties[prop_name] = dict[Any, Any](prop_info.items())
            if prop_name in required_fields:
                required_list.append(prop_name)

        return {"type": "object", "properties": param_properties, "required": required_list}

    @staticmethod
    def _extract_tools_from_schema(schema: dict[str, Any]) -> list[ToolDefinition]:
        """Extract tool definitions from a discriminated union input schema.

        Args:
            schema: JSON schema with $defs containing protocol-based types.

        Returns:
            List of ToolDefinition with parameters_schema per trigger.
        """
        from digitalkin.models.module.utility import UtilityProtocol

        tools: list[ToolDefinition] = []
        defs = schema.get("$defs", {})
        utility_protocols = {cls.__name__ for cls in UtilityProtocol.__subclasses__()}

        for def_name, def_schema in defs.items():
            if def_name in utility_protocols:
                continue

            protocol_prop = def_schema.get("properties", {}).get("protocol", {})
            if "const" not in protocol_prop:
                continue

            inlined = LlmReadySchema.inline_refs({**def_schema, "$defs": defs})
            tools.append(
                ToolDefinition(
                    name=protocol_prop.get("const", def_name),
                    description=def_schema.get("description", ""),
                    parameters_schema=ToolModuleInfo._build_parameters_from_schema(inlined),
                )
            )

        return tools


class ToolCache(BaseModel):
    """Registry cache storing resolved tool references by setup field name."""

    entries: dict[str, ToolModuleInfo] = Field(default_factory=dict)

    def add(self, tool_module_info: ToolModuleInfo) -> None:
        """Add a tool to the cache.

        Args:
            tool_module_info: Resolved tool module information.
        """
        setup_id = tool_module_info.setup_id
        self.entries[setup_id] = tool_module_info
        logger.debug(
            "Tool cached: module_id=%s",
            tool_module_info.module_id,
            extra={"setup_id": setup_id},
        )

    def get(
        self,
        setup_id: str,
    ) -> ToolModuleInfo | None:
        """Get a tool from cache, optionally querying registry on miss.

        Args:
            setup_id: Field name to look up.

        Returns:
            ToolModuleInfo if found, None otherwise.
        """
        return self.entries.get(setup_id)

    def clear(self) -> None:
        """Clear all cache entries."""
        self.entries.clear()

    def list_tools(self) -> list[str]:
        """List all cached tool names.

        Returns:
            List of setup field names in cache.
        """
        return list(self.entries.keys())
