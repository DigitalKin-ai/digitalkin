"""Tool cache for resolved tool references."""

from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field

from digitalkin.logger import logger
from digitalkin.models.services.registry import ModuleInfo
from digitalkin.services.registry import RegistryStrategy

if TYPE_CHECKING:
    from digitalkin.services.communication import CommunicationStrategy


class ToolParameter(BaseModel):
    """Definition of a single tool parameter.

    Attributes:
        name: Parameter name.
        type: JSON Schema type (string, integer, number, boolean, array, object).
        description: Parameter description for the LLM.
        required: Whether this parameter is required.
        enum: Optional list of allowed values.
        items: Optional schema for array item types.
        properties: Optional schema for object properties.
    """

    name: str
    type: str
    description: str
    required: bool = True
    enum: list[str] | None = None
    items: dict[str, Any] | None = None
    properties: dict[str, Any] | None = None


class ToolDefinition(BaseModel):
    """Complete definition of an LLM tool with grouped parameters.

    Attributes:
        name: Tool name (from protocol const or trigger class name).
        description: Tool description (from trigger docstring).
        parameters: List of parameter definitions.
    """

    name: str
    description: str
    parameters: list[ToolParameter] = Field(default_factory=list)


class ToolModuleInfo(ModuleInfo):
    """Module info for tool modules."""

    tools: list[ToolDefinition]
    setup_id: str


class ToolCache(BaseModel):
    """Registry cache storing resolved tool references by setup field name."""

    entries: dict[str, ToolModuleInfo] = Field(default_factory=dict)

    def add(self, setup_id: str, tool_module_info: ToolModuleInfo) -> None:
        """Add a tool to the cache.

        Args:
            setup_id: Field name from SetupModel used as cache key.
            tool_module_info: Resolved tool module information.
        """
        self.entries[setup_id] = tool_module_info
        logger.debug(
            "Tool cached",
            extra={"setup_id": setup_id, "module_id": tool_module_info.module_id},
        )

    async def get(
        self,
        setup_id: str,
        *,
        registry: RegistryStrategy | None = None,
        communication: "CommunicationStrategy | None" = None,
    ) -> ToolModuleInfo | None:
        """Get a tool from cache, optionally querying registry on miss.

        Args:
            setup_id: Field name to look up.
            registry: Optional registry to query on cache miss.
            communication: Optional communication strategy for schema fetching.

        Returns:
            ToolModuleInfo if found, None otherwise.
        """
        cached = self.entries.get(setup_id)
        if cached:
            return cached

        if registry and communication:
            try:
                setup_info = registry.get_setup(setup_id)
                if setup_info and setup_info.module_id:
                    info = registry.discover_by_id(setup_info.module_id)
                    if info:
                        tool_info = await module_info_to_tool_module_info(info, setup_id, communication)
                        self.add(setup_id, tool_info)
                        return tool_info
            except Exception:
                logger.exception("Registry lookup failed", extra={"setup_id": setup_id})

        return None

    def clear(self) -> None:
        """Clear all cache entries."""
        self.entries.clear()

    def list_tools(self) -> list[str]:
        """List all cached tool names.

        Returns:
            List of setup field names in cache.
        """
        return list(self.entries.keys())


async def module_info_to_tool_module_info(
    module_info: ModuleInfo,
    setup_id: str,
    communication: "CommunicationStrategy",
    *,
    llm_format: bool = True,
) -> ToolModuleInfo:
    """Convert ModuleInfo to ToolModuleInfo by fetching schemas via gRPC.

    Fetches the module's input schema and extracts tool definitions from
    the discriminated union structure.

    Args:
        module_info: Module info from registry.
        setup_id: Setup ID from tool configuration.
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

    tools = _extract_tools_from_schema(input_schema)

    return ToolModuleInfo(
        module_id=module_info.module_id,
        module_type=module_info.module_type,
        address=module_info.address,
        port=module_info.port,
        version=module_info.version,
        name=module_info.name,
        documentation=module_info.documentation,
        status=module_info.status,
        tools=tools,
        setup_id=setup_id,
    )


def _extract_tools_from_schema(schema: dict[str, Any]) -> list[ToolDefinition]:
    """Extract tool definitions from a discriminated union input schema.

    Args:
        schema: JSON schema with $defs containing protocol-based types.

    Returns:
        List of ToolDefinition with parameters grouped by trigger.
    """
    tools: list[ToolDefinition] = []
    defs = schema.get("$defs", {})

    # Skip SDK utility protocols
    utility_protocols = {"HealthcheckPingInput", "HealthcheckServicesInput", "HealthcheckStatusInput"}

    for def_name, def_schema in defs.items():
        if def_name in utility_protocols:
            continue

        properties = def_schema.get("properties", {})
        protocol_prop = properties.get("protocol", {})

        # Skip if no protocol const (not a tool input type)
        if "const" not in protocol_prop:
            continue

        # Extract tool-level info from trigger
        tool_name = protocol_prop.get("const", def_name)
        tool_description = def_schema.get("description", "")

        required_fields = set(def_schema.get("required", []))
        parameters: list[ToolParameter] = []

        for prop_name, prop_info in properties.items():
            if prop_name in {"protocol", "created_at"}:
                continue

            param = ToolParameter(
                name=prop_name,
                type=prop_info.get("type", "string"),
                description=prop_info.get("description", ""),
                required=prop_name in required_fields,
                enum=prop_info.get("enum"),
                items=prop_info.get("items"),
                properties=prop_info.get("properties"),
            )
            parameters.append(param)

        tools.append(
            ToolDefinition(
                name=tool_name,
                description=tool_description,
                parameters=parameters,
            )
        )

    return tools
