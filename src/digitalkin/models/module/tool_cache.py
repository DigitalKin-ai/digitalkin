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
    """Two-layer cache of resolved tool references, keyed by ``setup_id``.

    The layers differ by *lifetime*, which is the whole point of the split:

    - ``declared`` — the tools the setup itself selects, resolved once by
      ``SetupModel.build_tool_cache``. The servicer keeps this layer alive per
      ``setup_id`` and hands the same object to every mission of that setup.
    - ``dynamic`` — tools the agent loaded at runtime via
      ``ModuleContext.resolve_tool``. This layer is **mission-scoped**: it is
      rebuilt per mission from the ``loaded_tools`` storage collection, so a load
      persists across the turns of one conversation and never leaks into another
      mission of the same setup.

    Never write a runtime resolution into ``declared`` — doing so is what leaked
    dynamically-loaded tools across missions before the split existed.
    """

    declared: dict[str, ToolModuleInfo] = Field(
        default_factory=dict, description="Setup-selected tools; shared across missions of one setup."
    )
    dynamic: dict[str, ToolModuleInfo] = Field(
        default_factory=dict, description="Runtime-loaded tools; scoped to a single mission."
    )

    @property
    def entries(self) -> dict[str, ToolModuleInfo]:
        """Merged read-only view of both layers, ``dynamic`` winning on conflict.

        Returns:
            A fresh dict — mutating it does not touch either layer (and must not:
            ``declared`` is shared with every other mission of this setup).
        """
        return {**self.declared, **self.dynamic}

    def mission_view(self, dynamic: dict[str, ToolModuleInfo] | None = None) -> "ToolCache":
        """Build a per-mission view that shares this cache's ``declared`` entries.

        The mapping is shallow-copied so a mission can never mutate the shared
        declared layer, while the ``ToolModuleInfo`` values are reused as-is
        (they are treated as immutable, and re-validating them per turn is not free).

        Args:
            dynamic: Pre-resolved runtime tools to seed the mission layer with.

        Returns:
            A new ``ToolCache`` whose ``dynamic`` layer is private to this mission.
        """
        return ToolCache.model_construct(declared=dict(self.declared), dynamic=dynamic or {})

    def add(self, tool_module_info: ToolModuleInfo) -> None:
        """Add a setup-declared tool to the ``declared`` layer.

        Args:
            tool_module_info: Resolved tool module information.
        """
        setup_id = tool_module_info.setup_id
        self.declared[setup_id] = tool_module_info
        logger.debug(
            "Tool cached (declared): module_id=%s",
            tool_module_info.module_id,
            extra={"setup_id": setup_id},
        )

    def add_dynamic(self, tool_module_info: ToolModuleInfo) -> None:
        """Add a runtime-loaded tool to the mission-scoped ``dynamic`` layer.

        Args:
            tool_module_info: Resolved tool module information.
        """
        setup_id = tool_module_info.setup_id
        self.dynamic[setup_id] = tool_module_info
        logger.debug(
            "Tool cached (dynamic): module_id=%s",
            tool_module_info.module_id,
            extra={"setup_id": setup_id},
        )

    def get(
        self,
        setup_id: str,
    ) -> ToolModuleInfo | None:
        """Get a tool from either layer, preferring the mission-scoped one.

        Args:
            setup_id: Field name to look up.

        Returns:
            ToolModuleInfo if found, None otherwise.
        """
        return self.dynamic.get(setup_id) or self.declared.get(setup_id)

    def clear(self) -> None:
        """Clear both layers."""
        self.declared.clear()
        self.dynamic.clear()

    def list_tools(self) -> list[str]:
        """List all cached tool names across both layers.

        Returns:
            List of setup field names in cache.
        """
        return list(self.entries.keys())
