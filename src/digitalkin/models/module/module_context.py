"""Define the module context used in the triggers."""

import os
from collections.abc import AsyncGenerator, Callable, Coroutine
from datetime import tzinfo
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

from digitalkin.logger import logger
from digitalkin.models.module.tool_cache import ToolCache, ToolDefinition, ToolModuleInfo, ToolParameter
from digitalkin.services.agent.agent_strategy import AgentStrategy
from digitalkin.services.communication.communication_strategy import CommunicationStrategy
from digitalkin.services.cost.cost_strategy import CostStrategy
from digitalkin.services.filesystem.filesystem_strategy import FilesystemStrategy
from digitalkin.services.identity.identity_strategy import IdentityStrategy
from digitalkin.services.registry.registry_strategy import RegistryStrategy
from digitalkin.services.snapshot.snapshot_strategy import SnapshotStrategy
from digitalkin.services.storage.storage_strategy import StorageStrategy
from digitalkin.services.user_profile.user_profile_strategy import UserProfileStrategy


class Session(SimpleNamespace):
    """Session data container with mandatory setup_id and mission_id."""

    job_id: str
    mission_id: str
    setup_id: str
    setup_version_id: str
    timezone: tzinfo

    def __init__(
        self,
        job_id: str,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        timezone: tzinfo | None = None,
        **kwargs: dict[str, Any],
    ) -> None:
        """Init Module Session.

        Raises:
            ValueError: If mandatory args are missing.
        """
        if not setup_id:
            msg = "setup_id is mandatory"
            raise ValueError(msg)
        if not setup_version_id:
            msg = "setup_version_id is mandatory"
            raise ValueError(msg)
        if not mission_id:
            msg = "mission_id is mandatory"
            raise ValueError(msg)
        if not job_id:
            msg = "job_id is mandatory"
            raise ValueError(msg)

        self.job_id = job_id
        self.mission_id = mission_id
        self.setup_id = setup_id
        self.setup_version_id = setup_version_id
        self.timezone = timezone or ZoneInfo(os.environ.get("DIGITALKIN_TIMEZONE", "Europe/Paris"))

        super().__init__(**kwargs)

    def current_ids(self) -> dict[str, str]:
        """Return current session ids as a dictionary.

        Returns:
            A dictionary containing the current session ids.
        """
        return {
            "job_id": self.job_id,
            "mission_id": self.mission_id,
            "setup_id": self.setup_id,
            "setup_version_id": self.setup_version_id,
        }


class ModuleContext:
    """ModuleContext provides a container for strategies and resources used by a module.

    This context object is designed to be passed to module components, providing them with
    access to shared strategies and resources. Additional attributes may be set dynamically.
    """

    # services list
    agent: AgentStrategy
    communication: CommunicationStrategy
    cost: CostStrategy
    filesystem: FilesystemStrategy
    identity: IdentityStrategy
    registry: RegistryStrategy
    snapshot: SnapshotStrategy
    storage: StorageStrategy
    user_profile: UserProfileStrategy

    session: Session
    callbacks: SimpleNamespace
    metadata: SimpleNamespace
    helpers: SimpleNamespace
    state: SimpleNamespace = SimpleNamespace()
    tool_cache: ToolCache

    def __init__(  # noqa: PLR0913, PLR0917
        self,
        agent: AgentStrategy,
        communication: CommunicationStrategy,
        cost: CostStrategy,
        filesystem: FilesystemStrategy,
        identity: IdentityStrategy,
        registry: RegistryStrategy,
        snapshot: SnapshotStrategy,
        storage: StorageStrategy,
        user_profile: UserProfileStrategy,
        session: dict[str, Any],
        metadata: dict[str, Any] = {},
        helpers: dict[str, Any] = {},
        callbacks: dict[str, Any] = {},
        tool_cache: ToolCache | None = None,
    ) -> None:
        """Register mandatory services, session, metadata and callbacks.

        Args:
            agent: AgentStrategy.
            communication: CommunicationStrategy.
            cost: CostStrategy.
            filesystem: FilesystemStrategy.
            identity: IdentityStrategy.
            registry: RegistryStrategy.
            snapshot: SnapshotStrategy.
            storage: StorageStrategy.
            user_profile: UserProfileStrategy.
            metadata: dict defining differents Module metadata.
            helpers: dict different user defined helpers.
            session: dict referring the session IDs or informations.
            callbacks: Functions allowing user to agent interaction.
            tool_cache: ToolCache with pre-resolved tool references from setup.
        """
        self.agent = agent
        self.communication = communication
        self.cost = cost
        self.filesystem = filesystem
        self.identity = identity
        self.registry = registry
        self.snapshot = snapshot
        self.storage = storage
        self.user_profile = user_profile

        self.metadata = SimpleNamespace(**metadata)
        self.session = Session(**session)
        self.helpers = SimpleNamespace(**helpers)
        self.callbacks = SimpleNamespace(**callbacks)
        self.tool_cache = tool_cache or ToolCache()

    async def call_module_by_id(
        self,
        module_id: str,
        input_data: dict,
        setup_id: str,
        mission_id: str,
        callback: Callable[[dict], Coroutine[Any, Any, None]] | None = None,
    ) -> AsyncGenerator[dict, None]:
        """Call a module by ID, discovering address/port from registry.

        Args:
            module_id: Module identifier to look up in registry.
            input_data: Input data as dictionary.
            setup_id: Setup configuration ID.
            mission_id: Mission context ID.
            callback: Optional callback for each response.

        Yields:
            Streaming responses from module as dictionaries.
        """
        module_info = self.registry.discover_by_id(module_id)

        logger.debug(
            "Calling module by ID",
            extra={
                "module_id": module_id,
                "address": module_info.address,
                "port": module_info.port,
            },
        )

        async for response in self.communication.call_module(
            module_address=module_info.address,
            module_port=module_info.port,
            input_data=input_data,
            setup_id=setup_id,
            mission_id=mission_id,
            callback=callback,
        ):
            yield response

    async def get_module_schemas_by_id(
        self,
        module_id: str,
        *,
        llm_format: bool = False,
    ) -> dict[str, dict]:
        """Get module schemas by ID, discovering address/port from registry.

        Args:
            module_id: Module identifier to look up in registry.
            llm_format: If True, return LLM-optimized schema format.

        Returns:
            Dictionary containing schemas: {"input": ..., "output": ..., "setup": ..., "secret": ...}
        """
        module_info = self.registry.discover_by_id(module_id)

        logger.debug(
            "Getting module schemas by ID",
            extra={
                "module_id": module_id,
                "address": module_info.address,
                "port": module_info.port,
            },
        )

        return await self.communication.get_module_schemas(
            module_address=module_info.address,
            module_port=module_info.port,
            llm_format=llm_format,
        )

    def create_openai_style_tools(self, slug: str) -> list[dict[str, Any]]:
        """Create OpenAI-style function calling schemas for a tool module.

        Uses tool cache (fast path) with registry fallback. Returns one schema
        per ToolDefinition (protocol) in the module.

        Args:
            slug: Module ID to look up (checks cache first, then registry).

        Returns:
            List of OpenAI-style tool schemas, one per protocol. Empty if not found.
        """
        tool_module_info = self.tool_cache.get(slug)
        if not tool_module_info:
            return []

        return [
            {
                "type": "function",
                "function": {
                    "module_id": tool_module_info.module_id,
                    "toolkit_name": tool_module_info.tool_name or "undefined",
                    "name": tool_module_info.slug + "_" + tool_def.name,
                    "description": tool_def.description,
                    "parameters": ModuleContext._build_parameters_schema(tool_def.parameters),
                },
            }
            for tool_def in tool_module_info.tools
        ]

    @staticmethod
    def _build_parameters_schema(params: list[ToolParameter]) -> dict[str, Any]:
        """Convert ToolParameter list to JSON Schema.

        Args:
            params: List of tool parameters.

        Returns:
            JSON Schema object with properties and required fields.
        """
        return {
            "type": "object",
            "properties": {p.name: {"type": p.type, "description": p.description or ""} for p in params},
            "required": [p.name for p in params if p.required],
        }

    def create_tool_functions(
        self,
        slug: str,
    ) -> list[tuple[ToolDefinition, Callable[..., AsyncGenerator[dict, None]]]]:
        """Create tool functions for all protocols in a tool setup.

        Returns an async generator per ToolDefinition that calls the remote tool
        module via gRPC with the protocol auto-injected.

        This method only uses the tool cache (no registry fallback). Use this
        in sync contexts like __init__ methods.

        Args:
            slug: Setup ID to look up in cache.

        Returns:
            List of (ToolDefinition, async_generator_function) tuples. Empty if not found.
        """
        tool_module_info = self.tool_cache.entries.get(slug)
        if not tool_module_info:
            return []

        communication = self.communication
        session = self.session

        result = []
        for tool_def in tool_module_info.tools:
            # Capture tool_def in closure via separate method
            fn = ModuleContext._create_single_tool_function(communication, session, tool_module_info, tool_def)
            result.append((tool_def, fn))

        return result

    @staticmethod
    def _create_single_tool_function(
        communication: CommunicationStrategy,
        session: Session,
        tool_module_info: ToolModuleInfo,
        tool_def: ToolDefinition,
    ) -> Callable[..., AsyncGenerator[dict, None]]:
        """Create a single tool function for a specific protocol.

        Args:
            communication: Communication strategy for gRPC calls.
            session: Current session with setup_id and mission_id.
            tool_module_info: Tool module information containing address and port.
            tool_def: Tool definition with protocol name.

        Returns:
            Async generator function that calls the module with protocol injected.
        """
        protocol = tool_def.name

        async def tool_function(**kwargs: Any) -> AsyncGenerator[dict, None]:  # noqa: ANN401
            kwargs["protocol"] = protocol
            wrapped_input = {"root": kwargs}
            async for response in communication.call_module(
                module_address=tool_module_info.address,
                module_port=tool_module_info.port,
                input_data=wrapped_input,
                setup_id=tool_module_info.setup_id,
                mission_id=session.mission_id,
            ):
                yield response

        tool_function.__name__ = tool_def.name
        tool_function.__doc__ = tool_def.description

        return tool_function
