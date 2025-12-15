"""Module helpers for inter-module communication."""

from collections.abc import AsyncGenerator, Callable, Coroutine
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from digitalkin.logger import logger

if TYPE_CHECKING:
    from digitalkin.models.module.module_context import ModuleContext


class ModuleHelpers(SimpleNamespace):
    """Helpers for module-to-module communication.

    Extends SimpleNamespace to allow dynamic attribute assignment
    while providing built-in helper methods.
    """

    def __init__(self, context: "ModuleContext", **kwargs: dict[str, Any]) -> None:
        """Initialize helpers with context reference.

        Args:
            context: ModuleContext providing access to services.
            **kwargs: Additional attributes to set on the namespace.
        """
        super().__init__(**kwargs)
        self._context = context

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
            module_id: Module identifier to look up in registry
            input_data: Input data as dictionary
            setup_id: Setup configuration ID
            mission_id: Mission context ID
            callback: Optional callback for each response

        Yields:
            Streaming responses from module as dictionaries
        """
        module_info = self._context.registry.discover_by_id(module_id)

        logger.debug(
            "Calling module by ID",
            extra={
                "module_id": module_id,
                "address": module_info.address,
                "port": module_info.port,
            },
        )

        async for response in self._context.communication.call_module(
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
            module_id: Module identifier to look up in registry
            llm_format: If True, return LLM-optimized schema format

        Returns:
            Dictionary containing schemas: {"input": ..., "output": ..., "setup": ..., "secret": ...}
        """
        module_info = self._context.registry.discover_by_id(module_id)

        logger.debug(
            "Getting module schemas by ID",
            extra={
                "module_id": module_id,
                "address": module_info.address,
                "port": module_info.port,
            },
        )

        return await self._context.communication.get_module_schemas(
            module_address=module_info.address,
            module_port=module_info.port,
            llm_format=llm_format,
        )

    async def create_openai_style_tool(self, module_id: str) -> dict[str, Any] | None:
        """Create OpenAI-style function calling schema for a tool.

        Uses tool cache (fast path) with registry fallback. Fetches the tool's
        input schema and wraps it in OpenAI function calling format.

        Args:
            module_id: Module ID to look up (checks cache first, then registry)

        Returns:
            OpenAI-style tool schema if found:
            {
                "type": "function",
                "function": {
                    "name": "...",
                    "description": "...",
                    "parameters": {...}  # Input JSON Schema
                }
            }
            None if tool not found.
        """
        module_info = self._context.tool_cache.check_and_get(module_id, self._context.registry)
        if not module_info:
            return None

        schemas = await self._context.communication.get_module_schemas(
            module_address=module_info.address,
            module_port=module_info.port,
            llm_format=True,
        )

        return {
            "type": "function",
            "function": {
                "name": module_info.name or module_info.module_id,
                "description": module_info.documentation or "",
                "parameters": schemas["input"],
            },
        }

    def create_tool_function(
        self,
        module_id: str,
    ) -> Callable[..., AsyncGenerator[dict, None]] | None:
        """Create async generator function for a tool.

        Returns an async generator that calls the remote tool module via gRPC
        and yields each response as it arrives until end_of_stream or gRPC ends.

        Args:
            module_id: Module ID to look up (checks cache first, then registry)

        Returns:
            Async generator function if tool found, None otherwise.
            The function accepts **kwargs matching the tool's input schema
            and yields dict responses.
        """
        module_info = self._context.tool_cache.check_and_get(module_id, self._context.registry)
        if not module_info:
            return None

        # Capture references for closure
        communication = self._context.communication
        session = self._context.session
        address = module_info.address
        port = module_info.port

        async def tool_function(**kwargs: Any) -> AsyncGenerator[dict, None]:  # noqa: ANN401
            """Call remote tool module and yield responses.

            Yields:
                dict: Each response from the module until end_of_stream.
            """
            wrapped_input = {"root": kwargs}
            async for response in communication.call_module(
                module_address=address,
                module_port=port,
                input_data=wrapped_input,
                setup_id=session.setup_id,
                mission_id=session.mission_id,
            ):
                yield response

        tool_function.__name__ = module_info.name or module_info.module_id
        tool_function.__doc__ = module_info.documentation or ""

        return tool_function
