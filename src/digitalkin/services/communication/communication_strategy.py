"""Abstract base class for communication strategies."""

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

from digitalkin.logger import logger
from digitalkin.services.base_strategy import BaseStrategy


class CommunicationStrategy(BaseStrategy, ABC):
    """Abstract base class for module-to-module communication.

    This service enables:
    - Archetype → Tool communication
    - Archetype → Archetype communication
    - Tool → Tool communication
    - Any module → Any module communication

    The service wraps the Module Service protocol from agentic-mesh-protocol.
    """

    @abstractmethod
    async def close(self) -> None:
        """Release communication resources (channels, connection pools)."""
        ...

    @abstractmethod
    async def get_module_schemas(
        self,
        module_address: str,
        module_port: int,
        *,
        llm_format: bool = False,
    ) -> dict[str, dict]:
        """Get module schemas (input/output/setup/secret/cost).

        Args:
            module_address: Target module address
            module_port: Target module port
            llm_format: Return LLM-friendly format (simplified schema).
                Note: cost always returns actual data regardless of this flag.

        Returns:
            Dictionary containing schemas:
            {
                "input": {...},
                "output": {...},
                "setup": {...},
                "secret": {...},
                "cost": {...}
            }
        """
        ...

    async def get_module_config_schema(  # noqa: PLR6301
        self,
        module_address: str,
        module_port: int,
        *,
        llm_format: bool = False,
    ) -> dict[str, Any]:
        """Get the module's config-setup JSON schema (the fields a caller fills at setup/update).

        Concrete implementations that can reach the module override this. The default returns an
        empty schema so callers treat "no schema" as "skip validation".

        Args:
            module_address: Target module address.
            module_port: Target module port.
            llm_format: Return the LLM-friendly schema format.

        Returns:
            The config-setup JSON schema, or ``{}`` when unavailable.
        """
        logger.debug(
            "get_module_config_schema not implemented for %s:%d (llm_format=%s); content validation skipped",
            module_address,
            module_port,
            llm_format,
        )
        return {}

    @abstractmethod
    def call_module(
        self,
        module_address: str,
        module_port: int,
        input_data: dict | Any,
        setup_id: str,
        mission_id: str,
        callback: Callable[[Any], Awaitable[None]] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> AsyncGenerator[Any, None]:
        """Call a remote module via its GatewayService and stream outputs.

        Opens a dial-back BiDi against the target's gateway (`StartStream` +
        `Stream`). Filters ``stream.start``; stops on ``stream.end``.

        Args:
            module_address: Target module's gateway host.
            module_port: Target module's gateway port.
            input_data: First input delivered to the remote module
                (typically wrapped in ``{"root": {...}}``).
            setup_id: Setup configuration ID.
            mission_id: Mission context ID.
            callback: Optional async callback invoked with each output Struct.
            metadata: Optional gRPC metadata forwarded on StartStream (tenant /
                trace headers).

        Yields:
            ``google.protobuf.Struct`` per remote module output.
        """
        ...
