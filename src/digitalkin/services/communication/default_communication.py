"""Default communication implementation (local, for testing)."""

from collections.abc import AsyncGenerator, Awaitable, Callable
from typing import Any

from digitalkin.logger import logger
from digitalkin.services.communication.communication_strategy import CommunicationStrategy


class DefaultCommunication(CommunicationStrategy):
    """Default communication strategy (local implementation).

    This implementation is primarily for testing and development.
    For production, use GrpcCommunication to connect to remote modules.
    """

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
    ) -> None:
        """Initialize the default communication service.

        Args:
            mission_id: Mission identifier
            setup_id: Setup identifier
            setup_version_id: Setup version identifier
        """
        super().__init__(mission_id, setup_id, setup_version_id)
        logger.debug("Initialized DefaultCommunication (local)")

    async def get_module_schemas(  # Default stub implementation; self available for subclass overrides # noqa: PLR6301
        self,
        module_address: str,
        module_port: int,
        *,
        llm_format: bool = False,
    ) -> dict[str, dict]:
        """Get module schemas (local implementation returns empty schemas).

        Args:
            module_address: Target module address
            module_port: Target module port
            llm_format: Return LLM-friendly format

        Returns:
            Empty schemas dictionary
        """
        logger.debug(
            "DefaultCommunication.get_module_schemas called (returns empty)",
            extra={
                "module_address": module_address,
                "module_port": module_port,
                "llm_format": llm_format,
            },
        )
        return {
            "input": {},
            "output": {},
            "setup": {},
            "secret": {},
        }

    async def call_module(  # Default stub: no-op for local mode  # noqa: PLR6301
        self,
        module_address: str,
        module_port: int,
        input_data: dict | Any,  # noqa: ARG002
        setup_id: str,
        mission_id: str,
        callback: Callable[[Any], Awaitable[None]] | None = None,  # noqa: ARG002
        metadata: dict[str, str] | None = None,  # noqa: ARG002
    ) -> AsyncGenerator[Any, None]:
        """No-op stub for local-mode tests. Yields nothing.

        Use :class:`GrpcCommunication` for real M2M calls through the
        target module's GatewayService dial-back BiDi.

        Args:
            module_address: Ignored.
            module_port: Ignored.
            input_data: Ignored.
            setup_id: Ignored.
            mission_id: Ignored.
            callback: Ignored.
            metadata: Ignored.

        Yields:
            Nothing.
        """
        logger.debug(
            "DefaultCommunication.call_module is a local-mode no-op",
            extra={
                "module_address": module_address,
                "module_port": module_port,
                "setup_id": setup_id,
                "mission_id": mission_id,
            },
        )
        if False:
            yield None

    async def close(self) -> None:
        """No-op for local communication."""
