"""Default communication implementation (local, for testing)."""

from collections.abc import AsyncGenerator, Awaitable, Callable

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
        """Initialize with local communication (no remote connections)."""
        super().__init__(mission_id, setup_id, setup_version_id)
        logger.debug("Initialized DefaultCommunication (local)")

    # ══════════════════════════════════ Public Methods ══════════════════════════════════ #

    async def get_module_schemas(
        self,
        module_address: str,
        module_port: int,
        *,
        llm_format: bool = False,
    ) -> dict[str, dict]:
        """Return empty schemas (local stub)."""
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

    async def call_module(
        self,
        module_address: str,
        module_port: int,
        _input_data: dict,  # Strategy interface parameter, not used in local stub
        setup_id: str,
        mission_id: str,
        callback: Callable[[dict], Awaitable[None]] | None = None,
        _metadata: dict[str, str] | None = None,
    ) -> AsyncGenerator[dict, None]:
        """Call module (local implementation yields empty response).

        Args:
            module_address: Target module address
            module_port: Target module port
            _input_data: Input data (unused in local stub)
            setup_id: Setup ID
            mission_id: Mission ID
            callback: Optional callback
            _metadata: Optional gRPC metadata (unused in local stub).

        Yields:
            Empty response dictionary
        """
        logger.debug(
            "DefaultCommunication.call_module called (returns empty)",
            extra={
                "module_address": module_address,
                "module_port": module_port,
                "setup_id": setup_id,
                "mission_id": mission_id,
            },
        )

        # Yield empty response
        response = {"status": "error", "message": "Local communication not implemented"}
        if callback:
            await callback(response)
        yield response
        return  # Explicit return for async generator

    async def close(self) -> None:
        """No-op for local communication."""
