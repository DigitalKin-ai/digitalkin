"""Common factory functions for reducing code duplication in core module."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from digitalkin.logger import logger
from digitalkin.models.settings.queue import get_queue_settings

if TYPE_CHECKING:
    from digitalkin.models.module.tool_cache import ToolCache
    from digitalkin.modules._base_module import BaseModule


class ModuleFactory:
    """Factory for creating module instances with consistent configuration."""

    @staticmethod
    def create_module_instance(
        module_class: type[BaseModule],
        job_id: str,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        request_metadata: dict[str, str] | None = None,
        tool_cache: ToolCache | None = None,
    ) -> BaseModule:
        """Create a module instance with standard parameters.

        This factory method centralizes module instantiation to ensure
        consistent parameter passing across the codebase.

        Args:
            module_class: The module class to instantiate
            job_id: Unique job identifier
            mission_id: Mission identifier
            setup_id: Setup identifier
            setup_version_id: Setup version identifier
            request_metadata: gRPC request metadata (headers) to forward to the module.
            tool_cache: Pre-resolved ToolCache to inject on the module instance.

        Returns:
            Instantiated module

        Raises:
            ValueError: If job_id or mission_id is empty

        Example:
            module = ModuleFactory.create_module_instance(
                MyModule,
                job_id="job_123",
                mission_id="mission:test",
                setup_id="setup:config",
                setup_version_id="v1.0",
            )
        """
        # Validate parameters
        if not job_id:
            msg = "job_id cannot be empty"
            raise ValueError(msg)
        if not mission_id:
            msg = "mission_id cannot be empty"
            raise ValueError(msg)

        logger.debug(
            "Creating module instance: %s (setup_version_id=%s)",
            module_class.__name__,
            setup_version_id,
            extra={"job_id": job_id, "mission_id": mission_id, "setup_id": setup_id},
        )

        return module_class(
            job_id=job_id,
            mission_id=mission_id,
            setup_id=setup_id,
            setup_version_id=setup_version_id,
            request_metadata=request_metadata,
            tool_cache=tool_cache,
        )


class QueueFactory:
    """Factory for creating asyncio queues with consistent configuration."""

    @staticmethod
    def create_bounded_queue(maxsize: int | None = None) -> asyncio.Queue:
        """Create a bounded asyncio queue with standard configuration.

        Args:
            maxsize: Maximum queue size. ``None`` uses QueueSettings.max_size
                (default 1000); 0 means unlimited.

        Returns:
            Bounded asyncio.Queue instance

        Raises:
            ValueError: If maxsize is negative

        Example:
            queue = QueueFactory.create_bounded_queue()
            # or with custom size
            queue = QueueFactory.create_bounded_queue(maxsize=500)
            # unlimited queue
            queue = QueueFactory.create_bounded_queue(maxsize=0)
        """
        if maxsize is None:
            maxsize = get_queue_settings().max_size
        if maxsize < 0:
            msg = "maxsize must be >= 0"
            raise ValueError(msg)

        logger.debug("Creating bounded queue with maxsize: %d", maxsize)
        return asyncio.Queue(maxsize=maxsize)
