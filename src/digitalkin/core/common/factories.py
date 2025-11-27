"""Common factory functions for reducing code duplication in core module."""

import asyncio
import datetime

from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.logger import logger
from digitalkin.modules._base_module import BaseModule


class ConnectionFactory:
    """Factory for creating SurrealDB connections with consistent configuration."""

    @staticmethod
    async def create_surreal_connection(
        database: str = "task_manager",
        timeout: datetime.timedelta = datetime.timedelta(seconds=5),
        *,
        auto_init: bool = True,
    ) -> SurrealDBConnection:
        """Create and optionally initialize a SurrealDB connection.

        This factory method centralizes the creation of SurrealDB connections
        to ensure consistent configuration across the codebase.

        Args:
            database: Database name to connect to
            timeout: Connection timeout
            auto_init: Whether to automatically initialize the connection

        Returns:
            Initialized or uninitialized SurrealDBConnection instance

        Example:
            # Create and auto-initialize
            conn = await ConnectionFactory.create_surreal_connection("taskiq_worker")

            # Create without initialization
            conn = await ConnectionFactory.create_surreal_connection(auto_init=False)
            await conn.init_surreal_instance()
        """
        logger.debug(
            "Creating SurrealDB connection for database: %s, timeout: %s",
            database,
            timeout,
            extra={"database": database, "timeout": str(timeout)},
        )

        connection: SurrealDBConnection = SurrealDBConnection(database, timeout)

        if auto_init:
            await connection.init_surreal_instance()
            logger.debug("SurrealDB connection initialized for database: %s", database)

        return connection


class ModuleFactory:
    """Factory for creating module instances with consistent configuration."""

    @staticmethod
    def create_module_instance(
        module_class: type[BaseModule],
        job_id: str,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
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
            "Creating module instance: %s for job: %s",
            module_class.__name__,
            job_id,
            extra={
                "module_class": module_class.__name__,
                "job_id": job_id,
                "mission_id": mission_id,
                "setup_id": setup_id,
                "setup_version_id": setup_version_id,
            },
        )

        return module_class(
            job_id=job_id,
            mission_id=mission_id,
            setup_id=setup_id,
            setup_version_id=setup_version_id,
        )


class QueueFactory:
    """Factory for creating asyncio queues with consistent configuration."""

    # Default max queue size to prevent unbounded memory growth
    DEFAULT_MAX_QUEUE_SIZE = 1000

    @staticmethod
    def create_bounded_queue(maxsize: int = DEFAULT_MAX_QUEUE_SIZE) -> asyncio.Queue:
        """Create a bounded asyncio queue with standard configuration.

        Args:
            maxsize: Maximum queue size (default 1000, 0 means unlimited)

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
        if maxsize < 0:
            msg = "maxsize must be >= 0"
            raise ValueError(msg)

        logger.debug("Creating bounded queue with maxsize: %d", maxsize, extra={"maxsize": maxsize})
        return asyncio.Queue(maxsize=maxsize)
