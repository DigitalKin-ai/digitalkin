"""Background module manager with single instance."""

import asyncio
import datetime
import uuid
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import grpc

from digitalkin.core.common import ConnectionFactory, ModuleFactory
from digitalkin.core.job_manager.base_job_manager import BaseJobManager
from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.logger import logger
from digitalkin.models.core.task_monitor import TaskStatus
from digitalkin.models.module.module import ModuleCodeModel
from digitalkin.models.module.module_types import InputModelT, OutputModelT, SetupModelT
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_models import ServicesMode


class SingleJobManager(BaseJobManager[InputModelT, OutputModelT, SetupModelT]):
    """Manages a single instance of a module job.

    This class ensures that only one instance of a module job is active at a time.
    It provides functionality to create, stop, and monitor module jobs, as well as
    to handle their output data.
    """

    async def start(self) -> None:
        """Start manager."""
        self.channel = await ConnectionFactory.create_surreal_connection("task_manager", datetime.timedelta(seconds=5))

    def __init__(
        self,
        module_class: type[BaseModule],
        services_mode: ServicesMode,
        default_timeout: float = 10.0,
        max_concurrent_tasks: int = 100,
    ) -> None:
        """Initialize the job manager.

        Args:
            module_class: The class of the module to be managed.
            services_mode: The mode of operation for the services (e.g., ASYNC or SYNC).
            default_timeout: Default timeout for task operations
            max_concurrent_tasks: Maximum number of concurrent tasks
        """
        # Create local task manager for same-process execution
        task_manager = LocalTaskManager(default_timeout, max_concurrent_tasks)

        # Initialize base job manager with task manager
        super().__init__(module_class, services_mode, task_manager)

        self._lock = asyncio.Lock()

    async def generate_config_setup_module_response(self, job_id: str) -> SetupModelT | ModuleCodeModel:
        """Generate a stream consumer for a module's output data.

        This method creates an asynchronous generator that streams output data
        from a specific module job. If the module does not exist, it generates
        an error message.

        Args:
            job_id: The unique identifier of the job.

        Returns:
            SetupModelT | ModuleCodeModel: the SetupModelT object fully processed.
        """
        if (session := self.tasks_sessions.get(job_id, None)) is None:
            return ModuleCodeModel(
                code=str(grpc.StatusCode.NOT_FOUND),
                message=f"Module {job_id} not found",
            )

        logger.debug("Module %s found: %s", job_id, session.module)
        try:
            # Add timeout to prevent indefinite blocking
            return await asyncio.wait_for(session.queue.get(), timeout=30.0)
        except asyncio.TimeoutError:
            logger.error("Timeout waiting for config setup response from module %s", job_id)
            return ModuleCodeModel(
                code=str(grpc.StatusCode.DEADLINE_EXCEEDED),
                message=f"Module {job_id} did not respond within 30 seconds",
            )
        finally:
            logger.info(f"{job_id=}: {session.queue.empty()}")

    async def create_config_setup_instance_job(
        self,
        config_setup_data: SetupModelT,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
    ) -> str:
        """Create and start a new module setup configuration job.

        This method initializes a new module job, assigns it a unique job ID,
        and starts the config setup it in the background.

        Args:
            config_setup_data: The input data required to start the job.
            mission_id: The mission ID associated with the job.
            setup_id: The setup ID associated with the module.
            setup_version_id: The setup ID.

        Returns:
            str: The unique identifier (job ID) of the created job.

        Raises:
            Exception: If the module fails to start.
        """
        job_id = str(uuid.uuid4())
        # TODO: Ensure the job_id is unique.
        module = ModuleFactory.create_module_instance(self.module_class, job_id, mission_id, setup_id, setup_version_id)
        self.tasks_sessions[job_id] = TaskSession(job_id, mission_id, self.channel, module)

        try:
            await module.start_config_setup(
                config_setup_data,
                await self.job_specific_callback(self.add_to_queue, job_id),
            )
            logger.debug("Module %s (%s) started successfully", job_id, module.name)
        except Exception:
            # Remove the module from the manager in case of an error.
            del self.tasks_sessions[job_id]
            logger.exception("Failed to start module %s: %s", job_id)
            raise
        else:
            return job_id

    async def add_to_queue(self, job_id: str, output_data: OutputModelT | ModuleCodeModel) -> None:
        """Add output data to the queue for a specific job.

        This method is used as a callback to handle output data generated by a module job.

        Args:
            job_id: The unique identifier of the job.
            output_data: The output data produced by the job.
        """
        await self.tasks_sessions[job_id].queue.put(output_data.model_dump())

    @asynccontextmanager  # type: ignore
    async def generate_stream_consumer(self, job_id: str) -> AsyncIterator[AsyncGenerator[dict[str, Any], None]]:  # type: ignore
        """Generate a stream consumer for a module's output data.

        This method creates an asynchronous generator that streams output data
        from a specific module job. If the module does not exist, it generates
        an error message.

        Args:
            job_id: The unique identifier of the job.

        Yields:
            AsyncGenerator: A stream of output data or error messages.
        """
        if (session := self.tasks_sessions.get(job_id, None)) is None:

            async def _error_gen() -> AsyncGenerator[dict[str, Any], None]:  # noqa: RUF029
                """Generate an error message for a non-existent module.

                Yields:
                    AsyncGenerator: A generator yielding an error message.
                """
                yield {
                    "error": {
                        "error_message": f"Module {job_id} not found",
                        "code": grpc.StatusCode.NOT_FOUND,
                    }
                }

            yield _error_gen()
            return

        logger.debug("Session: %s with Module %s", job_id, session.module)

        async def _stream() -> AsyncGenerator[dict[str, Any], Any]:
            """Stream output data from the module with simple blocking pattern.

            This implementation uses a simple one-item-at-a-time pattern optimized
            for local execution where we have direct access to session status:
            1. Block waiting for each item
            2. Check termination conditions after each item
            3. Clean shutdown when task completes

            This pattern provides:
            - Immediate termination when task completes
            - Direct session status monitoring
            - Simple, predictable behavior for local tasks

            Yields:
                dict: Output data generated by the module.
            """
            while True:
                # Block for next item - if queue is empty but producer not finished yet
                msg = await session.queue.get()
                try:
                    yield msg
                finally:
                    # Always mark task as done, even if consumer raises exception
                    session.queue.task_done()

                # Check termination conditions after each message
                # This allows immediate shutdown when the task completes
                if (
                    session.is_cancelled.is_set()
                    or (session.status is TaskStatus.COMPLETED and session.queue.empty())
                    or session.status is TaskStatus.FAILED
                ):
                    logger.debug(
                        "Stream ending for job %s: cancelled=%s, status=%s, queue_empty=%s",
                        job_id,
                        session.is_cancelled.is_set(),
                        session.status,
                        session.queue.empty(),
                    )
                    break

        yield _stream()

    async def create_module_instance_job(
        self,
        input_data: InputModelT,
        setup_data: SetupModelT,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
    ) -> str:
        """Create and start a new module job.

        This method initializes a new module job, assigns it a unique job ID,
        and starts it in the background.

        Args:
            input_data: The input data required to start the job.
            setup_data: The setup configuration for the module.
            mission_id: The mission ID associated with the job.
            setup_id: The setup ID associated with the module.
            setup_version_id: The setup Version ID associated with the module.

        Returns:
            str: The unique identifier (job ID) of the created job.

        Raises:
            Exception: If the module fails to start.
        """
        job_id = str(uuid.uuid4())
        module = ModuleFactory.create_module_instance(self.module_class, job_id, mission_id, setup_id, setup_version_id)
        callback = await self.job_specific_callback(self.add_to_queue, job_id)

        await self.create_task(
            job_id,
            mission_id,
            module,
            module.start(input_data, setup_data, callback, done_callback=None),
        )
        logger.info("Managed task started: '%s'", job_id, extra={"task_id": job_id})
        return job_id

    async def stop_module(self, job_id: str) -> bool:
        """Stop a running module job.

        Args:
            job_id: The unique identifier of the job to stop.

        Returns:
            bool: True if the module was successfully stopped, False if it does not exist.

        Raises:
            Exception: If an error occurs while stopping the module.
        """
        logger.info(f"STOP required for {job_id=}")

        async with self._lock:
            session = self.tasks_sessions.get(job_id)

            if not session:
                logger.warning(f"session with id: {job_id} not found")
                return False
            try:
                await session.module.stop()
                await self.cancel_task(job_id, session.mission_id)
                logger.debug(f"session {job_id} ({session.module.name}) stopped successfully")
            except Exception as e:
                logger.error(f"Error while stopping module {job_id}: {e}")
                raise
            else:
                return True

    async def get_module_status(self, job_id: str) -> TaskStatus:
        """Retrieve the status of a module job.

        Args:
            job_id: The unique identifier of the job.

        Returns:
            ModuleStatus: The status of the module.
        """
        session = self.tasks_sessions.get(job_id, None)
        return session.status if session is not None else TaskStatus.FAILED

    async def wait_for_completion(self, job_id: str) -> None:
        """Wait for a task to complete by awaiting its asyncio.Task.

        Args:
            job_id: The unique identifier of the job to wait for.

        Raises:
            KeyError: If the job_id is not found in tasks.
        """
        if job_id not in self._task_manager.tasks:
            msg = f"Job {job_id} not found"
            raise KeyError(msg)
        await self._task_manager.tasks[job_id]

    async def stop_all_modules(self) -> None:
        """Stop all currently running module jobs.

        This method ensures that all active jobs are gracefully terminated
        and closes the SurrealDB connection.
        """
        # Snapshot job IDs while holding lock
        async with self._lock:
            job_ids = list(self.tasks_sessions.keys())

        # Release lock before calling stop_module (which has its own lock)
        if job_ids:
            stop_tasks = [self.stop_module(job_id) for job_id in job_ids]
            await asyncio.gather(*stop_tasks, return_exceptions=True)

        # Close SurrealDB connection after stopping all modules
        if hasattr(self, "channel"):
            try:
                await self.channel.close()
                logger.info("SingleJobManager: SurrealDB connection closed")
            except Exception as e:
                logger.warning("Failed to close SurrealDB connection: %s", e)

    async def list_modules(self) -> dict[str, dict[str, Any]]:
        """List all modules along with their statuses.

        Returns:
            dict[str, dict[str, Any]]: A dictionary containing information about all modules and their statuses.
        """
        return {
            job_id: {
                "name": session.module.name,
                "status": session.module.status,
                "class": session.module.__class__.__name__,
            }
            for job_id, session in self.tasks_sessions.items()
        }
