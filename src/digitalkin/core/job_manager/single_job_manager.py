"""Background module manager with single instance."""

import asyncio
import datetime
import uuid
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

import grpc

from digitalkin.core.common import ConnectionFactory, ModuleFactory
from digitalkin.core.job_manager.base_job_manager import BaseJobManager
from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
from digitalkin.core.task_manager.task_session import TaskSession

if TYPE_CHECKING:
    from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.logger import logger
from digitalkin.models.core.task_monitor import TaskStatus
from digitalkin.models.module.base_types import DataModel, InputModelT, OutputModelT, SetupModelT
from digitalkin.models.module.module import ModuleCodeModel
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_models import ServicesMode


class SingleJobManager(BaseJobManager[InputModelT, OutputModelT, SetupModelT]):
    """Manages a single instance of a module job.

    This class ensures that only one instance of a module job is active at a time.
    It provides functionality to create, stop, and monitor module jobs, as well as
    to handle their output data.
    """

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
        self.channel: SurrealDBConnection | None = None

    async def start(self) -> None:
        """Start manager."""
        self.channel = await ConnectionFactory.create_surreal_connection("task_manager", datetime.timedelta(seconds=5))

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
            logger.debug(
                "Config setup response retrieved",
                extra={"job_id": job_id, "queue_empty": session.queue.empty()},
            )

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
            RuntimeError: If start() was not called before creating jobs.
            Exception: If the module fails to start.
        """
        job_id = str(uuid.uuid4())
        module = ModuleFactory.create_module_instance(self.module_class, job_id, mission_id, setup_id, setup_version_id)
        if self.channel is None:
            msg = "JobManager.start() must be called before creating jobs"
            raise RuntimeError(msg)
        self.tasks_sessions[job_id] = TaskSession(job_id, mission_id, self.channel, module, owns_connection=False)

        try:
            await module.start_config_setup(
                config_setup_data,
                await self.job_specific_callback(self.add_to_queue, job_id),
            )
            logger.debug("Module %s (%s) started successfully", job_id, module.name)
        except Exception:
            # Remove the module from the manager in case of an error.
            del self.tasks_sessions[job_id]
            logger.exception("Failed to start module", extra={"job_id": job_id})
            raise
        else:
            return job_id

    async def add_to_queue(self, job_id: str, output_data: DataModel | ModuleCodeModel) -> None:
        """Add output data to the queue for a specific job.

        Uses timeout-based backpressure: if the queue is full after 5s,
        drops the oldest message to make room for the new one.
        Rejects writes after stream is closed to prevent message loss.

        Args:
            job_id: The unique identifier of the job.
            output_data: The output data produced by the job.
        """
        session = self.tasks_sessions.get(job_id)
        if session is None:
            logger.warning("Queue write rejected - session not found", extra={"job_id": job_id})
            return

        if session.stream_closed:
            logger.debug("Queue write rejected - stream closed", extra={"job_id": job_id})
            return

        try:
            await asyncio.wait_for(session.queue.put(output_data.model_dump(mode="json")), timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Queue full, dropping oldest message", extra={"job_id": job_id})
            try:
                session.queue.get_nowait()
                session.queue.task_done()
            except asyncio.QueueEmpty:
                pass
            session.queue.put_nowait(output_data.model_dump(mode="json"))

    @asynccontextmanager
    async def generate_stream_consumer(self, job_id: str) -> AsyncIterator[AsyncGenerator[dict[str, Any], None]]:
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

            async def _error_gen() -> AsyncGenerator[  # noqa: RUF029
                dict[str, Any], None
            ]:  # Async generator type required by caller even though body uses yield
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
            """Stream output data from the module with bounded blocking.

            Uses a 1-second timeout on queue.get() to periodically re-check
            termination flags, preventing indefinite hangs when the task crashes
            without producing output.

            Termination behavior:
            - cancelled: abort immediately (abnormal, discard remaining)
            - stream_closed / COMPLETED / FAILED: drain remaining queue items, then exit

            Yields:
                dict: Output data generated by the module.
            """
            while True:
                if session.cancelled:
                    logger.debug("Stream cancelled for job %s", job_id)
                    break

                # If no more output will be produced, drain remaining items and exit
                if session.stream_closed or session.status in {TaskStatus.COMPLETED, TaskStatus.FAILED}:
                    while not session.queue.empty():
                        msg = session.queue.get_nowait()
                        try:
                            yield msg
                        finally:
                            session.queue.task_done()
                    logger.debug(
                        "Stream drained for job %s: status=%s, stream_closed=%s",
                        job_id,
                        session.status,
                        session.stream_closed,
                    )
                    break

                try:
                    msg = await asyncio.wait_for(session.queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue

                try:
                    yield msg
                finally:
                    session.queue.task_done()

                if session.cancelled:
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
            module.start(input_data, setup_data, callback, done_callback=None),  # type: ignore[arg-type]
            shared_connection=self.channel,
        )
        logger.info("Managed task started: '%s'", job_id, extra={"task_id": job_id})
        return job_id

    async def clean_session(self, task_id: str, mission_id: str) -> bool:
        """Clean a task's session.

        Args:
            task_id: Unique identifier for the task.
            mission_id: Mission identifier.

        Returns:
            bool: True if the task was successfully cleaned, False otherwise.
        """
        return await self._task_manager.clean_session(task_id, mission_id)

    async def stop_module(self, job_id: str) -> bool:
        """Stop a running module job.

        Args:
            job_id: The unique identifier of the job to stop.

        Returns:
            bool: True if the module was successfully stopped, False if it does not exist.

        Raises:
            Exception: If an error occurs while stopping the module.
        """
        logger.info("Stop module requested", extra={"job_id": job_id})

        async with self._lock:
            session = self.tasks_sessions.get(job_id)

            if not session:
                logger.warning("Session not found", extra={"job_id": job_id})
                return False
            try:
                await session.module.stop()
                await self.cancel_task(job_id, session.mission_id)
                logger.debug(
                    "Module stopped successfully",
                    extra={"job_id": job_id, "mission_id": session.mission_id},
                )
            except Exception:
                logger.exception("Error stopping module", extra={"job_id": job_id})
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
        if self.channel is not None:
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
