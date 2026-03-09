"""Taskiq job manager module."""

try:
    import taskiq  # Verify taskiq is installed before module loads

except ImportError:
    msg = "Install digitalkin[taskiq] to use this functionality\n$ uv pip install digitalkin[taskiq]."
    raise ImportError(msg)

import asyncio
import contextlib
import json
import os
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from rstream import Consumer, ConsumerOffsetSpecification, MessageContext, OffsetType

from digitalkin.core.common import QueueFactory
from digitalkin.core.job_manager.base_job_manager import BaseJobManager
from digitalkin.core.job_manager.taskiq_broker import STREAM, STREAM_RETENTION, TASKIQ_BROKER, cleanup_global_resources
from digitalkin.core.task_manager.remote_task_manager import RemoteTaskManager
from digitalkin.logger import logger
from digitalkin.models.module.module_types import InputModelT, OutputModelT, SetupModelT
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_models import ServicesMode

if __debug__:
    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from taskiq.task import AsyncTaskiqTask


class TaskiqJobManager(BaseJobManager[InputModelT, OutputModelT, SetupModelT]):
    """Taskiq job manager for running modules in Taskiq tasks."""

    services_mode: ServicesMode

    @staticmethod
    def _define_consumer() -> Consumer:
        """Get from the env the connection parameter to RabbitMQ.

        Returns:
            Consumer
        """
        host: str = os.environ.get("RABBITMQ_RSTREAM_HOST", "localhost")
        port: str = os.environ.get("RABBITMQ_RSTREAM_PORT", "5552")
        username: str = os.environ.get("RABBITMQ_RSTREAM_USERNAME", "guest")
        password: str = os.environ.get("RABBITMQ_RSTREAM_PASSWORD", "guest")

        logger.info("Connection to RabbitMQ: %s:%s.", host, port)
        return Consumer(host=host, port=int(port), username=username, password=password)

    async def _on_message(
        self,
        message: bytes,
        message_context: MessageContext,  # noqa: ARG002 #TODO
    ) -> None:  # RStream callback signature
        """Internal callback: parse JSON and route to the correct job queue."""
        try:
            data = json.loads(message.decode("utf-8"))
        except json.JSONDecodeError:
            return
        job_id = data.get("job_id")
        if not job_id:
            return
        if queue := self.job_queues.get(job_id):
            await queue.put(data.get("output_data"))

    async def start(self) -> None:
        """Start the TaskiqJobManager (no-op for external connections)."""
        await self._start()

    async def _start(self) -> None:
        await TASKIQ_BROKER.startup()

        self.stream_consumer = self._define_consumer()

        await self.stream_consumer.create_stream(
            STREAM,
            exists_ok=True,
            arguments={"max-length-bytes": STREAM_RETENTION},
        )
        await self.stream_consumer.start()

        start_spec = ConsumerOffsetSpecification(OffsetType.LAST)
        # on_message use bytes instead of AMQPMessage
        await self.stream_consumer.subscribe(
            stream=STREAM,
            subscriber_name=f"""subscriber_{os.environ.get("SERVER_NAME", "module_servicer")}""",
            callback=self._on_message,  # RStream expects (bytes, MessageContext) callback # type: ignore
            offset_specification=start_spec,
        )

        # Wrap the consumer task with error handling
        async def run_consumer_with_error_handling() -> None:
            try:
                await self.stream_consumer.run()
            except asyncio.CancelledError:
                logger.debug("Stream consumer task cancelled")
                raise
            except Exception as e:
                logger.error("Stream consumer task failed: %s", e, exc_info=True, extra={"error": str(e)})
                raise

        self.stream_consumer_task = asyncio.create_task(
            run_consumer_with_error_handling(),
            name="stream_consumer_task",
        )

    async def _stop(self) -> None:
        """Stop the TaskiqJobManager and clean up all resources."""
        # Signal the consumer to stop
        await self.stream_consumer.close()
        # Cancel the background task
        self.stream_consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.stream_consumer_task

        # Clean up job queues
        queue_count = len(self.job_queues)
        self.job_queues.clear()
        logger.info("TaskiqJobManager: Cleared %d job queues", queue_count)

        # Call global cleanup for producer and broker
        await cleanup_global_resources()

    def __init__(
        self,
        module_class: type[BaseModule],
        services_mode: ServicesMode,
        default_timeout: float = 300.0,
        stream_timeout: float = 30.0,
    ) -> None:
        """Initialize the Taskiq job manager.

        Args:
            module_class: The class of the module to be managed
            services_mode: The mode of operation for the services
            default_timeout: Default timeout for task operations
            stream_timeout: Timeout for stream consumer operations (default: 15.0s for distributed systems)
        """
        # Create remote task manager for distributed execution
        task_manager = RemoteTaskManager(default_timeout)

        # Initialize base job manager with task manager
        super().__init__(module_class, services_mode, task_manager)

        logger.warning("TaskiqJobManager initialized with app: %s", TASKIQ_BROKER)
        self.job_queues: dict[str, asyncio.Queue] = {}
        self.max_queue_size = 1000
        self.stream_timeout = stream_timeout
        self._config_setup_timeout = float(os.environ.get("DIGITALKIN_CONFIG_SETUP_TIMEOUT", "30.0"))

    async def generate_config_setup_module_response(self, job_id: str) -> SetupModelT:
        """Generate a stream consumer for a module's output data.

        Args:
            job_id: The unique identifier of the job.

        Returns:
            SetupModelT: the SetupModelT object fully processed.

        Raises:
            asyncio.TimeoutError: If waiting for the setup response times out.
        """
        queue = QueueFactory.create_bounded_queue(maxsize=self.max_queue_size)
        self.job_queues[job_id] = queue

        try:
            # Add timeout to prevent indefinite blocking
            item = await asyncio.wait_for(queue.get(), timeout=self._config_setup_timeout)
        except asyncio.TimeoutError:
            logger.error(
                "Timeout waiting for config setup response for job %s (%.1fs)", job_id, self._config_setup_timeout
            )
            raise
        else:
            queue.task_done()
            return item
        finally:
            logger.info("generate_config_setup_module_response: job_id=%s: %s", job_id, self.job_queues[job_id].empty())
            self.job_queues.pop(job_id, None)

    async def create_config_setup_instance_job(
        self,
        config_setup_data: SetupModelT,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        request_metadata: dict[str, str] | None = None,
    ) -> str:
        """Create and start a new module setup configuration job.

        Args:
            config_setup_data: The input data required to start the job.
            mission_id: The mission ID associated with the job.
            setup_id: The setup ID associated with the module.
            setup_version_id: The setup ID.
            request_metadata: gRPC request metadata (headers) to forward to the module.

        Returns:
            str: The unique identifier (job ID) of the created job.

        Raises:
            TypeError: If the function is called with bad data type.
            ValueError: If the module fails to start.
        """
        task = TASKIQ_BROKER.find_task("digitalkin.core.job_manager.taskiq_broker:run_config_module")

        if task is None:
            msg = "Task not found"
            raise ValueError(msg)

        if config_setup_data is None:
            msg = "config_setup_data must be a valid model with model_dump method"
            raise TypeError(msg)

        # Submit task to Taskiq
        running_task: AsyncTaskiqTask[Any] = await task.kiq(
            mission_id,
            setup_id,
            setup_version_id,
            self.module_class,
            self.services_mode,
            config_setup_data.model_dump(mode="json"),  # SetupModelT generic bound to BaseModel # type: ignore
            request_metadata,
        )

        job_id = running_task.task_id

        # Create module instance for metadata
        module = self.module_class(
            job_id,
            mission_id=mission_id,
            setup_id=setup_id,
            setup_version_id=setup_version_id,
            request_metadata=request_metadata,
        )

        # Register task in TaskManager (remote mode)
        async def _dummy_coro() -> None:
            """Dummy coroutine - actual execution happens in worker."""

        await self.create_task(
            job_id,
            mission_id,
            module,
            _dummy_coro(),
        )

        logger.info("Registered config task: %s, waiting for initial result", job_id)
        result = await running_task.wait_result(timeout=10)
        logger.info("Job %s with data %s", job_id, result)
        return job_id

    @asynccontextmanager
    async def generate_stream_consumer(self, job_id: str) -> AsyncIterator[AsyncGenerator[dict[str, Any], None]]:
        """Generate a stream consumer for the RStream stream.

        Args:
            job_id: The job ID to filter messages.

        Yields:
            messages: The stream messages from the associated module.
        """
        queue = QueueFactory.create_bounded_queue(maxsize=self.max_queue_size)
        self.job_queues[job_id] = queue

        async def _stream() -> AsyncGenerator[dict[str, Any], Any]:
            """Generate the stream with batch-drain optimization.

            Yields:
                dict: generated object from the module
            """
            while True:
                # Block for first item with timeout to allow termination checks
                get_task = asyncio.create_task(queue.get())
                done, _ = await asyncio.wait([get_task], timeout=self.stream_timeout)

                if done:
                    item = get_task.result()
                    queue.task_done()
                    yield item

                    # Drain all immediately available items (micro-batch optimization)
                    batch_count = 0
                    max_batch_size = 100  # Safety limit to prevent memory spikes
                    while batch_count < max_batch_size and not queue.empty():
                        item = queue.get_nowait()
                        queue.task_done()
                        yield item
                        batch_count += 1
                    continue

                # Timeout — cancel pending get and check job status
                get_task.cancel()
                logger.warning("Stream consumer timeout for job %s, checking if job is still active", job_id)

                if job_id not in self.tasks_sessions:
                    logger.info("Job %s no longer registered, ending stream", job_id)
                    break

                status = await self.get_module_status(job_id)

                if status in {"cancelled", "failed"}:
                    logger.info("Job %s has terminal status %s, draining queue and ending stream", job_id, status)

                    while not queue.empty():
                        item = queue.get_nowait()
                        queue.task_done()
                        yield item

                    break

        try:
            yield _stream()
        finally:
            self.job_queues.pop(job_id, None)

    async def create_module_instance_job(
        self,
        input_data: InputModelT,
        setup_data: SetupModelT,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        request_metadata: dict[str, str] | None = None,
    ) -> str:
        """Launches the module_task in Taskiq, returns the Taskiq task id as job_id.

        Args:
            input_data: Input data for the module
            setup_data: Setup data for the module
            mission_id: Mission ID for the module
            setup_id: The setup ID associated with the module.
            setup_version_id: The setup ID associated with the module.
            request_metadata: gRPC request metadata (headers) to forward to the module.

        Returns:
            job_id: The Taskiq task id.

        Raises:
            ValueError: If the task is not found.
        """
        task = TASKIQ_BROKER.find_task("digitalkin.core.job_manager.taskiq_broker:run_start_module")

        if task is None:
            msg = "Task not found"
            raise ValueError(msg)

        # Submit task to Taskiq
        running_task: AsyncTaskiqTask[Any] = await task.kiq(
            mission_id,
            setup_id,
            setup_version_id,
            self.module_class,
            self.services_mode,
            input_data.model_dump(mode="json"),
            setup_data.model_dump(mode="json"),
            request_metadata,
        )
        job_id = running_task.task_id

        # Create module instance for metadata
        module = self.module_class(
            job_id,
            mission_id=mission_id,
            setup_id=setup_id,
            setup_version_id=setup_version_id,
            request_metadata=request_metadata,
        )

        # Register task in TaskManager (remote mode)
        async def _dummy_coro() -> None:
            """Dummy coroutine - actual execution happens in worker."""

        await self.create_task(
            job_id,
            mission_id,
            module,
            _dummy_coro(),
        )

        logger.info("Registered remote task: %s, waiting for initial result", job_id)
        result = await running_task.wait_result(timeout=10)
        logger.debug("Job %s with data %s", job_id, result)
        return job_id

    async def get_module_status(self, job_id: str) -> str:
        """Get module status from local session.

        Args:
            job_id: The unique identifier of the job.

        Returns:
            Status string (e.g. "pending", "running", "completed", "failed", "cancelled").
        """
        session = self.tasks_sessions.get(job_id)
        if session is None:
            logger.warning("Job %s not found in registry", job_id)
            return "failed"
        return session.status

    async def wait_for_completion(self, job_id: str) -> None:
        """Wait for a task to complete by polling its status.

        Args:
            job_id: The unique identifier of the job to wait for.

        Raises:
            KeyError: If the job_id is not found in tasks_sessions.
        """
        if job_id not in self.tasks_sessions:
            msg = f"Job {job_id} not found"
            raise KeyError(msg)

        # Poll task status until terminal state
        terminal_states = {"completed", "failed", "cancelled"}
        while True:
            status = await self.get_module_status(job_id)
            if status in terminal_states:
                logger.debug("Job %s reached terminal state: %s", job_id, status)
                break
            await asyncio.sleep(0.5)  # Poll interval

    async def stop_module(self, job_id: str) -> bool:
        """Stop a running module using TaskManager.

        Args:
            job_id: The Taskiq task id to stop.

        Returns:
            bool: True if the signal was successfully sent, False otherwise.
        """
        if job_id not in self.tasks_sessions:
            logger.warning("Job %s not found in registry", job_id)
            return False

        try:
            session = self.tasks_sessions[job_id]
            # Use TaskManager's cancel_task method which handles signal sending
            await self.cancel_task(job_id, session.mission_id)
            logger.info("Cancel signal sent for job %s via TaskManager", job_id)

            # Clean up queue after cancellation
            self.job_queues.pop(job_id, None)
            logger.debug("Cleaned up queue for job %s", job_id)
        except Exception:
            logger.exception("Error stopping job %s", job_id)
            return False
        return True

    async def stop_all_modules(self) -> None:
        """Stop all running modules tracked in the registry."""
        stop_tasks = [self.stop_module(job_id) for job_id in list(self.tasks_sessions.keys())]
        if stop_tasks:
            results = await asyncio.gather(*stop_tasks, return_exceptions=True)
            logger.info("Stopped %d modules, results: %s", len(results), results)

    async def list_modules(self) -> dict[str, dict[str, Any]]:
        """List all modules tracked in the registry with their statuses.

        Returns:
            dict[str, dict[str, Any]]: A dictionary containing information about all tracked modules.
        """
        return {
            job_id: {
                "name": self.module_class.__name__,
                "status": session.status,
                "class": self.module_class.__name__,
                "mission_id": session.mission_id,
            }
            for job_id, session in self.tasks_sessions.items()
        }
