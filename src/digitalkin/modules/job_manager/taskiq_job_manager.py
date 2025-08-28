"""Taskiq job manager module."""

try:
    import taskiq  # noqa: F401

except ImportError:
    msg = "Install digitalkin[taskiq] to use this functionality\n$ uv pip install digitalkin[taskiq]."
    raise ImportError(msg)

import asyncio
import contextlib
import json
import os
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Generic

from rstream import Consumer, ConsumerOffsetSpecification, MessageContext, OffsetType

from digitalkin.logger import logger
from digitalkin.models.module import InputModelT, SetupModelT
from digitalkin.models.module.module import ModuleStatus
from digitalkin.modules._base_module import BaseModule
from digitalkin.modules.job_manager.base_job_manager import BaseJobManager
from digitalkin.modules.job_manager.taskiq_broker import STREAM, STREAM_RETENTION, TASKIQ_BROKER
from digitalkin.services.services_models import ServicesMode

if TYPE_CHECKING:
    from taskiq.task import AsyncTaskiqTask


class TaskiqJobManager(BaseJobManager, Generic[InputModelT, SetupModelT]):
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

    async def _on_message(self, message: bytes, message_context: MessageContext) -> None:  # noqa: ARG002
        """Internal callback: parse JSON and route to the correct job queue."""
        try:
            data = json.loads(message.decode("utf-8"))
        except json.JSONDecodeError:
            return
        job_id = data.get("job_id")
        if not job_id:
            return
        queue = self.job_queues.get(job_id)
        if queue:
            await queue.put(data.get("output_data"))

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
            callback=self._on_message,  # type: ignore
            offset_specification=start_spec,
        )
        self.stream_consumer_task = asyncio.create_task(
            self.stream_consumer.run(),
            name="stream_consumer_task",
        )

    async def _stop(self) -> None:
        # Signal the consumer to stop
        await self.stream_consumer.close()
        # Cancel the background task
        self.stream_consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.stream_consumer_task

    def __init__(
        self,
        module_class: type[BaseModule],
        services_mode: ServicesMode,
    ) -> None:
        """Initialize the Taskiq job manager."""
        super().__init__(module_class, services_mode)

        logger.warning("TaskiqJobManager initialized with app: %s", TASKIQ_BROKER)
        self.services_mode = services_mode
        self.job_queues: dict[str, asyncio.Queue] = {}
        self.max_queue_size = 1000

    async def generate_config_setup_module_response(self, job_id: str) -> SetupModelT:
        """Generate a stream consumer for a module's output data.

        This method creates an asynchronous generator that streams output data
        from a specific module job. If the module does not exist, it generates
        an error message.

        Args:
            job_id: The unique identifier of the job.

        Returns:
            SetupModelT: the SetupModelT object fully processed.
        """
        queue: asyncio.Queue = asyncio.Queue(maxsize=self.max_queue_size)
        self.job_queues[job_id] = queue

        try:
            item = await queue.get()
            queue.task_done()
            return item
        finally:
            logger.info(f"generate_config_setup_module_response: {job_id=}: {self.job_queues[job_id].empty()}")
            self.job_queues.pop(job_id, None)

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
            setup_data: The setup configuration for the module.
            mission_id: The mission ID associated with the job.
            setup_id: The setup ID associated with the module.
            setup_version_id: The setup ID.

        Returns:
            str: The unique identifier (job ID) of the created job.

        Raises:
            TypeError: If the function is called with bad data type.
            ValueError: If the module fails to start.
        """
        task = TASKIQ_BROKER.find_task("digitalkin.modules.job_manager.taskiq_broker:run_config_module")

        if task is None:
            msg = "Task not found"
            raise ValueError(msg)

        if config_setup_data is None:
            msg = "config_setup_data must be a valid model with model_dump method"
            raise TypeError(msg)

        running_task: AsyncTaskiqTask[Any] = await task.kiq(
            mission_id,
            setup_id,
            setup_version_id,
            self.module_class,
            self.services_mode,
            config_setup_data.model_dump(),  # type: ignore
        )

        job_id = running_task.task_id
        result = await running_task.wait_result(timeout=10)
        logger.info("Job %s with data %s", job_id, result)
        return job_id

    @asynccontextmanager  # type: ignore
    async def generate_stream_consumer(self, job_id: str) -> AsyncIterator[AsyncGenerator[dict[str, Any], None]]:  # type: ignore
        """Generate a stream consumer for the RStream stream.

        Args:
            job_id: The job ID to filter messages.

        Yields:
            messages: The stream messages from the associated module.
        """
        queue: asyncio.Queue = asyncio.Queue(maxsize=self.max_queue_size)
        self.job_queues[job_id] = queue

        async def _stream() -> AsyncGenerator[dict[str, Any], Any]:
            """Generate the stream allowing flowless communication.

            Yields:
                dict: generated object from the module
            """
            while True:
                item = await queue.get()
                queue.task_done()
                yield item

                while True:
                    try:
                        item = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    queue.task_done()
                    yield item

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
    ) -> str:
        """Launches the module_task in Taskiq, returns the Taskiq task id as job_id.

        Args:
            input_data: Input data for the module
            setup_data: Setup data for the module
            mission_id: Mission ID for the module
            setup_id: The setup ID associated with the module.
            setup_version_id: The setup ID associated with the module.

        Returns:
            job_id: The Taskiq task id.

        Raises:
            ValueError: If the task is not found.
        """
        task = TASKIQ_BROKER.find_task("digitalkin.modules.job_manager.taskiq_broker:run_start_module")

        if task is None:
            msg = "Task not found"
            raise ValueError(msg)

        running_task: AsyncTaskiqTask[Any] = await task.kiq(
            mission_id,
            setup_id,
            setup_version_id,
            self.module_class,
            self.services_mode,
            input_data.model_dump(),
            setup_data.model_dump(),
        )
        job_id = running_task.task_id
        result = await running_task.wait_result(timeout=10)
        logger.debug("Job %s with data %s", job_id, result)
        return job_id

    async def stop_module(self, job_id: str) -> bool:
        """Revoke (terminate) the Taskiq task with id.

        Args:
            job_id: The Taskiq task id to stop.

        Raises:
            bool: True if the task was successfully revoked, False otherwise.
        """
        msg = "stop_module not implemented in TaskiqJobManager"
        raise NotImplementedError(msg)

    async def stop_all_modules(self) -> None:
        """Stop all running modules."""
        msg = "stop_all_modules not implemented in TaskiqJobManager"
        raise NotImplementedError(msg)

    async def get_module_status(self, job_id: str) -> ModuleStatus | None:
        """Query a module status."""
        msg = "get_module_status not implemented in TaskiqJobManager"
        raise NotImplementedError(msg)

    async def list_modules(self) -> dict[str, dict[str, Any]]:
        """List all modules."""
        msg = "list_modules not implemented in TaskiqJobManager"
        raise NotImplementedError(msg)
