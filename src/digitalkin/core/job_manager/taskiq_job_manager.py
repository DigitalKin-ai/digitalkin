"""Taskiq job manager module."""

try:
    import taskiq  # Verify taskiq is installed before module loads

except ImportError:
    msg = "Install digitalkin[taskiq] to use this functionality\n$ uv pip install digitalkin[taskiq]."
    raise ImportError(msg)

import asyncio
import contextlib
import datetime
import json
import os
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from rstream import Consumer, ConsumerOffsetSpecification, MessageContext, OffsetType

from digitalkin.core.common import QueueFactory
from digitalkin.core.job_manager.base_job_manager import BaseJobManager
from digitalkin.core.job_manager.taskiq_broker import TASKIQ_BROKER, TaskiqBrokerConfig
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
    stream_consumer: Consumer
    stream_consumer_task: asyncio.Task[None]
    _reaper_task: asyncio.Task[None]

    @staticmethod
    async def _on_consumer_closed(reason: Any) -> None:
        """Log RStream consumer connection closure for diagnostics.

        Args:
            reason: Connection close reason from rstream.
        """
        logger.error("RStream consumer connection closed: %s", reason)

    @staticmethod
    def _define_consumer() -> Consumer:
        """Create RStream consumer with connection recovery and diagnostics.

        Returns:
            Consumer connected to RabbitMQ.
        """
        host: str = os.environ.get("RABBITMQ_RSTREAM_HOST", "localhost")
        port: str = os.environ.get("RABBITMQ_RSTREAM_PORT", "5552")
        username: str = os.environ.get("RABBITMQ_RSTREAM_USERNAME", "guest")
        password: str = os.environ.get("RABBITMQ_RSTREAM_PASSWORD", "guest")

        from digitalkin.core.job_manager.taskiq_broker import _rstream_ssl_context

        logger.info("RStream consumer connecting to %s:%s", host, port)
        return Consumer(
            host=host,
            port=int(port),
            username=username,
            password=password,
            ssl_context=_rstream_ssl_context(),
            connection_name="digitalkin_consumer",
            on_close_handler=TaskiqJobManager._on_consumer_closed,
        )

    async def _on_message(
        self,
        message: bytes,
        message_context: MessageContext,  # noqa: ARG002
    ) -> None:  # RStream callback signature
        """Internal callback: parse JSON and route to the correct job queue."""
        try:
            data = json.loads(message)
        except (json.JSONDecodeError, UnicodeDecodeError):
            logger.warning("RStream message decode failed (size=%d)", len(message))
            return
        job_id = data.get("job_id")
        if not job_id:
            return
        output_data = data.get("output_data")
        if queue := self.job_queues.get(job_id):
            await queue.put(output_data)

        # Bridge session status from RStream terminal markers
        session = self.tasks_sessions.get(job_id)
        if session is None or not isinstance(output_data, dict):
            return
        if "code" in output_data:
            if session.status not in {"cancelled", "failed"}:
                session.status = "failed"
                logger.info("Job %s marked failed from RStream error (code=%s)", job_id, output_data.get("code"))
        elif isinstance(output_data.get("root"), dict) and output_data["root"].get("protocol") == "end_of_stream":
            if session.status not in {"cancelled", "failed"}:
                session.status = "completed"
                logger.info("Job %s marked completed from RStream end_of_stream", job_id)
            session.close_stream()

    async def _run_consumer_with_restart(self) -> None:
        """Run the RStream consumer with automatic restart on failure.

        Raises:
            CancelledError: If the task is cancelled.
        """
        max_retries = int(os.environ.get("DIGITALKIN_RSTREAM_MAX_RETRIES", "10"))
        base_delay = 1.0
        max_delay = 60.0
        attempt = 0

        while True:
            try:
                await self.stream_consumer.run()
                break  # Normal exit (consumer closed gracefully)
            except asyncio.CancelledError:
                raise
            except Exception:
                attempt += 1
                if attempt > max_retries:
                    logger.exception("Stream consumer failed after %d retries, giving up", max_retries)
                    for session in list(self.tasks_sessions.values()):
                        if session.status == "pending":
                            session.status = "failed"
                            session.close_stream()
                    break
                delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                logger.exception(
                    "Stream consumer failed (attempt %d/%d), restarting in %.1fs", attempt, max_retries, delay
                )
                await asyncio.sleep(delay)
                # Reconnect
                self.stream_consumer = self._define_consumer()
                await self.stream_consumer.create_stream(
                    TaskiqBrokerConfig.STREAM,
                    exists_ok=True,
                    arguments={"max-length-bytes": TaskiqBrokerConfig.STREAM_RETENTION},
                )
                await self.stream_consumer.start()
                await self.stream_consumer.subscribe(
                    stream=TaskiqBrokerConfig.STREAM,
                    subscriber_name=f"""subscriber_{os.environ.get("SERVER_NAME", "module_servicer")}""",
                    callback=self._on_message,  # type: ignore[arg-type]
                    offset_specification=ConsumerOffsetSpecification(OffsetType.LAST),
                    initial_credit=int(os.environ.get("DIGITALKIN_RSTREAM_INITIAL_CREDIT", "50")),
                )
                logger.info("Stream consumer reconnected (attempt %d)", attempt)

    async def _reap_orphan_sessions(self) -> None:
        """Mark sessions stuck in pending beyond timeout as failed.

        Handles hard worker crashes where no EndOfStreamOutput arrives.
        """
        orphan_timeout = float(os.environ.get("DIGITALKIN_ORPHAN_SESSION_TIMEOUT", "600.0"))
        check_interval = float(os.environ.get("DIGITALKIN_ORPHAN_CHECK_INTERVAL", "60.0"))

        while True:
            try:
                await asyncio.sleep(check_interval)
            except asyncio.CancelledError:
                return
            now = datetime.datetime.now(datetime.timezone.utc)
            for task_id, session in list(self.tasks_sessions.items()):
                if session.status != "pending":
                    continue
                elapsed = (now - session.created_at).total_seconds()
                if elapsed > orphan_timeout:
                    logger.warning("Orphan session: %s (pending %.0fs)", task_id, elapsed)
                    session.status = "failed"
                    session.close_stream()
                    await self._task_manager._cleanup_task(task_id, session.mission_id)  # noqa: SLF001

    async def start(self) -> None:
        """Start the TaskiqJobManager (no-op for external connections)."""
        await self._start()

    async def _start(self) -> None:
        await TASKIQ_BROKER.startup()

        self.stream_consumer = self._define_consumer()

        await self.stream_consumer.create_stream(
            TaskiqBrokerConfig.STREAM,
            exists_ok=True,
            arguments={"max-length-bytes": TaskiqBrokerConfig.STREAM_RETENTION},
        )
        await self.stream_consumer.start()

        start_spec = ConsumerOffsetSpecification(OffsetType.LAST)
        # Higher initial_credit allows prefetching more messages from the broker,
        # reducing round-trip latency for high-throughput streaming.
        await self.stream_consumer.subscribe(
            stream=TaskiqBrokerConfig.STREAM,
            subscriber_name=f"""subscriber_{os.environ.get("SERVER_NAME", "module_servicer")}""",
            callback=self._on_message,  # type: ignore[arg-type]
            offset_specification=start_spec,
            initial_credit=int(os.environ.get("DIGITALKIN_RSTREAM_INITIAL_CREDIT", "50")),
        )

        self.stream_consumer_task = asyncio.create_task(
            self._run_consumer_with_restart(),
            name="stream_consumer_task",
        )

        self._reaper_task = asyncio.create_task(self._reap_orphan_sessions(), name="orphan_session_reaper")

    async def stop(self) -> None:
        """Stop the TaskiqJobManager, cancel workers, and clean up all resources."""
        # 1. Cancel reaper
        self._reaper_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._reaper_task

        # 2. Cancel all running modules (sends cancel signals to workers)
        await self.stop_all_modules()

        # 3. Clean remaining sessions (releases semaphore slots)
        for task_id in list(self.tasks_sessions.keys()):
            session = self.tasks_sessions.get(task_id)
            if session is not None:
                await self._task_manager._cleanup_task(task_id, session.mission_id)  # noqa: SLF001

        # 4. Close RStream consumer
        await self.stream_consumer.close()
        self.stream_consumer_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self.stream_consumer_task

        # 5. Clear job queues
        queue_count = len(self.job_queues)
        self.job_queues.clear()
        logger.info("TaskiqJobManager stopped: cleared %d queues", queue_count)

        # 6. Close producer and broker
        await TaskiqBrokerConfig.cleanup_global_resources()

    def __init__(
        self,
        module_class: type[BaseModule],
        services_mode: ServicesMode,
        default_timeout: float = 300.0,
        stream_timeout: float = float(os.environ.get("DIGITALKIN_RSTREAM_TIMEOUT", "30.0")),
    ) -> None:
        """Initialize the Taskiq job manager.

        Args:
            module_class: The class of the module to be managed
            services_mode: The mode of operation for the services
            default_timeout: Default timeout for task operations
            stream_timeout: Timeout for stream consumer operations
        """
        # Create remote task manager for distributed execution
        task_manager = RemoteTaskManager(default_timeout)

        # Initialize base job manager with task manager
        super().__init__(module_class, services_mode, task_manager)

        self.job_queues: dict[str, asyncio.Queue] = {}
        self.max_queue_size = int(os.environ.get("DIGITALKIN_RSTREAM_QUEUE_SIZE", "1000"))
        self.stream_timeout = stream_timeout
        self._config_setup_timeout = float(os.environ.get("DIGITALKIN_CONFIG_SETUP_TIMEOUT", "30.0"))
        logger.info(
            "TaskiqJobManager initialized (queue_size=%d, stream_timeout=%.1fs)",
            self.max_queue_size,
            self.stream_timeout,
        )

    async def generate_config_setup_module_response(self, job_id: str) -> SetupModelT:
        """Generate a stream consumer for a module's output data.

        Args:
            job_id: The unique identifier of the job.

        Returns:
            SetupModelT: the SetupModelT object fully processed.

        Raises:
            asyncio.TimeoutError: If waiting for the setup response times out.
        """
        if job_id not in self.job_queues:
            self.job_queues[job_id] = QueueFactory.create_bounded_queue(maxsize=self.max_queue_size)
        queue = self.job_queues[job_id]

        try:
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
            self.job_queues.pop(job_id, None)
            if (session := self.tasks_sessions.get(job_id)) is not None:
                await self._task_manager._cleanup_task(job_id, session.mission_id)  # noqa: SLF001

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
        registry_config = self.module_class.services_config_params.get("registry")

        running_task: AsyncTaskiqTask[Any] = await task.kiq(
            mission_id,
            setup_id,
            setup_version_id,
            self.module_class,
            self.services_mode,
            config_setup_data.model_dump(mode="json"),  # SetupModelT generic bound to BaseModel # type: ignore
            request_metadata,
            registry_config,
        )

        job_id = running_task.task_id

        # Pre-create queue to avoid message drop race
        self.job_queues[job_id] = QueueFactory.create_bounded_queue(maxsize=self.max_queue_size)

        try:
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
        except Exception:
            self.job_queues.pop(job_id, None)
            raise

        logger.info("Registered config task: %s", job_id)
        if os.environ.get("DIGITALKIN_TASKIQ_RESULT_BACKEND_URL"):
            result = await running_task.wait_result(timeout=10)
            logger.debug("Job %s config result: %s", job_id, result)
        return job_id

    @asynccontextmanager
    async def generate_stream_consumer(self, job_id: str) -> AsyncIterator[AsyncGenerator[dict[str, Any], None]]:  # noqa: C901, PLR0915
        """Generate a stream consumer for the RStream stream.

        Args:
            job_id: The job ID to filter messages.

        Yields:
            messages: The stream messages from the associated module.
        """
        if job_id not in self.job_queues:
            self.job_queues[job_id] = QueueFactory.create_bounded_queue(maxsize=self.max_queue_size)
        queue = self.job_queues[job_id]

        async def _stream() -> AsyncGenerator[dict[str, Any], Any]:  # noqa: C901
            """Generate the stream with batch-drain optimization.

            Yields:
                dict: generated object from the module
            """
            consecutive_timeouts = 0
            max_consecutive_timeouts = int(os.environ.get("DIGITALKIN_RSTREAM_MAX_TIMEOUTS", "10"))

            while True:
                # Block for first item with timeout to allow termination checks
                get_task = asyncio.create_task(queue.get())
                done, _ = await asyncio.wait([get_task], timeout=self.stream_timeout)

                if done:
                    consecutive_timeouts = 0
                    item = get_task.result()
                    queue.task_done()
                    yield item

                    # Drain all immediately available items (micro-batch optimization).
                    # Cap at min(qsize, 100) to bound memory per yield cycle.
                    drain_limit = min(queue.qsize(), 100)
                    for _ in range(drain_limit):
                        try:
                            item = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                        queue.task_done()
                        yield item
                    continue

                # Timeout — cancel pending get and check job status
                get_task.cancel()
                consecutive_timeouts += 1
                logger.warning(
                    "Stream consumer timeout for job %s (%d/%d), checking if job is still active",
                    job_id,
                    consecutive_timeouts,
                    max_consecutive_timeouts,
                )

                if consecutive_timeouts >= max_consecutive_timeouts:
                    logger.error(
                        "Job %s: max consecutive timeouts (%d) reached, ending stream",
                        job_id,
                        max_consecutive_timeouts,
                    )
                    break

                if job_id not in self.tasks_sessions:
                    logger.info("Job %s no longer registered, ending stream", job_id)
                    break

                session = self.tasks_sessions[job_id]
                if session.stream_closed:
                    logger.info("Job %s stream closed, draining queue and ending stream", job_id)
                    while not queue.empty():
                        item = queue.get_nowait()
                        queue.task_done()
                        yield item
                    break

                status = await self.get_module_status(job_id)
                if status in {"cancelled", "failed", "completed"}:
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

        # Forward registry config so the worker can initialize GrpcRegistry
        registry_config = self.module_class.services_config_params.get("registry")

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
            registry_config,
        )
        job_id = running_task.task_id

        # Pre-create queue to avoid message drop race
        self.job_queues[job_id] = QueueFactory.create_bounded_queue(maxsize=self.max_queue_size)

        try:
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
        except Exception:
            self.job_queues.pop(job_id, None)
            raise

        logger.info("Registered remote task: %s", job_id)
        if os.environ.get("DIGITALKIN_TASKIQ_RESULT_BACKEND_URL"):
            result = await running_task.wait_result(timeout=10)
            logger.debug("Job %s result: %s", job_id, result)
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

    async def wait_for_completion(self, job_id: str, max_wait: float = 600.0) -> None:
        """Wait for a task to complete via stream-closed event.

        Relies on ``_on_message`` setting ``_stream_closed`` when
        ``end_of_stream`` arrives from RStream. Falls back to ``max_wait``
        timeout for crash scenarios.

        Args:
            job_id: The unique identifier of the job to wait for.
            max_wait: Maximum time in seconds to wait before giving up.

        Raises:
            asyncio.TimeoutError: If max_wait is exceeded.
        """
        session = self.tasks_sessions.get(job_id)
        if session is None:
            return
        try:
            await asyncio.wait_for(session._stream_closed.wait(), timeout=max_wait)  # noqa: SLF001
        except asyncio.TimeoutError:
            logger.error("Job %s: max wait time (%.1fs) exceeded", job_id, max_wait)
            raise
        logger.debug("Job %s: stream closed, completion detected (status=%s)", job_id, session.status)

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
