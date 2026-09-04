"""Background module manager with single instance.

Supports optional Redis Streams for durable output persistence.
When a ``RedisClient`` is provided, output is written to both
the in-memory queue (for local consumers) and a Redis Stream
(for crash recovery and reconnection via ``from_seq``).
"""

from __future__ import annotations

import asyncio
import uuid
from typing import TYPE_CHECKING, Any

import grpc

from digitalkin.core.common import ModuleFactory
from digitalkin.core.job_manager.base_job_manager import BaseJobManager
from digitalkin.core.profiling.step_timer import StepTimer
from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.logger import logger
from digitalkin.models.core.job_manager_models import BackpressureStrategy
from digitalkin.models.module.base_types import DataModel, InputModelT, OutputModelT, SetupModelT
from digitalkin.models.module.module import ModuleCodeModel
from digitalkin.models.settings.task_manager import get_job_manager_settings
from digitalkin.services.task_manager.redis_task_manager import RedisTaskManager

if TYPE_CHECKING:
    from collections.abc import Callable

    from digitalkin.core.task_manager.redis.redis_client import RedisClient
    from digitalkin.models.services.services import ServicesMode
    from digitalkin.modules._base_module import BaseModule


class SingleJobManager(BaseJobManager[InputModelT, OutputModelT, SetupModelT]):
    """Manages a single instance of a module job.

    This class ensures that only one instance of a module job is active at a time.
    It provides functionality to create, stop, and monitor module jobs, as well as
    to handle their output data.

    When ``redis_client`` is provided, output is dual-written to both the
    in-memory queue and a Redis Stream for crash recovery and reconnection.
    """

    # Defaults safe when __init__ is bypassed (e.g., object.__new__ in tests).
    _redis_client: RedisClient

    def __init__(
        self,
        module_class: type[BaseModule],
        services_mode: ServicesMode,
        redis_client: RedisClient,
        default_timeout: float = 300.0,
    ) -> None:
        """Initialize the job manager.

        Concurrency / backpressure / setup-timeout come from
        ``JobManagerSettings`` and ``TaskManagerSettings``.

        Args:
            module_class: The class of the module to be managed.
            services_mode: The mode of operation for the services (e.g., ASYNC or SYNC).
            default_timeout: Default timeout for task operations.
            redis_client: Redis client for signal delivery and stream persistence.
        """
        task_manager = LocalTaskManager(default_timeout)
        super().__init__(module_class, services_mode, task_manager)

        self._lock = asyncio.Lock()
        self._redis_client = redis_client
        # task-id-stateless; safe to share across preload_instance calls.
        self._redis_task_manager = RedisTaskManager(self._redis_client)

    async def start(self) -> None:
        """Start manager (no-op, no external connections needed)."""

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
            timeout = get_job_manager_settings().config_setup_timeout
            return await asyncio.wait_for(session.queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            timeout = get_job_manager_settings().config_setup_timeout
            logger.error("Timeout waiting for config setup response from module %s", job_id)
            return ModuleCodeModel(
                code=str(grpc.StatusCode.DEADLINE_EXCEEDED),
                message=f"Module {job_id} did not respond within {timeout} seconds",
            )
        finally:
            self.tasks_sessions.pop(job_id, None)
            try:
                await session.cleanup()
            except Exception:
                logger.exception("Config setup session cleanup failed", extra={"job_id": job_id})

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
            Exception: If the module fails to start.
        """
        job_id = str(uuid.uuid4())
        module = ModuleFactory.create_module_instance(
            self.module_class, job_id, mission_id, setup_id, setup_version_id, request_metadata=request_metadata
        )
        self.tasks_sessions[job_id] = TaskSession(job_id, mission_id, module)

        try:
            await module.start_config_setup(
                config_setup_data,
                await self.job_specific_callback(self.add_to_queue, job_id),
            )
            logger.debug("Module %s (%s) started successfully", job_id, module.name)
        except Exception:
            session = self.tasks_sessions.pop(job_id, None)
            if session is not None:
                try:
                    await session.cleanup()
                except Exception:
                    logger.debug("Session cleanup failed during error handling", exc_info=True)
            logger.exception("Failed to start module", extra={"job_id": job_id})
            raise
        else:
            return job_id

    async def add_to_queue(self, job_id: str, output_data: DataModel | ModuleCodeModel) -> None:
        """Add output data to the queue for a specific job.

        Behavior depends on the configured backpressure strategy:
        - BLOCK: await with timeout, raise TimeoutError if queue stays full.
        - DROP_OLDEST: wait briefly, then drop oldest message to make room.
        - REJECT: attempt non-blocking put, discard new message if full.

        Rejects writes after stream is closed to prevent message loss.

        Args:
            job_id: The unique identifier of the job.
            output_data: The output data produced by the job.

        Raises:
            asyncio.TimeoutError: When using BLOCK strategy and the queue remains full past the timeout.
        """
        session = self.tasks_sessions.get(job_id)
        if session is None:
            logger.debug("Queue write rejected - session not found", extra={"job_id": job_id})
            return

        data = output_data.model_dump(mode="json")

        # Lock guards only the session validity check; queue.put() runs outside.
        async with session._write_lock:  # noqa: SLF001
            if self.tasks_sessions.get(job_id) is None:
                logger.debug("Queue write rejected - session removed during lock wait", extra={"job_id": job_id})
                return
            if session.stream_closed:
                logger.debug("Queue write rejected - stream closed", extra={"job_id": job_id})
                return

        logger.debug("debug:add_to_queue job_id=%s queue_depth=%s", job_id, session.queue.qsize())

        jm_settings = get_job_manager_settings()
        strategy = jm_settings.backpressure_strategy
        if strategy == BackpressureStrategy.BLOCK:
            await asyncio.wait_for(session.queue.put(data), timeout=jm_settings.backpressure_timeout)
        elif strategy == BackpressureStrategy.DROP_OLDEST:
            try:
                await asyncio.wait_for(session.queue.put(data), timeout=5.0)
            except asyncio.TimeoutError:
                try:
                    dropped = session.queue.get_nowait()
                    session.queue.task_done()
                    logger.warning(
                        "Queue full, DROPPED oldest message (content=%s)",
                        repr(dropped)[:2048],
                        extra={"job_id": job_id},
                    )
                except asyncio.QueueEmpty:
                    pass
                session.queue.put_nowait(data)
        elif strategy == BackpressureStrategy.REJECT:
            try:
                session.queue.put_nowait(data)
            except asyncio.QueueFull:
                logger.warning(
                    "Queue full, REJECTED new message (content=%s)",
                    repr(data)[:2048],
                    extra={"job_id": job_id},
                )

    async def preload_instance(
        self,
        setup_data: SetupModelT,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        request_metadata: dict[str, str] | None = None,
        job_id: str | None = None,
        tool_cache: Any = None,
        callback: Callable | None = None,
        setup: Any = None,
        invalidate_setup: Callable[[], None] | None = None,
    ) -> tuple[Any, str, Callable]:
        """Build a module instance and run its idempotent ``prepare()``.

        Lets the orchestrator pay init costs in parallel with the
        consumer's first reply.

        Args:
            setup_data: Setup configuration.
            mission_id: Mission ID.
            setup_id: Setup ID.
            setup_version_id: Setup version ID.
            request_metadata: gRPC request headers.
            job_id: Optional externally-provided job ID.
            tool_cache: Pre-resolved ToolCache.
            callback: Direct output callback; ``None`` wires the in-memory queue.
            setup: Borrowed SetupStrategy (servicer's shared instance); wired
                before ``prepare()`` so ``initialize()`` can build setup toolkits.
            invalidate_setup: Callback clearing the servicer's setup cache after
                an agent-driven setup edit; installed on ``context.callbacks``.

        Returns:
            ``(module, job_id, callback)``.
        """
        timer = StepTimer()
        job_id = job_id or str(uuid.uuid4())
        module = ModuleFactory.create_module_instance(
            self.module_class,
            job_id,
            mission_id,
            setup_id,
            setup_version_id,
            request_metadata=request_metadata,
            tool_cache=tool_cache,
        )
        timer.mark("factory_create")

        module.context.task_manager = self._redis_task_manager
        # Borrowed services must be wired before prepare(): initialize() runs
        # inside prepare() and is where modules build their toolkits.
        if setup is not None:
            module.context.setup = setup
        if invalidate_setup is not None:
            module.context.callbacks.invalidate_setup = invalidate_setup
        timer.mark("redis_task_manager")

        if callback is None:
            callback = await self.job_specific_callback(self.add_to_queue, job_id)
            timer.mark("default_callback")

        await module.prepare(setup_data, callback)
        timer.mark("prepare")
        timer.log("preload_instance", task_id=job_id)
        return module, job_id, callback

    async def run_instance(
        self,
        module: Any,
        job_id: str,
        mission_id: str,
        input_data: InputModelT,
        setup_data: SetupModelT,
        callback: Callable,
    ) -> str:
        """Run a pre-prepared module instance with input.

        ``module`` must come from :meth:`preload_instance`. Schedules
        the run in the task manager and returns the job_id.

        Args:
            module: Pre-prepared module instance.
            job_id: Job/task ID assigned by ``preload_instance``.
            mission_id: Mission ID for task manager scoping.
            input_data: The first input (the query) to feed ``run()``.
            setup_data: The setup the instance was prepared with.
            callback: Output callback (already attached to context).

        Returns:
            The ``job_id`` (echoed for caller convenience).
        """
        timer = StepTimer()
        await self.create_task(
            job_id,
            mission_id,
            module,
            module.start(input_data, setup_data, callback, done_callback=None),
        )
        timer.mark("create_task")
        timer.log("run_instance", task_id=job_id)
        logger.debug("Managed task started: '%s'", job_id, extra={"task_id": job_id})
        return job_id

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
            if session.module is not None
        }
