"""Redis-based task dispatcher — replaces gRPC loopback for module execution.

Listens on a Redis Stream for dispatch commands from the Gateway.
For each task: resolves setup, creates the module, writes output to
a proto stream. The Gateway reads output via ProtoStreamReader.

Runs in the same process as the Gateway (embedded mode) or standalone.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format, struct_pb2

from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter
from digitalkin.logger import logger

if TYPE_CHECKING:
    from digitalkin.core.task_manager.redis.redis_client import RedisClient
    from digitalkin.grpc_servers.module_servicer import ModuleServicer


class TaskDispatcher:
    """Dispatches module tasks from a Redis Stream.

    Gateway XADDs a task spec → TaskDispatcher XREADs it →
    resolves setup → runs module → output goes to ProtoStreamWriter.

    Reuses ModuleServicer's setup resolution, tool cache, and job manager
    to avoid duplicating complex validation/caching logic.
    """

    _redis_client: RedisClient
    _servicer: ModuleServicer
    _dispatch_key: str
    _listen_task: asyncio.Task[None] | None
    _stop_event: asyncio.Event

    def __init__(
        self,
        redis_client: RedisClient,
        servicer: ModuleServicer,
        dispatch_key: str,
    ) -> None:
        """Initialize the task dispatcher.

        Args:
            redis_client: Shared Redis connection pool.
            servicer: ModuleServicer for setup resolution and job management.
            dispatch_key: Redis Stream key to listen for dispatch commands.
        """
        self._redis_client = redis_client
        self._servicer = servicer
        self._dispatch_key = dispatch_key
        self._listen_task = None
        self._stop_event = asyncio.Event()
        self._active_tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        """Start listening for dispatch commands."""
        self._stop_event = asyncio.Event()
        self._listen_task = asyncio.create_task(
            self._listen_loop(),
            name="task_dispatcher",
        )
        logger.info("TaskDispatcher started on %s", self._dispatch_key)

    async def stop(self) -> None:
        """Stop the listener and wait for cleanup."""
        self._stop_event.set()
        if self._listen_task is not None and not self._listen_task.done():
            self._listen_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listen_task
        self._listen_task = None

    async def _listen_loop(self) -> None:
        """XREAD loop — wakes immediately when Gateway XADDs a task."""
        last_id = "$"  # Only new messages from startup
        try:
            while not self._stop_event.is_set():
                result = await self._redis_client.xread(
                    {self._dispatch_key: last_id},
                    count=1,
                    block=1000,
                )
                if not result:
                    continue
                for _stream_name, entries in result:
                    for entry_id, fields in entries:
                        last_id = entry_id if isinstance(entry_id, str) else entry_id.decode()
                        task = asyncio.create_task(
                            self._handle_dispatch(fields),
                            name=f"dispatch_{last_id}",
                        )
                        self._active_tasks.add(task)
                        task.add_done_callback(self._active_tasks.discard)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("TaskDispatcher listen loop crashed")

    async def _handle_dispatch(self, fields: dict[bytes, bytes]) -> None:
        """Process a single dispatch command.

        Args:
            fields: Redis Stream entry fields (task_id, pb, setup_id, mission_id).
        """
        task_id = fields.get(b"task_id", b"").decode()
        setup_id = fields.get(b"setup_id", b"").decode()
        mission_id = fields.get(b"mission_id", b"").decode()
        input_pb = fields.get(b"pb", b"")

        if not task_id:
            logger.warning("TaskDispatcher: missing task_id in dispatch")
            return

        proto_writer = ProtoStreamWriter(task_id, self._redis_client)
        try:
            # Parse proto input
            input_struct = struct_pb2.Struct()
            if input_pb:
                input_struct.ParseFromString(input_pb)

            input_data = self._servicer.module_class.create_input_model(
                json_format.MessageToDict(input_struct),
            )

            # Resolve setup (reuses ModuleServicer's cache + coalescing)
            setup_version = await self._servicer._resolve_setup(setup_id, mission_id)  # noqa: SLF001
            setup_data = await self._servicer.module_class.create_setup_model(setup_version.content)

            # Resolve tool cache
            tool_cache = self._servicer._tool_cache_by_setup.get(setup_version.setup_id)  # noqa: SLF001

            # Run module via job manager
            job_id = await self._servicer.job_manager.create_module_instance_job(
                input_data,
                setup_data,
                mission_id=mission_id,
                setup_id=setup_version.setup_id,
                setup_version_id=setup_version.id,
                request_metadata={"x-task-id": task_id},
                job_id=task_id,
                tool_cache=tool_cache,
            )

            # Consume output and write to proto stream
            async with self._servicer.job_manager.generate_stream_consumer(job_id) as stream:
                async for message in stream:
                    if message.get("root", {}).get("protocol") == "end_of_stream":
                        break
                    s = struct_pb2.Struct()
                    s.update(message)
                    await proto_writer.write_struct(s)

            # Wait for task completion
            try:
                await asyncio.wait_for(
                    self._servicer.job_manager.wait_for_completion(job_id),
                    timeout=30.0,
                )
            except asyncio.TimeoutError:
                logger.warning("Task completion timeout: task_id=%s", task_id)

            # Clean up session
            try:
                await self._servicer.job_manager.clean_session(job_id, mission_id=mission_id)
            except Exception:
                logger.exception("Task cleanup error: task_id=%s", task_id)

        except Exception:
            logger.exception("TaskDispatcher error: task_id=%s", task_id)
        finally:
            try:
                await proto_writer.write_eos()
            except Exception:
                logger.exception("TaskDispatcher EOS write failed: task_id=%s", task_id)
