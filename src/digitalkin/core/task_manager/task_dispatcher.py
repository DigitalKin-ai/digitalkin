"""Redis-based task dispatcher — replaces gRPC loopback for module execution.

Listens on a Redis Stream for dispatch commands from the Gateway.
For each task: resolves setup, creates the module, writes output to
a proto stream. The Gateway reads output via ProtoStreamReader.

Runs in the same process as the Gateway (embedded mode) or standalone.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any

from google.protobuf import json_format, struct_pb2
from redis.exceptions import RedisError

from digitalkin.core.profiling.step_timer import StepTimer
from digitalkin.grpc_servers.gateway_constants import INPUT_WAIT_TIMEOUT_S
from digitalkin.grpc_servers.stream_error_codes import StreamErrorCode
from digitalkin.logger import logger

if TYPE_CHECKING:
    from digitalkin.core.task_manager.redis.redis_client import RedisClient
    from digitalkin.grpc_servers.module_servicer import ModuleServicer
    from digitalkin.grpc_servers.stream_registry import StreamRegistry


class TaskDispatcher:
    """Dispatches module tasks from a Redis Stream.

    Gateway XADDs a task spec → TaskDispatcher XREADs it →
    resolves setup → runs module → output goes directly to Redis via XADD.

    Reuses ModuleServicer's setup resolution, tool cache, and job manager
    to avoid duplicating complex validation/caching logic.
    """

    _redis_client: RedisClient
    _servicer: ModuleServicer
    _dispatch_key: str
    _listen_task: asyncio.Task[None] | None
    _stop_event: asyncio.Event
    _registry: StreamRegistry | None
    _input_wait_timeout_s: float

    def __init__(
        self,
        redis_client: RedisClient,
        servicer: ModuleServicer,
        dispatch_key: str,
        registry: StreamRegistry | None = None,
        input_wait_timeout_s: float = INPUT_WAIT_TIMEOUT_S,
    ) -> None:
        """Initialize the task dispatcher.

        Args:
            redis_client: Shared Redis connection pool.
            servicer: ModuleServicer for setup resolution and job management.
            dispatch_key: Redis Stream key to listen for dispatch commands.
            registry: Gateway StreamRegistry for fetching the session. The
                first module input arrives via the session's ``input_queue``,
                fed by the consumer's first ``StreamClient.data``.
            input_wait_timeout_s: Max seconds to wait for the first upstream
                input on session.input_queue. Defaults to
                :data:`INPUT_WAIT_TIMEOUT_S` from gateway settings
                (env: ``DIGITALKIN_DISPATCHER_INPUT_WAIT_S``).
        """
        self._redis_client = redis_client
        self._servicer = servicer
        self._dispatch_key = dispatch_key
        self._registry = registry
        self._input_wait_timeout_s = input_wait_timeout_s
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
        """XREAD loop — wakes immediately when Gateway XADDs a task.

        Adaptive count: starts at 1, doubles when batch is full (up to 100),
        resets to 1 on idle. Eliminates serial drain at high concurrency.

        Crash recovery: try/except inside the loop with exponential backoff
        (0.1s → 10s cap). Transient Redis errors retry instead of killing dispatch.
        """
        last_id = "$"
        count = 1
        backoff = 0.1
        while not self._stop_event.is_set():
            try:
                result = await self._redis_client.xread(
                    {self._dispatch_key: last_id},
                    count=count,
                    block=1000,
                )
                if not result:
                    count = 1
                    continue
                for _stream_name, entries in result:
                    count = min(count * 2, 100) if len(entries) >= count else max(1, len(entries))
                    for entry_id, fields in entries:
                        last_id = entry_id if isinstance(entry_id, str) else entry_id.decode()
                        task = asyncio.create_task(
                            self._handle_dispatch(fields),
                            name=f"dispatch_{last_id}",
                        )
                        self._active_tasks.add(task)
                        task.add_done_callback(self._active_tasks.discard)
                backoff = 0.1
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("TaskDispatcher iteration error, retrying in %.1fs", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)

    async def _emit_fatal_to_redis(
        self,
        task_id: str,
        code: str,
        message: str,
        *,
        log_extra: dict[str, str],
    ) -> None:
        """Write ``stream.error(fatal=true)`` + EOS to the task's Redis stream.

        Mirror of ``GatewayServicer._emit_fatal_to_redis`` — keep both in
        sync. Duplicated rather than imported because the dispatcher and
        the gateway run in different deployment shapes (embedded vs
        standalone) and may not always share a process.

        Args:
            task_id: Task whose stream gets the error.
            code: Stable code from :class:`StreamErrorCode`.
            message: Human-readable detail.
            log_extra: ``{"task_id", "setup_id", "mission_id"}`` for log
                correlation.
        """
        error_struct = struct_pb2.Struct()
        error_struct.update({
            "root": {
                "protocol": "stream.error",
                "code": code,
                "message": message,
                "fatal": True,
            },
        })
        stream_key = f"task:{task_id}:stream"
        try:
            await self._redis_client.xadd(
                stream_key,
                {"pb": error_struct.SerializeToString()},
            )
            await self._redis_client.xadd(stream_key, {"eos": b"true"})
            await self._redis_client.expire(stream_key, 60)
            logger.error(
                "stream.error emitted: code=%s message=%s",
                code,
                message,
                extra=log_extra,
            )
        except RedisError:
            logger.exception(
                "Could not emit stream.error to Redis (Redis is also down): code=%s message=%s",
                code,
                message,
                extra=log_extra,
            )

    async def _handle_dispatch(self, fields: dict[bytes, bytes]) -> None:  # noqa: C901, PLR0914, PLR0915
        """Process a single dispatch command.

        Args:
            fields: Redis Stream entry fields (task_id, pb, setup_id, mission_id).
        """
        timer = StepTimer()
        task_id = fields.get(b"task_id", b"").decode()
        setup_id = fields.get(b"setup_id", b"").decode()
        mission_id = fields.get(b"mission_id", b"").decode()
        input_pb = fields.get(b"pb", b"")
        ts_ns_raw = fields.get(b"ts_ns", b"0").decode()
        try:
            dispatch_delay = (time.perf_counter_ns() - int(ts_ns_raw)) / 1e6
            logger.info("TaskDispatcher XREAD pickup delay: %.1fms task_id=%s", dispatch_delay, task_id)
        except (ValueError, OverflowError):
            pass
        timer.mark("entry")

        if not task_id:
            logger.warning("TaskDispatcher: missing task_id in dispatch")
            return

        log_extra = {"task_id": task_id, "setup_id": setup_id, "mission_id": mission_id}
        stream_key = f"task:{task_id}:stream"
        try:
            # First input arrives via the session's input_queue (fed by the
            # client's first StreamClient.data). Legacy callers may still
            # pass `pb` on the dispatch entry — fall back to that if the
            # registry isn't wired or the session is missing.
            input_struct = struct_pb2.Struct()
            session = self._registry.get(task_id) if self._registry is not None else None
            timer.mark("registry_lookup")
            if session is not None:
                try:
                    item = await asyncio.wait_for(
                        session.input_queue.get(),
                        timeout=self._input_wait_timeout_s,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "TaskDispatcher: no upstream input within %.1fs",
                        self._input_wait_timeout_s,
                        extra=log_extra,
                    )
                    await self._emit_fatal_to_redis(
                        task_id,
                        code=StreamErrorCode.INPUT_WAIT_TIMEOUT.value,
                        message=(
                            f"no upstream input within {self._input_wait_timeout_s}s — "
                            "dial-back is the only input source"
                        ),
                        log_extra=log_extra,
                    )
                    return
                logger.info(
                    "TaskDispatcher: upstream input received — proceeding to module dispatch",
                    extra=log_extra,
                )
                timer.mark("input_wait")
                if item is None:
                    return
                # Upstream messages are stored as {"_proto": Struct}
                proto_payload = item.get("_proto") if isinstance(item, dict) else None
                if proto_payload is not None:
                    input_struct = proto_payload
                elif isinstance(item, dict):
                    input_struct.update(item)
            elif input_pb:
                input_struct.ParseFromString(input_pb)
                timer.mark("input_pb_parse")

            input_dict = json_format.MessageToDict(input_struct)
            timer.mark("struct_to_dict")

            input_data = self._servicer.module_class.create_input_model(input_dict)
            timer.mark("pydantic_input")

            # Resolve setup (reuses ModuleServicer's cache + coalescing)
            setup_version = await self._servicer._resolve_setup(setup_id, mission_id)  # noqa: SLF001
            timer.mark("setup_resolve")

            setup_data = await self._servicer.module_class.create_setup_model(setup_version.content)
            timer.mark("setup_model")

            # Resolve tool cache
            tool_cache = self._servicer._tool_cache_by_setup.get(setup_version.setup_id)  # noqa: SLF001
            timer.mark("tool_cache_lookup")

            # Direct callback: module output → XADD → Redis (no buffer, no flush)
            async def _on_output(output_data: Any) -> None:
                data = output_data.model_dump(mode="json")
                if data.get("root", {}).get("protocol") == "stream.end":
                    await self._redis_client.xadd(stream_key, {"eos": b"true"})
                    await self._redis_client.expire(stream_key, 60)
                    return
                s = struct_pb2.Struct()
                s.update(data)
                await self._redis_client.xadd(stream_key, {"pb": s.SerializeToString()})

            # Run module with direct Redis callback
            await self._servicer.job_manager.create_module_instance_job(
                input_data,
                setup_data,
                mission_id=mission_id,
                setup_id=setup_version.setup_id,
                setup_version_id=setup_version.id,
                request_metadata={"x-task-id": task_id},
                job_id=task_id,
                tool_cache=tool_cache,
                callback=_on_output,
            )
            timer.mark("create_job")
            timer.log("Dispatch", task_id)

            # Fire-and-forget: auto-cleanup handles wait + session cleanup via done callback.
            # Don't block the dispatch handler — EOS is already written by _on_output callback.

        except Exception as exc:
            logger.exception("TaskDispatcher: module job failed", extra=log_extra)
            await self._emit_fatal_to_redis(
                task_id,
                code=StreamErrorCode.MODULE_RUNTIME_ERROR.value,
                message=f"module execution failed: {type(exc).__name__}: {exc}",
                log_extra=log_extra,
            )
