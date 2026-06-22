"""GatewayService gRPC servicer: StartStream, Stream, SendSignal."""

from __future__ import annotations

import asyncio
import contextlib
import json
import random
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import grpc
from agentic_mesh_protocol.gateway.v1 import gateway_pb2
from google.protobuf import struct_pb2
from grpc._cython.cygrpc import UsageError as _GrpcUsageError  # noqa: PLC2701
from redis.exceptions import RedisError

from digitalkin.core.exceptions import RedisUnreachableError
from digitalkin.core.profiling.step_timer import StepTimer
from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader
from digitalkin.core.task_manager.redis.redis_idempotency import RedisIdempotency
from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
from digitalkin.grpc_servers.interceptors.request_ids import RequestContext
from digitalkin.grpc_servers.m2m_call_registry import M2MCallRegistry
from digitalkin.grpc_servers.stream_registry import StreamRegistry
from digitalkin.grpc_servers.stream_session import StreamSession
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.validators import GatewayValidator
from digitalkin.logger import logger
from digitalkin.models.core.redis import ClaimResult
from digitalkin.models.grpc_servers.stream_error_codes import StreamErrorCode
from digitalkin.models.settings.gateway import get_gateway_settings
from digitalkin.services.communication.exceptions import InvalidConsumerAddressError
from digitalkin.services.communication.grpc_communication import GrpcCommunication

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator, Callable

    from digitalkin.core.task_manager.module_runner import ModuleRunner
    from digitalkin.core.task_manager.redis.redis_client import RedisClient


class GatewayServicer:
    """Inter-module broker. All data flows through Redis Streams."""

    _registry: StreamRegistry
    _redis_client: RedisClient

    @staticmethod
    def _sentinel(seq: int, task_id: str, protocol: str, **fields: Any) -> Any:
        """Build a StreamClient carrying a control sentinel.

        ``seq=0`` marks gateway-emitted control entries; Redis-replayed
        entries start at 1.

        Args:
            seq: Sequence number; 0 for gateway control entries.
            task_id: Task ID echoed on the wire.
            protocol: Sentinel protocol name (``stream.*``).
            fields: Additional Struct fields under ``data.root``.

        Returns:
            StreamClient proto.
        """
        s = struct_pb2.Struct()
        s.update({"root": {"protocol": protocol, **fields}})
        return gateway_pb2.StreamClient(from_seq=seq, task_id=task_id, data=s)

    async def _fatal_close(self, task_id: str, code: str, message: str) -> AsyncGenerator:
        """Yield ``stream.error(fatal=true)`` then ``stream.end``.

        Args:
            task_id: Task ID.
            code: Status code name (``INVALID_ARGUMENT``, ``NOT_FOUND``, ...).
            message: Human-readable detail.

        Yields:
            StreamClient sentinels.
        """
        yield self._sentinel(
            0,
            task_id,
            "stream.error",
            code=code,
            message=message,
            fatal=True,
        )
        yield self._sentinel(0, task_id, "stream.end")

    def __init__(
        self,
        redis_client: RedisClient,
        cache_handler: Any = None,
        client_config: Any = None,
        module_runner: ModuleRunner | None = None,
    ) -> None:
        """Initialize the gateway servicer.

        Args:
            redis_client: Redis for stream persistence and signals.
            cache_handler: Async callback for cache invalidation signals.
            client_config: ClientConfig for outbound dial-back.
            module_runner: Orchestrator invoked once the consumer's first
                reply lands. Required in embedded mode.
        """
        self._registry = StreamRegistry(redis_client)
        self._redis_client = redis_client
        self._idempotency = RedisIdempotency(redis_client)
        self._cache_handler = cache_handler
        self._client_config = client_config
        self._module_runner = module_runner
        self._m2m = M2MCallRegistry()

    @property
    def m2m(self) -> M2MCallRegistry:
        """M2M call registry shared with ``GrpcCommunication``."""
        return self._m2m

    def _spawn(self, coro: Any, *, name: str) -> asyncio.Task[Any]:
        """Schedule ``coro`` as a supervised fire-and-forget task.

        Args:
            coro: Coroutine to schedule.
            name: asyncio task name.

        Returns:
            The created task.
        """
        task = asyncio.create_task(coro, name=name)
        self._registry.monitor_task(task)
        return task

    async def start(self) -> None:
        """Start the M2M call-registry TTL sweeper and PSUBSCRIBE the signal listener.

        Pre-warms both Redis pools so the first XADD and first XREAD don't pay
        DNS+TCP+AUTH on cold connections.

        Raises:
            RedisUnreachableError: Redis ping failed; gateway cannot serve traffic.
        """
        if not await self._redis_client.verify():
            raise RedisUnreachableError(GatewayValidator.mask_redis_url(self._redis_client.url))
        await self._m2m.start()
        listener = SharedRedisListener.singleton_or_none()
        if listener is not None:
            try:
                await listener.start()
            except Exception:
                logger.warning(
                    "SharedRedisListener.start() failed at boot — first-task PSUBSCRIBE will retry lazily",
                    exc_info=True,
                )

    async def stop(self) -> None:
        """Shut down registries and cancel the M2M sweeper. Does not close the borrowed RedisClient."""
        await self._m2m.stop()
        await self._registry.shutdown()

    async def AssociateTask(self, request: Any, context: grpc.aio.ServicerContext) -> Any:  # noqa: ARG002, PLR6301
        """Not served by the SDK — the backend mints sub-tasks.

        Present only so the generated ``add_GatewayServiceServicer_to_server`` finds all
        four RPCs; nothing dials the module for it. Callers use the backend endpoint.
        """
        await context.abort(grpc.StatusCode.UNIMPLEMENTED, "AssociateTask is served by the backend")

    async def StartStream(  # noqa: PLR0911
        self,
        request: Any,
        context: grpc.aio.ServicerContext,
    ) -> Any:
        """Register a task session and schedule the dial-back.

        Args:
            request: StartStreamRequest proto.
            context: gRPC service context.

        Returns:
            StartStreamResponse(accepted, task_id).
        """
        timer = StepTimer()
        task_id = request.task_id
        log_extra = {
            "task_id": task_id,
            "setup_id": request.setup_id,
            "mission_id": request.mission_id,
        }
        # Bind IDs for this handler's logs + any outbound gRPC (task-local context).
        RequestContext.bind(task_id=task_id, setup_id=request.setup_id, mission_id=request.mission_id)

        err = (
            GatewayValidator.validate_id(task_id, "task_id")
            or GatewayValidator.validate_id(request.setup_id, "setup_id")
            or GatewayValidator.validate_id(request.mission_id, "mission_id")
        )
        timer.mark("validate_ids")
        if err is not None:
            logger.warning("Invalid ID in StartStream: %s", err, extra=log_extra)
            return gateway_pb2.StartStreamResponse(accepted=False, task_id=task_id)

        md = dict(context.invocation_metadata() or [])
        raw_address = md.get("x-client-address", "")
        if isinstance(raw_address, bytes):
            raw_address = raw_address.decode("utf-8", errors="replace")
        client_address = raw_address.strip()
        addr_err = GatewayValidator.validate_address(client_address, "x-client-address")
        timer.mark("validate_address")
        if addr_err is not None:
            logger.warning(
                "StartStream rejected: %s (value=%r)",
                addr_err,
                client_address,
                extra=log_extra,
            )
            return gateway_pb2.StartStreamResponse(accepted=False, task_id=task_id)

        if self._registry.get(task_id) is not None:
            return gateway_pb2.StartStreamResponse(accepted=False, task_id=task_id)
        timer.mark("dedup_check")

        # Durable at-most-once guard: survives session teardown and spans replicas.
        # Reconnection is server-driven (the live dial-back auto re-dials the same
        # consumer), so a re-issued StartStream for an already-claimed/running task is
        # REFUSED — one task_id maps to exactly one dial.
        try:
            claim = await self._idempotency.claim(task_id, SharedRedisListener.PROCESS_ID)
        except RedisError:
            return gateway_pb2.StartStreamResponse(accepted=False, task_id=task_id)
        timer.mark("idempotency_claim")
        if claim is not ClaimResult.CLAIMED:
            return gateway_pb2.StartStreamResponse(accepted=False, task_id=task_id)

        session = StreamSession(task_id=task_id)
        accepted = await self._registry.register(
            session,
            setup_id=request.setup_id,
            mission_id=request.mission_id,
        )
        timer.mark("registry_register")
        if not accepted:
            logger.warning("Session rejected (capacity)", extra=log_extra)
            # Release the claim so this task can be retried once capacity frees up.
            with contextlib.suppress(RedisError):
                await self._idempotency.release(task_id)
            return gateway_pb2.StartStreamResponse(accepted=False, task_id=task_id)

        # Seed stream.start so the consumer's first XREAD finds data immediately.
        start_info = struct_pb2.Struct()
        start_info.update({
            "root": {
                "protocol": "stream.start",
                "task_id": task_id,
                "mission_id": request.mission_id,
                "setup_id": request.setup_id,
                "started_at": datetime.now(tz=timezone.utc).isoformat(),
            },
        })
        timer.mark("build_start_info")
        try:
            await self._redis_client.xadd(
                f"task:{task_id}:stream",
                {"pb": start_info.SerializeToString(), "seq": "0"},
            )
        except RedisError:
            # Claimed + registered but the stream couldn't be seeded: undo both so a retry re-runs.
            with contextlib.suppress(RedisError):
                await self._idempotency.release(task_id)
            await self._registry.unregister(task_id)
            return gateway_pb2.StartStreamResponse(accepted=False, task_id=task_id)
        timer.mark("xadd_stream_start")

        logger.info("→ Dial-back scheduled to consumer %s", client_address, extra=log_extra)
        self._spawn(
            self._dial_consumer(
                task_id=task_id,
                mission_id=request.mission_id,
                setup_id=request.setup_id,
                address=client_address,
            ),
            name=f"dial_consumer_{task_id}",
        )
        timer.mark("schedule_dial_consumer")
        timer.log("StartStream", task_id)

        logger.info(
            "Task accepted: active_sessions=%d",
            self._registry.active_count,
            extra=log_extra,
        )
        return gateway_pb2.StartStreamResponse(accepted=True, task_id=task_id)

    async def _emit_fatal_to_redis(
        self,
        task_id: str,
        code: str,
        message: str,
        *,
        log_extra: dict[str, str],
    ) -> None:
        """Write ``stream.error(fatal=true)`` + EOS to the task's Redis stream.

        Converts dial-back failures into the in-band protocol error
        consumers observe. Never raises.

        Args:
            task_id: Task whose stream gets the error.
            code: Stable code from :class:`StreamErrorCode`.
            message: Human-readable detail.
            log_extra: ``{task_id, setup_id, mission_id}`` for log correlation.
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
            await self._redis_client.expire(stream_key, get_gateway_settings().stream.redis_stream_ttl)
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

    async def Stream(  # noqa: C901, PLR0911, PLR0912
        self,
        request_iterator: AsyncIterator[Any],
        context: grpc.aio.ServicerContext,  # noqa: ARG002
    ) -> AsyncGenerator[Any, None]:
        """BiDi: receive StreamServer from client, yield StreamClient back.

        First StreamServer carries ``task_id``, resume cursor in ``seq``,
        and the query in ``data``. Errors flow as ``stream.error`` +
        ``stream.end`` sentinels — never via ``context.abort``.

        Args:
            request_iterator: BiDi stream of StreamServer from the client.
            context: gRPC service context.

        Yields:
            StreamClient — sentinels and module output.
        """
        try:
            first_msg = await anext(request_iterator)
        except StopAsyncIteration:
            return

        task_id = first_msg.task_id
        from_seq = first_msg.seq

        if GatewayValidator.validate_id(task_id, "task_id") is not None:
            async for out in self._fatal_close(task_id, "INVALID_ARGUMENT", "invalid task_id"):
                yield out
            return

        # Dial-back-receive: remote gateway delivering outputs for an
        # outbound call we initiated. Marked by ``stream.init`` + known task_id.
        root_field = first_msg.data.fields.get("root") if first_msg.data else None
        if root_field is not None:
            protocol_field = root_field.struct_value.fields.get("protocol")
            if protocol_field is not None and protocol_field.string_value == "stream.init":
                if not self._m2m.has(task_id):
                    logger.warning("[m2m-dialback] no outbound entry for task_id=%s", task_id)
                    async for out in self._fatal_close(
                        task_id,
                        StreamErrorCode.DIAL_BACK_INTERNAL.value,
                        "unknown outbound task_id",
                    ):
                        yield out
                    return
                async for out in self._m2m.handle_dial_back_receive(task_id, request_iterator):
                    yield out
                return

        if from_seq > get_gateway_settings().stream.from_seq_limit:
            async for out in self._fatal_close(task_id, "INVALID_ARGUMENT", "seq out of range"):
                yield out
            return

        session = self._registry.get(task_id)

        # Late client: session finished but data still in Redis.
        if session is None:
            try:
                stream_len = await self._redis_client.xlen(f"task:{task_id}:stream")
            except RedisError:
                async for out in self._fatal_close(
                    task_id, StreamErrorCode.REDIS_UNAVAILABLE.value, "redis unavailable"
                ):
                    yield out
                return
            if stream_len > 0:
                async for resp in self._consume_guarded(task_id, from_seq):
                    yield resp
                return
            async for out in self._fatal_close(task_id, "NOT_FOUND", "task not found"):
                yield out
            return

        # First message's data is the query.
        input_key = f"task:{task_id}:input"
        if first_msg.data and len(first_msg.data.fields) > 0:
            try:
                await self._redis_client.xadd(
                    input_key,
                    {"pb": first_msg.data.SerializeToString()},
                )
            except RedisError:
                async for out in self._fatal_close(
                    task_id, StreamErrorCode.REDIS_UNAVAILABLE.value, "redis unavailable"
                ):
                    yield out
                return

        upstream_task = self._spawn(
            self._read_peer_upstream(request_iterator, task_id, session),
            name=f"peer_upstream_{task_id}",
        )

        try:
            async for resp in self._consume_guarded(task_id, from_seq):
                yield resp
        finally:
            if not upstream_task.done():
                upstream_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await upstream_task
            removed = await self._registry.unregister(task_id)
            if removed is not None:
                await removed.teardown()

    async def _read_peer_upstream(
        self,
        request_iterator: AsyncIterator,
        task_id: str,
        session: StreamSession,
    ) -> None:
        """Drain follow-up upstream messages onto the task's input stream.

        Args:
            request_iterator: BiDi stream from the client.
            task_id: Task identifier (input stream key).
            session: Stream session (stop-event check).
        """
        input_key = f"task:{task_id}:input"
        try:
            async for msg in request_iterator:
                if session._stop_event.is_set():  # noqa: SLF001
                    break
                if msg.data and len(msg.data.fields) > 0:
                    await self._redis_client.xadd(
                        input_key,
                        {"pb": msg.data.SerializeToString()},
                    )
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Peer upstream reader error: task_id=%s", task_id)

    async def SendSignal(
        self,
        request: Any,
        context: grpc.aio.ServicerContext,  # noqa: ARG002
    ) -> Any:
        """Forward control signal via Redis pub/sub or dispatch cache invalidation.

        Args:
            request: ClientSignalRequest proto.
            context: gRPC service context.

        Returns:
            ClientSignalResponse proto.
        """
        timer = StepTimer()
        action_name = gateway_pb2.SignalAction.Name(request.action)
        task_id = request.task_id
        last_mark = "init"
        log_extra = {"task_id": task_id, "action": action_name}

        try:  # noqa: PLW0717
            if action_name.startswith("INVALIDATE_"):
                setup_id_for_invalidate = task_id
                if self._cache_handler is not None:
                    await self._cache_handler(action_name, setup_id_for_invalidate)
                    timer.mark("cache_handler")
                    last_mark = "cache_handler"
                payload = json.dumps({
                    "action": action_name.lower(),
                    "setup_id": setup_id_for_invalidate,
                    "published_at_ns": time.time_ns(),
                    # Tag origin so this process skips its own fan-out (no double invalidate).
                    "origin": SharedRedisListener.PROCESS_ID,
                })
                try:
                    await self._redis_client.publish("signal_ch:_global_", payload)
                    timer.mark("global_publish")
                    last_mark = "global_publish"
                except RedisError:
                    logger.warning(
                        "[gateway] INVALIDATE fan-out publish failed — local-only invalidation applied",
                        extra=log_extra,
                        exc_info=True,
                    )
                logger.debug(
                    "[perf] SendSignal: %s path=cache total=%.2fms action=%s setup_id=%s",
                    timer.format_steps(),
                    timer.total_ms(),
                    action_name,
                    setup_id_for_invalidate,
                    extra=log_extra,
                )
                return gateway_pb2.ClientSignalResponse(success=True, task_id=setup_id_for_invalidate)

            if GatewayValidator.validate_id(task_id, "task_id") is not None:
                logger.warning(
                    "[gateway] SendSignal_failed: failure=InvalidTaskId at_step=%s "
                    "elapsed_ms=%.2f action=%s task_id=%s",
                    last_mark,
                    timer.elapsed_now_ms(),
                    action_name,
                    task_id,
                    extra=log_extra,
                )
                return gateway_pb2.ClientSignalResponse(success=False, task_id=task_id)
            timer.mark("validate_task_id")
            last_mark = "validate_task_id"

            session = self._registry.get(task_id)
            timer.mark("registry_lookup")
            last_mark = "registry_lookup"
            if session is None:
                logger.warning(
                    "[gateway] SendSignal_failed: failure=TaskNotFound at_step=%s elapsed_ms=%.2f action=%s task_id=%s",
                    last_mark,
                    timer.elapsed_now_ms(),
                    action_name,
                    task_id,
                    extra=log_extra,
                )
                return gateway_pb2.ClientSignalResponse(success=False, task_id=task_id)

            action_lower = action_name.lower()
            payload = json.dumps({
                "action": action_lower,
                "task_id": task_id,
                "published_at_ns": time.time_ns(),
            })
            await self._redis_client.publish(f"signal_ch:{task_id}", payload)
            timer.mark("redis_publish")
            last_mark = "redis_publish"
            logger.debug(
                "[perf] SendSignal: %s path=redis total=%.2fms action=%s task_id=%s",
                timer.format_steps(),
                timer.total_ms(),
                action_name,
                task_id,
                extra=log_extra,
            )
            return gateway_pb2.ClientSignalResponse(success=True, task_id=task_id)

        except Exception as exc:
            logger.warning(
                "[gateway] SendSignal_failed: failure=%s at_step=%s elapsed_ms=%.2f action=%s task_id=%s",
                type(exc).__name__,
                last_mark,
                timer.elapsed_now_ms(),
                action_name,
                task_id,
                extra=log_extra,
            )
            return gateway_pb2.ClientSignalResponse(success=False, task_id=task_id)

    async def _consume_from_redis(
        self,
        task_id: str,
        from_seq: int,
        *,
        resume: bool = False,
    ) -> AsyncGenerator:
        """Zero-copy read from Redis Stream into ``StreamClient`` messages.

        Always terminates with an explicit ``stream.end`` sentinel.

        Args:
            task_id: Task reference ID.
            from_seq: Resume point (consumer's last-seen wire label).
            resume: If True, seek to the consumer's exact cursor via stored seq
                (``skip_to_seq = from_seq - 1``) instead of the gateway's saved
                cursor, and label frames from the stored seq so trim gaps surface.

        Yields:
            StreamClient messages.
        """
        t0 = time.perf_counter_ns()
        reader = ProtoStreamReader(task_id, self._redis_client)
        skip_to_seq: int | None = None
        if resume:
            skip_to_seq = from_seq - 1
        elif from_seq > 0:
            await reader.restore_cursor()
        t1 = time.perf_counter_ns()

        seq = from_seq
        first = True

        async for struct_data in reader.read_structs(skip_to_seq=skip_to_seq):
            if first:
                t2 = time.perf_counter_ns()
                logger.info(
                    "Stream: cursor=%.1fms xread_wait=%.1fms total_to_first=%.1fms task_id=%s",
                    (t1 - t0) / 1e6,
                    (t2 - t1) / 1e6,
                    (t2 - t0) / 1e6,
                    task_id,
                )
                first = False
            seq = reader._last_seq + 1 if resume else seq + 1  # noqa: SLF001
            yield gateway_pb2.StreamClient(from_seq=seq, task_id=task_id, data=struct_data)

        # Reader EOS — emit an explicit stream.end so every stream ends uniformly.
        t_after_reader = time.perf_counter_ns()
        seq = reader._last_seq + 2 if resume else seq + 1  # noqa: SLF001
        yield self._sentinel(seq, task_id, "stream.end")
        t_after_yield = time.perf_counter_ns()
        logger.info(
            "[close-debug] gateway_stream_end: reader_to_yield=%.2fms t_yielded_ns=%d task_id=%s",
            (t_after_yield - t_after_reader) / 1e6,
            t_after_yield,
            task_id,
        )

    async def _consume_guarded(
        self,
        task_id: str,
        from_seq: int,
    ) -> AsyncGenerator:
        """Drive ``_consume_from_redis`` under an idle deadline.

        The Redis reader only stops on an ``eos`` marker. A producer that dies
        without writing one (module crash or cancellation — ``TaskExecutor``
        closes only the in-memory stream, never the Redis stream) would
        otherwise make a consumer's ``Stream`` RPC hang forever. If no new
        entry arrives within ``read_idle_timeout_s``, emit ``stream.error`` +
        ``stream.end`` and return.

        Args:
            task_id: Task reference ID.
            from_seq: Resume point.

        Yields:
            StreamClient messages, then a terminal sentinel on idle timeout.
        """
        idle = get_gateway_settings().stream.read_idle_timeout_s
        reader = aiter(self._consume_from_redis(task_id, from_seq))
        while True:
            try:
                resp = await asyncio.wait_for(anext(reader), timeout=idle)
            except StopAsyncIteration:
                return
            except asyncio.TimeoutError:
                yield self._sentinel(
                    0,
                    task_id,
                    "stream.error",
                    code=StreamErrorCode.STREAM_IDLE_TIMEOUT.value,
                    message=f"no stream output or EOS within {idle:.0f}s",
                    fatal=True,
                )
                yield self._sentinel(0, task_id, "stream.end")
                return
            except RedisError:
                yield self._sentinel(
                    0,
                    task_id,
                    "stream.error",
                    code=StreamErrorCode.REDIS_UNAVAILABLE.value,
                    message="redis unavailable during stream read",
                    fatal=True,
                )
                yield self._sentinel(0, task_id, "stream.end")
                return
            yield resp

    async def _dial_consumer(  # noqa: C901
        self,
        task_id: str,
        mission_id: str,
        setup_id: str,
        address: str,
    ) -> None:
        """Dial the consumer's GatewayService.Stream, with server-side auto-reconnect.

        First attempt is a fresh dial (``stream.init`` → consumer query →
        ``ModuleRunner`` → drain from seq 0). If the BiDi dies before the stream
        is fully delivered and the module is still producing, re-dial the SAME
        address in resume mode (``stream.resume`` → consumer replies with its
        ``from_seq`` → drain from that cursor, deduping the Redis queue) with
        jittered backoff until the client returns or the reconnect window
        (``DIGITALKIN_GATEWAY_DIAL_BACK_RECONNECT_WINDOW_S``) elapses. The module
        runs as a separate task, spawned exactly once, so it keeps writing to
        Redis across reconnects.

        Args:
            task_id: Task to push.
            mission_id: Mission ID (logging context).
            setup_id: Setup ID (logging context).
            address: ``host:port`` of the consumer's GatewayService.
        """
        log_extra = {"task_id": task_id, "setup_id": setup_id, "mission_id": mission_id}
        # Bind IDs so the dial-back BiDi + logs carry them (task-local spawned context).
        RequestContext.bind(task_id=task_id, setup_id=setup_id, mission_id=mission_id)

        module_spawned = False

        def _mark_spawned() -> None:
            nonlocal module_spawned
            module_spawned = True

        reconnect = get_gateway_settings().dial_reconnect
        deadline: float | None = None
        attempt = 0
        resume = False
        try:
            while True:
                disconnected = await self._run_dial_attempt(
                    task_id=task_id,
                    mission_id=mission_id,
                    setup_id=setup_id,
                    address=address,
                    resume=resume,
                    on_runner_spawn=_mark_spawned,
                )
                if not disconnected:
                    break
                # Client BiDi died mid-stream. Re-dial only while there is a
                # retained stream to resume and the reboot window hasn't elapsed.
                if not module_spawned:
                    break
                session = self._registry.get(task_id)
                if session is None or session._stop_event.is_set():  # noqa: SLF001
                    break
                try:
                    stream_len = await self._redis_client.xlen(f"task:{task_id}:stream")
                except RedisError:
                    break
                if stream_len == 0:
                    break
                now = time.monotonic()
                if deadline is None:
                    deadline = now + reconnect.window_s
                if now >= deadline:
                    break
                attempt += 1
                delay = min(random.uniform(reconnect.backoff_base_s, reconnect.backoff_max_s), deadline - now)  # noqa: S311
                await asyncio.sleep(max(0.0, delay))
                resume = True
        finally:
            # End-of-stream cleanup (once). Output stream is left intact for replay.
            try:
                removed = await self._registry.unregister(task_id)
                if removed is not None:
                    await removed.teardown()
            except Exception:
                logger.exception("end-of-stream unregister failed", extra=log_extra)

    async def _run_dial_attempt(  # noqa: C901, PLR0912, PLR0914, PLR0915
        self,
        *,
        task_id: str,
        mission_id: str,
        setup_id: str,
        address: str,
        resume: bool,
        on_runner_spawn: Callable[[], None],
    ) -> bool:
        """Run one dial-back BiDi attempt (fresh or resume).

        Fresh (``resume=False``): sends ``stream.init``, spawns the
        ``ModuleRunner`` on the consumer's first reply (calling
        ``on_runner_spawn``), drains from seq 0. Resume: sends ``stream.resume``,
        reads the consumer's cursor from the first reply's ``from_seq``, skips
        the runner, drains from that cursor. Releases the channel before
        returning; does NOT unregister the session (the caller owns lifecycle).

        Args:
            task_id: Task to push.
            mission_id: Mission ID (logging context).
            setup_id: Setup ID (logging context).
            address: ``host:port`` of the consumer's GatewayService.
            resume: Re-attach to an existing task's output instead of starting it.
            on_runner_spawn: Called once, when this attempt spawns the module runner.

        Returns:
            True if the BiDi died before delivering ``stream.end`` (a re-dial
            candidate); False on clean completion or a terminal failure.
        """
        log_extra = {"task_id": task_id, "setup_id": setup_id, "mission_id": mission_id}

        async def _fail(code: str, message: str) -> None:
            """Emit a fatal to Redis (fresh dial) or log only (resume).

            In resume mode the durable stream is authoritative and still being
            written by the runner; injecting ``stream.error``+``eos`` would
            poison it, so failures are logged and the stream left intact.
            """
            if resume:
                logger.warning(
                    "resume-dial failed (stream left intact for retry): %s %s",
                    code,
                    message,
                    extra=log_extra,
                )
            else:
                await self._emit_fatal_to_redis(task_id, code=code, message=message, log_extra=log_extra)

        cfg = self._client_config
        if cfg is None:
            logger.error("Dial-back unavailable: no client_config configured", extra=log_extra)
            await _fail(
                StreamErrorCode.DIAL_BACK_INTERNAL.value,
                "gateway has no client_config to dial back with",
            )
            return False
        # setup_version_id is unused on the dial-back path.
        comm = GrpcCommunication(
            mission_id=mission_id,
            setup_id=setup_id,
            setup_version_id="",
            client_config=cfg,
        )
        # Resume re-dials a peer that just died — force a fresh channel so we
        # never reuse a cached connection left wedged mid-reconnect.
        if resume:
            await comm.evict_consumer_channel(address)
        t_dial0 = time.perf_counter_ns()
        try:
            stub, release = comm.dial_consumer_stream(address)
        except InvalidConsumerAddressError as exc:
            # Defence-in-depth — StartStream's validate_address should have caught this.
            logger.exception("dial_consumer: invalid address %r", address, extra=log_extra)
            await _fail(
                StreamErrorCode.DIAL_BACK_UNREACHABLE.value,
                f"dial-back channel build failed: {exc}",
            )
            return False
        except OSError as exc:
            logger.exception("dial_consumer: channel build failed addr=%s", address, extra=log_extra)
            await _fail(
                StreamErrorCode.DIAL_BACK_UNREACHABLE.value,
                f"dial-back channel build failed: {type(exc).__name__}: {exc}",
            )
            return False
        t_stub = time.perf_counter_ns()
        logger.info("→ Dial-back channel ready to %s", address, extra=log_extra)

        def _ch_state(chan: Any) -> str:
            """Best-effort connectivity probe.

            Returns:
                The state enum name, or ``err:<exc>`` on failure.
            """
            try:
                state = chan.get_state(try_to_connect=False)
                return str(state.name)
            except Exception as exc:
                return f"err:{type(exc).__name__}"

        logger.info(
            "[dial-debug] channel_ready dt_init=%.3fms ch_state=%s channel_id=%s ref_count=%d cache_keys=%d",
            (t_stub - t_dial0) / 1e6,
            _ch_state(comm._channel),  # noqa: SLF001
            id(comm._channel),  # noqa: SLF001
            GrpcClientWrapper._ref_counts.get(comm._channel_cache_key or "", 0),  # noqa: SLF001
            len(GrpcClientWrapper._channel_cache),  # noqa: SLF001
            extra=log_extra,
        )

        session = self._registry.get(task_id)
        if session is None:
            logger.warning(
                "Dial-back aborted — session disappeared before channel was ready",
                extra=log_extra,
            )
            await release()
            return False

        # Outbound is StreamServer, inbound is StreamClient — both share
        # field tags so re-wrapping ``_consume_from_redis`` output is a rename.
        handshake = "stream.resume" if resume else "stream.init"
        init_struct = struct_pb2.Struct()
        init_struct.update({"root": {"protocol": handshake}})
        init_server = gateway_pb2.StreamServer(seq=0, task_id=task_id, data=init_struct)

        # Gate the output drain on the consumer's first reply (query, or cursor on resume).
        output_started = asyncio.Event()
        # Set when ``_outgoing()`` exits; bounds the inbound close wait.
        outgoing_done = asyncio.Event()
        # Consumer's resume cursor, captured from the first reply's ``from_seq``.
        resume_cursor = 0
        # Set once the reader reaches EOS and stream.end is delivered to the consumer.
        delivered_eos = False

        async def _outgoing() -> AsyncGenerator:
            nonlocal delivered_eos
            try:
                yield init_server
                logger.info(
                    "→ %s sent, waiting for consumer reply before draining outputs",
                    handshake,
                    extra={"task_id": task_id, "mission_id": mission_id, "setup_id": setup_id},
                )
                await output_started.wait()
                logger.info(
                    "✓ Output drain started — streaming module outputs to consumer",
                    extra={"task_id": task_id, "mission_id": mission_id, "setup_id": setup_id},
                )
                idle_timeout = get_gateway_settings().dial_back_idle_timeout_s
                reader_iter = aiter(
                    self._consume_from_redis(task_id, from_seq=resume_cursor if resume else 0, resume=resume)
                )
                while True:
                    try:
                        cli_msg = await asyncio.wait_for(anext(reader_iter), timeout=idle_timeout)
                    except StopAsyncIteration:
                        delivered_eos = True
                        break
                    except asyncio.TimeoutError:
                        logger.warning(
                            "Dial-back idle %.0fs exceeded — no module output, closing BiDi",
                            idle_timeout,
                            extra=log_extra,
                        )
                        await _fail(
                            StreamErrorCode.DIAL_BACK_IDLE_TIMEOUT.value,
                            f"dial-back idle timeout: no module output in {idle_timeout:.0f}s",
                        )
                        return
                    except RedisError:
                        for sc in (
                            self._sentinel(
                                0,
                                task_id,
                                "stream.error",
                                code=StreamErrorCode.REDIS_UNAVAILABLE.value,
                                message="redis unavailable during stream read",
                                fatal=True,
                            ),
                            self._sentinel(0, task_id, "stream.end"),
                        ):
                            yield gateway_pb2.StreamServer(task_id=task_id, seq=sc.from_seq, data=sc.data)
                        return
                    yield gateway_pb2.StreamServer(
                        task_id=task_id,
                        seq=cli_msg.from_seq,
                        data=cli_msg.data,
                    )
            finally:
                outgoing_done.set()

        async def _runner_fatal(code: str, message: str) -> None:
            await self._emit_fatal_to_redis(task_id, code=code, message=message, log_extra=log_extra)

        retriable = False
        t_pre_stream = time.perf_counter_ns()
        logger.info(
            "[dial-debug] pre_stream dt_since_ready=%.3fms ch_state=%s",
            (t_pre_stream - t_stub) / 1e6,
            _ch_state(comm._channel),  # noqa: SLF001
            extra=log_extra,
        )
        try:  # noqa: PLW0717
            logger.info(
                "→ Opening BiDi to consumer %s (sending %s)",
                address,
                handshake,
                extra=log_extra,
            )
            responses = stub.Stream(_outgoing(), timeout=get_gateway_settings().dial_back_max_lifetime_s)
            first = True
            # After `_outgoing()` exits, bound the inbound wait by
            # ``dial_back_close_grace_s`` for non-conforming consumers.
            response_iter = aiter(responses)
            while True:
                grace = get_gateway_settings().dial_back_close_grace_s
                if outgoing_done.is_set():
                    read: Any = asyncio.wait_for(anext(response_iter), timeout=grace)
                else:
                    # Bound a read parked before ``outgoing_done`` fires: once the outputs
                    # (incl. a fatal stream.error+EOS) finish draining, switch to the close-grace
                    # wait instead of parking to the ``dial_back_max_lifetime_s`` RPC deadline.
                    pending = asyncio.ensure_future(anext(response_iter))
                    drained = asyncio.ensure_future(outgoing_done.wait())
                    await asyncio.wait({pending, drained}, return_when=asyncio.FIRST_COMPLETED)
                    drained.cancel()
                    read = pending if pending.done() else asyncio.wait_for(pending, timeout=grace)
                try:
                    upstream = await read
                except StopAsyncIteration:
                    break
                except asyncio.TimeoutError:
                    logger.info(
                        "Consumer didn't close response stream within %.1fs after stream.end — closing BiDi",
                        grace,
                        extra=log_extra,
                    )
                    break

                # Resume: first reply carries the cursor in ``from_seq`` (empty
                # data), so it must be handled before the data gate below.
                if first and resume:
                    limit = get_gateway_settings().stream.from_seq_limit
                    resume_cursor = min(upstream.from_seq, limit)
                    logger.info(
                        "← Consumer resume cursor=%d received — resuming output (no re-run)",
                        resume_cursor,
                        extra=log_extra,
                    )
                    output_started.set()
                    first = False
                    continue

                if not (upstream.data and len(upstream.data.fields) > 0):
                    continue
                if first:
                    logger.info(
                        "← First consumer reply received — starting module runner",
                        extra=log_extra,
                    )
                    if self._module_runner is None:
                        await _fail(
                            StreamErrorCode.DIAL_BACK_INTERNAL.value,
                            "gateway has no ModuleRunner configured",
                        )
                        output_started.set()
                        return False
                    self._spawn(
                        self._module_runner.run(
                            upstream.data,
                            task_id=task_id,
                            setup_id=setup_id,
                            mission_id=mission_id,
                            on_fatal=_runner_fatal,
                        ),
                        name=f"module_runner_{task_id}",
                    )
                    on_runner_spawn()
                    output_started.set()
                    first = False
                    continue
                # Follow-up multi-turn input → task's input stream.
                with contextlib.suppress(RedisError):
                    await self._redis_client.xadd(
                        f"task:{task_id}:input",
                        {"pb": upstream.data.SerializeToString()},
                    )
        except grpc.aio.AioRpcError as exc:
            code_name = exc.code().name
            details = exc.details() or ""
            if exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                # Name which deadline fired (dial_back_max_lifetime_s).
                details = (
                    f"dial-back BiDi hit the {get_gateway_settings().dial_back_max_lifetime_s:.0f}s "
                    f"safety ceiling (dial_back_max_lifetime_s) after "
                    f"{(time.perf_counter_ns() - t_dial0) / 1e9:.1f}s "
                    f"(output_started={output_started.is_set()})"
                )
            logger.warning(
                "dial_consumer BiDi failed: [%s] %s addr=%s",
                code_name,
                details,
                address,
                extra=log_extra,
            )
            if not output_started.is_set():
                await _fail(
                    StreamErrorCode.DIAL_BACK_RPC_ERROR.value,
                    f"dial-back BiDi failed: [{code_name}] {details}",
                )
                # Suppress DIAL_BACK_NO_QUERY in finally — RPC error already emitted.
                output_started.set()
            # Client BiDi died before the stream finished → re-dial candidate.
            retriable = not delivered_eos
        except _GrpcUsageError:
            t_fail = time.perf_counter_ns()
            logger.warning(
                "[dial-debug] UsageError raised dt_total=%.3fms dt_pre_to_call=%.3fms ch_state=%s addr=%s",
                (t_fail - t_dial0) / 1e6,
                (t_fail - t_pre_stream) / 1e6,
                _ch_state(comm._channel),  # noqa: SLF001
                address,
                extra=log_extra,
            )
            if not output_started.is_set():
                await _fail(
                    StreamErrorCode.DIAL_BACK_RPC_ERROR.value,
                    "dial-back channel closed before BiDi could start",
                )
                output_started.set()
            retriable = not delivered_eos
        except (RuntimeError, AssertionError, ValueError):
            logger.exception("dial_consumer unexpected error", extra=log_extra)
            if not output_started.is_set():
                await _fail(
                    StreamErrorCode.DIAL_BACK_INTERNAL.value,
                    "dial-back internal error (see gateway logs)",
                )
                output_started.set()
        finally:
            if not output_started.is_set():
                logger.warning(
                    "Dial-back finished without consumer ever replying (address=%s) — emitting DIAL_BACK_NO_QUERY",
                    address,
                    extra=log_extra,
                )
                await _fail(
                    StreamErrorCode.DIAL_BACK_NO_QUERY.value,
                    "consumer never replied (dial-back BiDi closed without reply)",
                )
            # Unblock _outgoing if consumer never replied.
            output_started.set()
            await release()
        return retriable
