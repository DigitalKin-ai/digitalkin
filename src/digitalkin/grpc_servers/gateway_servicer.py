"""GatewayService gRPC servicer — 3 RPCs, full module isolation.

All inter-module communication flows through the Gateway + Redis.
Modules write their output directly to Redis (no producer-side gRPC);
the Gateway only exposes the consumer-facing surface:

- ``StartStream``: unary. Consumer asks Gateway to dispatch a task.
  Returns ACK + task_id; ``stream.start`` is seeded as the first Redis entry.
- ``Stream``: BiDi. Consumer reads output from Redis via Gateway and may
  send upstream input. Lifecycle is sentinel-based, in-band:
  every event (start, end, error, warning, status) flows as a Struct
  in ``StreamServer.data`` keyed under ``data.root.protocol``.
- ``SendSignal``: unary. CANCEL or INVALIDATE_* via Redis pub/sub.

**Sentinel protocol invariant:** every ``Stream`` call ends with exactly
one ``stream.end`` entry, regardless of how the stream concluded. Fatal
errors are emitted as ``stream.error(fatal=true)`` immediately followed
by ``stream.end``. Recoverable issues use ``stream.error(fatal=false)``
or ``stream.warn`` and the stream continues. ``context.abort`` is never
called on ``Stream`` — uniform observation is the whole point.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import grpc
from agentic_mesh_protocol.gateway.v1 import gateway_pb2
from google.protobuf import struct_pb2
from grpc._cython.cygrpc import UsageError as _GrpcUsageError
from redis.exceptions import RedisError

from digitalkin.core.profiling.step_timer import StepTimer
from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader
from digitalkin.grpc_servers.gateway_constants import (
    DIAL_BACK_BIDI_TIMEOUT_S,
    MAX_FROM_SEQ,
    MAX_STREAMS,
    validate_address,
    validate_id,
)
from digitalkin.grpc_servers.stream_error_codes import StreamErrorCode
from digitalkin.grpc_servers.stream_registry import StreamRegistry
from digitalkin.grpc_servers.stream_session import StreamSession
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.logger import logger
from digitalkin.services.communication.grpc_communication import (
    GrpcCommunication,
    InvalidConsumerAddressError,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

    from digitalkin.core.task_manager.module_runner import ModuleRunner
    from digitalkin.core.task_manager.redis.redis_client import RedisClient
    from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker

# Real (non-TYPE_CHECKING) import — `ModuleRunner` is the orchestrator
# the gateway invokes; we need the runtime symbol.


class GatewayServicer:
    """Inter-module broker with full isolation via Redis.

    Modules only talk to the Gateway. Data flows through Redis Streams.
    Gateway manages session lifecycle and persists output to Redis.
    """

    _registry: StreamRegistry
    _redis_client: RedisClient
    _circuit_breaker: CircuitBreaker | None

    @staticmethod
    def _sentinel(seq: int, task_id: str, protocol: str, **fields: Any) -> Any:
        """Build a StreamClient carrying a control sentinel (server→client wire).

        Under dev2 of agentic-mesh-protocol the Stream RPC is
        ``rpc Stream(stream StreamServer) returns (stream StreamClient)``,
        so the server-side response type is ``StreamClient``. ``from_seq``
        on StreamClient is the wire-equivalent of the old ``seq`` field
        (same tag 1, same uint64).

        ``seq=0`` distinguishes gateway-emitted control entries (validation
        errors, late-consumer rejections) from Redis-replayed entries
        (which start at seq=1). Clients dispatch on
        ``data.root.protocol`` regardless of seq.

        Args:
            seq: Sequence number; 0 for gateway control entries.
            task_id: Task ID echoed on the wire for client routing.
            protocol: Sentinel protocol name (``stream.*``).
            fields: Additional Struct fields under ``data.root``.

        Returns:
            StreamClient proto.
        """
        s = struct_pb2.Struct()
        s.update({"root": {"protocol": protocol, **fields}})
        return gateway_pb2.StreamClient(from_seq=seq, task_id=task_id, data=s)

    async def _fatal_close(self, task_id: str, code: str, message: str) -> AsyncGenerator:
        """Yield ``stream.error(fatal=true)`` then ``stream.end`` and return.

        Args:
            task_id: Task ID this fatal error applies to.
            code: Status code name (``INVALID_ARGUMENT``, ``NOT_FOUND``, ...).
            message: Human-readable detail.

        Yields:
            StreamServer sentinels (error then end).
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
        max_streams: int = MAX_STREAMS,
        circuit_breaker: CircuitBreaker | None = None,
        cache_handler: Any = None,
        client_config: Any = None,
        module_runner: ModuleRunner | None = None,
    ) -> None:
        """Initialize the gateway servicer.

        Args:
            redis_client: Redis for stream persistence and signals.
            max_streams: Maximum concurrent sessions (cluster-wide with Redis).
            circuit_breaker: Optional circuit breaker for recording success/failure.
            cache_handler: Async callback for cache invalidation signals (from ModuleServer).
            client_config: ClientConfig for outbound dial-back to consumers
                (used by ``_dial_consumer``).
            module_runner: Orchestrator invoked by ``_dial_consumer`` after the
                consumer's first reply lands. Required in embedded mode; the
                dial-back is the sole entry point for module execution and
                cannot proceed without it.
        """
        self._registry = StreamRegistry(redis_client, max_streams=max_streams)
        self._circuit_breaker = circuit_breaker
        self._redis_client = redis_client
        self._cache_handler = cache_handler
        self._client_config = client_config
        self._module_runner = module_runner

    def _spawn(self, coro: Any, *, name: str) -> asyncio.Task[Any]:
        """Schedule ``coro`` as a fire-and-forget task supervised by the reaper.

        Hands the task to ``StreamRegistry.monitor_task`` which keeps a strong
        reference, logs unhandled exceptions, and cancels still-running tasks
        on shutdown. No bookkeeping is duplicated at the servicer level.

        Args:
            coro: Coroutine to schedule.
            name: asyncio task name (for logs and ``[lat-audit]``).

        Returns:
            The created task.
        """
        task = asyncio.create_task(coro, name=name)
        self._registry.monitor_task(task)
        return task

    async def start(self) -> None:
        """No-op start hook (no periodic reaper anymore — done-callbacks reap)."""

    async def stop(self) -> None:
        """Shut down the registry — which cancels monitored tasks and reaps sessions."""
        await self._registry.shutdown()

    async def StartStream(
        self,
        request: Any,
        context: grpc.aio.ServicerContext,
    ) -> Any:
        """Register a task session and dispatch the module in background.

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

        err = (
            validate_id(task_id, "task_id")
            or validate_id(request.setup_id, "setup_id")
            or validate_id(request.mission_id, "mission_id")
        )
        timer.mark("validate_ids")
        if err is not None:
            logger.warning("Invalid ID in StartStream: %s", err, extra=log_extra)
            return gateway_pb2.StartStreamResponse(accepted=False, task_id=task_id)

        # Validate dial-back address up front, before any side effects.
        md = dict(context.invocation_metadata() or [])
        raw_address = md.get("x-client-address", "")
        if isinstance(raw_address, bytes):
            raw_address = raw_address.decode("utf-8", errors="replace")
        client_address = raw_address.strip()
        addr_err = validate_address(client_address, "x-client-address")
        timer.mark("validate_address")
        if addr_err is not None:
            logger.warning(
                "StartStream rejected: %s (value=%r)",
                addr_err,
                client_address,
                extra=log_extra,
            )
            return gateway_pb2.StartStreamResponse(accepted=False, task_id=task_id)

        # Dedup: if session already exists locally, return existing
        if self._registry.get(task_id) is not None:
            logger.debug("Dedup: session exists, reusing", extra=log_extra)
            return gateway_pb2.StartStreamResponse(accepted=True, task_id=task_id)
        timer.mark("dedup_check")

        session = StreamSession(task_id=task_id)
        accepted = await self._registry.register(
            session,
            setup_id=request.setup_id,
            mission_id=request.mission_id,
        )
        timer.mark("registry_register")
        if not accepted:
            logger.warning("Session rejected (capacity)", extra=log_extra)
            return gateway_pb2.StartStreamResponse(accepted=False, task_id=task_id)

        # Seed output stream with stream.start so Stream's first XREAD finds
        # data immediately, advancing the cursor to a real entry ID. Direct
        # XADD (not ProtoStreamWriter) for guaranteed immediate write.
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
        await self._redis_client.xadd(
            f"task:{task_id}:stream",
            {"pb": start_info.SerializeToString(), "seq": "0"},
        )
        timer.mark("xadd_stream_start")

        # Server-initiated dial-back to consumer (mandatory; address
        # validated at the top of this method). The dial-back IS the
        # dispatcher: it opens the BiDi, receives the consumer's first
        # reply (the query), and runs the module via ``ModuleRunner.run``.
        # No second background task — there is no separate dispatch flow.
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

        The consumer's ``_consume_from_redis`` loop yields the error
        Struct and emits the terminating ``stream.end`` itself when the
        reader exits on EOS. This is the single point that converts a
        server-side failure into the in-band protocol error consumers
        can observe — never raise out of the dial-back background task.

        Args:
            task_id: Task whose stream gets the error.
            code: Stable code from :class:`StreamErrorCode`.
            message: Human-readable detail.
            log_extra: ``{"task_id", "setup_id", "mission_id"}`` for log
                correlation. Code is in the message string per the
                project rule on ``extra=`` containing only global IDs.
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

    async def Stream(  # noqa: C901, PLR0912
        self,
        request_iterator: AsyncIterator[Any],
        context: grpc.aio.ServicerContext,  # noqa: ARG002
    ) -> AsyncGenerator[Any, None]:
        """BiDi: client sends StreamServer messages, server yields StreamClient.

        Dev2 of agentic-mesh-protocol defines the RPC as
        ``rpc Stream(stream StreamServer) returns (stream StreamClient)``.
        The naming is inverted from intuition: ``StreamServer`` is the
        message **sent to** the server (upstream input + resume cursor)
        and ``StreamClient`` is the message **sent to** the client
        (module output + lifecycle sentinels).

        First StreamServer identifies the task (``task_id``), carries the
        resume cursor in ``seq``, and carries the query in ``data``; the
        Struct is delivered to the SDK module as its first input.
        Subsequent StreamServer messages provide additional upstream input
        (optional).

        Server emits StreamClient per Redis stream entry. First entry is
        ``stream.start`` (seeded by StartStream); last is ``stream.end``.
        Errors are emitted as ``stream.error(fatal=true)`` followed by
        ``stream.end`` — never via ``context.abort``.

        Args:
            request_iterator: BiDi stream of StreamServer from the client.
            context: gRPC service context.

        Yields:
            StreamClient — sentinels and SDK module output.
        """
        try:
            first_msg = await anext(request_iterator)
        except StopAsyncIteration:
            return

        task_id = first_msg.task_id
        from_seq = first_msg.seq

        if validate_id(task_id, "task_id") is not None:
            async for out in self._fatal_close(task_id, "INVALID_ARGUMENT", "invalid task_id"):
                yield out
            return

        if from_seq > MAX_FROM_SEQ:
            async for out in self._fatal_close(task_id, "INVALID_ARGUMENT", "seq out of range"):
                yield out
            return

        session = self._registry.get(task_id)

        # Late client: session already finished but data is in Redis.
        if session is None:
            stream_len = await self._redis_client.xlen(f"task:{task_id}:stream")
            if stream_len > 0:
                async for resp in self._consume_from_redis(task_id, from_seq):
                    yield resp
                return
            async for out in self._fatal_close(task_id, "NOT_FOUND", "task not found"):
                yield out
            return

        # Deliver first message's data (the query) to the input stream.
        input_key = f"task:{task_id}:input"
        if first_msg.data and len(first_msg.data.fields) > 0:
            await self._redis_client.xadd(
                input_key,
                {"pb": first_msg.data.SerializeToString()},
            )

        # Background: read additional upstream messages → Redis input stream
        upstream_task = self._spawn(
            self._read_peer_upstream(request_iterator, task_id, session),
            name=f"peer_upstream_{task_id}",
        )

        # Stream output downstream to client
        try:
            async for resp in self._consume_from_redis(task_id, from_seq):
                yield resp
        finally:
            if not upstream_task.done():
                upstream_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await upstream_task
            # Clean up session after client finishes reading
            removed = await self._registry.unregister(task_id)
            if removed is not None:
                await removed.teardown()

    async def _read_peer_upstream(
        self,
        request_iterator: AsyncIterator,
        task_id: str,
        session: StreamSession,
    ) -> None:
        """Read peer-initiated upstream StreamClient messages and XADD raw
        proto bytes onto the task's input stream. Empty Structs skipped.

        The first message's data is consumed by :meth:`Stream` itself
        (it's the query); this drains the rest.

        Args:
            request_iterator: BiDi stream of StreamClient from the client.
            task_id: Task identifier (used for the Redis input stream key).
            session: Stream session for the stop-event check.
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

    async def SendSignal(  # noqa: PLR0911
        self,
        request: Any,
        context: grpc.aio.ServicerContext,  # noqa: ARG002
    ) -> Any:
        """Forward control signal via Redis pub/sub, or dispatch cache invalidation.

        Cache invalidation signals (INVALIDATE_*) are handled directly by
        ModuleServer — no task_id, no session, no Redis pub/sub.
        Task signals (CANCEL) require task_id + active session.

        Args:
            request: ClientSignalRequest proto.
            context: gRPC service context.

        Returns:
            ClientSignalResponse proto.
        """
        action_name = gateway_pb2.SignalAction.Name(request.action)

        # Cache invalidation — server-wide, handled by ModuleServer
        if action_name.startswith("INVALIDATE_"):
            if self._cache_handler is not None:
                try:
                    await self._cache_handler(action_name)
                    return gateway_pb2.ClientSignalResponse(success=True, task_id="")
                except Exception:
                    logger.exception("Cache invalidation failed: %s", action_name)
                    return gateway_pb2.ClientSignalResponse(success=False, task_id="")
            return gateway_pb2.ClientSignalResponse(success=False, task_id="")

        # Task signals — require task_id + session
        task_id = request.task_id
        if validate_id(task_id, "task_id") is not None:
            return gateway_pb2.ClientSignalResponse(success=False, task_id=task_id)

        action_lower = action_name.lower()
        session = self._registry.get(task_id)
        if session is None:
            logger.warning("SendSignal: task not found: %s", task_id)
            return gateway_pb2.ClientSignalResponse(success=False, task_id=task_id)

        try:
            payload = json.dumps({"action": action_lower, "task_id": task_id})
            await self._redis_client.publish(f"signal_ch:{task_id}", payload)
        except RedisError:
            logger.exception("SendSignal Redis publish failed: task_id=%s action=%s", task_id, action_lower)
            return gateway_pb2.ClientSignalResponse(success=False, task_id=task_id)
        return gateway_pb2.ClientSignalResponse(success=True, task_id=task_id)

    async def _consume_from_redis(
        self,
        task_id: str,
        from_seq: int,
    ) -> AsyncGenerator:
        """Zero-copy read from Redis Stream for the consumer.

        Proto Struct bytes go directly from Redis to gRPC response —
        no dict conversion, no JSON parsing. ~0.1-0.5ms per message
        instead of ~3-8ms on the JSON path.

        Yields ``StreamClient`` messages (the dev2 server→client wire
        type). The ``from_seq`` field carries the monotonic sequence
        number — same tag as the legacy ``StreamServer.seq``. Lifecycle
        is encoded in ``data.root.protocol`` sentinels written by the
        producer (``stream.start`` already seeded by StartStream;
        ``stream.end`` emitted explicitly below on reader EOS).

        Callers that need a ``StreamServer`` (the dial-back outbound
        direction) re-wrap each yielded message; both messages share
        identical field tags so the conversion is a field rename.

        Args:
            task_id: Task reference ID.
            from_seq: Resume point.

        Yields:
            StreamClient messages.
        """
        t0 = time.perf_counter_ns()
        reader = ProtoStreamReader(task_id, self._redis_client)  # type: ignore[arg-type]
        if from_seq > 0:
            await reader.restore_cursor()
        t1 = time.perf_counter_ns()

        seq = from_seq
        first = True

        async for struct_data in reader.read_structs():
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
            seq += 1
            yield gateway_pb2.StreamClient(from_seq=seq, task_id=task_id, data=struct_data)

        # Reader exited because EOS was hit (Redis `{"eos": "true"}` marker is
        # consumed silently by ProtoStreamReader). Emit an explicit stream.end
        # sentinel so the wire contract is uniform: every stream ends with
        # exactly one stream.end entry, regardless of how it concluded.
        t_after_reader = time.perf_counter_ns()
        seq += 1
        yield self._sentinel(seq, task_id, "stream.end")
        t_after_yield = time.perf_counter_ns()
        logger.info(
            "[close-debug] gateway_stream_end: reader_to_yield=%.2fms t_yielded_ns=%d task_id=%s",
            (t_after_yield - t_after_reader) / 1e6,
            t_after_yield,
            task_id,
        )

    async def _dial_consumer(  # noqa: C901, PLR0912, PLR0915
        self,
        task_id: str,
        mission_id: str,
        setup_id: str,
        address: str,
    ) -> None:
        """Dial the consumer's GatewayService.Stream and run the BiDi.

        Flow:

        1. Send ``stream.init`` as the first ``StreamServer``.
        2. Read the consumer's first ``StreamServer`` — that's the query.
        3. Push the query onto ``session.input_queue`` so the dispatcher
           unblocks (matches the M2M client-initiated path).
        4. Concurrently:

           - forward subsequent ``StreamClient`` messages from the
             consumer → ``session.input_queue`` (multi-turn input).
           - pull module outputs from ``_consume_from_redis`` and push
             each as a ``StreamServer(task_id, from_seq=<seq>, data=…)``
             to the consumer.

        5. End cleanly when ``stream.end`` flows through.

        A fresh ``GrpcCommunication`` is built per call so logging context
        (mission_id, setup_id) reflects the actual task. The underlying
        gRPC channel is still pooled at the ``GrpcClientWrapper`` class
        level — multiple concurrent tasks dialing the same consumer share
        one HTTP/2 connection.

        Args:
            task_id: Task to push.
            mission_id: Mission ID (carried for logging context).
            setup_id: Setup ID (carried for logging context).
            address: ``"host:port"`` of the consumer's GatewayService.
        """
        log_extra = {"task_id": task_id, "setup_id": setup_id, "mission_id": mission_id}
        cfg = self._client_config
        if cfg is None:
            logger.error("Dial-back unavailable: no client_config configured", extra=log_extra)
            await self._emit_fatal_to_redis(
                task_id,
                code=StreamErrorCode.DIAL_BACK_INTERNAL.value,
                message="gateway has no client_config to dial back with",
                log_extra=log_extra,
            )
            return
        # setup_version_id is unknown at StartStream time and is not used
        # by the dial-back path (no setup-version-scoped resources are
        # touched here). Pass the empty string to make that explicit.
        comm = GrpcCommunication(
            mission_id=mission_id,
            setup_id=setup_id,
            setup_version_id="",
            client_config=cfg,
        )
        t_dial0 = time.perf_counter_ns()
        try:
            stub, release = comm.dial_consumer_stream(address)
        except InvalidConsumerAddressError as exc:
            # Defence-in-depth: StartStream's validate_address should have
            # caught this. Reaching here means the contract drifted.
            logger.exception("dial_consumer: invalid address %r", address, extra=log_extra)
            await self._emit_fatal_to_redis(
                task_id,
                code=StreamErrorCode.DIAL_BACK_UNREACHABLE.value,
                message=f"dial-back channel build failed: {exc}",
                log_extra=log_extra,
            )
            return
        except OSError as exc:
            logger.exception("dial_consumer: channel build failed addr=%s", address, extra=log_extra)
            await self._emit_fatal_to_redis(
                task_id,
                code=StreamErrorCode.DIAL_BACK_UNREACHABLE.value,
                message=f"dial-back channel build failed: {type(exc).__name__}: {exc}",
                log_extra=log_extra,
            )
            return
        t_stub = time.perf_counter_ns()
        logger.info("→ Dial-back channel ready to %s", address, extra=log_extra)

        def _ch_state(chan: Any) -> str:
            """Best-effort connectivity probe — returns enum name or '?' on failure."""
            try:
                state = chan.get_state(try_to_connect=False)
                return getattr(state, "name", str(state))
            except Exception as exc:  # noqa: BLE001
                return f"err:{type(exc).__name__}"

        logger.info(
            "[dial-debug] channel_ready dt_init=%.3fms ch_state=%s channel_id=%s ref_count=%d cache_keys=%d",
            (t_stub - t_dial0) / 1e6,
            _ch_state(comm._channel),
            id(comm._channel),
            GrpcClientWrapper._ref_counts.get(comm._channel_cache_key, 0),
            len(GrpcClientWrapper._channel_cache),
            extra=log_extra,
        )

        session = self._registry.get(task_id)
        if session is None:
            logger.warning(
                "Dial-back aborted — session disappeared before channel was ready",
                extra=log_extra,
            )
            await release()
            return

        # Dial-back contract under dev2's RPC signature
        # ``Stream(stream StreamServer) returns (stream StreamClient)``:
        # - gateway is the gRPC client and sends ``StreamServer``
        #   (module output + sentinels) on the request stream.
        # - gateway receives ``StreamClient`` (query + follow-up upstream)
        #   on the response stream.
        # ``_consume_from_redis`` yields ``StreamClient`` for the regular
        # consumer-facing path; here we re-wrap each one into the
        # ``StreamServer`` the dial-back wire expects (field tags are
        # identical so it's a rename: ``from_seq`` → ``seq``).
        init_struct = struct_pb2.Struct()
        init_struct.update({"root": {"protocol": "stream.init"}})
        init_server = gateway_pb2.StreamServer(seq=0, task_id=task_id, data=init_struct)

        # Gate the output drain on the first upstream message arriving:
        # the dispatcher can't produce anything until the query lands on
        # session.input_queue, which happens when we receive the consumer's
        # first StreamClient reply (carrying the query).
        output_started = asyncio.Event()

        async def _outgoing() -> AsyncGenerator:
            yield init_server
            logger.info(
                "→ stream.init sent, waiting for consumer query before draining outputs",
                extra={"task_id": task_id, "mission_id": mission_id, "setup_id": setup_id},
            )
            await output_started.wait()
            logger.info(
                "✓ Output drain started — streaming module outputs to consumer",
                extra={"task_id": task_id, "mission_id": mission_id, "setup_id": setup_id},
            )
            async for cli_msg in self._consume_from_redis(task_id, from_seq=0):
                yield gateway_pb2.StreamServer(
                    task_id=task_id,
                    seq=cli_msg.from_seq,
                    data=cli_msg.data,
                )

        async def _runner_fatal(code: str, message: str) -> None:
            await self._emit_fatal_to_redis(task_id, code=code, message=message, log_extra=log_extra)

        # No per-stream heartbeat task: the dial-back's existence is itself
        # the proof of liveness. The session's heartbeat zset entry is seeded
        # at register() and ZREM'd at unregister() (end-of-stream cleanup in
        # this method's finally). The registry reaper backstops only the
        # abnormal case where this finally never runs.
        t_pre_stream = time.perf_counter_ns()
        logger.info(
            "[dial-debug] pre_stream dt_since_ready=%.3fms ch_state=%s",
            (t_pre_stream - t_stub) / 1e6,
            _ch_state(comm._channel),
            extra=log_extra,
        )
        try:
            logger.info(
                "→ Opening BiDi to consumer %s (sending stream.init)",
                address,
                extra=log_extra,
            )
            responses = stub.Stream(_outgoing(), timeout=DIAL_BACK_BIDI_TIMEOUT_S)
            first = True
            # `responses` items are deserialised by gRPC as the proto's
            # response type (StreamServer), but per the dial-back contract
            # the consumer is sending StreamClient bytes — the field tags
            # match so reading `.data` / `.task_id` works either way. The
            # consumer-emitted message is the query (first) or follow-up
            # upstream input (subsequent).
            async for upstream in responses:
                if not (upstream.data and len(upstream.data.fields) > 0):
                    continue
                if first:
                    logger.info(
                        "← First consumer reply received — starting module runner",
                        extra=log_extra,
                    )
                    if self._module_runner is None:
                        await self._emit_fatal_to_redis(
                            task_id,
                            code=StreamErrorCode.DIAL_BACK_INTERNAL.value,
                            message="gateway has no ModuleRunner configured",
                            log_extra=log_extra,
                        )
                        output_started.set()
                        return
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
                    output_started.set()
                    first = False
                    continue
                # Follow-up multi-turn input: XADD raw proto bytes to the
                # task's input stream. Modules that opt in to multi-turn
                # input consume it via ProtoStreamReader on this key.
                await self._redis_client.xadd(
                    f"task:{task_id}:input",
                    {"pb": upstream.data.SerializeToString()},
                )
        except grpc.aio.AioRpcError as exc:
            code_name = exc.code().name
            details = exc.details() or ""
            logger.warning(
                "dial_consumer BiDi failed: [%s] %s addr=%s",
                code_name,
                details,
                address,
                extra=log_extra,
            )
            if not output_started.is_set():
                await self._emit_fatal_to_redis(
                    task_id,
                    code=StreamErrorCode.DIAL_BACK_RPC_ERROR.value,
                    message=f"dial-back BiDi failed: [{code_name}] {details}",
                    log_extra=log_extra,
                )
                # Suppress the DIAL_BACK_NO_QUERY in the finally block —
                # we already emitted the more specific RPC error.
                output_started.set()
        except _GrpcUsageError:
            t_fail = time.perf_counter_ns()
            logger.warning(
                "[dial-debug] UsageError raised dt_total=%.3fms dt_pre_to_call=%.3fms ch_state=%s addr=%s",
                (t_fail - t_dial0) / 1e6,
                (t_fail - t_pre_stream) / 1e6,
                _ch_state(comm._channel),
                address,
                extra=log_extra,
            )
            if not output_started.is_set():
                await self._emit_fatal_to_redis(
                    task_id,
                    code=StreamErrorCode.DIAL_BACK_RPC_ERROR.value,
                    message="dial-back channel closed before BiDi could start",
                    log_extra=log_extra,
                )
                output_started.set()
        except (RuntimeError, AssertionError, ValueError):
            logger.exception("dial_consumer unexpected error", extra=log_extra)
            if not output_started.is_set():
                await self._emit_fatal_to_redis(
                    task_id,
                    code=StreamErrorCode.DIAL_BACK_INTERNAL.value,
                    message="dial-back internal error (see gateway logs)",
                    log_extra=log_extra,
                )
                output_started.set()
        finally:
            if not output_started.is_set():
                logger.warning(
                    "Dial-back finished without consumer ever sending a query "
                    "(address=%s) — emitting DIAL_BACK_NO_QUERY",
                    address,
                    extra=log_extra,
                )
                await self._emit_fatal_to_redis(
                    task_id,
                    code=StreamErrorCode.DIAL_BACK_NO_QUERY.value,
                    message="consumer never sent the query (dial-back BiDi closed without reply)",
                    log_extra=log_extra,
                )
            # Defensive: unblock _outgoing if consumer never replied.
            output_started.set()
            await release()
            # End-of-stream cleanup: mirror the consumer-side Stream RPC's
            # finally (gateway_servicer.py:426-434). The Redis output stream
            # `task:{id}:stream` is left intact for replay/resume by a
            # future consumer Stream RPC.
            try:
                removed = await self._registry.unregister(task_id)
                if removed is not None:
                    await removed.teardown()
            except Exception:  # noqa: BLE001 — finally must not raise out
                logger.exception("end-of-stream unregister failed", extra=log_extra)
