"""GatewayService gRPC servicer — 4 RPCs, full module isolation.

All M2M communication flows through the Gateway + Redis. Modules never
see each other directly.

- ``StartStream``: unary. Client requests, Gateway starts the module,
  returns ACK + task_id.
- ``ProduceStream``: BiDi. Module A sends output to Gateway → Redis.
  Gateway forwards Module B's data from Redis → Module A.
- ``ConsumeStream``: BiDi. Module B reads output from Redis via Gateway.
  Module B sends data → Gateway → Redis → Module A reads.
- ``SendSignal``: unary. Cancel/pause via Redis pub/sub.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from agentic_mesh_protocol.gateway.v1 import gateway_pb2
from google.protobuf import json_format, struct_pb2, timestamp_pb2

from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter
from digitalkin.grpc_servers.gateway_constants import (
    MAX_FROM_SEQ,
    MAX_STREAMS,
    STREAM_BATCH_SIZE,
    validate_id,
)
from digitalkin.grpc_servers.stream_registry import StreamRegistry
from digitalkin.grpc_servers.stream_session import StreamSession
from digitalkin.logger import logger

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

    import grpc

    from digitalkin.core.task_manager.redis.redis_client import RedisClient
    from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker


class GatewayServicer:
    """M2M broker with full module isolation via Redis.

    Modules only talk to the Gateway. Data flows through Redis Streams.
    Gateway manages session lifecycle and persists output to Redis.
    """

    _registry: StreamRegistry
    _redis_client: RedisClient | None
    _circuit_breaker: CircuitBreaker | None
    _dispatch_key: str

    @staticmethod
    def _output_response(task_id: str, job_id: str, data: dict[str, Any], seq: int) -> Any:
        """Build StreamOutput GatewayResponse.

        Returns:
            GatewayResponse proto.
        """
        s = struct_pb2.Struct()
        s.update(data)
        ts = timestamp_pb2.Timestamp()
        ts.FromDatetime(datetime.now(tz=timezone.utc))
        return gateway_pb2.GatewayResponse(
            output=gateway_pb2.StreamOutput(
                task_id=task_id,
                job_id=job_id,
                data=s,
                seq=seq,
                timestamp=ts,
            ),
        )

    @staticmethod
    def _status_response(task_id: str, job_id: str, state: int) -> Any:
        """Build StreamStatus GatewayResponse.

        Returns:
            GatewayResponse proto.
        """
        return gateway_pb2.GatewayResponse(
            status=gateway_pb2.StreamStatus(
                task_id=task_id,
                job_id=job_id,
                state=state,  # type: ignore[arg-type]
            ),
        )

    @staticmethod
    def _error_response(task_id: str, job_id: str, code: int, message: str) -> Any:
        """Build StreamError GatewayResponse.

        Returns:
            GatewayResponse proto.
        """
        return gateway_pb2.GatewayResponse(
            error=gateway_pb2.StreamError(
                task_id=task_id,
                job_id=job_id or "",
                code=code,
                message=message,
            ),
        )

    def __init__(
        self,
        redis_client: RedisClient | None = None,
        max_streams: int = MAX_STREAMS,
        circuit_breaker: CircuitBreaker | None = None,
        dispatch_key: str = "dispatch:module",
    ) -> None:
        """Initialize the gateway servicer.

        Args:
            redis_client: Redis for stream persistence, dispatch, and signals.
            max_streams: Maximum concurrent sessions (cluster-wide with Redis).
            circuit_breaker: Optional circuit breaker for recording success/failure.
            dispatch_key: Redis Stream key for task dispatch to the module.

        Raises:
            RuntimeError: If redis_client is None.
        """
        self._registry = StreamRegistry(max_streams=max_streams, redis_client=redis_client)
        self._circuit_breaker = circuit_breaker
        self._redis_client = redis_client
        self._dispatch_key = dispatch_key

        if redis_client is None:
            msg = "GatewayServicer: no Redis — gateway requires Redis for production use"
            raise RuntimeError(msg)

    async def start(self) -> None:
        """Start the registry reaper."""
        await self._registry.start_reaper()

    async def stop(self) -> None:
        """Shut down all sessions and the reaper."""
        await self._registry.shutdown()

    async def StartStream(
        self,
        request: Any,
        context: grpc.aio.ServicerContext,  # noqa: ARG002
    ) -> Any:
        """Register a task session and start the module in background.

        Args:
            request: StartStreamRequest proto.
            context: gRPC service context.

        Returns:
            StartStreamResponse(task_id, accepted).
        """
        task_id = request.task_id

        err = (
            validate_id(task_id, "task_id")
            or validate_id(request.setup_id, "setup_id")
            or validate_id(request.mission_id, "mission_id")
        )
        if err is not None:
            logger.warning("Invalid ID in StartStream: %s", err)
            return gateway_pb2.StartStreamResponse(task_id=task_id, accepted=False)

        # Dedup: if session already exists locally, return existing
        if self._registry.get(task_id) is not None:
            logger.debug("Dedup: session exists for task_id=%s, reusing", task_id)
            return gateway_pb2.StartStreamResponse(task_id=task_id, accepted=True)

        session = StreamSession(task_id=task_id)
        accepted = await self._registry.register(
            session,
            setup_id=request.setup_id,
            mission_id=request.mission_id,
        )
        if not accepted:
            logger.warning("Session rejected (capacity): task_id=%s", task_id)
            return gateway_pb2.StartStreamResponse(task_id=task_id, accepted=False)

        # Start module in background
        session._forward_task = asyncio.create_task(  # noqa: SLF001
            self._start_module(session, request),
            name=f"start_module_{task_id}",
        )

        logger.info(
            "Task accepted: task_id=%s active_sessions=%d",
            task_id,
            self._registry.active_count,
        )
        return gateway_pb2.StartStreamResponse(task_id=task_id, accepted=True)

    def _cb_success(self) -> None:
        """Record circuit breaker success if configured."""
        if self._circuit_breaker is not None:
            self._circuit_breaker.record_success()

    def _cb_failure(self) -> None:
        """Record circuit breaker failure if configured."""
        if self._circuit_breaker is not None:
            self._circuit_breaker.record_failure()

    async def _start_module(self, session: StreamSession, request: Any) -> None:
        """Dispatch module execution via Redis.

        XADDs task spec to the dispatch stream. The TaskDispatcher picks
        it up, runs the module, and writes output to the proto stream.
        Gateway reads output via ProtoStreamReader in ConsumeStream.

        Args:
            session: The stream session.
            request: The StartStreamRequest.
        """
        try:
            await self._redis_client.xadd(
                self._dispatch_key,
                {
                    "task_id": session.task_id,
                    "pb": request.input.SerializeToString(),
                    "setup_id": request.setup_id,
                    "mission_id": request.mission_id,
                },
            )
            self._cb_success()
            logger.debug("Task dispatched to Redis: task_id=%s", session.task_id)
        except Exception:
            logger.exception("Task dispatch failed: task_id=%s", session.task_id)
            self._cb_failure()
            # Write EOS so ConsumeStream doesn't hang
            try:
                proto_writer = ProtoStreamWriter(session.task_id, self._redis_client)
                await proto_writer.write_eos()
            except Exception:
                logger.exception("Fallback EOS failed: task_id=%s", session.task_id)

    async def ProduceStream(
        self,
        request_iterator: AsyncIterator,
        context: grpc.aio.ServicerContext,  # noqa: ARG002
    ) -> AsyncGenerator:
        """Module A sends output, Gateway persists to Redis.

        Gateway sends Module B's data back to A.

        Args:
            request_iterator: BiDi stream from Module A.
            context: gRPC service context.

        Yields:
            ProduceStreamResponse — forwarded data from Module B.
        """
        # Read init
        try:
            first_msg = await anext(request_iterator)
        except StopAsyncIteration:
            return

        if first_msg.WhichOneof("payload") != "init":
            return

        task_id = first_msg.init.task_id
        if validate_id(task_id, "task_id") is not None:
            return

        session = self._registry.get(task_id)
        if session is None:
            return

        # Background: persist Module A's output to Redis Stream (zero-copy proto path)
        persist_task = asyncio.create_task(
            self._persist_producer_output(task_id, request_iterator, session, self._redis_client),
            name=f"persist_{task_id}",
        )

        # Forward Module B's data from input_queue → Module A
        reusable_struct = struct_pb2.Struct()
        try:
            while not session._stop_event.is_set() and not persist_task.done():  # noqa: SLF001
                try:
                    item = await asyncio.wait_for(session.input_queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    if persist_task.done():
                        logger.info("Producer stream closed: task_id=%s", task_id)
                        break
                    continue
                if item is None:
                    break

                reusable_struct.Clear()
                reusable_struct.update(item)
                yield gateway_pb2.ProduceStreamResponse(
                    data=gateway_pb2.ProduceStreamData(task_id=task_id, data=reusable_struct),
                )
        finally:
            if not persist_task.done():
                persist_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await persist_task

    @staticmethod
    async def _persist_producer_output(
        task_id: str,
        request_iterator: AsyncIterator,
        session: StreamSession,
        redis_client: RedisClient | None,
    ) -> None:
        """Read Module A's output from BiDi and persist to Redis Stream.

        Zero-copy hot path: proto Struct bytes go directly to Redis
        via ``ProtoStreamWriter.write_struct()`` — no dict conversion,
        no JSON encoding. ~0.1-0.5ms per message instead of ~6-23ms.

        Args:
            task_id: Task reference ID.
            request_iterator: BiDi stream from Module A.
            session: Stream session for output_queue fallback.
            redis_client: Redis client for proto stream persistence.
        """
        proto_writer = None
        if redis_client is not None:
            from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter

            proto_writer = ProtoStreamWriter(task_id, redis_client)
            # Continue after any entries already in the stream
            await proto_writer.restore_seq()

        seq = 1
        try:
            async for msg in request_iterator:
                if session._stop_event.is_set():  # noqa: SLF001
                    break
                payload_type = msg.WhichOneof("payload")
                if payload_type == "output":
                    seq += 1
                    if proto_writer is not None:
                        # Zero-copy: proto Struct → binary bytes → Redis
                        await proto_writer.write_struct(msg.output.data)
                    else:
                        # Fallback: dict → output_queue (in-memory)
                        data_dict = json_format.MessageToDict(msg.output.data)
                        data_dict["_seq"] = seq
                        await session.enqueue_output(data_dict)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Producer output persist error: task_id=%s", task_id)
        finally:
            if proto_writer is not None:
                await proto_writer.write_eos()

    async def ConsumeStream(  # noqa: C901, PLR0911, PLR0912
        self,
        request_iterator: AsyncIterator,
        context: grpc.aio.ServicerContext,  # noqa: ARG002
    ) -> AsyncGenerator:
        """Module B reads output from Redis (or queue), sends data to Module A.

        Args:
            request_iterator: BiDi stream from Module B.
            context: gRPC service context.

        Yields:
            GatewayResponse — output from Module A.
        """
        # Read init
        try:
            first_msg = await anext(request_iterator)
        except StopAsyncIteration:
            return

        if first_msg.WhichOneof("payload") != "init":
            yield self._error_response("", "", 3, "First message must be ConsumeStreamInit")
            return

        task_id = first_msg.init.task_id
        from_seq = first_msg.init.from_seq

        err = validate_id(task_id, "task_id")
        if err is not None:
            yield self._error_response(task_id, "", 3, "Invalid request")
            return

        if from_seq < 0 or from_seq > MAX_FROM_SEQ:
            yield self._error_response(task_id, "", 3, "Invalid request parameters")
            return

        session = self._registry.get(task_id)

        # Late consumer: session already finished but data is in Redis.
        # Read directly from Redis without requiring an active session.
        if session is None and self._redis_client is not None:
            stream_len = await self._redis_client.xlen(f"task:{task_id}:stream")
            if stream_len > 0:
                async for resp in self._consume_from_redis(task_id, from_seq):
                    yield resp
                return
            yield self._error_response(task_id, "", 5, "Task not found")
            return

        if session is None:
            yield self._error_response(task_id, "", 5, "Task not found")
            return

        # Background: read Module B's upstream data → input_queue → Module A
        upstream_task = asyncio.create_task(
            self._read_consumer_upstream(request_iterator, session),
            name=f"upstream_{task_id}",
        )

        # Stream output downstream to Module B (Redis only — no queue fallback)
        try:
            if self._redis_client is None:
                yield self._error_response(task_id, "", 13, "Service unavailable")
                return
            async for resp in self._consume_from_redis(task_id, from_seq):
                yield resp
        finally:
            if not upstream_task.done():
                upstream_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await upstream_task
            # Clean up session after consumer finishes reading
            removed = await self._registry.unregister(task_id)
            if removed is not None:
                await removed.teardown()

    @staticmethod
    async def _read_consumer_upstream(
        request_iterator: AsyncIterator,
        session: StreamSession,
    ) -> None:
        """Read Module B's data and put on input_queue for Module A.

        Args:
            request_iterator: BiDi stream from Module B.
            session: Stream session with input_queue.
        """
        try:
            async for msg in request_iterator:
                if session._stop_event.is_set():  # noqa: SLF001
                    break
                payload_type = msg.WhichOneof("payload")
                if payload_type == "data":
                    # Keep proto Struct as-is — avoid MessageToDict conversion
                    await session.enqueue_input({"_proto": msg.data.data})
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception("Consumer upstream reader error: task_id=%s", session.task_id)

    async def SendSignal(
        self,
        request: Any,
        context: grpc.aio.ServicerContext,  # noqa: ARG002
    ) -> Any:
        """Forward control signal via Redis pub/sub.

        Args:
            request: ClientSignalRequest proto.
            context: gRPC service context.

        Returns:
            ClientSignalResponse proto.
        """
        task_id = request.task_id
        if validate_id(task_id, "task_id") is not None:
            return gateway_pb2.ClientSignalResponse(success=False, task_id=task_id)

        action_value = request.action
        action_name = gateway_pb2.SignalAction.Name(action_value).removeprefix("SIGNAL_ACTION_").lower()

        session = self._registry.get(task_id)
        if session is None:
            logger.warning("SendSignal: task not found: %s", task_id)
            return gateway_pb2.ClientSignalResponse(success=False, task_id=task_id)

        try:
            payload = json.dumps({"action": action_name, "task_id": task_id})
            await self._redis_client.publish(f"signal_ch:{task_id}", payload)
        except Exception:
            logger.exception("SendSignal Redis publish failed: task_id=%s action=%s", task_id, action_name)
            return gateway_pb2.ClientSignalResponse(success=False, task_id=task_id)
        return gateway_pb2.ClientSignalResponse(success=True, task_id=task_id)

    async def _consume_from_redis(
        self,
        task_id: str,
        from_seq: int,
    ) -> AsyncGenerator:
        """Zero-copy read from Redis Stream for Module B.

        Proto Struct bytes go directly from Redis to gRPC response —
        no dict conversion, no JSON parsing. ~0.1-0.5ms per message
        instead of ~3-8ms on the JSON path.

        Args:
            task_id: Task reference ID.
            from_seq: Resume point.

        Yields:
            GatewayResponse messages.
        """
        from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamReader

        reader = ProtoStreamReader(task_id, self._redis_client)  # type: ignore[arg-type]
        await reader.restore_cursor()

        seq = from_seq
        job_id = task_id

        # Reuse timestamp across batch — one syscall per XREAD, not per message
        batch_ts = timestamp_pb2.Timestamp()
        batch_ts.FromDatetime(datetime.now(tz=timezone.utc))
        batch_count = 0

        async for struct_data in reader.read_structs():
            seq += 1
            batch_count += 1

            # Refresh timestamp once per XREAD batch (every 50 messages)
            if batch_count >= STREAM_BATCH_SIZE:
                batch_ts = timestamp_pb2.Timestamp()
                batch_ts.FromDatetime(datetime.now(tz=timezone.utc))
                batch_count = 0

            yield gateway_pb2.GatewayResponse(
                output=gateway_pb2.StreamOutput(
                    task_id=task_id,
                    job_id=job_id,
                    data=struct_data,
                    seq=seq,
                    timestamp=batch_ts,
                ),
            )

        yield self._status_response(task_id, job_id, gateway_pb2.STREAM_STATE_COMPLETED)
