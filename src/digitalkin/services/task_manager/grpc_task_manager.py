"""gRPC implementation of TaskManagerStrategy using TaskManagerService."""

from __future__ import annotations

import asyncio
import contextlib
import os
import random
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar

import grpc
from agentic_mesh_protocol.task_manager.v1 import (
    task_manager_dto_pb2,
    task_manager_message_pb2,
    task_manager_service_pb2_grpc,
)
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp

from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.grpc_error_handler import GrpcErrorHandlerMixin
from digitalkin.logger import logger
from digitalkin.models.core.task_monitor import SignalMessage
from digitalkin.services.task_manager.task_manager_strategy import TaskManagerServiceError, TaskManagerStrategy

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator

    from digitalkin.models.grpc_servers.models import ClientConfig

_PollFn = Callable[[list[str]], Awaitable[list[task_manager_message_pb2.Task]]]

_RETRYABLE_CODES = frozenset({
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.INTERNAL,
})


class _SharedChannelResource:
    """Abstract base for per-channel singleton resources with lifecycle management.

    Subclasses must define their own _instances class variable and implement
    get_or_create() and close(). Resources are reference-counted: the singleton
    is closed and removed only when the last holder calls release().
    """

    def __init__(self) -> None:
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._refcount: int = 0

    @classmethod
    def pop_instance(cls, key: str) -> Any:
        """Remove and return the singleton for key, or None if absent.

        Returns:
            The popped instance, or None if no instance was registered for key.
        """
        return cls._instances.pop(key, None)  # type: ignore[attr-defined]

    @classmethod
    async def release(cls, key: str) -> None:
        """Decrement refcount and close the singleton when the last holder releases it.

        Args:
            key: Channel key identifying the shared resource.
        """
        inst = cls._instances.get(key)  # type: ignore[attr-defined]
        if inst is None:
            return
        inst._refcount -= 1  # noqa: SLF001
        if inst._refcount <= 0:  # noqa: SLF001
            cls._instances.pop(key, None)  # type: ignore[attr-defined]
            await inst.close()

    @classmethod
    async def close_all(cls) -> None:
        """Close all instances for this resource type. Called during server shutdown."""
        for inst in list(cls._instances.values()):  # type: ignore[attr-defined]
            await inst.close()
        cls._instances.clear()  # type: ignore[attr-defined]


class _SharedPoller(_SharedChannelResource):
    """Coordinates GetSignals polling for all tasks sharing a gRPC stub.

    Instead of N independent polling loops (one per task), a single poller
    iterates all registered task_ids with controlled concurrency and
    distributes results to per-task queues. This reduces RPC storm from
    N concurrent polls to batched sequential/parallel calls.
    """

    _instances: ClassVar[dict[str, _SharedPoller]] = {}

    @classmethod
    def get_or_create(
        cls,
        key: str,
        poll_fn: _PollFn,
        poll_interval: float,
        initial_poll_interval: float,
    ) -> _SharedPoller:
        """Get existing poller for this address or create a new one.

        Args:
            key: Unique identifier for the poller.
            poll_fn: Async callable that fetches signals for a list of task IDs.
            poll_interval: Maximum seconds between GetSignals polls.
            initial_poll_interval: Starting poll interval before exponential ramp-up.

        Returns:
            _SharedPoller: Shared poller for this address.
        """
        if key not in cls._instances:
            cls._instances[key] = cls(poll_fn, poll_interval, initial_poll_interval)
        inst = cls._instances[key]
        inst._refcount += 1  # noqa: SLF001
        return inst

    @classmethod
    def signal_stop_instance(cls, key: str, task_id: str) -> None:
        """Wake and immediately unregister task_id from the poller at key.

        Called by unsubscribe_signals to stop polling even if the consumer
        generator was never iterated (and its finally block never ran).

        Args:
            key: Channel key identifying the shared poller.
            task_id: Task to stop polling for.
        """
        if (poller := cls._instances.get(key)) is not None:
            poller.wake(task_id)
            poller.unregister(task_id)

    def __init__(
        self,
        poll_fn: _PollFn,
        poll_interval: float,
        initial_poll_interval: float,
    ) -> None:
        super().__init__()
        self._poll_fn = poll_fn
        self._poll_interval = poll_interval
        self._initial_poll_interval = initial_poll_interval
        self._task_queues: dict[str, asyncio.Queue[task_manager_message_pb2.Task | None]] = {}
        self._last_seen_ts: dict[str, tuple[int, int]] = {}

    def register(self, task_id: str) -> asyncio.Queue[task_manager_message_pb2.Task | None]:
        """Register a task_id for polling. Returns queue for signal delivery.

        Args:
            task_id: Unique task identifier.

        Returns:
            asyncio.Queue[task_manager_message_pb2.Task | None]: Queue for signal delivery.
        """
        queue: asyncio.Queue[task_manager_message_pb2.Task | None] = asyncio.Queue(
            maxsize=int(os.environ.get("DIGITALKIN_SIGNAL_QUEUE_SIZE", "512"))
        )
        self._task_queues[task_id] = queue
        if self._task is None or self._task.done():
            # Recreate stop_event in the current event loop (the old one may belong to a closed loop)
            self._stop_event = asyncio.Event()
            self._task = asyncio.create_task(self._poll_loop(), name="shared_signal_poller")
        # else: task already running — new task_id in _task_queues is picked up next poll
        return queue

    def unregister(self, task_id: str) -> None:
        """Remove a task_id from polling. Stops poller when empty.

        Args:
            task_id: Unique task identifier.
        """
        self._task_queues.pop(task_id, None)
        self._last_seen_ts.pop(task_id, None)
        if not self._task_queues:
            self._stop_event.set()

    def wake(self, task_id: str) -> None:
        """Send a None sentinel to wake up a blocked consumer for task_id.

        Args:
            task_id: Unique task identifier.
        """
        if (queue := self._task_queues.get(task_id)) is not None:
            with contextlib.suppress(Exception):
                queue.put_nowait(None)

    def _dispatch_signal(self, task_proto: task_manager_message_pb2.Task) -> bool:
        """Enqueue a signal proto if it has not already been seen.

        Args:
            task_proto: Signal to dispatch.

        Returns:
            True if the signal was queued (new), False if skipped.
        """
        queue = self._task_queues.get(task_proto.task_id)
        if queue is None:
            return False
        ts_key: tuple[int, int] | None = None
        if task_proto.HasField("created_at"):
            ts_key = (task_proto.created_at.seconds, task_proto.created_at.nanos)
        if ts_key is not None and ts_key <= self._last_seen_ts.get(task_proto.task_id, (-1, -1)):
            return False
        if ts_key is not None:
            self._last_seen_ts[task_proto.task_id] = ts_key
        try:
            queue.put_nowait(task_proto)
        except asyncio.QueueFull:
            if task_proto.action in {"stop", "cancel"}:
                with contextlib.suppress(asyncio.QueueEmpty):
                    queue.get_nowait()
                queue.put_nowait(task_proto)
                logger.warning(
                    "Signal queue full for task_id=%s, dropped oldest for critical %s",
                    task_proto.task_id,
                    task_proto.action,
                )
            else:
                logger.warning("Signal queue full for task_id=%s, dropping signal", task_proto.task_id)
        if task_proto.action in {"stop", "cancel"}:
            try:
                queue.put_nowait(None)
            except Exception:
                logger.debug("Could not enqueue None sentinel for task_id=%s", task_proto.task_id)
            self.unregister(task_proto.task_id)
        return True

    async def _poll_loop(self) -> None:
        """Single loop polling GetSignals for all registered task_ids."""
        stop_event = self._stop_event
        current_interval = self._initial_poll_interval
        try:
            while not stop_event.is_set():
                task_ids = list(self._task_queues.keys())
                if not task_ids:
                    break

                had_signals = False
                try:
                    for task_proto in await self._poll_fn(task_ids):
                        if self._dispatch_signal(task_proto):
                            had_signals = True
                except Exception:
                    logger.warning("GetSignals failed, retrying with backoff", exc_info=True)

                if had_signals:
                    current_interval = self._initial_poll_interval
                else:
                    current_interval = min(current_interval * 2, self._poll_interval)

                jittered = current_interval + random.uniform(0, current_interval * 0.5)  # noqa: S311
                stop_task = asyncio.create_task(stop_event.wait())
                await asyncio.wait([stop_task], timeout=jittered)
                stop_task.cancel()
                if stop_event.is_set():
                    break
        finally:
            self._task = None

    async def close(self) -> None:
        """Stop the poller and drain all queues."""
        self._stop_event.set()
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        # Wake up any blocked queue consumers
        for queue in self._task_queues.values():
            with contextlib.suppress(Exception):
                queue.put_nowait(None)
        self._task_queues.clear()
        self._last_seen_ts.clear()


class _SharedSendBuffer(_SharedChannelResource):
    """Batches outbound SendSignals RPCs within a fixed time window.

    Instead of one RPC per send_signal() call, signal protos are accumulated
    and flushed together either when the batch hits max_batch_size items or
    after flush_interval seconds — whichever comes first.

    Relies on asyncio's single-threaded execution model: list operations
    between await points are atomic, so no locks are needed.
    """

    _instances: ClassVar[dict[str, _SharedSendBuffer]] = {}

    @classmethod
    def get_or_create(cls, key: str, stub: Any, grpc_timeout: float) -> _SharedSendBuffer:
        """Get existing buffer for this channel key or create a new one.

        Args:
            key: Unique channel identifier.
            stub: gRPC stub for SendSignals calls.
            grpc_timeout: Seconds before the RPC times out.

        Returns:
            _SharedSendBuffer: Shared buffer for this channel.
        """
        if key not in cls._instances:
            cls._instances[key] = cls(stub, grpc_timeout)
        inst = cls._instances[key]
        inst._refcount += 1  # noqa: SLF001
        return inst

    def __init__(self, stub: Any, grpc_timeout: float) -> None:
        super().__init__()
        self._stub = stub
        self._grpc_timeout = grpc_timeout
        self._flush_interval = float(os.environ.get("DIGITALKIN_SIGNAL_FLUSH_INTERVAL", "0.1"))
        self._max_batch_size = int(os.environ.get("DIGITALKIN_SIGNAL_MAX_BATCH_SIZE", "50"))
        self._max_retries = int(os.environ.get("DIGITALKIN_SIGNAL_SEND_RETRIES", "3"))
        self._backoff_base = float(os.environ.get("DIGITALKIN_SIGNAL_SEND_BACKOFF_MS", "100")) / 1000
        # List of (proto, future) pairs pending a flush. Swapped atomically in _flush().
        self._pending: list[tuple[task_manager_message_pb2.Task, asyncio.Future[bool]]] = []

    async def send(self, task_proto: task_manager_message_pb2.Task) -> bool:
        """Enqueue a signal proto and wait for the batch flush.

        Args:
            task_proto: Task protobuf message to send.

        Returns:
            True when the signal was accepted by the server.

        Raises:
            TaskManagerServiceError: If the batch RPC fails or the server rejects it.
        """
        future: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        self._pending.append((task_proto, future))

        if len(self._pending) >= self._max_batch_size:
            # Batch full — flush immediately without waiting for the timer.
            await self._flush()
        elif self._task is None or self._task.done():
            # Arm the deadline timer for this new batch window.
            self._stop_event = asyncio.Event()
            self._task = asyncio.create_task(self._flush_after_interval(), name="send_signal_flush")

        return await future

    async def _flush_after_interval(self) -> None:
        """Sleep for FLUSH_INTERVAL (or until stopped), then flush."""
        stop_event = self._stop_event
        try:
            stop_wait = asyncio.create_task(stop_event.wait())
            done, _ = await asyncio.wait([stop_wait], timeout=self._flush_interval)
            if not done:
                stop_wait.cancel()
            await self._flush()
        except Exception:
            logger.warning("SendBuffer flush timer crashed", exc_info=True)
        finally:
            self._task = None

    async def _flush(self) -> None:
        """Send all pending signals in one batched RPC and resolve their futures.

        Atomically swaps out the pending list so new enqueues during the RPC
        land in a fresh batch, not the in-flight one.  Retries on transient
        gRPC errors (DEADLINE_EXCEEDED, UNAVAILABLE, INTERNAL) with
        exponential backoff and jitter.
        """
        batch, self._pending = self._pending, []
        if not batch:
            return

        task_protos = [t for t, _ in batch]
        futures = [f for _, f in batch]
        exc: Exception | None = None

        for attempt in range(1 + self._max_retries):
            exc = None
            try:
                req = task_manager_dto_pb2.SendSignalsRequest(tasks=task_protos)
                resp = await self._stub.SendSignals(req, timeout=self._grpc_timeout)
                if not resp.success:
                    exc = TaskManagerServiceError(f"SendSignals batch rejected ({len(task_protos)} tasks)")
                    break  # Server rejected — not retryable
                break  # Success
            except grpc.aio.AioRpcError as e:
                if e.code() in _RETRYABLE_CODES and attempt < self._max_retries:
                    delay = self._backoff_base * (2**attempt)
                    jitter = random.uniform(0, delay * 0.5)  # noqa: S311
                    logger.warning(
                        "SendSignals attempt %d/%d failed (%s), retrying in %.0fms",
                        attempt + 1,
                        1 + self._max_retries,
                        e.code().name,
                        (delay + jitter) * 1000,
                    )
                    await asyncio.sleep(delay + jitter)
                    continue
                exc = e
                break
            except Exception as e:
                exc = e
                break

        for f in futures:
            if not f.done():
                if exc is not None:
                    f.set_exception(exc)
                else:
                    f.set_result(True)

    async def close(self) -> None:
        """Flush all pending signals and stop the timer task."""
        self._stop_event.set()
        if self._task is not None and not self._task.done():
            with contextlib.suppress(Exception):
                await self._task
        # Drain any items enqueued after the timer task started.
        await self._flush()


class GrpcTaskManager(TaskManagerStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """gRPC-backed task signal service using TaskManagerService.

    Signal polling is delegated to a shared _SharedPoller per gRPC address,
    so N concurrent tasks share one controlled polling loop instead of
    N independent loops hammering the TaskManagerService.
    """

    service_name: str = "TaskManagerService"

    _subscriptions: dict[str, asyncio.Event]
    _sub_task_ids: dict[str, str]

    def __init__(
        self,
        mission_id: str,  # noqa: ARG002
        setup_id: str,  # noqa: ARG002
        setup_version_id: str,  # noqa: ARG002
        client_config: ClientConfig,
        *,
        poll_interval: float = float(os.environ.get("DIGITALKIN_SIGNAL_POLL_INTERVAL", "1.0")),
        initial_poll_interval: float = float(os.environ.get("DIGITALKIN_SIGNAL_INITIAL_POLL_INTERVAL", "0.1")),
    ) -> None:
        """Initialize with client config.

        Args:
            mission_id: Mission identifier (unused, required by init_strategy convention).
            setup_id: Setup identifier (unused, required by init_strategy convention).
            setup_version_id: Setup version identifier (unused, required by init_strategy convention).
            client_config: gRPC client configuration.
            poll_interval: Maximum seconds between GetSignals polls.
            initial_poll_interval: Starting poll interval before exponential ramp-up.

        Raises:
            ImportError: If agentic_mesh_protocol.task_manager.v1 is not installed.
        """
        if task_manager_service_pb2_grpc is None:
            msg = (
                "GrpcTaskManager requires 'agentic_mesh_protocol[task_manager]'. "
                "Install the proto package to use remote task manager signals."
            )
            raise ImportError(msg)
        channel = self._init_channel(client_config)
        self.stub = task_manager_service_pb2_grpc.TaskManagerServiceStub(channel)
        self._subscriptions = {}
        self._sub_task_ids = {}
        self._poll_interval = poll_interval
        self._initial_poll_interval = initial_poll_interval
        self._grpc_timeout = float(os.environ.get("DIGITALKIN_GRPC_TIMEOUT", "30"))
        self._poll_timeout = float(os.environ.get("DIGITALKIN_POLL_TIMEOUT", "1"))
        # Lazy buffer: created on first send_signal to ensure correct event loop and stub
        self._send_buffer_key = self._channel_cache_key or "default"
        self._send_buffer_acquired = False

    @staticmethod
    def _signal_to_task_proto(signal: SignalMessage) -> task_manager_message_pb2.Task:
        """Convert a SignalMessage to a Task proto message.

        Args:
            signal: Validated signal message.

        Returns:
            Task protobuf message.
        """
        task = task_manager_message_pb2.Task(
            task_id=signal.task_id,
            mission_id=signal.mission_id,
            setup_id=signal.setup_id,
            setup_version_id=signal.setup_version_id,
            action=signal.action.value,
            cancellation_reason=signal.cancellation_reason.value if signal.cancellation_reason is not None else "none",
        )

        created_at = Timestamp()
        created_at.FromDatetime(signal.timestamp)
        task.created_at.CopyFrom(created_at)

        payload = dict(signal.payload)
        if signal.error_message is not None:
            payload["error_message"] = signal.error_message
        if signal.exception_traceback is not None:
            payload["exception_traceback"] = signal.exception_traceback
        payload_struct = Struct()
        if payload:
            payload_struct.update(payload)
        task.payload.CopyFrom(payload_struct)

        return task

    @staticmethod
    def _task_proto_to_signal_dict(task: task_manager_message_pb2.Task) -> dict[str, Any]:
        """Convert a Task proto message to a SignalMessage-compatible dict.

        Args:
            task: Task protobuf message.

        Returns:
            Dict matching SignalMessage.model_dump(exclude_none=True) format.
        """
        result: dict[str, Any] = {
            "task_id": task.task_id,
            "mission_id": task.mission_id,
            "setup_id": task.setup_id,
            "setup_version_id": task.setup_version_id,
            "action": task.action,
            "cancellation_reason": task.cancellation_reason if task.cancellation_reason not in {"", "none"} else None,
        }

        if task.HasField("created_at"):
            result["timestamp"] = task.created_at.ToDatetime(tzinfo=timezone.utc)
        else:
            result["timestamp"] = datetime.now(timezone.utc)

        payload: dict[str, Any] = {}
        if task.HasField("payload"):
            payload = dict(task.payload)
        result["error_message"] = payload.pop("error_message", None)
        result["exception_traceback"] = payload.pop("exception_traceback", None)
        result["payload"] = payload

        signal = SignalMessage.model_validate(result)
        return signal.model_dump(exclude_none=True)

    async def send_signal(self, task_id: str, data: dict[str, Any]) -> dict[str, Any]:
        """Enqueue a signal for batched delivery via gRPC SendSignals.

        Signals are accumulated in a shared per-channel send buffer and flushed
        in a single SendSignalsRequest either when the batch hits 50 items or
        after 100 ms — whichever comes first.

        Args:
            task_id: Unique task identifier.
            data: Signal data to upsert.

        Returns:
            The upserted record as a dict.

        Raises:
            TaskManagerServiceError: If the gRPC call fails or the server rejects the request.
        """
        async with self.handle_grpc_errors("send_signal", TaskManagerServiceError):
            data["task_id"] = task_id
            signal = SignalMessage.model_validate(data)
            logger.debug("SendSignals queued: task_id=%s action=%s", task_id, signal.action.value)
            if self._send_buffer_acquired:
                buffer = _SharedSendBuffer._instances.get(self._send_buffer_key)  # noqa: SLF001
            else:
                self._send_buffer_acquired = True
                buffer = None
            if buffer is None:
                buffer = _SharedSendBuffer.get_or_create(self._send_buffer_key, self.stub, self._grpc_timeout)
            await buffer.send(self._signal_to_task_proto(signal))
            logger.info("SendSignals: task_id=%s action=%s", task_id, signal.action.value)
            return data

    async def _get_signals(self, task_ids: list[str]) -> list[task_manager_message_pb2.Task]:
        """Fetch signals for task_ids via poll_grpc. Returns [] on timeout or error.

        Args:
            task_ids: Task identifiers to fetch signals for.

        Returns:
            List of Task protos, or [] if DEADLINE_EXCEEDED or any error.
        """
        try:
            resp = await self.poll_grpc(
                "GetSignals",
                task_manager_dto_pb2.GetSignalsRequest(task_ids=task_ids),
                timeout=self._poll_timeout,
            )
            return list(resp.tasks) if resp is not None else []
        except Exception:
            logger.debug("GetSignals failed for %d tasks", len(task_ids))
            return []

    async def subscribe_signals(self, task_id: str) -> tuple[str, AsyncGenerator[dict[str, Any], None]]:
        """Subscribe to signal updates via the shared poller.

        Instead of an independent polling loop, this registers the task_id
        with the shared _SharedPoller and yields signals from a queue.

        Args:
            task_id: Unique task identifier to poll signals for.

        Returns:
            Tuple of (subscription_id, async generator of signal dicts).
        """
        sub_id = str(uuid.uuid4())
        stop_event = asyncio.Event()
        self._subscriptions[sub_id] = stop_event
        self._sub_task_ids[sub_id] = task_id
        logger.debug("subscribe_signals: created subscription %s for task %s", sub_id, task_id)

        poller = _SharedPoller.get_or_create(
            key=self._channel_cache_key or "default",
            poll_fn=self._get_signals,
            poll_interval=self._poll_interval,
            initial_poll_interval=self._initial_poll_interval,
        )
        queue = poller.register(task_id)

        async def _queue_consumer() -> AsyncGenerator[dict[str, Any], None]:
            get_task: asyncio.Task[task_manager_message_pb2.Task | None] | None = None
            try:
                while not stop_event.is_set():
                    get_task = asyncio.create_task(queue.get())
                    done, _ = await asyncio.wait([get_task], timeout=self._poll_interval * 2)
                    if not done:  # type: ignore
                        get_task.cancel()
                        get_task = None
                        continue
                    task_proto = get_task.result()
                    get_task = None
                    if task_proto is None:
                        break

                    yield self._task_proto_to_signal_dict(task_proto)
            finally:
                if get_task is not None and not get_task.done():
                    get_task.cancel()
                poller.unregister(task_id)
                self._subscriptions.pop(sub_id, None)
                self._sub_task_ids.pop(sub_id, None)

        return sub_id, _queue_consumer()

    async def unsubscribe_signals(self, sub_id: str) -> None:
        """Stop the subscription and wake its consumer via the shared poller.

        Args:
            sub_id: Subscription identifier.
        """
        stop_event = self._subscriptions.pop(sub_id, None)
        task_id = self._sub_task_ids.pop(sub_id, None)
        if stop_event is not None:
            stop_event.set()
        if task_id is not None:
            _SharedPoller.signal_stop_instance(self._channel_cache_key or "default", task_id)

    async def close(self) -> None:
        """Stop all subscriptions, flush pending signals, and close the gRPC channel."""
        for sub_id in list(self._subscriptions):
            with contextlib.suppress(Exception):
                await self.unsubscribe_signals(sub_id)
        key = self._channel_cache_key or "default"
        # Decrement refcount; shared resources are only closed when the last holder releases.
        if self._send_buffer_acquired:
            with contextlib.suppress(Exception):
                await _SharedSendBuffer.release(key)
        with contextlib.suppress(Exception):
            await _SharedPoller.release(key)
        await self.close_channel()
        logger.info("GrpcTaskManager closed (%s)", self.service_name)
