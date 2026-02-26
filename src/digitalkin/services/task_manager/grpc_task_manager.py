"""gRPC implementation of TaskManagerStrategy using TaskManagerService."""

from __future__ import annotations

import asyncio
import contextlib
import os
import random
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar

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

try:
    from agentic_mesh_protocol.task_manager.v1 import (
        task_manager_dto_pb2,
        task_manager_message_pb2,
        task_manager_service_pb2_grpc,
    )
except (ImportError, ModuleNotFoundError):
    task_manager_dto_pb2 = None  # type: ignore[assignment]
    task_manager_message_pb2 = None  # type: ignore[assignment]
    task_manager_service_pb2_grpc = None  # type: ignore[assignment]


class _SharedPoller:
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
        stub: Any,
        poll_timeout: float,
        poll_interval: float,
        initial_poll_interval: float,
    ) -> _SharedPoller:
        """Get existing poller for this address or create a new one.

        Args:
            key: Unique identifier for the poller.
            stub: gRPC stub for the TaskManagerService.
            poll_timeout: Maximum seconds to wait for GetSignals response.
            poll_interval: Maximum seconds between GetSignals polls.
            initial_poll_interval: Starting poll interval before exponential ramp-up.

        Returns:
            _SharedPoller: Shared poller for this address.
        """
        if key not in cls._instances:
            cls._instances[key] = cls(stub, poll_timeout, poll_interval, initial_poll_interval)
        return cls._instances[key]

    @classmethod
    async def close_all(cls) -> None:
        """Close all shared pollers. Called during server shutdown."""
        for poller in cls._instances.values():
            await poller.close()
        cls._instances.clear()

    def __init__(
        self,
        stub: Any,
        poll_timeout: float,
        poll_interval: float,
        initial_poll_interval: float,
    ) -> None:
        self._stub = stub
        self._poll_timeout = poll_timeout
        self._poll_interval = poll_interval
        self._initial_poll_interval = initial_poll_interval
        self._task_queues: dict[str, asyncio.Queue[task_manager_message_pb2.Task | None]] = {}
        self._poll_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    def register(self, task_id: str) -> asyncio.Queue[task_manager_message_pb2.Task | None]:
        """Register a task_id for polling. Returns queue for signal delivery.

        Args:
            task_id: Unique task identifier.

        Returns:
            asyncio.Queue[task_manager_message_pb2.Task | None]: Queue for signal delivery.
        """
        queue: asyncio.Queue[task_manager_message_pb2.Task | None] = asyncio.Queue()
        self._task_queues[task_id] = queue
        if self._poll_task is None or self._poll_task.done():
            # Recreate Event in the current event loop (the old one may belong to a closed loop)
            self._stop_event = asyncio.Event()
            self._poll_task = asyncio.create_task(self._poll_loop(), name="shared_signal_poller")
        return queue

    def unregister(self, task_id: str) -> None:
        """Remove a task_id from polling. Stops poller when empty.

        Args:
            task_id: Unique task identifier.
        """
        self._task_queues.pop(task_id, None)
        if not self._task_queues:
            self._stop_event.set()

    async def _poll_loop(self) -> None:
        """Single loop polling GetSignals for all registered task_ids.

        Raises:
            Exception: If GetSignals fails.
        """
        current_interval = self._initial_poll_interval
        try:
            while not self._stop_event.is_set():
                task_ids = list(self._task_queues.keys())
                if not task_ids:
                    break

                had_signals = False
                try:
                    req = task_manager_dto_pb2.GetSignalsRequest(task_ids=task_ids)
                    resp = await self._stub.GetSignals(req, timeout=self._poll_timeout)
                    for task_proto in resp.tasks:
                        queue = self._task_queues.get(task_proto.task_id)
                        if queue is not None:
                            await queue.put(task_proto)
                            had_signals = True
                except Exception:
                    logger.debug("GetSignals bulk poll failed for %d tasks", len(task_ids))

                if had_signals:
                    current_interval = self._initial_poll_interval
                else:
                    current_interval = min(current_interval * 2, self._poll_interval)

                jittered = current_interval + random.uniform(0, current_interval * 0.5)  # noqa: S311
                wait_task = asyncio.ensure_future(self._stop_event.wait())
                done, _ = await asyncio.wait([wait_task], timeout=jittered)
                if done:
                    break
                wait_task.cancel()
        except Exception:
            logger.warning("Shared signal poller crashed, will restart on next register", exc_info=True)
        finally:
            self._poll_task = None

    async def close(self) -> None:
        """Stop the poller and drain all queues."""
        self._stop_event.set()
        if self._poll_task is not None and not self._poll_task.done():
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
        # Wake up any blocked queue consumers
        for queue in self._task_queues.values():
            with contextlib.suppress(Exception):
                queue.put_nowait(None)
        self._task_queues.clear()


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
        poll_interval: float = 1.0,
        initial_poll_interval: float = 0.1,
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
        """Create or update a signal record via gRPC SendSignals.

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
            logger.debug("SendSignals: task_id=%s action=%s", task_id, signal.action.value)
            task_proto = self._signal_to_task_proto(signal)

            req = task_manager_dto_pb2.SendSignalsRequest(tasks=[task_proto])
            resp = await self.exec_grpc_query("SendSignals", req, timeout=self._grpc_timeout)

            logger.debug("SendSignals response: success=%s", resp.success)

            if not resp.success:
                msg = f"SendSignals rejected for task {task_id}"
                raise TaskManagerServiceError(msg)

            logger.info("SendSignals: task_id=%s, action=%s", task_id, signal.action.value)
            return data

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
            stub=self.stub,
            poll_timeout=self._poll_timeout,
            poll_interval=self._poll_interval,
            initial_poll_interval=self._initial_poll_interval,
        )
        queue = poller.register(task_id)

        async def _queue_consumer() -> AsyncGenerator[dict[str, Any], None]:
            last_seen_ts: datetime | None = None
            try:
                while not stop_event.is_set():
                    get_task = asyncio.ensure_future(queue.get())
                    done, _ = await asyncio.wait([get_task], timeout=self._poll_interval * 2)
                    if not done:  # type: ignore
                        get_task.cancel()
                        continue
                    task_proto = get_task.result()
                    if task_proto is None:
                        break

                    # Dedup: skip signals already seen based on timestamp
                    sig_ts = (
                        task_proto.created_at.ToDatetime(tzinfo=timezone.utc)
                        if task_proto.HasField("created_at")
                        else None
                    )
                    if last_seen_ts is not None and sig_ts is not None and sig_ts <= last_seen_ts:
                        continue
                    if sig_ts is not None:
                        last_seen_ts = sig_ts

                    yield self._task_proto_to_signal_dict(task_proto)
            finally:
                poller.unregister(task_id)
                self._subscriptions.pop(sub_id, None)
                self._sub_task_ids.pop(sub_id, None)

        return sub_id, _queue_consumer()

    async def unsubscribe_signals(self, sub_id: str) -> None:
        """Stop the subscription and unregister from the shared poller.

        Args:
            sub_id: Subscription identifier.
        """
        stop_event = self._subscriptions.pop(sub_id, None)
        task_id = self._sub_task_ids.pop(sub_id, None)
        if stop_event is not None:
            stop_event.set()
        if task_id is not None:
            poller_key = self._channel_cache_key or "default"
            if poller_key in _SharedPoller._instances:  # noqa: SLF001 # pylint: disable=protected-access
                poller = _SharedPoller._instances[poller_key]  # noqa: SLF001 # pylint: disable=protected-access
                # Wake up blocked queue.get() with sentinel
                queue = poller._task_queues.get(task_id)  # noqa: SLF001 # pylint: disable=protected-access
                if queue is not None:
                    with contextlib.suppress(Exception):
                        queue.put_nowait(None)
                poller.unregister(task_id)

    async def close(self) -> None:
        """Stop all subscriptions and close the gRPC channel."""
        for sub_id in list(self._subscriptions):
            with contextlib.suppress(Exception):
                await self.unsubscribe_signals(sub_id)
        await self.close_channel()
        logger.info("GrpcTaskManager closed (%s)", self.service_name)
