"""gRPC implementation of TaskManagerStrategy using TaskManagerService."""

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any

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
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.task_manager.task_manager_strategy import TaskManagerServiceError, TaskManagerStrategy


class GrpcTaskManager(TaskManagerStrategy, GrpcClientWrapper, GrpcErrorHandlerMixin):
    """gRPC-backed task signal service using TaskManagerService."""

    service_name: str = "TaskManagerService"

    _subscriptions: dict[str, asyncio.Event]

    def __init__(
        self,
        mission_id: str,  # noqa: ARG002
        setup_id: str,  # noqa: ARG002
        setup_version_id: str,  # noqa: ARG002
        client_config: ClientConfig,
        *,
        poll_interval: float = 1.0,
    ) -> None:
        """Initialize with client config.

        Args:
            mission_id: Mission identifier (unused, required by init_strategy convention).
            setup_id: Setup identifier (unused, required by init_strategy convention).
            setup_version_id: Setup version identifier (unused, required by init_strategy convention).
            client_config: gRPC client configuration. Falls back to env vars if not provided.
            poll_interval: Seconds between GetSignals polls.
        """
        channel = self._init_channel(client_config)
        self.stub = task_manager_service_pb2_grpc.TaskManagerServiceStub(channel)
        self._subscriptions = {}
        self._poll_interval = poll_interval
        self._grpc_timeout = float(os.environ.get("DIGITALKIN_GRPC_TIMEOUT", "30"))

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
        if payload:
            payload_struct = Struct()
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
            task_proto = self._signal_to_task_proto(signal)

            req = task_manager_dto_pb2.SendSignalsRequest(tasks=[task_proto])
            resp = await self.exec_grpc_query("SendSignals", req, timeout=self._grpc_timeout)

            if not resp.success:
                msg = f"SendSignals rejected for task {task_id}"
                raise TaskManagerServiceError(msg)

            return data

    async def subscribe_signals(self, task_id: str) -> tuple[str, AsyncGenerator[dict[str, Any], None]]:
        """Subscribe to signal updates by polling GetSignals.

        Args:
            task_id: Unique task identifier to poll signals for.

        Returns:
            Tuple of (subscription_id, async generator of signal dicts).
        """
        sub_id = str(uuid.uuid4())
        stop_event = asyncio.Event()
        self._subscriptions[sub_id] = stop_event
        logger.debug("subscribe_signals: created subscription %s for task %s", sub_id, task_id)

        async def _poll_generator() -> AsyncGenerator[dict[str, Any], None]:
            try:
                while not stop_event.is_set():
                    try:
                        req = task_manager_dto_pb2.GetSignalsRequest(task_id=task_id)
                        resp = await self.exec_grpc_query("GetSignals", req, timeout=self._grpc_timeout)
                        logger.debug(
                            "GetSignals poll: %d task(s) returned, task_ids=%s",
                            len(resp.tasks),
                            [t.task_id for t in resp.tasks],
                        )
                        for task_proto in resp.tasks:
                            logger.debug(
                                "GetSignals task: task_id=%s, action=%s, cancellation_reason=%s",
                                task_proto.task_id,
                                task_proto.action,
                                task_proto.cancellation_reason,
                            )
                            yield self._task_proto_to_signal_dict(task_proto)
                    except Exception:
                        logger.warning(
                            "GetSignals poll failed, retrying in %ss", self._poll_interval, exc_info=True
                        )
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=self._poll_interval)
                        break
                    except TimeoutError:
                        pass
            finally:
                with contextlib.suppress(Exception):
                    self._subscriptions.pop(sub_id, None)
                logger.debug("subscribe_signals: subscription %s ended", sub_id)

        return sub_id, _poll_generator()

    async def unsubscribe_signals(self, sub_id: str) -> None:
        """Stop the polling loop for the given subscription.

        Args:
            sub_id: Subscription identifier.
        """
        stop_event = self._subscriptions.pop(sub_id, None)
        if stop_event is not None:
            try:
                stop_event.set()
            except Exception:
                logger.warning("Failed to set stop event for subscription %s", sub_id)

    async def close(self) -> None:
        """Stop all subscriptions and close the gRPC channel."""
        for sub_id in list(self._subscriptions):
            with contextlib.suppress(Exception):
                await self.unsubscribe_signals(sub_id)
        try:
            await GrpcClientWrapper.close(self)
        except Exception:
            logger.warning("Failed to close gRPC channels for %s", self.service_name)
