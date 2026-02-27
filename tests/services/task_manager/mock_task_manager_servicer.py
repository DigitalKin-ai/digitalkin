"""Mock TaskManager Servicer for testing the GrpcTaskManager service."""

from datetime import datetime, timezone
from typing import Any

import grpc
from agentic_mesh_protocol.task_manager.v1 import (
    task_manager_dto_pb2,
    task_manager_message_pb2,
    task_manager_service_pb2_grpc,
)
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp

from digitalkin.logger import logger


class MockTaskManagerServicer(task_manager_service_pb2_grpc.TaskManagerServiceServicer):
    """Mock implementation of TaskManagerService for testing.

    Stores tasks in memory and returns them on GetSignals requests.
    Supports configurable latency and failure injection for stress testing.
    """

    def __init__(self) -> None:
        """Initialize the mock servicer with empty task storage."""
        super().__init__()
        # task_id -> list of Task proto messages
        self.tasks: dict[str, list[dict[str, Any]]] = {}
        self.send_count: int = 0
        self.get_count: int = 0

        # Failure injection
        self._fail_send: bool = False
        self._fail_get: bool = False
        self._reject_send: bool = False

    def SendSignals(
        self,
        request: task_manager_dto_pb2.SendSignalsRequest,
        context: grpc.ServicerContext,
    ) -> task_manager_dto_pb2.SendSignalsResponse:
        """Store task signals.

        Args:
            request: SendSignalsRequest containing task messages.
            context: gRPC context.

        Returns:
            SendSignalsResponse with success status.
        """
        self.send_count += 1

        if self._fail_send:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Injected SendSignals failure")
            return task_manager_dto_pb2.SendSignalsResponse(success=False)

        if self._reject_send:
            return task_manager_dto_pb2.SendSignalsResponse(success=False)

        for task_proto in request.tasks:
            if not task_proto.task_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("task_id is required")
                return task_manager_dto_pb2.SendSignalsResponse(success=False)

            task_dict = {
                "task_id": task_proto.task_id,
                "mission_id": task_proto.mission_id,
                "setup_id": task_proto.setup_id,
                "setup_version_id": task_proto.setup_version_id,
                "action": task_proto.action,
                "cancellation_reason": task_proto.cancellation_reason,
                "payload": dict(task_proto.payload) if task_proto.HasField("payload") else {},
            }

            if task_proto.HasField("created_at"):
                task_dict["created_at"] = task_proto.created_at.ToDatetime(tzinfo=timezone.utc)
            else:
                task_dict["created_at"] = datetime.now(timezone.utc)

            if task_proto.task_id not in self.tasks:
                self.tasks[task_proto.task_id] = []
            self.tasks[task_proto.task_id].append(task_dict)

            logger.debug("MockTaskManager: stored signal task_id=%s action=%s", task_proto.task_id, task_proto.action)

        return task_manager_dto_pb2.SendSignalsResponse(success=True)

    def _build_task_protos(self, task_ids: list[str]) -> list[task_manager_message_pb2.Task]:
        """Build Task protos for given task_ids from stored data.

        Args:
            task_ids: List of task identifiers to look up.

        Returns:
            List of Task proto messages.
        """
        task_protos = []
        for tid in task_ids:
            for task_dict in self.tasks.get(tid, []):
                task_proto = task_manager_message_pb2.Task(
                    task_id=task_dict["task_id"],
                    mission_id=task_dict["mission_id"],
                    setup_id=task_dict["setup_id"],
                    setup_version_id=task_dict["setup_version_id"],
                    action=task_dict["action"],
                    cancellation_reason=task_dict.get("cancellation_reason", "none"),
                )

                ts = Timestamp()
                ts.FromDatetime(task_dict["created_at"])
                task_proto.created_at.CopyFrom(ts)

                payload_struct = Struct()
                payload = task_dict.get("payload", {})
                if payload:
                    payload_struct.update(payload)
                task_proto.payload.CopyFrom(payload_struct)

                task_protos.append(task_proto)
        return task_protos

    def GetSignals(
        self,
        request: task_manager_dto_pb2.GetSignalsRequest,
        context: grpc.ServicerContext,
    ) -> task_manager_dto_pb2.GetSignalsResponse:
        """Return stored signals for task_id (single) or task_ids (bulk).

        Args:
            request: GetSignalsRequest with task_id or task_ids.
            context: gRPC context.

        Returns:
            GetSignalsResponse with matching task signals.
        """
        self.get_count += 1

        if self._fail_get:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Injected GetSignals failure")
            return task_manager_dto_pb2.GetSignalsResponse(tasks=[])

        bulk_ids = list(request.task_ids)
        if bulk_ids:
            return task_manager_dto_pb2.GetSignalsResponse(tasks=self._build_task_protos(bulk_ids))

        if not request.task_id:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("task_id is required")
            return task_manager_dto_pb2.GetSignalsResponse(tasks=[])

        return task_manager_dto_pb2.GetSignalsResponse(tasks=self._build_task_protos([request.task_id]))
