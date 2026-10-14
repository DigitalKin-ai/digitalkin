"""GrpcCommunication.call_module wire contract against agentic-mesh-protocol 1.2.0 messages.

``call_module`` sends ``AssociateTaskRequest`` to the backend and ``StartStreamRequest`` to the target.
Schema reads are covered by ``tests/gateway/test_grpc_communication_schemas.py``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_dto_pb2
from google.protobuf import json_format, struct_pb2

from digitalkin.grpc_servers.interceptors.request_ids import RequestContext
from digitalkin.grpc_servers.m2m_call_registry import M2MCallRegistry
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.settings.gateway import get_gateway_settings
from digitalkin.models.settings.utils.channel import SecurityMode
from digitalkin.services.communication.grpc_communication import GrpcCommunication

pytestmark = [pytest.mark.timeout(10), pytest.mark.grpc]


def _struct(d: dict[str, Any]) -> struct_pb2.Struct:
    s = struct_pb2.Struct()
    s.update(d)
    return s


def _comm(m2m: M2MCallRegistry | None = None) -> GrpcCommunication:
    config = ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE)
    return GrpcCommunication(
        "missions:m",
        "setups:s",
        "setup_versions:v",
        config,
        m2m_calls=m2m,
        gateway_backend_config=config if m2m is not None else None,
    )


class _FakeGatewayStub:
    """Target GatewayService stub: records StartStream, then feeds the caller's registered queue."""

    def __init__(self, m2m: M2MCallRegistry, outputs: list[dict[str, Any]]) -> None:
        self._m2m = m2m
        self._outputs = outputs
        self.start_requests: list[gateway_dto_pb2.StartStreamRequest] = []
        self.start_metadata: list[dict[str, str]] = []
        self.registered_queries: list[dict[str, Any]] = []

    async def StartStream(
            self, request: gateway_dto_pb2.StartStreamRequest, metadata: tuple[tuple[str, str], ...] = ()
    ) -> gateway_dto_pb2.StartStreamResponse:
        self.start_requests.append(request)
        self.start_metadata.append(dict(metadata))
        entry = self._m2m.get(request.task_id)
        assert entry is not None
        self.registered_queries.append(json_format.MessageToDict(entry.query))
        for out in self._outputs:
            entry.output_queue.put_nowait(_struct(out))
        entry.output_queue.put_nowait(None)
        return gateway_dto_pb2.StartStreamResponse(accepted=True, task_id=request.task_id)


class TestCallModuleWire:
    async def test_associate_then_start_stream_requests(self) -> None:
        m2m = M2MCallRegistry()
        comm = _comm(m2m)
        backend_query = AsyncMock(
            return_value=gateway_dto_pb2.AssociateTaskResponse(task_id="tasks:child", parent_task_id="tasks:parent")
        )
        comm._gateway_backend.exec_grpc_query = backend_query  # type: ignore[union-attr, method-assign]
        gateway = _FakeGatewayStub(m2m, [{"root": {"protocol": "transform", "value": "out-1"}}])
        comm._get_or_create_channel = MagicMock()  # type: ignore[method-assign]
        comm._get_or_create_stub = MagicMock(return_value=gateway)  # type: ignore[method-assign]

        token = RequestContext.bind(task_id="tasks:parent")
        try:
            outputs = [
                json_format.MessageToDict(out)
                async for out in comm.call_module(
                    module_address="10.0.0.7",
                    module_port=50061,
                    input_data={"root": {"protocol": "transform", "text": "hi"}},
                    setup_id="setups:target",
                    mission_id="missions:target",
                    metadata={"x-trace": "abc", "x-client-address": "spoofed:1"},
                )
            ]
        finally:
            RequestContext.reset(token)
            await comm.close()

        assert outputs == [{"root": {"protocol": "transform", "value": "out-1"}}]

        backend_query.assert_awaited_once()
        rpc_name, assoc_request = backend_query.await_args.args
        assert rpc_name == "AssociateTask"
        assert isinstance(assoc_request, gateway_dto_pb2.AssociateTaskRequest)
        assert assoc_request.parent_task_id == "tasks:parent"
        assert backend_query.await_args.kwargs["timeout"] == get_gateway_settings().m2m.call_associate_timeout_s
        ((idem_header, idem_key),) = backend_query.await_args.kwargs["metadata"]
        assert idem_header == "x-idempotency-key"
        assert len(idem_key) == 32
        int(idem_key, 16)

        (start_request,) = gateway.start_requests
        assert isinstance(start_request, gateway_dto_pb2.StartStreamRequest)
        assert (start_request.task_id, start_request.setup_id, start_request.mission_id) == (
            "tasks:child",
            "setups:target",
            "missions:target",
        )
        assert gateway.start_metadata == [
            {"x-trace": "abc", "x-client-address": m2m.effective_advertise_address()},
        ]
        assert gateway.registered_queries == [{"root": {"protocol": "transform", "text": "hi"}}]
        comm._get_or_create_channel.assert_called_once_with("10.0.0.7", 50061)
        assert not m2m.entries
