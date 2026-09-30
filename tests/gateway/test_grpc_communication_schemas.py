"""``GrpcCommunication`` schema lookups against a real ModuleService (``ModuleResult`` envelope).

A real in-process gRPC server runs a fake ModuleService behind the SDK's
``ValidationServerInterceptor``, so the requests the client builds must pass the
protocol's ``buf.validate`` rules (``module_id`` is required) exactly as on a
real target module.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import grpc
import pytest
from agentic_mesh_protocol.module.v1 import module_dto_pb2, module_messages_pb2, module_service_pb2_grpc
from agentic_mesh_protocol.pagination.v1 import bulk_pb2
from google.protobuf import struct_pb2

from digitalkin.grpc_servers.interceptors.validation import ValidationServerInterceptor
from digitalkin.models.grpc_servers.models import ClientConfig, SecurityMode
from digitalkin.services.communication.default_communication import DefaultCommunication
from digitalkin.services.communication.exceptions import CommunicationServiceError
from digitalkin.services.communication.grpc_communication import GrpcCommunication

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

pytestmark = [pytest.mark.grpc, pytest.mark.timeout(15)]

MODULE_ID = "modules:target"


def _schema(name: str) -> struct_pb2.Struct:
    s = struct_pb2.Struct()
    s.update({"title": name})
    return s


class _FakeModuleServicer(module_service_pb2_grpc.ModuleServiceServicer):
    """Answers every schema RPC with a ``ModuleResult``; ``failing`` RPCs answer an ``OperationError``."""

    def __init__(self, failing: frozenset[str] = frozenset()) -> None:
        self.failing = failing
        self.requests: dict[str, Any] = {}

    def _result(self, rpc: str, request: Any, **outcome: struct_pb2.Struct) -> module_messages_pb2.ModuleResult:
        self.requests[rpc] = request
        if rpc in self.failing:
            return module_messages_pb2.ModuleResult(
                identifier=request.module_id,
                error=bulk_pb2.OperationError(code="SCHEMA_UNAVAILABLE", message=f"{rpc} failed"),
            )
        return module_messages_pb2.ModuleResult(identifier=request.module_id, **outcome)

    async def GetModuleInput(self, request: Any, context: Any) -> Any:
        return module_dto_pb2.GetModuleInputResponse(
            result=self._result("GetModuleInput", request, input_schema=_schema("input")),
        )

    async def GetModuleOutput(self, request: Any, context: Any) -> Any:
        return module_dto_pb2.GetModuleOutputResponse(
            result=self._result("GetModuleOutput", request, output_schema=_schema("output")),
        )

    async def GetModuleSetup(self, request: Any, context: Any) -> Any:
        return module_dto_pb2.GetModuleSetupResponse(
            result=self._result("GetModuleSetup", request, setup_schema=_schema("setup")),
        )

    async def GetModuleSecret(self, request: Any, context: Any) -> Any:
        return module_dto_pb2.GetModuleSecretResponse(
            result=self._result("GetModuleSecret", request, secret_schema=_schema("secret")),
        )

    async def GetModuleCost(self, request: Any, context: Any) -> Any:
        return module_dto_pb2.GetModuleCostResponse(
            result=self._result("GetModuleCost", request, cost_schema=_schema("cost")),
        )

    async def GetConfigSetupModule(self, request: Any, context: Any) -> Any:
        return module_dto_pb2.GetConfigSetupModuleResponse(
            result=self._result("GetConfigSetupModule", request, config_setup_schema=_schema("config")),
        )


@pytest.fixture
async def serve() -> AsyncIterator[Any]:
    """Start ``_FakeModuleServicer`` servers behind the validation interceptor, stopped on teardown.

    Yields:
        A factory that starts a server for a servicer and returns its port.
    """
    servers: list[grpc.aio.Server] = []

    async def _start(servicer: _FakeModuleServicer) -> int:
        server = grpc.aio.server(interceptors=[ValidationServerInterceptor()])
        module_service_pb2_grpc.add_ModuleServiceServicer_to_server(servicer, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        servers.append(server)
        return port

    yield _start
    for server in servers:
        await server.stop(grace=0.1)


@pytest.fixture
async def comm() -> AsyncIterator[GrpcCommunication]:
    client = GrpcCommunication(
        mission_id="missions:test",
        setup_id="setups:test",
        setup_version_id="setup_versions:test",
        client_config=ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE),
    )
    yield client
    await client.close()


class TestGetModuleSchemas:
    async def test_unwraps_every_schema_from_its_result(self, serve: Any, comm: GrpcCommunication) -> None:
        servicer = _FakeModuleServicer()
        port = await serve(servicer)

        schemas = await comm.get_module_schemas("127.0.0.1", port, module_id=MODULE_ID, llm_format=True)

        assert schemas == {
            "input": {"title": "input"},
            "output": {"title": "output"},
            "setup": {"title": "setup"},
            "secret": {"title": "secret"},
            "cost": {"title": "cost"},
        }
        assert {rpc: r.module_id for rpc, r in servicer.requests.items()} == dict.fromkeys(
            ("GetModuleInput", "GetModuleOutput", "GetModuleSetup", "GetModuleSecret", "GetModuleCost"), MODULE_ID
        )
        assert servicer.requests["GetModuleInput"].llm_format is True
        assert servicer.requests["GetModuleCost"].llm_format is False  # cost never uses the LLM format

    async def test_operation_error_raises_service_error(self, serve: Any, comm: GrpcCommunication) -> None:
        port = await serve(_FakeModuleServicer(failing=frozenset({"GetModuleSetup"})))

        with pytest.raises(CommunicationServiceError, match="modules:target: SCHEMA_UNAVAILABLE GetModuleSetup failed"):
            await comm.get_module_schemas("127.0.0.1", port, module_id=MODULE_ID)

    async def test_invalid_module_id_is_rejected_by_the_target(self, serve: Any, comm: GrpcCommunication) -> None:
        port = await serve(_FakeModuleServicer())

        with pytest.raises(grpc.aio.AioRpcError) as exc_info:
            await comm.get_module_schemas("127.0.0.1", port, module_id="not-a-module-id")

        assert exc_info.value.code() == grpc.StatusCode.INVALID_ARGUMENT


class TestGetModuleConfigSchema:
    async def test_unwraps_config_setup_schema(self, serve: Any, comm: GrpcCommunication) -> None:
        servicer = _FakeModuleServicer()
        port = await serve(servicer)

        schema = await comm.get_module_config_schema("127.0.0.1", port, module_id=MODULE_ID)

        assert schema == {"title": "config"}
        assert servicer.requests["GetConfigSetupModule"].module_id == MODULE_ID

    async def test_operation_error_raises_service_error(self, serve: Any, comm: GrpcCommunication) -> None:
        port = await serve(_FakeModuleServicer(failing=frozenset({"GetConfigSetupModule"})))

        with pytest.raises(CommunicationServiceError, match="SCHEMA_UNAVAILABLE"):
            await comm.get_module_config_schema("127.0.0.1", port, module_id=MODULE_ID)


class TestDefaultCommunicationSchemas:
    async def test_default_returns_empty_schemas(self) -> None:
        schemas = await DefaultCommunication("missions:m", "setups:s", "setup_versions:v").get_module_schemas(
            "127.0.0.1", 1, module_id=MODULE_ID
        )

        assert schemas == {"input": {}, "output": {}, "setup": {}, "secret": {}}

    async def test_default_config_schema_is_empty(self) -> None:
        schema = await DefaultCommunication("missions:m", "setups:s", "setup_versions:v").get_module_config_schema(
            "127.0.0.1", 1, module_id=MODULE_ID
        )

        assert schema == {}
