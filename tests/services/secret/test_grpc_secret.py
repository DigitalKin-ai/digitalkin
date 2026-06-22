"""Tests for GrpcSecret (backed by UserProfileService.GetSetupSecret)."""

import asyncio
import types
from concurrent import futures
from typing import Any

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.user_profile.v1 import (
    user_profile_pb2,
    user_profile_service_pb2,
    user_profile_service_pb2_grpc,
)
from google.protobuf import struct_pb2

from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.secret.grpc_secret import GrpcSecret

pytestmark = pytest.mark.timeout(20)

MISSION_ID = "missions:test"
SETUP_ID = "setups:test"
_service = user_profile_service_pb2.DESCRIPTOR.services_by_name["UserProfileService"]


async def _test_exec_grpc_query(self: Any, query_endpoint: str, request: Any) -> Any:
    response = getattr(self.stub, query_endpoint)(request)
    return await response if asyncio.iscoroutine(response) else response


@pytest.fixture
def thread_pool():
    pool = futures.ThreadPoolExecutor(max_workers=10)
    yield pool
    pool.shutdown(wait=True, cancel_futures=True)


@pytest.fixture
def test_channel() -> grpc_testing.Channel:
    return grpc_testing.channel([_service], grpc_testing.strict_real_time())


@pytest.fixture
def dummy_client_config() -> ClientConfig:
    from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode

    return ClientConfig(
        host="[::]", port=50051, mode=ControlFlow.ASYNC, security=SecurityMode.INSECURE, credentials=None
    )


@pytest.fixture
def client(test_channel: grpc_testing.Channel, dummy_client_config: ClientConfig) -> GrpcSecret:
    secret_client = GrpcSecret(
        mission_id=MISSION_ID, setup_id=SETUP_ID, setup_version_id="v", client_config=dummy_client_config
    )
    secret_client.stub = user_profile_service_pb2_grpc.UserProfileServiceStub(test_channel)
    secret_client.exec_grpc_query = types.MethodType(_test_exec_grpc_query, secret_client)
    return secret_client


class TestGrpcSecret:
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_secret_success(
        self,
        client: GrpcSecret,
        test_channel: grpc_testing.Channel,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """A resolved secret returns its dict, forwarding setup_id + mission_id."""
        method_desc = _service.methods_by_name["GetSetupSecret"]
        future = thread_pool.submit(asyncio.run, client.get_secret())
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert request.setup_id == SETUP_ID
        assert request.mission_id == MISSION_ID

        secret = struct_pb2.Struct()
        secret.update({"api_key": "xyz"})
        rpc.terminate(user_profile_pb2.GetSetupSecretResponse(success=True, secret=secret), (), grpc.StatusCode.OK, "")
        assert future.result(timeout=5.0) == {"api_key": "xyz"}

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_get_secret_not_found_returns_none(
        self,
        client: GrpcSecret,
        test_channel: grpc_testing.Channel,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """success=False resolves to None."""
        method_desc = _service.methods_by_name["GetSetupSecret"]
        future = thread_pool.submit(asyncio.run, client.get_secret())
        _, _request, rpc = test_channel.take_unary_unary(method_desc)
        rpc.terminate(user_profile_pb2.GetSetupSecretResponse(success=False), (), grpc.StatusCode.OK, "")
        assert future.result(timeout=5.0) is None
