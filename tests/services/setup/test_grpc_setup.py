"""Tests for GrpcSetup against the 5-RPC SetupService protocol."""

import asyncio
import datetime
from concurrent import futures
from unittest.mock import AsyncMock, Mock

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.setup.v1 import (
    setup_pb2,
    setup_service_pb2,
    setup_service_pb2_grpc,
)

from digitalkin.grpc_servers.exceptions import PermissionDeniedError, ServerError
from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.registry import RegistrySetupStatus
from digitalkin.models.services.storage import Visibility
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode
from digitalkin.services.setup.exceptions import SetupServiceError
from digitalkin.services.setup.grpc_setup import GrpcSetup
from digitalkin.services.setup.setup_strategy import SetupData
from mock_setup_servicer import MockSetupServicer
from tests.fixtures.grpc_fixtures import AsyncStubWrapper, FakeContext

service_name = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]


@pytest.fixture
def thread_pool():
    """Create thread pool and ensure cleanup.

    Returns:
        ThreadPoolExecutor instance
    """
    pool = futures.ThreadPoolExecutor(max_workers=1)
    yield pool
    pool.shutdown(wait=True, cancel_futures=True)


@pytest.fixture
def test_channel() -> grpc_testing.Channel:
    """Mock a gRPC channel.

    Returns:
        Mock gRPC Channel
    """
    return grpc_testing.channel([service_name], grpc_testing.strict_real_time())


@pytest.fixture
def mock_servicer() -> MockSetupServicer:
    """Return an instance of the mock servicer.

    Returns:
        Mock Setup Servicer
    """
    return MockSetupServicer()


@pytest.fixture
def client(test_channel: grpc_testing.Channel) -> GrpcSetup:
    """Instantiate a GrpcSetup client that uses the test channel.

    Returns:
        gRPC client as GrpcSetup
    """
    dummy_config = ClientConfig(
        host="[::]",
        port=50151,
        mode=ControlFlow.ASYNC,
        security=SecurityMode.INSECURE,
        credentials=None,
    )
    client = GrpcSetup()
    client.__post_init__(dummy_config)
    client.stub = AsyncStubWrapper(setup_service_pb2_grpc.SetupServiceStub(test_channel))
    return client


def _seed_setup(mock_servicer: MockSetupServicer, name: str = "seeded") -> setup_pb2.Setup:
    """Create a setup directly in the mock servicer's store."""
    response = mock_servicer.CreateSetup(
        setup_pb2.CreateSetupRequest(name=name, content={"k": "v"}), FakeContext()
    )
    return mock_servicer.setups[response.setup.id]


def _exchange(client_call, test_channel: grpc_testing.Channel, method: str, servicer_fn):
    """Intercept the pending RPC, run the servicer, terminate, return (request, result-getter)."""
    method_desc = service_name.methods_by_name[method]
    _, request, rpc = test_channel.take_unary_unary(method_desc)
    context = FakeContext()
    response = servicer_fn(request, context)
    rpc.send_initial_metadata(())
    rpc.terminate(response, (), context._code or grpc.StatusCode.OK, context._details or "")
    return request


class TestCreateSetup:
    """create_setup sends {name, content} and assembles SetupData from the response."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_create_setup_success(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        future = thread_pool.submit(asyncio.run, client.create_setup({"name": "my setup", "content": {"a": 1}}))
        request = _exchange(future, test_channel, "CreateSetup", mock_servicer.CreateSetup)

        # The client only sends name + content — identifiers derive server-side.
        assert request.name == "my setup"
        assert dict(request.content) == {"a": 1}

        result = future.result()
        assert isinstance(result, SetupData)
        assert result.name == "my setup"
        assert result.organisation_id == "ctx-org"
        assert result.owner_id == "ctx-owner"
        assert result.module_id == "ctx-module"
        assert result.status == RegistrySetupStatus.READY
        assert result.visibility == Visibility.PRIVATE
        # Version arrived via the response-level sibling (fallback merge path).
        assert result.current_setup_version.content == {"a": 1}
        assert result.current_setup_version.setup_id == result.id

    @pytest.mark.grpc
    @pytest.mark.validation
    async def test_create_setup_missing_fields_no_rpc(self, client: GrpcSetup) -> None:
        with pytest.raises(ValueError, match="name and content"):
            await client.create_setup({"name": "", "content": {"a": 1}})
        with pytest.raises(ValueError, match="name and content"):
            await client.create_setup({"name": "x", "content": "not-a-dict"})

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_create_setup_permission_denied(self, client: GrpcSetup) -> None:
        """setup's handler lets a permission error pass through unwrapped (not SetupServiceError)."""
        CircuitBreaker.remove("SetupService")
        client.stub = Mock()
        client.stub.CreateSetup = AsyncMock(side_effect=PermissionDeniedError("[/SetupService/CreateSetup] denied"))

        with pytest.raises(PermissionDeniedError):
            await client.create_setup({"name": "x", "content": {}})

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_create_setup_server_refusal(self, client: GrpcSetup) -> None:
        CircuitBreaker.remove("SetupService")
        client.stub = Mock()
        client.stub.CreateSetup = AsyncMock(return_value=setup_pb2.CreateSetupResponse(success=False))

        with pytest.raises(SetupServiceError, match="refused"):
            await client.create_setup({"name": "x", "content": {}})


class TestGetSetup:
    """get_setup reads by id, optionally pinning a version."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_setup_success(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        seeded = _seed_setup(mock_servicer)

        future = thread_pool.submit(asyncio.run, client.get_setup({"setup_id": seeded.id}))
        request = _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)

        assert request.setup_id == seeded.id
        assert not request.HasField("version")  # no empty-string presence

        result = future.result()
        assert result.id == seeded.id
        assert result.name == "seeded"
        # Embedded current_setup_version wins (preferred merge path).
        assert result.current_setup_version.content == {"k": "v"}

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_get_setup_pins_version(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        seeded = _seed_setup(mock_servicer)

        future = thread_pool.submit(asyncio.run, client.get_setup({"setup_id": seeded.id, "version": "1.0.0"}))
        request = _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)

        assert request.HasField("version")
        assert request.version == "1.0.0"
        future.result()

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_get_setup_not_found(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        future = thread_pool.submit(asyncio.run, client.get_setup({"setup_id": "nonexistent_id"}))
        _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)

        with pytest.raises(ServerError, match="NOT_FOUND"):
            future.result()

    @pytest.mark.grpc
    @pytest.mark.validation
    async def test_get_setup_missing_id_no_rpc(self, client: GrpcSetup) -> None:
        with pytest.raises(ValueError, match="setup_id is required"):
            await client.get_setup({})


class TestUpdateSetup:
    """update_setup sends {setup_id, name, content} and returns the updated SetupData."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_update_setup_success(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        seeded = _seed_setup(mock_servicer)

        future = thread_pool.submit(
            asyncio.run,
            client.update_setup({"setup_id": seeded.id, "name": "renamed", "content": {"a": 2}}),
        )
        request = _exchange(future, test_channel, "UpdateSetup", mock_servicer.UpdateSetup)

        assert request.setup_id == seeded.id
        assert request.name == "renamed"
        assert dict(request.content) == {"a": 2}

        result = future.result()
        assert result.name == "renamed"
        assert result.current_setup_version.content == {"a": 2}

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_update_setup_server_refusal(self, client: GrpcSetup) -> None:
        CircuitBreaker.remove("SetupService")
        client.stub = Mock()
        client.stub.UpdateSetup = AsyncMock(return_value=setup_pb2.UpdateSetupResponse(success=False))

        with pytest.raises(SetupServiceError, match="refused"):
            await client.update_setup({"setup_id": "s1", "name": "x", "content": {}})

    @pytest.mark.grpc
    @pytest.mark.validation
    async def test_update_setup_missing_fields_no_rpc(self, client: GrpcSetup) -> None:
        with pytest.raises(ValueError, match="setup_id, name and content"):
            await client.update_setup({"setup_id": "s1", "name": "", "content": {}})


class TestDeleteSetup:
    """delete_setup returns the server's success flag."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_delete_setup_success(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        seeded = _seed_setup(mock_servicer)

        future = thread_pool.submit(asyncio.run, client.delete_setup({"setup_id": seeded.id}))
        request = _exchange(future, test_channel, "DeleteSetup", mock_servicer.DeleteSetup)

        assert request.setup_id == seeded.id
        assert future.result() is True
        assert seeded.id not in mock_servicer.setups

    @pytest.mark.grpc
    @pytest.mark.validation
    async def test_delete_setup_missing_id_no_rpc(self, client: GrpcSetup) -> None:
        with pytest.raises(ValueError, match="setup_id is required"):
            await client.delete_setup({})


class TestChangeVisibility:
    """change_visibility encodes the scope fail-closed and returns the updated setup."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    @pytest.mark.parametrize(
        ("scope", "proto_name"),
        [
            ("public", "VISIBILITY_PUBLIC"),
            ("internal", "VISIBILITY_INTERNAL"),
            ("private", "VISIBILITY_PRIVATE"),
        ],
    )
    def test_change_visibility_success(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
        scope: str,
        proto_name: str,
    ) -> None:
        seeded = _seed_setup(mock_servicer)

        future = thread_pool.submit(
            asyncio.run, client.change_visibility({"setup_id": seeded.id, "visibility": scope})
        )
        request = _exchange(future, test_channel, "ChangeVisibility", mock_servicer.ChangeVisibility)

        assert request.setup_id == seeded.id
        assert request.visibility == setup_pb2.Visibility.Value(proto_name)

        result = future.result()
        assert result.visibility == Visibility(proto_name)
        assert mock_servicer.setups[seeded.id].visibility == setup_pb2.Visibility.Value(proto_name)

    @pytest.mark.grpc
    @pytest.mark.validation
    @pytest.mark.parametrize("scope", ["", "unspecified", "PUBLIC ", "org", None])
    async def test_change_visibility_invalid_scope_no_rpc(self, client: GrpcSetup, scope: object) -> None:
        with pytest.raises(ValueError, match="invalid visibility"):
            await client.change_visibility({"setup_id": "s1", "visibility": scope})

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_change_visibility_server_refusal(self, client: GrpcSetup) -> None:
        CircuitBreaker.remove("SetupService")
        client.stub = Mock()
        client.stub.ChangeVisibility = AsyncMock(return_value=setup_pb2.ChangeVisibilityResponse(success=False))

        with pytest.raises(SetupServiceError, match="refused"):
            await client.change_visibility({"setup_id": "s1", "visibility": "public"})


class TestResponseMerging:
    """_to_setup_data merge semantics."""

    def test_missing_version_everywhere_raises(self) -> None:
        setup = setup_pb2.Setup(id="s1", name="n", organisation_id="o", owner_id="u", module_id="m")
        with pytest.raises(SetupServiceError, match="without a setup version"):
            GrpcSetup._to_setup_data(setup, setup_pb2.SetupVersion())

    def test_embedded_version_wins_over_sibling(self) -> None:
        now = datetime.datetime.now(datetime.timezone.utc)
        setup = setup_pb2.Setup(
            id="s1",
            name="n",
            organisation_id="o",
            owner_id="u",
            module_id="m",
            current_setup_version=setup_pb2.SetupVersion(
                id="v-embedded", setup_id="s1", version="2.0.0", content={"a": 1}, creation_date=now
            ),
        )
        sibling = setup_pb2.SetupVersion(id="v-sibling", setup_id="s1", version="1.0.0", content={}, creation_date=now)
        result = GrpcSetup._to_setup_data(setup, sibling)
        assert result.current_setup_version.id == "v-embedded"
        assert result.current_setup_version.version == "2.0.0"


class TestSetupVersions:
    """ListSetupVersions / SetCurrentSetupVersion, and the set_as_current flag on updates."""

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_update_setup_activates_the_new_version_by_default(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """UpdateSetup cuts a version rather than editing in place, so the flag must be set."""
        setup = _seed_setup(mock_servicer)
        future = thread_pool.submit(
            asyncio.run,
            client.update_setup({"setup_id": setup.id, "name": "renamed", "content": {"a": 2}}),
        )
        request = _exchange(future, test_channel, "UpdateSetup", mock_servicer.UpdateSetup)

        assert request.set_as_current is True
        assert future.result().current_setup_version.content == {"a": 2}

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_update_setup_can_leave_the_new_version_inactive(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        setup = _seed_setup(mock_servicer)
        original = setup.current_setup_version.id
        future = thread_pool.submit(
            asyncio.run,
            client.update_setup(
                {"setup_id": setup.id, "name": "renamed", "content": {"a": 2}, "set_as_current": False}
            ),
        )
        request = _exchange(future, test_channel, "UpdateSetup", mock_servicer.UpdateSetup)

        assert request.set_as_current is False
        assert future.result().current_setup_version.id == original

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_list_setup_versions_returns_page_total_and_current(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        setup = _seed_setup(mock_servicer)
        for i in range(2):
            mock_servicer.UpdateSetup(
                setup_pb2.UpdateSetupRequest(
                    setup_id=setup.id, name="seeded", content={"a": i}, set_as_current=True
                ),
                FakeContext(),
            )

        future = thread_pool.submit(asyncio.run, client.list_setup_versions({"setup_id": setup.id}))
        request = _exchange(future, test_channel, "ListSetupVersions", mock_servicer.ListSetupVersions)
        page = future.result()

        # An unset limit must not reach the wire as 0 — the proto floors it at 1.
        assert request.limit == 20
        assert page.total_count == 3
        assert page.current_setup_version_id == setup.current_setup_version.id
        # Most recent first.
        assert [v.content for v in page.setup_versions] == [{"a": 1}, {"a": 0}, {"k": "v"}]

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_list_setup_versions_paginates(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        setup = _seed_setup(mock_servicer)
        mock_servicer.UpdateSetup(
            setup_pb2.UpdateSetupRequest(setup_id=setup.id, name="seeded", content={"a": 1}, set_as_current=True),
            FakeContext(),
        )

        future = thread_pool.submit(
            asyncio.run, client.list_setup_versions({"setup_id": setup.id, "limit": 1, "offset": 1})
        )
        request = _exchange(future, test_channel, "ListSetupVersions", mock_servicer.ListSetupVersions)
        page = future.result()

        assert (request.limit, request.offset) == (1, 1)
        assert page.total_count == 2
        assert [v.content for v in page.setup_versions] == [{"k": "v"}]

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_set_current_setup_version_rolls_back(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        setup = _seed_setup(mock_servicer)
        original = setup.current_setup_version.id
        mock_servicer.UpdateSetup(
            setup_pb2.UpdateSetupRequest(setup_id=setup.id, name="seeded", content={"a": 9}, set_as_current=True),
            FakeContext(),
        )
        assert setup.current_setup_version.id != original

        future = thread_pool.submit(
            asyncio.run,
            client.set_current_setup_version({"setup_id": setup.id, "setup_version_id": original}),
        )
        request = _exchange(
            future, test_channel, "SetCurrentSetupVersion", mock_servicer.SetCurrentSetupVersion
        )
        result = future.result()

        assert request.setup_version_id == original
        assert result.current_setup_version.id == original
        assert result.current_setup_version.content == {"k": "v"}

    @pytest.mark.parametrize(
        ("method", "payload"),
        [
            ("list_setup_versions", {}),
            ("set_current_setup_version", {"setup_id": "s1"}),
            ("set_current_setup_version", {"setup_version_id": "v1"}),
        ],
    )
    def test_missing_identifiers_are_rejected_before_the_wire(
        self, client: GrpcSetup, method: str, payload: dict
    ) -> None:
        with pytest.raises(ValueError, match="required"):
            asyncio.run(getattr(client, method)(payload))
