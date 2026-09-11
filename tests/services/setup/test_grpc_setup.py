"""Tests for GrpcSetup against the 5-RPC SetupService protocol."""

import asyncio
import datetime
from concurrent import futures
from typing import Any
from unittest.mock import AsyncMock, Mock

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.setup.v1 import (
    setup_pb2,
    setup_service_pb2,
    setup_service_pb2_grpc,
)
from mock_setup_servicer import MockSetupServicer
from pydantic import ValidationError
from tests.fixtures.grpc_fixtures import AsyncStubWrapper, FakeContext

from digitalkin.grpc_servers.exceptions import PermissionDeniedError, ServerError
from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.registry import RegistrySetupStatus
from digitalkin.models.services.storage import Visibility
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode
from digitalkin.services.setup.default_setup import DefaultSetup
from digitalkin.services.setup.exceptions import SetupServiceError
from digitalkin.services.setup.grpc_setup import GrpcSetup
from digitalkin.services.setup.setup_strategy import SetupData

service_name = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]

# The readable half of `documentation` lands on SetupVersion in a protocol release later than
# the pinned 1.0.2.dev1; probe the descriptor so the round-trip test arms itself on upgrade
# instead of sitting red (or being forgotten) in the meantime.


@pytest.fixture
def thread_pool():
    """Create thread pool and ensure cleanup.

    Yields:
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


def _seed_setup(mock_servicer: MockSetupServicer, name: str = "seeded", documentation: str = "") -> setup_pb2.Setup:
    """Create a setup directly in the mock servicer's store."""
    response = mock_servicer.CreateSetup(
        setup_pb2.CreateSetupRequest(name=name, content={"k": "v"}, documentation=documentation), FakeContext()
    )
    return mock_servicer.setups[response.setup.id]


def _exchange(client_call, test_channel: grpc_testing.Channel, method: str, servicer_fn) -> Any:
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
    @pytest.mark.integration
    def test_create_setup_sends_documentation(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        future = thread_pool.submit(
            asyncio.run,
            client.create_setup({"name": "s", "content": {}, "documentation": "what it does"}),
        )
        request = _exchange(future, test_channel, "CreateSetup", mock_servicer.CreateSetup)

        assert request.documentation == "what it does"
        future.result()

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_create_setup_reads_documentation_back_off_the_version(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """The text round-trips on the version that was cut with it."""
        future = thread_pool.submit(
            asyncio.run,
            client.create_setup({"name": "s", "content": {}, "documentation": "what it does"}),
        )
        _exchange(future, test_channel, "CreateSetup", mock_servicer.CreateSetup)

        assert future.result().current_setup_version.documentation == "what it does"

    @pytest.mark.grpc
    @pytest.mark.validation
    async def test_create_setup_missing_fields_no_rpc(self, client: GrpcSetup) -> None:
        with pytest.raises(ValueError, match="name and content"):
            await client.create_setup({"name": "", "content": {"a": 1}})
        with pytest.raises(ValueError, match="name and content"):
            await client.create_setup({"name": "x", "content": "not-a-dict"})

    @pytest.mark.grpc
    @pytest.mark.validation
    async def test_create_setup_oversized_output_format_spec_no_rpc(self, client: GrpcSetup) -> None:
        """The guard trips before the channel is touched, and stays a ValueError."""
        with pytest.raises(ValueError, match="must stay under 4096"):
            await client.create_setup({"name": "x", "content": {"output_format_spec": "x" * 4096}})

    @pytest.mark.grpc
    @pytest.mark.validation
    async def test_create_setup_oversized_documentation_no_rpc(self, client: GrpcSetup) -> None:
        """Over the proto's max_len 300: refused before the channel is touched, as a ValueError."""
        with pytest.raises(ValueError, match="documentation is 301 characters"):
            await client.create_setup({"name": "x", "content": {"a": 1}, "documentation": "x" * 301})

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_create_setup_permission_denied(self, client: GrpcSetup) -> None:
        """Setup's handler lets a permission error pass through unwrapped (not SetupServiceError)."""
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
    def test_get_setup_reads_documentation_off_the_version(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """A get surfaces the text stored with the setup's active version."""
        seeded = _seed_setup(mock_servicer, documentation="the house voice")

        future = thread_pool.submit(asyncio.run, client.get_setup({"setup_id": seeded.id}))
        _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)

        assert future.result().current_setup_version.documentation == "the house voice"

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
    @pytest.mark.integration
    def test_update_setup_sends_documentation(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        seeded = _seed_setup(mock_servicer)

        future = thread_pool.submit(
            asyncio.run,
            client.update_setup({"setup_id": seeded.id, "name": "n", "content": {}, "documentation": "revised"}),
        )
        request = _exchange(future, test_channel, "UpdateSetup", mock_servicer.UpdateSetup)

        assert request.documentation == "revised"
        future.result()

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

    @pytest.mark.grpc
    @pytest.mark.validation
    async def test_update_setup_oversized_output_format_spec_no_rpc(self, client: GrpcSetup) -> None:
        with pytest.raises(ValueError, match="must stay under 4096"):
            await client.update_setup({"setup_id": "s1", "name": "x", "content": {"output_format_spec": "x" * 4096}})

    @pytest.mark.grpc
    @pytest.mark.validation
    async def test_update_setup_oversized_documentation_no_rpc(self, client: GrpcSetup) -> None:
        with pytest.raises(ValueError, match="documentation is 301 characters"):
            await client.update_setup({"setup_id": "s1", "name": "x", "content": {"a": 1}, "documentation": "x" * 301})


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

        future = thread_pool.submit(asyncio.run, client.change_visibility({"setup_id": seeded.id, "visibility": scope}))
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

    def test_sibling_version_is_the_source(self) -> None:
        """The response-level setup_version fills the model when the Setup embeds none.

        Not "whatever the Setup carries": an embedded ``current_setup_version`` still wins.
        ``TestMissingContentDiagnostic`` covers what that precedence costs when the two
        disagree.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        setup = setup_pb2.Setup(id="s1", name="n", organisation_id="o", owner_id="u", module_id="m")
        sibling = setup_pb2.SetupVersion(
            id="v-sibling", setup_id="s1", version="1.0.0", content={"a": 1}, creation_date=now
        )

        result = GrpcSetup._to_setup_data(setup, sibling)

        assert result.current_setup_version.id == "v-sibling"
        assert result.current_setup_version.version == "1.0.0"
        assert result.current_setup_version.content == {"a": 1}


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
            client.update_setup({
                "setup_id": setup.id,
                "name": "renamed",
                "content": {"a": 2},
                "set_as_current": False,
            }),
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
                setup_pb2.UpdateSetupRequest(setup_id=setup.id, name="seeded", content={"a": i}, set_as_current=True),
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
        request = _exchange(future, test_channel, "SetCurrentSetupVersion", mock_servicer.SetCurrentSetupVersion)
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


class TestAuthoredStructureOnTheWire:
    """A supplied key map crosses the wire exactly as the agent wrote it."""

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_create_sends_the_authored_map(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        authored = {"llm.provider": "which backend routes the call", "region": "where it runs"}
        future = thread_pool.submit(
            asyncio.run,
            client.create_setup({
                "name": "n",
                "content": {"llm": {"provider": "litellm"}, "region": "eu-west"},
                "structure": authored,
            }),
        )

        request = _exchange(future, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        result = future.result()

        assert dict(request.structure) == authored
        assert result.current_setup_version.structure == authored

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_update_sends_the_authored_map(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        create = thread_pool.submit(asyncio.run, client.create_setup({"name": "n", "content": {"a": "one"}}))
        _exchange(create, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        setup = create.result()

        future = thread_pool.submit(
            asyncio.run,
            client.update_setup({
                "setup_id": setup.id,
                "name": "n",
                "content": {"a": "two"},
                "structure": {"a": "the a knob"},
            }),
        )

        request = _exchange(future, test_channel, "UpdateSetup", mock_servicer.UpdateSetup)
        future.result()

        assert dict(request.structure) == {"a": "the a knob"}


class TestStructureOnTheWire:
    """With no map supplied none is sent, and one key path is forwarded on reads."""

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_create_without_a_map_sends_none(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        """The agent writes the map; nothing is invented for a caller that supplied none."""
        content = {"llm": {"provider": "litellm"}, "region": "eu-west"}
        future = thread_pool.submit(asyncio.run, client.create_setup({"name": "n", "content": content}))

        request = _exchange(future, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        future.result()

        assert dict(request.structure) == {}

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_update_without_a_map_sends_none(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        """A revision carries only the map its own call supplied — the old one is not reused."""
        create = thread_pool.submit(
            asyncio.run, client.create_setup({"name": "n", "content": {"a": "one"}, "structure": {"a": "knob"}})
        )
        _exchange(create, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        setup = create.result()

        future = thread_pool.submit(
            asyncio.run,
            client.update_setup({"setup_id": setup.id, "name": "n", "content": {"a": "two", "b": "new"}}),
        )
        request = _exchange(future, test_channel, "UpdateSetup", mock_servicer.UpdateSetup)
        future.result()

        assert dict(request.structure) == {}

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_get_forwards_the_structure_key_and_server_projects(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        create = thread_pool.submit(
            asyncio.run,
            client.create_setup({"name": "n", "content": {"llm": {"provider": "litellm"}, "region": "eu"}}),
        )
        _exchange(create, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        setup = create.result()

        future = thread_pool.submit(
            asyncio.run, client.get_setup({"setup_id": setup.id, "structure_key": "llm.provider"})
        )
        request = _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)
        result = future.result()

        assert request.structure_key == "llm.provider"
        assert result.current_setup_version.content == {"llm.provider": "litellm"}

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_get_without_a_key_sends_an_empty_string(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        create = thread_pool.submit(asyncio.run, client.create_setup({"name": "n", "content": {"a": 1}}))
        _exchange(create, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        setup = create.result()

        future = thread_pool.submit(asyncio.run, client.get_setup({"setup_id": setup.id}))
        request = _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)
        result = future.result()

        assert not request.structure_key
        assert result.current_setup_version.content == {"a": 1}

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_get_leaves_structure_uncarried(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        """GetSetupResponse carries no structure field: None, not a claimed-empty map."""
        create = thread_pool.submit(asyncio.run, client.create_setup({"name": "n", "content": {"a": 1}}))
        _exchange(create, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        setup = create.result()

        future = thread_pool.submit(asyncio.run, client.get_setup({"setup_id": setup.id}))
        _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)

        assert future.result().current_setup_version.structure is None

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_get_with_an_unresolvable_key_is_not_found(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        create = thread_pool.submit(asyncio.run, client.create_setup({"name": "n", "content": {"a": 1}}))
        _exchange(create, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        setup = create.result()

        future = thread_pool.submit(asyncio.run, client.get_setup({"setup_id": setup.id, "structure_key": "nope"}))
        _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)

        with pytest.raises(ServerError, match="NOT_FOUND"):
            future.result()


@pytest.mark.contract
class TestLocalRemoteParity:
    """LOCAL and REMOTE must agree on the structure surface, or behaviour changes with deployment.

    ``DefaultSetup`` mirrors the backend by design (`.claude/rules/services.md`), and the
    projection contract in particular is easy to drift: the wire cannot distinguish an empty
    ``structure_key`` from an absent one, so both strategies must read "" as the whole document.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_an_authored_map_is_stored_identically(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        """Neither strategy edits the map, so both keep an entry the content does not have."""
        payload = {
            "name": "n",
            "content": {"llm": {"model": "gpt-4o"}},
            "structure": {"llm.model": "which model answers", "absent.key": "not in this document"},
        }

        future = thread_pool.submit(asyncio.run, client.create_setup(dict(payload)))
        _exchange(future, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        remote = future.result()
        local = asyncio.run(DefaultSetup().create_setup(dict(payload)))

        assert remote.current_setup_version.structure == payload["structure"]
        assert local.current_setup_version.structure == remote.current_setup_version.structure

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_an_absent_map_stays_absent_identically(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        payload = {"name": "n", "content": {"llm": {"model": "gpt-4o"}, "region": "eu"}}

        future = thread_pool.submit(asyncio.run, client.create_setup(dict(payload)))
        _exchange(future, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        remote = future.result()
        local = asyncio.run(DefaultSetup().create_setup(dict(payload)))

        assert local.current_setup_version.structure == remote.current_setup_version.structure

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_a_plain_read_carries_no_structure_on_either_side(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        """GetSetupResponse has no structure field, so neither strategy may return one.

        DefaultSetup holds the stored map and could serve it. Doing so would let code read
        the map locally and silently get none in production, which is the divergence this
        class exists to catch.
        """
        payload = {"name": "n", "content": {"llm": {"model": "gpt-4o"}}, "structure": {"llm.model": "which model"}}

        create = thread_pool.submit(asyncio.run, client.create_setup(dict(payload)))
        _exchange(create, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        created = create.result()
        future = thread_pool.submit(asyncio.run, client.get_setup({"setup_id": created.id}))
        _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)
        remote = future.result()

        strategy = DefaultSetup()
        local_created = asyncio.run(strategy.create_setup(dict(payload)))
        local = asyncio.run(strategy.get_setup({"setup_id": local_created.id}))

        assert remote.current_setup_version.structure is None
        assert local.current_setup_version.structure is None
        # ...and the map really was stored; only the read drops it.
        assert strategy.setups[local_created.id].current_setup_version.structure == {"llm.model": "which model"}

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_an_unresolvable_key_is_refused_on_either_side(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        """The backend answers NOT_FOUND for a key the content does not have, never the full read."""
        content = {"llm": {"model": "gpt-4o"}, "region": "eu"}

        create = thread_pool.submit(asyncio.run, client.create_setup({"name": "n", "content": content}))
        _exchange(create, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        created = create.result()
        future = thread_pool.submit(
            asyncio.run, client.get_setup({"setup_id": created.id, "structure_key": "does.not.exist"})
        )
        _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)

        strategy = DefaultSetup()
        local_created = asyncio.run(strategy.create_setup({"name": "n", "content": content}))

        with pytest.raises(ServerError, match="NOT_FOUND"):
            future.result()
        with pytest.raises(SetupServiceError, match=r"no path does\.not\.exist"):
            asyncio.run(strategy.get_setup({"setup_id": local_created.id, "structure_key": "does.not.exist"}))

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    @pytest.mark.parametrize("key", ["", "llm.model"])
    def test_projection_agrees_on_the_key(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool, key: str
    ) -> None:
        """An empty key means the whole document on both sides — the wire has no presence to say otherwise."""
        content = {"llm": {"model": "gpt-4o"}, "region": "eu"}

        create = thread_pool.submit(asyncio.run, client.create_setup({"name": "n", "content": content}))
        _exchange(create, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        created = create.result()
        future = thread_pool.submit(asyncio.run, client.get_setup({"setup_id": created.id, "structure_key": key}))
        _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)
        remote = future.result()

        local_strategy = DefaultSetup()
        local_created = asyncio.run(local_strategy.create_setup({"name": "n", "content": content}))
        local = asyncio.run(local_strategy.get_setup({"setup_id": local_created.id, "structure_key": key}))

        assert local.current_setup_version.content == remote.current_setup_version.content


@pytest.mark.regression
class TestMissingContentDiagnostic:
    """A version arriving without content must say which of the two fields was read.

    Production (archetype-ada, 2026-09-08/09) failed here with a bare pydantic
    "current_setup_version.content Field required", which named neither the setup nor
    which SetupVersion the SDK had taken — the response carries the version twice.
    """

    @staticmethod
    def _stub() -> setup_pb2.SetupVersion:
        """A SetupVersion with identity but no content Struct — the shape production returned."""
        return setup_pb2.SetupVersion(id="setup_versions:01ABC", setup_id="setups:01X", version="1.0.0")

    def test_names_the_sibling_when_only_it_has_content(self) -> None:
        """The actionable case: the payload was in the field the SDK did not read."""
        full = self._stub()
        full.content.update({"a": 1})
        setup = setup_pb2.Setup(id="setups:01X", name="n", module_id="m")
        setup.current_setup_version.CopyFrom(self._stub())

        with pytest.raises(SetupServiceError) as excinfo:
            GrpcSetup._to_setup_data(setup, full)

        assert "read from setup.current_setup_version" in str(excinfo.value)
        assert "sibling setup_version carries content" in str(excinfo.value)
        assert "setups:01X" in str(excinfo.value)

    def test_says_so_when_neither_carries_content(self) -> None:
        """The backend-side case: nothing to read anywhere."""
        setup = setup_pb2.Setup(id="setups:01X", name="n", module_id="m")
        setup.current_setup_version.CopyFrom(self._stub())

        with pytest.raises(SetupServiceError, match="sibling setup_version empty too"):
            GrpcSetup._to_setup_data(setup, setup_pb2.SetupVersion())

    def test_reports_the_sibling_as_the_source_when_no_embedded_version(self) -> None:
        setup = setup_pb2.Setup(id="setups:01X", name="n", module_id="m")

        with pytest.raises(SetupServiceError, match=r"read from setup_version; sibling setup_version not populated"):
            GrpcSetup._to_setup_data(setup, self._stub())

    def test_an_empty_but_present_content_is_accepted(self) -> None:
        """{} is a legitimate configuration; only an unset Struct is the failure."""
        version = self._stub()
        version.content.SetInParent()
        version.creation_date.FromDatetime(datetime.datetime.now(datetime.timezone.utc))
        setup = setup_pb2.Setup(id="setups:01X", name="n", module_id="m")
        setup.current_setup_version.CopyFrom(version)

        assert GrpcSetup._to_setup_data(setup, version).current_setup_version.content == {}

    def test_creation_date_is_the_same_trap_one_field_over(self) -> None:
        """An unset Timestamp is dropped like an unset Struct, and the model requires it too.

        Production always sent it (as epoch), so this is latent rather than live — but it
        fails with the same opaque "Field required" the content check was added to replace.
        """
        version = self._stub()
        version.content.SetInParent()
        setup = setup_pb2.Setup(id="setups:01X", name="n", module_id="m")
        setup.current_setup_version.CopyFrom(version)

        with pytest.raises(ValidationError, match="creation_date"):
            GrpcSetup._to_setup_data(setup, version)
