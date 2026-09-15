"""Tests for GrpcSetup against the SetupService + SetupVersionService protocol."""

import asyncio
import datetime
import logging
from concurrent import futures
from typing import Any
from unittest.mock import AsyncMock, Mock

import grpc
import grpc_testing
import protovalidate
import pytest
from agentic_mesh_protocol.common.v1 import common_enums_pb2
from agentic_mesh_protocol.pagination.v1 import bulk_pb2, pagination_pb2
from agentic_mesh_protocol.setup.v1 import (
    setup_dto_pb2,
    setup_enums_pb2,
    setup_messages_pb2,
    setup_service_pb2,
    setup_version_dto_pb2,
    setup_version_service_pb2,
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
from digitalkin.services.setup.grpc_setup import GrpcSetup, SetupServicesStub
from digitalkin.services.setup.setup_strategy import SetupData, SetupVersionData

setup_service = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
version_service = setup_version_service_pb2.DESCRIPTOR.services_by_name["SetupVersionService"]


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
    """Mock a gRPC channel serving both setup services.

    Returns:
        Mock gRPC Channel
    """
    return grpc_testing.channel([setup_service, version_service], grpc_testing.strict_real_time())


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
    client.stub = AsyncStubWrapper(SetupServicesStub(test_channel))
    return client


def _seed_setup(
        mock_servicer: MockSetupServicer, name: str = "seeded", documentation: str = ""
) -> setup_messages_pb2.Setup:
    """Create a setup directly in the mock servicer's store."""
    response = mock_servicer.CreateSetup(
        setup_dto_pb2.CreateSetupRequest(
            name=name, revision=setup_messages_pb2.SetupRevision(content={"k": "v"}, documentation=documentation)
        ),
        FakeContext(),
    )
    return mock_servicer.setups[response.result.setup.id]


def _cut(mock_servicer: MockSetupServicer, setup_id: str, content: dict) -> None:
    """Cut and activate a new version directly in the mock servicer's store."""
    mock_servicer.UpdateSetup(
        setup_dto_pb2.UpdateSetupRequest(
            setup_id=setup_id, revision=setup_messages_pb2.SetupRevision(content=content), set_as_current=True
        ),
        FakeContext(),
    )


def _stage(mock_servicer: MockSetupServicer, setup_id: str, label: str = "2.0.0") -> setup_messages_pb2.SetupVersion:
    """Cut a staged (not current) version directly in the mock servicer's store."""
    response = mock_servicer.CreateSetupVersion(
        setup_version_dto_pb2.CreateSetupVersionRequest(
            setup_id=setup_id,
            version=label,
            revision=setup_messages_pb2.SetupRevision(content={"k": "staged"}, documentation="staged doc"),
        ),
        FakeContext(),
    )
    return response.result.version


def _setup_msg(setup_id: str) -> setup_messages_pb2.Setup:
    """A complete Setup message, as a listing would carry it."""
    return setup_messages_pb2.Setup(
        id=setup_id,
        name=setup_id,
        organization_id="organizations:o",
        owner_id="users:u",
        module_id="modules:m",
        status="READY",
        current_setup_version=setup_messages_pb2.SetupVersion(
            id="setup_versions:" + setup_id,
            setup_id=setup_id,
            version="1.0.0",
            content={"a": 1},
            created_at=datetime.datetime.now(datetime.timezone.utc),
        ),
    )


def _refuse_with(code: grpc.StatusCode) -> Any:
    """A servicer function answering any request with a bare gRPC status, no result."""

    def servicer_fn(request: Any, context: FakeContext) -> None:
        context.set_code(code)
        context.set_details(f"{code.name} from the service")

    return servicer_fn


def _error_result(identifier: str, code: str = "NOT_FOUND") -> setup_messages_pb2.SetupResult:
    """A SetupResult whose outcome is an OperationError."""
    return setup_messages_pb2.SetupResult(
        identifier=identifier, error=bulk_pb2.OperationError(code=code, message="refused by the service")
    )


def _exchange(client_call, test_channel: grpc_testing.Channel, method: str, servicer_fn) -> Any:
    """Intercept the pending RPC on whichever setup service owns it, run the servicer, terminate."""
    method_desc = setup_service.methods_by_name.get(method) or version_service.methods_by_name[method]
    _, request, rpc = test_channel.take_unary_unary(method_desc)
    context = FakeContext()
    response = servicer_fn(request, context)
    rpc.send_initial_metadata(())
    rpc.terminate(response, (), context._code or grpc.StatusCode.OK, context._details or "")
    return request


def _mock_stub(method: str, response: Any) -> Mock:
    """A stub whose single RPC returns ``response``."""
    CircuitBreaker.remove("SetupService")
    stub = Mock()
    stub.configure_mock(**{method: AsyncMock(return_value=response)})
    return stub


class TestCreateSetup:
    """create_setup sends {name, revision} and assembles SetupData from the result."""

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

        # The client only sends name + revision — identifiers derive server-side.
        assert request.name == "my setup"
        assert dict(request.revision.content) == {"a": 1}
        assert not request.HasField("module_id")

        result = future.result()
        assert isinstance(result, SetupData)
        assert result.name == "my setup"
        assert result.organisation_id == "organizations:ctx"
        assert result.owner_id == "users:ctx"
        assert result.module_id == "modules:ctx"
        assert result.status == RegistrySetupStatus.READY
        assert result.visibility == Visibility.PRIVATE
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

        assert request.revision.documentation == "what it does"
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
    async def test_create_setup_operation_error(self, client: GrpcSetup) -> None:
        """A result holding an OperationError surfaces as SetupServiceError with its code."""
        client.stub = _mock_stub(
            "CreateSetup", setup_dto_pb2.CreateSetupResponse(result=_error_result("x", "ALREADY_EXISTS"))
        )

        with pytest.raises(SetupServiceError, match="x: ALREADY_EXISTS refused by the service"):
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
        assert not request.HasField("structure_key")

        result = future.result()
        assert result.id == seeded.id
        assert result.name == "seeded"
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
        """An unknown id comes back as an OperationError result, raised as SetupServiceError."""
        future = thread_pool.submit(asyncio.run, client.get_setup({"setup_id": "setups:nonexistent"}))
        _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)

        with pytest.raises(SetupServiceError, match="setups:nonexistent: NOT_FOUND"):
            future.result()

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_get_setup_transport_not_found_stays_server_error(self, client: GrpcSetup) -> None:
        """A NOT_FOUND status (not a result) is still normalised by exec_grpc_query as ServerError."""
        CircuitBreaker.remove("SetupService")
        client.stub = Mock()
        client.stub.GetSetup = AsyncMock(side_effect=ServerError("[NOT_FOUND] no such setup"))

        with pytest.raises(ServerError, match="NOT_FOUND"):
            await client.get_setup({"setup_id": "setups:x"})

    @pytest.mark.grpc
    @pytest.mark.validation
    async def test_get_setup_missing_id_no_rpc(self, client: GrpcSetup) -> None:
        with pytest.raises(ValueError, match="setup_id is required"):
            await client.get_setup({})


class TestUpdateSetup:
    """update_setup sends {setup_id, name, revision} and returns the updated SetupData."""

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
        assert dict(request.revision.content) == {"a": 2}
        assert not request.HasField("status")

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

        assert request.revision.documentation == "revised"
        future.result()

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_update_setup_unknown_id_is_refused(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        future = thread_pool.submit(
            asyncio.run, client.update_setup({"setup_id": "setups:gone", "name": "x", "content": {}})
        )
        _exchange(future, test_channel, "UpdateSetup", mock_servicer.UpdateSetup)

        with pytest.raises(SetupServiceError, match="setups:gone: NOT_FOUND"):
            future.result()

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
    """delete_setup derives its bool from the result outcome."""

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
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_delete_setup_operation_error_is_false(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """An OperationError result reads as a failed deletion, as the former success=False did."""
        future = thread_pool.submit(asyncio.run, client.delete_setup({"setup_id": "setups:gone"}))
        _exchange(future, test_channel, "DeleteSetup", mock_servicer.DeleteSetup)

        assert future.result() is False

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
            ("public", "PUBLIC"),
            ("internal", "INTERNAL"),
            ("private", "PRIVATE"),
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
        assert request.visibility == common_enums_pb2.Visibility.Value(proto_name)

        result = future.result()
        assert result.visibility == Visibility(scope)
        assert mock_servicer.setups[seeded.id].visibility == common_enums_pb2.Visibility.Value(proto_name)

    @pytest.mark.grpc
    @pytest.mark.validation
    @pytest.mark.parametrize("scope", ["", "unspecified", "PUBLIC ", "org", None])
    async def test_change_visibility_invalid_scope_no_rpc(self, client: GrpcSetup, scope: object) -> None:
        with pytest.raises(ValueError, match="invalid visibility"):
            await client.change_visibility({"setup_id": "s1", "visibility": scope})

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_change_visibility_operation_error(self, client: GrpcSetup) -> None:
        client.stub = _mock_stub(
            "ChangeVisibility",
            setup_dto_pb2.ChangeVisibilityResponse(result=_error_result("setups:s1", "PERMISSION_DENIED")),
        )

        with pytest.raises(SetupServiceError, match="setups:s1: PERMISSION_DENIED"):
            await client.change_visibility({"setup_id": "setups:s1", "visibility": "public"})


class TestResponseMapping:
    """_to_setup_data reads the Setup's embedded version and maps the renamed wire fields."""

    @staticmethod
    def _version(**fields: Any) -> setup_messages_pb2.SetupVersion:
        return setup_messages_pb2.SetupVersion(
            id="setup_versions:01ABC", setup_id="setups:01X", version="1.0.0", **fields
        )

    def test_missing_version_raises(self) -> None:
        setup = setup_messages_pb2.Setup(id="setups:01X", name="n", module_id="modules:m")
        with pytest.raises(SetupServiceError, match="without a setup version"):
            GrpcSetup._to_setup_data(setup)

    def test_wire_names_map_onto_the_sdk_fields(self) -> None:
        """organization_id and created_at keep their SDK names organisation_id / creation_date."""
        now = datetime.datetime.now(datetime.timezone.utc)
        setup = setup_messages_pb2.Setup(
            id="setups:01X",
            organization_id="organizations:o",
            owner_id="users:u",
            module_id="modules:m",
            name="n",
            current_setup_version=self._version(content={"a": 1}, structure={"a": "the a knob"}, created_at=now),
        )

        result = GrpcSetup._to_setup_data(setup)

        assert result.organisation_id == "organizations:o"
        assert result.current_setup_version.creation_date == now
        assert result.current_setup_version.structure == {"a": "the a knob"}

    @pytest.mark.parametrize(
        ("wire", "expected"),
        [
            ("READY", RegistrySetupStatus.READY),
            ("DRAFT", RegistrySetupStatus.DRAFT),
            ("VALIDATING", RegistrySetupStatus.VALIDATING),
            ("SETUP_STATUS_UNSPECIFIED", RegistrySetupStatus.UNSPECIFIED),
        ],
    )
    def test_status_maps_by_name_not_number(self, wire: str, expected: RegistrySetupStatus) -> None:
        """SetupStatus was renumbered (DRAFT 0 → 1, READY 2 → 3): the SDK must read the name."""
        setup = setup_messages_pb2.Setup(
            id="setups:01X",
            name="n",
            status=wire,
            current_setup_version=self._version(content={}, created_at=datetime.datetime.now(datetime.timezone.utc)),
        )

        assert GrpcSetup._to_setup_data(setup).status == expected

    def test_an_unset_structure_reads_as_none(self) -> None:
        """A version cut without a map carries none — None, not a claimed-empty map."""
        setup = setup_messages_pb2.Setup(
            id="setups:01X",
            name="n",
            current_setup_version=self._version(content={}, created_at=datetime.datetime.now(datetime.timezone.utc)),
        )

        assert GrpcSetup._to_setup_data(setup).current_setup_version.structure is None


class TestSetupVersions:
    """ListSetupVersions / SetCurrentSetupVersion (SetupVersionService), and set_as_current on updates."""

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
            _cut(mock_servicer, setup.id, {"a": i})

        future = thread_pool.submit(asyncio.run, client.list_setup_versions({"setup_id": setup.id}))
        request = _exchange(future, test_channel, "ListSetupVersions", mock_servicer.ListSetupVersions)
        page = future.result()

        # An unset limit must not reach the wire as 0 — the proto floors it at 1.
        assert request.pagination.limit == 20
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
        _cut(mock_servicer, setup.id, {"a": 1})

        future = thread_pool.submit(
            asyncio.run, client.list_setup_versions({"setup_id": setup.id, "limit": 1, "offset": 1})
        )
        request = _exchange(future, test_channel, "ListSetupVersions", mock_servicer.ListSetupVersions)
        page = future.result()

        assert (request.pagination.limit, request.pagination.offset) == (1, 1)
        assert page.total_count == 2
        assert [v.content for v in page.setup_versions] == [{"k": "v"}]

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_list_setup_versions_drops_error_results(
            self, client: GrpcSetup, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A result holding an OperationError is dropped from the page and logged; the others are kept."""
        monkeypatch.setattr(logging.getLogger("digitalkin"), "propagate", True)
        kept = setup_messages_pb2.SetupVersion(
            id="setup_versions:kept",
            setup_id="setups:s1",
            version="1.0.0",
            content={"a": 1},
            created_at=datetime.datetime.now(datetime.timezone.utc),
        )
        client.stub = _mock_stub(
            "ListSetupVersions",
            setup_version_dto_pb2.ListSetupVersionsResponse(
                results=[
                    setup_messages_pb2.SetupResult(identifier=kept.id, version=kept),
                    _error_result("setup_versions:broken", "INTERNAL"),
                ],
                bulk=bulk_pb2.BulkResponse(
                    total_processed=2, total_failed=1, pagination=pagination_pb2.PaginationResponse(total_count=2)
                ),
                current_setup_version_id=kept.id,
            ),
        )

        with caplog.at_level(logging.WARNING, logger="digitalkin"):
            page = await client.list_setup_versions({"setup_id": "setups:s1"})

        assert [v.id for v in page.setup_versions] == ["setup_versions:kept"]
        assert any(
            "ListSetupVersions dropped result setup_versions:broken: INTERNAL refused by the service" in r.getMessage()
            for r in caplog.records
        )
        assert page.total_count == 2
        assert page.current_setup_version_id == "setup_versions:kept"

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
        _cut(mock_servicer, setup.id, {"a": 9})
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

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_set_current_setup_version_unknown_version_is_refused(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        setup = _seed_setup(mock_servicer)

        future = thread_pool.submit(
            asyncio.run,
            client.set_current_setup_version({"setup_id": setup.id, "setup_version_id": "setup_versions:gone"}),
        )
        _exchange(future, test_channel, "SetCurrentSetupVersion", mock_servicer.SetCurrentSetupVersion)

        with pytest.raises(SetupServiceError, match="setup_versions:gone: NOT_FOUND"):
            future.result()

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


class TestListSetups:
    """list_setups sends optional filters, statuses by name and a bounded page."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_list_setups_success(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        first = _seed_setup(mock_servicer, name="first")
        second = _seed_setup(mock_servicer, name="second")

        future = thread_pool.submit(asyncio.run, client.list_setups({}))
        request = _exchange(future, test_channel, "ListSetups", mock_servicer.ListSetups)
        page = future.result()

        # Absent filters stay unset — "" would fail the id prefix rules.
        assert not request.HasField("organization_id")
        assert not request.HasField("owner_id")
        assert not request.HasField("module_id")
        assert list(request.statuses) == []
        assert (request.pagination.limit, request.pagination.offset) == (20, 0)
        assert page.total_count == 2
        assert [s.id for s in page.setups] == [first.id, second.id]
        assert all(isinstance(s, SetupData) for s in page.setups)
        assert page.setups[0].current_setup_version.content == {"k": "v"}

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_list_setups_sends_filters_and_statuses_by_name(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """SetupStatus was renumbered: the SDK member reaches the wire by its name."""
        ready = _seed_setup(mock_servicer, name="ready")
        paused = _seed_setup(mock_servicer, name="paused")
        paused.status = setup_enums_pb2.SetupStatus.Value("PAUSED")

        future = thread_pool.submit(
            asyncio.run,
            client.list_setups({
                "organization_id": "organizations:ctx",
                "owner_id": "users:ctx",
                "module_id": "modules:ctx",
                "statuses": [RegistrySetupStatus.READY, "draft"],
            }),
        )
        request = _exchange(future, test_channel, "ListSetups", mock_servicer.ListSetups)
        page = future.result()

        assert (request.organization_id, request.owner_id, request.module_id) == (
            "organizations:ctx",
            "users:ctx",
            "modules:ctx",
        )
        assert [setup_enums_pb2.SetupStatus.Name(s) for s in request.statuses] == ["READY", "DRAFT"]
        assert [s.id for s in page.setups] == [ready.id]
        assert page.setups[0].status == RegistrySetupStatus.READY
        assert page.total_count == 1

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.parametrize(("limit", "wire_limit"), [(None, 20), (0, 20), (-3, 1), (1, 1), (500, 100)])
    def test_list_setups_paginates_within_the_proto_bounds(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
            limit: int | None,
            wire_limit: int,
    ) -> None:
        """PaginationRequest.limit is 1..100: an out-of-range limit is clamped, never sent."""
        seeded = [_seed_setup(mock_servicer, name=f"s{i}") for i in range(3)]

        future = thread_pool.submit(asyncio.run, client.list_setups({"limit": limit, "offset": 1}))
        request = _exchange(future, test_channel, "ListSetups", mock_servicer.ListSetups)
        page = future.result()

        assert (request.pagination.limit, request.pagination.offset) == (wire_limit, 1)
        assert page.total_count == 3
        assert [s.id for s in page.setups] == [s.id for s in seeded[1: 1 + wire_limit]]

    @pytest.mark.grpc
    @pytest.mark.validation
    @pytest.mark.parametrize("statuses", [[RegistrySetupStatus.UNSPECIFIED], ["bogus"], ["READY", "unspecified"], [3]])
    async def test_list_setups_undefined_status_no_rpc(self, client: GrpcSetup, statuses: list) -> None:
        """Fail closed: an unmirrored status is refused rather than dropped, which would widen the filter."""
        client.stub = Mock()

        with pytest.raises(ValueError, match="invalid statuses"):
            await client.list_setups({"statuses": statuses})
        client.stub.ListSetups.assert_not_called()

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_list_setups_drops_error_results(
            self, client: GrpcSetup, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """A result holding an OperationError is dropped from the page and logged; the others are kept."""
        monkeypatch.setattr(logging.getLogger("digitalkin"), "propagate", True)
        client.stub = _mock_stub(
            "ListSetups",
            setup_dto_pb2.ListSetupsResponse(
                results=[
                    setup_messages_pb2.SetupResult(identifier="setups:kept", setup=_setup_msg("setups:kept")),
                    _error_result("setups:broken", "INTERNAL"),
                ],
                bulk=bulk_pb2.BulkResponse(
                    total_processed=2, total_failed=1, pagination=pagination_pb2.PaginationResponse(total_count=2)
                ),
            ),
        )

        with caplog.at_level(logging.WARNING, logger="digitalkin"):
            page = await client.list_setups({})

        assert [s.id for s in page.setups] == ["setups:kept"]
        assert page.total_count == 2
        assert any(
            "ListSetups dropped result setups:broken: INTERNAL refused by the service" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_list_setups_drops_a_setup_without_a_version(
            self, client: GrpcSetup, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """current_setup_version is absent while a setup has none yet; SetupData cannot hold that."""
        monkeypatch.setattr(logging.getLogger("digitalkin"), "propagate", True)
        bare = setup_messages_pb2.Setup(id="setups:bare", name="bare", module_id="modules:m")
        client.stub = _mock_stub(
            "ListSetups",
            setup_dto_pb2.ListSetupsResponse(
                results=[
                    setup_messages_pb2.SetupResult(identifier=bare.id, setup=bare),
                    setup_messages_pb2.SetupResult(identifier="setups:kept", setup=_setup_msg("setups:kept")),
                ],
                bulk=bulk_pb2.BulkResponse(pagination=pagination_pb2.PaginationResponse(total_count=2)),
            ),
        )

        with caplog.at_level(logging.WARNING, logger="digitalkin"):
            page = await client.list_setups({})

        assert [s.id for s in page.setups] == ["setups:kept"]
        assert any(
            "ListSetups dropped setup setups:bare (bare): no setup version yet" in r.getMessage()
            for r in caplog.records
        )


class TestCreateSetupVersion:
    """create_setup_version cuts a version from a SetupRevision and returns SetupVersionData."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_create_setup_version_success(
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
            client.create_setup_version({
                "setup_id": setup.id,
                "version": "2.0.0",
                "content": {"a": 2},
                "documentation": "second cut",
                "structure": {"a": "the a knob"},
            }),
        )
        request = _exchange(future, test_channel, "CreateSetupVersion", mock_servicer.CreateSetupVersion)
        result = future.result()

        assert (request.setup_id, request.version) == (setup.id, "2.0.0")
        assert dict(request.revision.content) == {"a": 2}
        assert request.revision.documentation == "second cut"
        assert dict(request.revision.structure) == {"a": "the a knob"}
        assert request.set_as_current is False
        assert isinstance(result, SetupVersionData)
        assert (result.setup_id, result.version) == (setup.id, "2.0.0")
        assert result.content == {"a": 2}
        assert result.documentation == "second cut"
        assert result.structure == {"a": "the a knob"}
        # Staged by default: the setup keeps serving its current version.
        assert mock_servicer.setups[setup.id].current_setup_version.id == original

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_create_setup_version_can_activate_it(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        setup = _seed_setup(mock_servicer)

        future = thread_pool.submit(
            asyncio.run,
            client.create_setup_version({
                "setup_id": setup.id,
                "version": "2.0.0",
                "content": {"a": 2},
                "set_as_current": True,
            }),
        )
        request = _exchange(future, test_channel, "CreateSetupVersion", mock_servicer.CreateSetupVersion)
        result = future.result()

        assert request.set_as_current is True
        assert mock_servicer.setups[setup.id].current_setup_version.id == result.id

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_create_setup_version_unknown_setup_is_refused(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        future = thread_pool.submit(
            asyncio.run,
            client.create_setup_version({"setup_id": "setups:gone", "version": "2.0.0", "content": {}}),
        )
        _exchange(future, test_channel, "CreateSetupVersion", mock_servicer.CreateSetupVersion)

        with pytest.raises(SetupServiceError, match="setups:gone: NOT_FOUND"):
            future.result()

    @pytest.mark.grpc
    @pytest.mark.validation
    @pytest.mark.parametrize(
        "payload",
        [
            {"version": "2.0.0", "content": {}},
            {"setup_id": "setups:s1", "content": {}},
            {"setup_id": "setups:s1", "version": "", "content": {}},
            {"setup_id": "setups:s1", "version": "2.0.0"},
            {"setup_id": "setups:s1", "version": "2.0.0", "content": "not-a-dict"},
        ],
    )
    async def test_create_setup_version_missing_fields_no_rpc(self, client: GrpcSetup, payload: dict) -> None:
        with pytest.raises(ValueError, match="setup_id, version and content"):
            await client.create_setup_version(payload)

    @pytest.mark.grpc
    @pytest.mark.validation
    async def test_create_setup_version_oversized_content_no_rpc(self, client: GrpcSetup) -> None:
        base = {"setup_id": "setups:s1", "version": "2.0.0"}
        with pytest.raises(ValueError, match="must stay under 4096"):
            await client.create_setup_version({**base, "content": {"output_format_spec": "x" * 4096}})
        with pytest.raises(ValueError, match="documentation is 301 characters"):
            await client.create_setup_version({**base, "content": {}, "documentation": "x" * 301})


class TestGetSetupVersion:
    """get_setup_version reads one version by id."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_setup_version_success(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        setup = _seed_setup(mock_servicer, documentation="the house voice")
        version_id = setup.current_setup_version.id

        future = thread_pool.submit(asyncio.run, client.get_setup_version({"setup_version_id": version_id}))
        request = _exchange(future, test_channel, "GetSetupVersion", mock_servicer.GetSetupVersion)
        result = future.result()

        assert request.setup_version_id == version_id
        assert (result.id, result.setup_id, result.version) == (version_id, setup.id, "1.0.0")
        assert result.content == {"k": "v"}
        assert result.documentation == "the house voice"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_get_setup_version_unknown_id_is_refused(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        future = thread_pool.submit(asyncio.run, client.get_setup_version({"setup_version_id": "setup_versions:gone"}))
        _exchange(future, test_channel, "GetSetupVersion", mock_servicer.GetSetupVersion)

        with pytest.raises(SetupServiceError, match="setup_versions:gone: NOT_FOUND"):
            future.result()

    @pytest.mark.grpc
    @pytest.mark.validation
    async def test_get_setup_version_missing_id_no_rpc(self, client: GrpcSetup) -> None:
        with pytest.raises(ValueError, match="setup_version_id is required"):
            await client.get_setup_version({})


class TestUpdateSetupVersion:
    """update_setup_version sends only the supplied fields; the CEL rule needs at least one."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_update_setup_version_sends_only_the_supplied_field(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        setup = _seed_setup(mock_servicer, documentation="kept")
        version_id = setup.current_setup_version.id

        future = thread_pool.submit(
            asyncio.run, client.update_setup_version({"setup_version_id": version_id, "version": "1.0.1"})
        )
        request = _exchange(future, test_channel, "UpdateSetupVersion", mock_servicer.UpdateSetupVersion)
        result = future.result()

        assert request.setup_version_id == version_id
        assert request.HasField("version")
        assert not request.HasField("content")
        assert not request.HasField("documentation")
        assert not request.HasField("structure")
        assert result.version == "1.0.1"
        assert result.content == {"k": "v"}
        assert result.documentation == "kept"

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_update_setup_version_edits_the_active_version_in_place(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        setup = _seed_setup(mock_servicer)
        version_id = setup.current_setup_version.id

        future = thread_pool.submit(
            asyncio.run,
            client.update_setup_version({
                "setup_version_id": version_id,
                "content": {"a": 3},
                "documentation": "edited",
                "structure": {"a": "the a knob"},
            }),
        )
        request = _exchange(future, test_channel, "UpdateSetupVersion", mock_servicer.UpdateSetupVersion)
        result = future.result()

        assert not request.HasField("version")
        assert dict(request.content) == {"a": 3}
        assert request.documentation == "edited"
        assert dict(request.structure) == {"a": "the a knob"}
        assert result.id == version_id
        assert (result.content, result.documentation, result.structure) == ({"a": 3}, "edited", {"a": "the a knob"})
        # No new version is cut: the setup serves the edited one.
        assert mock_servicer.setups[setup.id].current_setup_version.content["a"] == 3
        assert len(mock_servicer.versions[setup.id]) == 1

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_update_setup_version_empty_documentation_clears_it(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """An empty documentation is a real value (it clears the text), unlike an absent key."""
        setup = _seed_setup(mock_servicer, documentation="old")

        future = thread_pool.submit(
            asyncio.run,
            client.update_setup_version({"setup_version_id": setup.current_setup_version.id, "documentation": ""}),
        )
        request = _exchange(future, test_channel, "UpdateSetupVersion", mock_servicer.UpdateSetupVersion)

        assert request.HasField("documentation")
        assert not request.documentation
        assert not future.result().documentation

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_update_setup_version_empty_label_stays_unset(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Version is optional with min_len 1: "" must not reach the wire as a set value."""
        setup = _seed_setup(mock_servicer)

        future = thread_pool.submit(
            asyncio.run,
            client.update_setup_version({
                "setup_version_id": setup.current_setup_version.id,
                "version": "",
                "content": {"a": 1},
            }),
        )
        request = _exchange(future, test_channel, "UpdateSetupVersion", mock_servicer.UpdateSetupVersion)

        assert not request.HasField("version")
        assert future.result().version == "1.0.0"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_update_setup_version_unknown_id_is_refused(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        future = thread_pool.submit(
            asyncio.run, client.update_setup_version({"setup_version_id": "setup_versions:gone", "version": "2"})
        )
        _exchange(future, test_channel, "UpdateSetupVersion", mock_servicer.UpdateSetupVersion)

        with pytest.raises(SetupServiceError, match="setup_versions:gone: NOT_FOUND"):
            future.result()

    @pytest.mark.grpc
    @pytest.mark.validation
    @pytest.mark.parametrize(
        "payload",
        [
            {"setup_version_id": "setup_versions:v1"},
            {"setup_version_id": "setup_versions:v1", "version": ""},
            {"setup_version_id": "setup_versions:v1", "content": None, "documentation": None, "structure": None},
        ],
    )
    async def test_update_setup_version_without_a_field_no_rpc(self, client: GrpcSetup, payload: dict) -> None:
        """The request's CEL rule refuses an empty update; the client refuses it first."""
        client.stub = Mock()

        with pytest.raises(ValueError, match="at least one of version, content, documentation or structure"):
            await client.update_setup_version(payload)
        client.stub.UpdateSetupVersion.assert_not_called()

    @pytest.mark.grpc
    @pytest.mark.validation
    @pytest.mark.parametrize(
        ("payload", "message"),
        [
            ({"version": "2"}, "setup_version_id is required"),
            ({"setup_version_id": "setup_versions:v1", "content": "not-a-dict"}, "content must be an object"),
            (
                    {"setup_version_id": "setup_versions:v1", "content": {"output_format_spec": "x" * 4096}},
                    "must stay under 4096",
            ),
            ({"setup_version_id": "setup_versions:v1", "documentation": "x" * 301}, "documentation is 301 characters"),
        ],
    )
    async def test_update_setup_version_invalid_payload_no_rpc(
            self, client: GrpcSetup, payload: dict, message: str
    ) -> None:
        with pytest.raises(ValueError, match=message):
            await client.update_setup_version(payload)


class TestDeleteSetupVersion:
    """delete_setup_version derives its bool from the result outcome."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_delete_setup_version_success(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        setup = _seed_setup(mock_servicer)
        staged = _stage(mock_servicer, setup.id)

        future = thread_pool.submit(asyncio.run, client.delete_setup_version({"setup_version_id": staged.id}))
        request = _exchange(future, test_channel, "DeleteSetupVersion", mock_servicer.DeleteSetupVersion)

        assert request.setup_version_id == staged.id
        assert future.result() is True
        assert [v.id for v in mock_servicer.versions[setup.id]] == [setup.current_setup_version.id]

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    @pytest.mark.parametrize("current", [False, True])
    def test_delete_setup_version_operation_error_is_false(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
            current: bool,
    ) -> None:
        """NOT_FOUND (unknown id) and FAILED_PRECONDITION (the active version) both read as False."""
        setup = _seed_setup(mock_servicer)
        version_id = setup.current_setup_version.id if current else "setup_versions:gone"

        future = thread_pool.submit(asyncio.run, client.delete_setup_version({"setup_version_id": version_id}))
        _exchange(future, test_channel, "DeleteSetupVersion", mock_servicer.DeleteSetupVersion)

        assert future.result() is False
        assert len(mock_servicer.versions[setup.id]) == 1

    @pytest.mark.grpc
    @pytest.mark.validation
    async def test_delete_setup_version_missing_id_no_rpc(self, client: GrpcSetup) -> None:
        with pytest.raises(ValueError, match="setup_version_id is required"):
            await client.delete_setup_version({})


class TestNewRpcStatusErrors:
    """A gRPC status (not a result) surfaces as ServerError on every new RPC, delete included."""

    # Each payload passes the client-side checks but breaks a buf.validate id prefix rule.
    _BAD_PREFIX_CALLS = (
        ("list_setups", "ListSetups", {"organization_id": "org:bad"}),
        ("create_setup_version", "CreateSetupVersion", {"setup_id": "bad", "version": "2", "content": {}}),
        ("get_setup_version", "GetSetupVersion", {"setup_version_id": "bad"}),
        ("update_setup_version", "UpdateSetupVersion", {"setup_version_id": "bad", "version": "2"}),
        ("delete_setup_version", "DeleteSetupVersion", {"setup_version_id": "bad"}),
    )

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    @pytest.mark.parametrize(("method", "rpc", "payload"), _BAD_PREFIX_CALLS)
    def test_invalid_argument_is_a_server_error(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
            method: str,
            rpc: str,
            payload: dict,
    ) -> None:
        """The mock validates like the server interceptor and answers INVALID_ARGUMENT."""
        future = thread_pool.submit(asyncio.run, getattr(client, method)(payload))
        _exchange(future, test_channel, rpc, getattr(mock_servicer, rpc))

        with pytest.raises(ServerError, match=r"\[INVALID_ARGUMENT\] invalid"):
            future.result()

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    @pytest.mark.parametrize(("method", "rpc", "payload"), _BAD_PREFIX_CALLS)
    def test_not_found_status_is_a_server_error(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            thread_pool: futures.ThreadPoolExecutor,
            method: str,
            rpc: str,
            payload: dict,
    ) -> None:
        future = thread_pool.submit(asyncio.run, getattr(client, method)(payload))
        _exchange(future, test_channel, rpc, _refuse_with(grpc.StatusCode.NOT_FOUND))

        with pytest.raises(ServerError, match=r"\[NOT_FOUND\] NOT_FOUND from the service"):
            future.result()


class TestNewRequestsPassProtovalidate:
    """Every request the new methods build satisfies its buf.validate rules."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.contract
    def test_built_requests_are_valid(
            self,
            client: GrpcSetup,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        setup = _seed_setup(mock_servicer)
        staged = _stage(mock_servicer, setup.id)
        calls = [
            (client.list_setups({"statuses": ["ready"], "module_id": "modules:ctx", "limit": 1000}), "ListSetups"),
            (
                client.create_setup_version({"setup_id": setup.id, "version": "3", "content": {}, "documentation": ""}),
                "CreateSetupVersion",
            ),
            (client.get_setup_version({"setup_version_id": staged.id}), "GetSetupVersion"),
            (client.update_setup_version({"setup_version_id": staged.id, "structure": {}}), "UpdateSetupVersion"),
            (client.delete_setup_version({"setup_version_id": staged.id}), "DeleteSetupVersion"),
        ]

        for coroutine, rpc in calls:
            future = thread_pool.submit(asyncio.run, coroutine)
            request = _exchange(future, test_channel, rpc, getattr(mock_servicer, rpc))
            protovalidate.validate(request)
            future.result()


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

        assert dict(request.revision.structure) == authored
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
        result = future.result()

        assert dict(request.revision.structure) == {"a": "the a knob"}
        assert result.current_setup_version.structure == {"a": "the a knob"}


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

        assert dict(request.revision.structure) == {}

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

        assert dict(request.revision.structure) == {}

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
    def test_get_with_an_empty_key_leaves_it_unset(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        """structure_key is proto3 optional with min_len 1: "" must not reach the wire as a set value."""
        create = thread_pool.submit(asyncio.run, client.create_setup({"name": "n", "content": {"a": 1}}))
        _exchange(create, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        setup = create.result()

        future = thread_pool.submit(asyncio.run, client.get_setup({"setup_id": setup.id, "structure_key": ""}))
        request = _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)
        result = future.result()

        assert not request.HasField("structure_key")
        assert result.current_setup_version.content == {"a": 1}

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_get_carries_the_version_structure(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        """SetupVersion carries structure, so a plain read returns the stored map."""
        create = thread_pool.submit(
            asyncio.run, client.create_setup({"name": "n", "content": {"a": 1}, "structure": {"a": "the a knob"}})
        )
        _exchange(create, test_channel, "CreateSetup", mock_servicer.CreateSetup)
        setup = create.result()

        future = thread_pool.submit(asyncio.run, client.get_setup({"setup_id": setup.id}))
        _exchange(future, test_channel, "GetSetup", mock_servicer.GetSetup)

        assert future.result().current_setup_version.structure == {"a": "the a knob"}

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

        with pytest.raises(SetupServiceError, match="NOT_FOUND"):
            future.result()


@pytest.mark.contract
class TestLocalRemoteParity:
    """LOCAL and REMOTE must agree on the structure surface, or behaviour changes with deployment.

    ``DefaultSetup`` mirrors the backend by design (`.claude/rules/services.md`), and the
    projection contract in particular is easy to drift.
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
    def test_a_plain_read_carries_the_map_on_either_side(
        self, client: GrpcSetup, test_channel: grpc_testing.Channel, mock_servicer, thread_pool
    ) -> None:
        """SetupVersion carries structure on reads, so both strategies return the stored map."""
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

        assert remote.current_setup_version.structure == {"llm.model": "which model"}
        assert local.current_setup_version.structure == remote.current_setup_version.structure

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

        with pytest.raises(SetupServiceError, match="NOT_FOUND"):
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
        """An empty key means the whole document on both sides."""
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
    """A version arriving without content must name the setup and version it came from.

    Production (archetype-ada, 2026-09-08/09) failed here with a bare pydantic
    "current_setup_version.content Field required", which named neither.
    """

    @staticmethod
    def _stub() -> setup_messages_pb2.SetupVersion:
        """A SetupVersion with identity but no content Struct — the shape production returned."""
        return setup_messages_pb2.SetupVersion(id="setup_versions:01ABC", setup_id="setups:01X", version="1.0.0")

    def test_names_setup_and_version(self) -> None:
        setup = setup_messages_pb2.Setup(id="setups:01X", name="n", module_id="modules:m")
        setup.current_setup_version.CopyFrom(self._stub())

        with pytest.raises(SetupServiceError, match=r"setup 'setups:01X' version 'setup_versions:01ABC'.*no content"):
            GrpcSetup._to_setup_data(setup)

    def test_an_empty_but_present_content_is_accepted(self) -> None:
        """{} is a legitimate configuration; only an unset Struct is the failure."""
        version = self._stub()
        version.content.SetInParent()
        version.created_at.FromDatetime(datetime.datetime.now(datetime.timezone.utc))
        setup = setup_messages_pb2.Setup(id="setups:01X", name="n", module_id="modules:m")
        setup.current_setup_version.CopyFrom(version)

        assert GrpcSetup._to_setup_data(setup).current_setup_version.content == {}

    def test_created_at_is_the_same_trap_one_field_over(self) -> None:
        """An unset Timestamp is dropped like an unset Struct, and the model requires it too."""
        version = self._stub()
        version.content.SetInParent()
        setup = setup_messages_pb2.Setup(id="setups:01X", name="n", module_id="modules:m")
        setup.current_setup_version.CopyFrom(version)

        with pytest.raises(ValidationError, match="creation_date"):
            GrpcSetup._to_setup_data(setup)
