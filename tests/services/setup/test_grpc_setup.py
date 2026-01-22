"""Test the grpc service."""

import asyncio
import datetime
import secrets
import string
from concurrent import futures

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.setup.v1 import (
    setup_dto_pb2,
    setup_service_pb2,
    setup_service_pb2_grpc,
    setup_messages_pb2
)
from agentic_mesh_protocol.setup.v1.setup_messages_pb2 import SetupVersion
from freezegun import freeze_time

from digitalkin.exception.setup import SetupServiceError
from digitalkin.models.grpc_servers.models import ClientConfig, SecurityMode, ServerMode
from digitalkin.models.services.setup import SetupVersionData, SetupData
from digitalkin.services.setup.setup_grpc import GrpcSetup
from tests.fixtures.grpc_fixtures import FakeContext, AsyncStubWrapper
from tests.services.setup.mock_setup_servicer import MockSetupServicer

service_instance = MockSetupServicer()
service_name = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]

alphabet = string.ascii_letters + string.digits

# --- Test Constants ---
MISSION_ID = "missions:test_mission"
SETUP_ID = "setups:test_setup"
SETUP_VERSION_ID = "setup_versions:test_version"

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
    # Create a strict real time test clock
    test_clock = grpc_testing.strict_real_time()
    # Create a test channel with our service descriptor and our fake servicer
    return grpc_testing.channel([service_name], test_clock)


@pytest.fixture
def mock_servicer() -> MockSetupServicer:
    """Return an instance of the mock servicer.

    Returns:
        Mock Setup Servicer
    """
    return MockSetupServicer()


@pytest.fixture
def client(test_channel: grpc_testing.Channel) -> GrpcSetup:
    """Instantiate a GrpcSetupService client that uses the test channel.

    Returns:
        gRPC client as GrpcSetup
    """
    # Create a dummy ServerConfig; its values are not used since we override _init_channel.
    dummy_config = ClientConfig(
        host="[::]",
        port=50151,
        mode=ServerMode.ASYNC,
        security=SecurityMode.INSECURE,
        credentials=None,
    )
    client = GrpcSetup(MISSION_ID, SETUP_ID, SETUP_VERSION_ID, dummy_config)
    # emulate real instance
    client.__post_init__(dummy_config)

    # Override the channel and stub to use our test channel
    client.stub = AsyncStubWrapper(setup_service_pb2_grpc.SetupServiceStub(test_channel))
    return client


def random_string(number: int = 16) -> str:
    return "".join(secrets.choice(alphabet) for _ in range(number))


@pytest.fixture
@freeze_time("2025-04-01 12:00:01")
def generate_setup_version_obj() -> SetupVersionData:
    setup_id = random_string()
    return SetupVersionData(
        id=random_string(),
        setup_id=setup_id,
        version="v" + random_string(8),
        content={random_string(8): random_string(8) for _ in range(5)},
        created_at=datetime.datetime.now(),  # noqa: DTZ005
    )


@pytest.fixture
def generate_setup_obj(generate_setup_version_obj: SetupVersionData) -> SetupData:
    # Create registration request with test setup data
    return SetupData(
        id=generate_setup_version_obj.setup_id,
        name=random_string(),
        organization_id=random_string(),
        owner_id=random_string(),
        module_id=random_string(),
        current_setup_version=generate_setup_version_obj,
    )


class TestCreateSetup:
    """Tests for create_setup() method.

    Verifies successful setup creation, request validation, and error handling
    for invalid data and duplicate names.
    """

    @freeze_time("2025-04-01 12:00:01")
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_create_setup_request_creation_success(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        generate_setup_obj: SetupData,
        generate_setup_version_obj: SetupVersionData,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successful create_setup with a good request.

        Verifies that create_setup create the good request.

        Args:
            grpc_test_server: Mock gRPC server for testing.
        """
        # Start the client call (this call will block until the response is simulated).
        future = thread_pool.submit(asyncio.run, client.create(generate_setup_obj.model_dump()))

        # Get the service and method descriptor.
        service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
        method_desc = service_desc.methods_by_name["CreateSetup"]

        # Intercept the pending unary-unary call.
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        # Use grpc_testing to send the response back to the client.
        rpc.send_initial_metadata(())
        rpc.terminate(
            # use the servicer to emulate a real request handling from a server
            setup_dto_pb2.CreateSetupResponse(result=setup_messages_pb2.SetupResult(success=True)),
            (),
            grpc.StatusCode.OK,
            "",
        )

        # Verify that the client call returns success.
        result = future.result()
        assert result.result.success is True

        # Verify the request correspond to the setup data
        assert request.name == generate_setup_obj.name
        assert request.organization_id == generate_setup_obj.organization_id
        assert request.owner_id == generate_setup_obj.owner_id
        assert request.current_setup_version.setup_id == generate_setup_obj.current_setup_version.setup_id
        assert request.current_setup_version.version == generate_setup_obj.current_setup_version.version
        assert (
                request.current_setup_version.created_at.ToDatetime()
                == generate_setup_obj.current_setup_version.created_at
        )
        assert dict(request.current_setup_version.content) == generate_setup_obj.current_setup_version.content

    @freeze_time("2025-04-01 12:00:01")
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_create_setup_success(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        generate_setup_obj: SetupData,
        generate_setup_version_obj: SetupVersionData,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successful create_setup.

        Verifies that create_setup RPC call with a valid request using the fake servicer.

        Args:
            grpc_test_server: Mock gRPC server for testing.
        """
        # Start the client call (this call will block until the response is simulated).
        future = thread_pool.submit(asyncio.run, client.create(generate_setup_obj.model_dump()))

        # Get the service and method descriptor.
        service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
        method_desc = service_desc.methods_by_name["CreateSetup"]

        # Intercept the pending unary-unary call.
        _, _request, rpc = test_channel.take_unary_unary(method_desc)

        # Use grpc_testing to send the response back to the client.
        rpc.send_initial_metadata(())
        request_obj = setup_dto_pb2.CreateSetupRequest(**{
            k: v for (k, v) in generate_setup_obj.model_dump().items() if k not in "id"
        })

        rpc.terminate(
            # use the servicer to emulate a real request handling from a server
            mock_servicer.CreateSetup(request_obj, FakeContext()),
            (),
            grpc.StatusCode.OK,
            "",
        )

        # Verify that the client call returns success.
        result = future.result()
        assert result.result.success is True

        setup = next(
            filter(
                lambda obj: getattr(obj, "name", None) == generate_setup_obj.name,
                mock_servicer.setups.values(),
            )
        )

        assert isinstance(setup, SetupData)
        assert setup.name == generate_setup_obj.name
        assert setup.organization_id == generate_setup_obj.organization_id
        assert setup.owner_id == generate_setup_obj.owner_id
        assert setup.current_setup_version.setup_id == generate_setup_obj.current_setup_version.setup_id
        assert setup.current_setup_version.version == generate_setup_obj.current_setup_version.version
        assert setup.current_setup_version.created_at == generate_setup_obj.current_setup_version.created_at
        assert setup.current_setup_version.content == generate_setup_obj.current_setup_version.content

    # Test RegisterModule
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_create_setup_validation_error(
        self,
        client: GrpcSetup,
        generate_setup_version_obj: SetupVersionData,
        generate_setup_obj: SetupData,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test registration of a duplicate module.

        Verifies that attempting to register a module with an ID that already exists
        results in an error response with ALREADY_EXISTS status code.

        Args:
            grpc_test_server: Mock gRPC server for testing.
            module_registry_obj: Pre-registered module fixture for testing duplicates.
        """
        # Try to register a module with an ID that already exists
        # Convert the module object to a request, excluding status and message fields
        generate_setup_obj.name = []
        generate_setup_obj.current_setup_version = None

        # Start the client call (this call will block until the response is simulated).
        future = thread_pool.submit(asyncio.run, client.create(generate_setup_obj.model_dump(warnings=False)))
        with pytest.raises(SetupServiceError, match="Unexpected error in CreateSetup"):
            future.result()


class TestGetSetup:
    """Tests for get_setup() method.

    Verifies successful retrieval of setup data, handling of non-existent setups,
    and retrieval with specific versions.
    """

    @freeze_time("2025-04-01 12:00:01")
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_setup_success(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        generate_setup_obj: SetupData,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully retrieving a setup.

        Verifies that get_setup returns the correct setup data.
        """
        service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
        create_method_desc = service_desc.methods_by_name["CreateSetup"]
        get_method_desc = service_desc.methods_by_name["GetSetup"]

        # First create a setup
        create_future = thread_pool.submit(asyncio.run, client.create(generate_setup_obj.model_dump()))
        _, _create_request, create_rpc = test_channel.take_unary_unary(create_method_desc)
        create_rpc.send_initial_metadata(())
        request_obj = setup_dto_pb2.CreateSetupRequest(**{
            k: v for (k, v) in generate_setup_obj.model_dump().items() if k != "id"
        })
        create_response = mock_servicer.CreateSetup(request_obj, FakeContext())
        create_rpc.terminate(create_response, (), grpc.StatusCode.OK, "")
        create_future.result()

        # Get the created setup's ID
        created_setup_id = next(iter(mock_servicer.setups.keys()))

        # Now get the setup
        get_future = thread_pool.submit(asyncio.run, client.get({"setup_id": created_setup_id}))
        _, get_request, get_rpc = test_channel.take_unary_unary(get_method_desc)

        assert get_request.setup_id == created_setup_id

        get_context = FakeContext()
        get_response = mock_servicer.GetSetup(get_request, get_context)
        get_rpc.send_initial_metadata(())
        get_rpc.terminate(get_response, (), grpc.StatusCode.OK, "")

        result = get_future.result()
        assert result is not None
        assert result.name == generate_setup_obj.name
        assert result.organization_id == generate_setup_obj.organization_id
        assert result.owner_id == generate_setup_obj.owner_id

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
        """Test getting a non-existent setup raises error.

        Verifies that attempting to get a non-existent setup results in error.
        """
        service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
        get_method_desc = service_desc.methods_by_name["GetSetup"]

        get_future = thread_pool.submit(asyncio.run, client.get({"setup_id": "nonexistent_id"}))
        _, get_request, get_rpc = test_channel.take_unary_unary(get_method_desc)

        get_context = FakeContext()
        get_response = mock_servicer.GetSetup(get_request, get_context)
        get_rpc.send_initial_metadata(())
        get_rpc.terminate(get_response, (), get_context._code, get_context._details)

        with pytest.raises(Exception):
            get_future.result()


class TestUpdateSetup:
    """Tests for update_setup() method.

    Verifies successful updates, handling of non-existent setups, and partial updates
    of setup data.
    """

    @freeze_time("2025-04-01 12:00:01")
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_update_setup_servicer_direct(
        self,
        mock_servicer: MockSetupServicer,
        generate_setup_obj: SetupData,
    ) -> None:
        """Test UpdateSetup servicer directly without grpc_testing interception.

        This tests the servicer logic without the grpc channel layer,
        avoiding grpc_testing framework issues.
        """
        # First create a setup in the servicer
        create_request = setup_dto_pb2.CreateSetupRequest(
            name=generate_setup_obj.name,
            organization_id=generate_setup_obj.organization_id,
            owner_id=generate_setup_obj.owner_id,
            module_id=generate_setup_obj.module_id,
            current_setup_version=SetupVersion(**generate_setup_obj.current_setup_version.model_dump()),
        )
        create_context = FakeContext()
        create_response = mock_servicer.CreateSetup(create_request, create_context)
        assert create_response.result.success is True

        # Get the created setup's ID
        created_setup_id = next(iter(mock_servicer.setups.keys()))

        # Now test UpdateSetup servicer method directly
        update_request = setup_dto_pb2.UpdateSetupRequest(
            setup_id=created_setup_id,
            name="Updated Name",
            owner_id="new_owner_id",
            current_setup_version=None,
        )
        update_context = FakeContext()
        update_response = mock_servicer.UpdateSetup(update_request, update_context)

        # Verify the update succeeded
        assert update_response.result.setup is not None
        assert update_context._code == grpc.StatusCode.OK

        # Verify the data was actually updated
        updated_setup = mock_servicer.setups[created_setup_id]
        assert updated_setup.name == "Updated Name"
        assert updated_setup.owner_id == "new_owner_id"

    @freeze_time("2025-04-01 12:00:01")
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_update_setup_success(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        generate_setup_obj: SetupData,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully updating a setup.

        Verifies that update_setup updates the setup data correctly.
        """
        service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]

        # First, manually add a setup to the mock servicer to avoid the create call
        setup_id = "test_setup_id_" + random_string(8)
        test_setup = SetupData(
            id=setup_id,
            name="Original Name",
            organization_id=generate_setup_obj.organization_id,
            owner_id="original_owner_id",
            module_id=generate_setup_obj.module_id,
            current_setup_version=generate_setup_obj.current_setup_version,
        )
        mock_servicer.setups[setup_id] = test_setup

        # Now update the setup
        updated_data = {
            "id": setup_id,
            "name": "Updated Name",
            "owner_id": "new_owner_id",
            "module_id": generate_setup_obj.module_id,
            "organization_id": generate_setup_obj.organization_id,
            "current_setup_version": generate_setup_obj.current_setup_version,
        }

        # Start the update call
        update_future = thread_pool.submit(asyncio.run, client.update(updated_data))

        # Intercept the call
        update_method_desc = service_desc.methods_by_name["UpdateSetup"]
        _, update_request, update_rpc = test_channel.take_unary_unary(update_method_desc)

        # Verify request
        assert update_request.setup_id == setup_id
        assert update_request.name == "Updated Name"
        assert update_request.owner_id == "new_owner_id"

        # Process with mock servicer
        update_context = FakeContext()
        update_response = mock_servicer.UpdateSetup(update_request, update_context)

        # Send response
        update_rpc.send_initial_metadata(())
        update_rpc.terminate(update_response, (), grpc.StatusCode.OK, "")

        # Get result
        result = update_future.result(timeout=5.0)
        assert result is True

        # Verify the update in mock servicer
        updated_setup = mock_servicer.setups[setup_id]
        assert updated_setup.name == "Updated Name"
        assert updated_setup.owner_id == "new_owner_id"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_update_setup_not_found(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        generate_setup_obj: SetupData,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test updating a non-existent setup returns False.

        Verifies that attempting to update a non-existent setup returns False.
        """
        service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
        update_method_desc = service_desc.methods_by_name["UpdateSetup"]

        updated_data = generate_setup_obj.model_dump()
        updated_data["id"] = "nonexistent_id"

        update_future = thread_pool.submit(asyncio.run, client.update(updated_data))
        _, update_request, update_rpc = test_channel.take_unary_unary(update_method_desc)

        update_context = FakeContext()
        update_response = mock_servicer.UpdateSetup(update_request, update_context)
        update_rpc.send_initial_metadata(())
        # When setup doesn't exist, return OK status with success=False
        update_rpc.terminate(update_response, (), grpc.StatusCode.OK, "")

        result = update_future.result()
        assert result is False


class TestDeleteSetup:
    """Tests for delete_setup() method.

    Verifies successful deletion of setups and proper handling of non-existent setups.
    """

    @freeze_time("2025-04-01 12:00:01")
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_delete_setup_success(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        generate_setup_obj: SetupData,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully deleting a setup.

        Verifies that delete_setup removes the setup from storage.
        """
        service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
        create_method_desc = service_desc.methods_by_name["CreateSetup"]
        delete_method_desc = service_desc.methods_by_name["DeleteSetup"]

        # First create a setup
        create_future = thread_pool.submit(asyncio.run, client.create(generate_setup_obj.model_dump()))
        _, _create_request, create_rpc = test_channel.take_unary_unary(create_method_desc)
        create_rpc.send_initial_metadata(())
        request_obj = setup_dto_pb2.CreateSetupRequest(**{
            k: v for (k, v) in generate_setup_obj.model_dump().items() if k != "id"
        })
        create_response = mock_servicer.CreateSetup(request_obj, FakeContext())
        create_rpc.terminate(create_response, (), grpc.StatusCode.OK, "")
        create_future.result()

        # Get the created setup's ID
        created_setup_id = next(iter(mock_servicer.setups.keys()))

        # Delete the setup
        delete_future = thread_pool.submit(asyncio.run, client.delete({"setup_id": created_setup_id}))
        _, delete_request, delete_rpc = test_channel.take_unary_unary(delete_method_desc)

        assert delete_request.setup_id == created_setup_id

        delete_context = FakeContext()
        delete_response = mock_servicer.DeleteSetup(delete_request, delete_context)
        delete_rpc.send_initial_metadata(())
        delete_rpc.terminate(delete_response, (), grpc.StatusCode.OK, "")

        result = delete_future.result()
        assert result is True

        # Verify deletion in mock servicer
        assert created_setup_id not in mock_servicer.setups

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_delete_setup_not_found(
        self,
        client: GrpcSetup,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockSetupServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test deleting a non-existent setup returns False.

        Verifies that attempting to delete a non-existent setup returns False.
        """
        service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
        delete_method_desc = service_desc.methods_by_name["DeleteSetup"]

        delete_future = thread_pool.submit(asyncio.run, client.delete({"setup_id": "nonexistent_id"}))
        _, delete_request, delete_rpc = test_channel.take_unary_unary(delete_method_desc)

        delete_context = FakeContext()
        delete_response = mock_servicer.DeleteSetup(delete_request, delete_context)
        delete_rpc.send_initial_metadata(())
        # When setup doesn't exist, return OK status with success=False
        delete_rpc.terminate(delete_response, (), grpc.StatusCode.OK, "")

        result = delete_future.result()
        assert result is False

class TestListSetups:
    """Tests for list_setups() method.

    Verifies listing all setups, filtering capabilities, and pagination support.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_list_setups_success(
            self,
            client,
            test_channel,
            thread_pool,
            mock_servicer,
            generate_setup_obj,
    ) -> None:
        """Test successfully listing all setups.

        Verifies that ListSetups returns all setups when no filters are applied.
        """
        service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
        create_method_desc = service_desc.methods_by_name["CreateSetup"]

        # Create three setups
        for i in range(3):
            create_future = thread_pool.submit(asyncio.run, client.create(generate_setup_obj.model_dump()))
            _, create_request, create_rpc = test_channel.take_unary_unary(create_method_desc)
            create_context = FakeContext()
            create_response = mock_servicer.CreateSetup(create_request, create_context)
            create_rpc.send_initial_metadata(())
            create_rpc.terminate(create_response, (), grpc.StatusCode.OK, "")
            create_future.result()

        # List all setups
        list_method_desc = service_desc.methods_by_name["ListSetups"]
        list_future = thread_pool.submit(asyncio.run, client.list({}))
        _, list_request, list_rpc = test_channel.take_unary_unary(list_method_desc)

        list_context = FakeContext()
        list_response = mock_servicer.ListSetups(list_request, list_context)
        list_rpc.send_initial_metadata(())
        list_rpc.terminate(list_response, (), grpc.StatusCode.OK, "")

        result = list_future.result()
        assert result["total_count"] == 3
        assert len(result["setups"]) == 3

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_list_setups_with_pagination(
            self,
            client,
            test_channel,
            thread_pool,
            mock_servicer,
            generate_setup_obj,
    ) -> None:
        """Test listing setups with pagination.

        Verifies that ListSetups correctly handles limit and offset.
        """
        service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
        create_method_desc = service_desc.methods_by_name["CreateSetup"]

        # Create 5 setups
        for i in range(5):
            create_future = thread_pool.submit(asyncio.run, client.create(generate_setup_obj.model_dump()))
            _, create_request, create_rpc = test_channel.take_unary_unary(create_method_desc)
            create_context = FakeContext()
            create_response = mock_servicer.CreateSetup(create_request, create_context)
            create_rpc.send_initial_metadata(())
            create_rpc.terminate(create_response, (), grpc.StatusCode.OK, "")
            create_future.result()

        # List first 2 setups
        list_method_desc = service_desc.methods_by_name["ListSetups"]
        list_future = thread_pool.submit(asyncio.run, client.list({"limit": 2, "offset": 0}))
        _, list_request, list_rpc = test_channel.take_unary_unary(list_method_desc)

        list_context = FakeContext()
        list_response = mock_servicer.ListSetups(list_request, list_context)
        list_rpc.send_initial_metadata(())
        list_rpc.terminate(list_response, (), grpc.StatusCode.OK, "")

        result = list_future.result()
        assert result["total_count"] == 5
        assert len(result["setups"]) == 2

        # List next 2 setups (offset 2)
        list_future2 = thread_pool.submit(asyncio.run, client.list({"limit": 2, "offset": 2}))
        _, list_request2, list_rpc2 = test_channel.take_unary_unary(list_method_desc)

        list_context2 = FakeContext()
        list_response2 = mock_servicer.ListSetups(list_request2, list_context2)
        list_rpc2.send_initial_metadata(())
        list_rpc2.terminate(list_response2, (), grpc.StatusCode.OK, "")

        result2 = list_future2.result()
        assert result2["total_count"] == 5
        assert len(result2["setups"]) == 2

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_list_setups_empty(
            self,
            client,
            test_channel,
            thread_pool,
            mock_servicer,
    ) -> None:
        """Test listing setups when no setups exist.

        Verifies that ListSetups returns an empty list when no setups match.
        """
        service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
        list_method_desc = service_desc.methods_by_name["ListSetups"]

        # List setups (empty database)
        list_future = thread_pool.submit(asyncio.run, client.list({}))
        _, list_request, list_rpc = test_channel.take_unary_unary(list_method_desc)

        list_context = FakeContext()
        list_response = mock_servicer.ListSetups(list_request, list_context)
        list_rpc.send_initial_metadata(())
        list_rpc.terminate(list_response, (), grpc.StatusCode.OK, "")

        result = list_future.result()
        assert result["total_count"] == 0
        assert len(result["setups"]) == 0

# ============================================================================
# Regression Tests
# ============================================================================
# This section contains tests for previously identified bugs and edge cases
# that were fixed. Each test should document the issue/PR that it addresses.
#
# Format:
# @pytest.mark.grpc
# @pytest.mark.integration
# @pytest.mark.regression
# def test_regression_issue_123(...):
#     """Test for regression of issue #123.
#
#     Issue: [Brief description of the bug]
#     Fixed in: PR #456 / commit abc123
#
#     Verifies: [What this test checks to prevent regression]
#     """
#
# Add regression tests below as bugs are discovered and fixed.
