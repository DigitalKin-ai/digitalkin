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
    setup_version_service_pb2_grpc,
    setup_version_service_pb2,
    setup_version_dto_pb2,
    setup_messages_pb2
)
from freezegun import freeze_time

from digitalkin.models.grpc_servers.models import ClientConfig, SecurityMode, ServerMode
from digitalkin.models.services.setup import SetupData, SetupVersionData
from digitalkin.services.setup.setup_grpc import GrpcSetup
from digitalkin.services.setup.version.setup_version_grpc import GrpcSetupVersion
from tests.fixtures.grpc_fixtures import FakeContext
from tests.services.setup.version.mock_setup_version_servicer import MockSetupVersionServicer

service_instance = MockSetupVersionServicer()
service_name = setup_version_service_pb2.DESCRIPTOR.services_by_name["SetupVersionService"]

alphabet = string.ascii_letters + string.digits

# --- Test Constants ---
MISSION_ID = "missions:test_mission"
SETUP_ID = "setups:test_setup_version"
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
def mock_servicer() -> MockSetupVersionServicer:
    """Return an instance of the mock servicer.

    Returns:
        Mock Setup Servicer
    """
    return MockSetupVersionServicer()


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
    client = GrpcSetupVersion(MISSION_ID, SETUP_ID, SETUP_VERSION_ID, dummy_config)
    # emulate real instance
    client.__post_init__(dummy_config)

    # Override the channel and stub to use our test channel
    client.stub = setup_version_service_pb2_grpc.SetupVersionServiceStub(test_channel)
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


class TestCreateSetupVersion:
    """Tests for create_setup_version() method.

    Verifies successful setup version creation, request validation, and error handling
    for invalid data.
    """

    @freeze_time("2025-04-01 12:00:01")
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_create_setup_version_request_creation_success(
            self,
            client: GrpcSetupVersion,
            test_channel: grpc_testing.Channel,
            generate_setup_version_obj: SetupVersionData,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successful create_setup_version with a good request.

        Verifies that create_setup create the good request.

        Args:
            grpc_test_server: Mock gRPC server for testing.
        """
        # Start the client call (this call will block until the response is simulated).
        future = thread_pool.submit(asyncio.run, client.create(generate_setup_version_obj.model_dump()))

        # Get the service and method descriptor.
        service_desc = setup_version_service_pb2.DESCRIPTOR.services_by_name["SetupVersionService"]
        method_desc = service_desc.methods_by_name["CreateSetupVersion"]

        # Intercept the pending unary-unary call.
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        # Use grpc_testing to send the response back to the client.
        rpc.send_initial_metadata(())
        rpc.terminate(
            # use the servicer to emulate a real request handling from a server
            setup_version_dto_pb2.CreateSetupVersionResponse(result=setup_messages_pb2.SetupResult(success=True)),
            (),
            grpc.StatusCode.OK,
            "",
        )

        # Verify that the client call returns success.
        result = future.result()
        assert result.result.success is True

        # Verify the request correspond to the setup data
        assert request.setup_id == generate_setup_version_obj.setup_id
        assert request.version == generate_setup_version_obj.version
        assert dict(request.content) == generate_setup_version_obj.content

    @freeze_time("2025-04-01 12:00:01")
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_create_setup_version_success(
            self,
            client: GrpcSetupVersion,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupVersionServicer,
            generate_setup_version_obj: SetupVersionData,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successful create_setup_version.

        Verifies that create_setup_version RPC call with a valid request using the fake servicer.

        Args:
            grpc_test_server: Mock gRPC server for testing.
        """
        # Start the client call (this call will block until the response is simulated).
        future = thread_pool.submit(asyncio.run, client.create(generate_setup_version_obj.model_dump()))

        # Get the service and method descriptor.
        service_desc = setup_version_service_pb2.DESCRIPTOR.services_by_name["SetupVersionService"]
        method_desc = service_desc.methods_by_name["CreateSetupVersion"]

        # Intercept the pending unary-unary call.
        _, _request, rpc = test_channel.take_unary_unary(method_desc)

        # Use grpc_testing to send the response back to the client.
        rpc.send_initial_metadata(())
        request_obj = setup_version_dto_pb2.CreateSetupVersionRequest(**{
            k: v for (k, v) in generate_setup_version_obj.model_dump().items() if k not in {"created_at", "id"}
        })

        rpc.terminate(
            # use the servicer to emulate a real request handling from a server
            mock_servicer.CreateSetupVersion(request_obj, FakeContext()),
            (),
            grpc.StatusCode.OK,
            "",
        )

        # Verify that the client call returns success.
        result = future.result()
        assert result.result.success is True

        setup_version = mock_servicer.setup_versions[generate_setup_version_obj.setup_id][
            generate_setup_version_obj.version
        ]

        assert isinstance(setup_version, SetupVersionData)
        # Verify the request correspond to the setup data
        assert setup_version.setup_id == generate_setup_version_obj.setup_id
        assert setup_version.version == generate_setup_version_obj.version
        assert setup_version.created_at == generate_setup_version_obj.created_at
        assert setup_version.content == generate_setup_version_obj.content

    # Test RegisterModule
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_create_setup_version_validation_error(
            self,
            client: GrpcSetupVersion,
            generate_setup_version_obj: SetupVersionData,
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
        generate_setup_version_obj.created_at = []
        generate_setup_version_obj.content = ""

        # Start the client call (this call will block until the response is simulated).
        future = thread_pool.submit(asyncio.run, client.create(generate_setup_version_obj.model_dump(warnings=False)))
        with pytest.raises(ValueError, match="Validation failed for Setup Version Creation"):
            future.result()


class TestGetSetupVersion:
    """Tests for get_setup_version() method.

    Verifies successful retrieval of setup version data and handling of non-existent versions.
    """

    @freeze_time("2025-04-01 12:00:01")
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_setup_version_success(
            self,
            client: GrpcSetupVersion,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupVersionServicer,
            generate_setup_version_obj: SetupVersionData,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully retrieving a setup version.

        Verifies that get_setup_version returns the correct setup version data.
        """
        service_desc = setup_version_service_pb2.DESCRIPTOR.services_by_name["SetupVersionService"]
        create_method_desc = service_desc.methods_by_name["CreateSetupVersion"]
        get_method_desc = service_desc.methods_by_name["GetSetupVersion"]

        # First create a setup version
        create_future = thread_pool.submit(asyncio.run, client.create(generate_setup_version_obj.model_dump()))
        _, _create_request, create_rpc = test_channel.take_unary_unary(create_method_desc)
        create_rpc.send_initial_metadata(())
        request_obj = setup_version_dto_pb2.CreateSetupVersionRequest(**{
            k: v for (k, v) in generate_setup_version_obj.model_dump().items() if k not in {"created_at", "id"}
        })
        create_response = mock_servicer.CreateSetupVersion(request_obj, FakeContext())
        create_rpc.terminate(create_response, (), grpc.StatusCode.OK, "")
        create_future.result()

        # Get the created version's ID (it's stored as version key in mock servicer)
        created_version = mock_servicer.setup_versions[generate_setup_version_obj.setup_id][
            generate_setup_version_obj.version
        ]

        # Now get the setup version by ID
        get_future = thread_pool.submit(asyncio.run, client.get({"setup_version_id": created_version.id}))
        _, get_request, get_rpc = test_channel.take_unary_unary(get_method_desc)

        assert get_request.setup_version_id == created_version.id

        get_context = FakeContext()
        get_response = mock_servicer.GetSetupVersion(get_request, get_context)
        get_rpc.send_initial_metadata(())
        get_rpc.terminate(get_response, (), grpc.StatusCode.OK, "")

        result = get_future.result()
        assert result is not None
        assert result.setup_id == generate_setup_version_obj.setup_id
        assert result.version == generate_setup_version_obj.version
        assert result.content == generate_setup_version_obj.content

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_get_setup_version_not_found(
            self,
            client: GrpcSetupVersion,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupVersionServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test getting a non-existent setup version raises error.

        Verifies that attempting to get a non-existent setup version results in error.
        """
        service_desc = setup_version_service_pb2.DESCRIPTOR.services_by_name["SetupVersionService"]
        get_method_desc = service_desc.methods_by_name["GetSetupVersion"]

        get_future = thread_pool.submit(asyncio.run, client.get({"setup_version_id": "nonexistent_version_id"}))
        _, get_request, get_rpc = test_channel.take_unary_unary(get_method_desc)

        get_context = FakeContext()
        get_response = mock_servicer.GetSetupVersion(get_request, get_context)
        get_rpc.send_initial_metadata(())
        get_rpc.terminate(get_response, (), get_context._code, get_context._details)

        with pytest.raises(Exception):
            get_future.result()


class TestSearchSetupVersions:
    """Tests for search_setup_versions() method.

    Verifies successful search of setup versions, filtering capabilities, and handling
    of empty results.
    """

    @freeze_time("2025-04-01 12:00:01")
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_search_setup_versions_success(
            self,
            client: GrpcSetupVersion,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupVersionServicer,
            generate_setup_version_obj: SetupVersionData,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully searching setup versions.

        Verifies that search_setup_versions returns matching versions.
        """
        service_desc = setup_version_service_pb2.DESCRIPTOR.services_by_name["SetupVersionService"]
        create_method_desc = service_desc.methods_by_name["CreateSetupVersion"]
        search_method_desc = service_desc.methods_by_name["SearchSetupVersions"]

        # Create a setup version
        create_future = thread_pool.submit(asyncio.run, client.create(generate_setup_version_obj.model_dump()))
        _, _create_request, create_rpc = test_channel.take_unary_unary(create_method_desc)
        create_rpc.send_initial_metadata(())
        request_obj = setup_version_dto_pb2.CreateSetupVersionRequest(**{
            k: v for (k, v) in generate_setup_version_obj.model_dump().items() if k not in {"created_at", "id"}
        })
        create_response = mock_servicer.CreateSetupVersion(request_obj, FakeContext())
        create_rpc.terminate(create_response, (), grpc.StatusCode.OK, "")
        create_future.result()

        # Search for versions
        search_future = thread_pool.submit(
            asyncio.run,
            client.search(
                {"setup_id": generate_setup_version_obj.setup_id, "version": generate_setup_version_obj.version}
            ),
        )
        _, search_request, search_rpc = test_channel.take_unary_unary(search_method_desc)

        assert search_request.setup_id == generate_setup_version_obj.setup_id
        assert search_request.version == generate_setup_version_obj.version

        search_context = FakeContext()
        search_response = mock_servicer.SearchSetupVersions(search_request, search_context)
        search_rpc.send_initial_metadata(())
        search_rpc.terminate(search_response, (), grpc.StatusCode.OK, "")

        result = search_future.result()
        assert len(result) == 1
        assert result[0].setup_id == generate_setup_version_obj.setup_id
        assert result[0].version == generate_setup_version_obj.version

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_search_setup_versions_empty_results(
            self,
            client: GrpcSetupVersion,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupVersionServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test searching for setup versions with no results.

        Verifies that search_setup_versions returns empty list when no matches found.
        """
        service_desc = setup_version_service_pb2.DESCRIPTOR.services_by_name["SetupVersionService"]
        search_method_desc = service_desc.methods_by_name["SearchSetupVersions"]

        search_future = thread_pool.submit(
            asyncio.run, client.search({"setup_id": "nonexistent_setup", "version": "v1.0.0"})
        )
        _, search_request, search_rpc = test_channel.take_unary_unary(search_method_desc)

        search_context = FakeContext()
        search_response = mock_servicer.SearchSetupVersions(search_request, search_context)
        search_rpc.send_initial_metadata(())
        search_rpc.terminate(search_response, (), search_context._code, search_context._details)

        with pytest.raises(Exception):
            search_future.result()


class TestUpdateSetupVersion:
    """Tests for update_setup_version() method.

    Verifies successful updates and handling of non-existent setup versions.
    """

    @freeze_time("2025-04-01 12:00:01")
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_update_setup_version_success(
            self,
            client: GrpcSetupVersion,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupVersionServicer,
            generate_setup_version_obj: SetupVersionData,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully updating a setup version.

        Verifies that update_setup_version updates the version data correctly.
        """
        service_desc = setup_version_service_pb2.DESCRIPTOR.services_by_name["SetupVersionService"]
        create_method_desc = service_desc.methods_by_name["CreateSetupVersion"]
        update_method_desc = service_desc.methods_by_name["UpdateSetupVersion"]

        # First create a setup version
        create_future = thread_pool.submit(asyncio.run, client.create(generate_setup_version_obj.model_dump()))
        _, _create_request, create_rpc = test_channel.take_unary_unary(create_method_desc)
        create_rpc.send_initial_metadata(())
        request_obj = setup_version_dto_pb2.CreateSetupVersionRequest(**{
            k: v for (k, v) in generate_setup_version_obj.model_dump().items() if k not in {"created_at", "id"}
        })
        create_response = mock_servicer.CreateSetupVersion(request_obj, FakeContext())
        create_rpc.terminate(create_response, (), grpc.StatusCode.OK, "")
        create_future.result()

        # Get the created version
        created_version = mock_servicer.setup_versions[generate_setup_version_obj.setup_id][
            generate_setup_version_obj.version
        ]

        # Update the setup version
        updated_data = generate_setup_version_obj.model_dump()
        updated_data["id"] = created_version.id
        updated_data["content"] = {"updated_key": "updated_value"}

        update_future = thread_pool.submit(asyncio.run, client.update(updated_data))
        _, update_request, update_rpc = test_channel.take_unary_unary(update_method_desc)

        assert update_request.setup_version_id == created_version.id

        update_context = FakeContext()
        update_response = mock_servicer.UpdateSetupVersion(update_request, update_context)
        update_rpc.send_initial_metadata(())
        update_rpc.terminate(update_response, (), grpc.StatusCode.OK, "")

        result = update_future.result()
        assert result is True

        # Verify the update in mock servicer
        updated_version = mock_servicer.setup_versions[generate_setup_version_obj.setup_id][
            generate_setup_version_obj.version
        ]
        assert updated_version.content == {"updated_key": "updated_value"}

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_update_setup_version_not_found(
            self,
            client: GrpcSetupVersion,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupVersionServicer,
            generate_setup_version_obj: SetupVersionData,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test updating a non-existent setup version returns False.

        Verifies that attempting to update a non-existent setup version returns False.
        """
        service_desc = setup_version_service_pb2.DESCRIPTOR.services_by_name["SetupVersionService"]
        update_method_desc = service_desc.methods_by_name["UpdateSetupVersion"]

        updated_data = generate_setup_version_obj.model_dump()
        updated_data["id"] = "nonexistent_version_id"

        update_future = thread_pool.submit(asyncio.run, client.update(updated_data))
        _, update_request, update_rpc = test_channel.take_unary_unary(update_method_desc)

        update_context = FakeContext()
        update_response = mock_servicer.UpdateSetupVersion(update_request, update_context)
        update_rpc.send_initial_metadata(())
        # When setup version doesn't exist, return OK status with success=False
        update_rpc.terminate(update_response, (), grpc.StatusCode.OK, "")

        result = update_future.result()
        assert result is False


class TestDeleteSetupVersion:
    """Tests for delete_setup_version() method.

    Verifies successful deletion of setup versions and proper handling of non-existent versions.
    """

    @freeze_time("2025-04-01 12:00:01")
    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_delete_setup_version_success(
            self,
            client: GrpcSetupVersion,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupVersionServicer,
            generate_setup_version_obj: SetupVersionData,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully deleting a setup version.

        Verifies that delete_setup_version removes the version from storage.
        """
        service_desc = setup_version_service_pb2.DESCRIPTOR.services_by_name["SetupVersionService"]
        create_method_desc = service_desc.methods_by_name["CreateSetupVersion"]
        delete_method_desc = service_desc.methods_by_name["DeleteSetupVersion"]

        # First create a setup version
        create_future = thread_pool.submit(asyncio.run, client.create(generate_setup_version_obj.model_dump()))
        _, _create_request, create_rpc = test_channel.take_unary_unary(create_method_desc)
        create_rpc.send_initial_metadata(())
        request_obj = setup_version_dto_pb2.CreateSetupVersionRequest(**{
            k: v for (k, v) in generate_setup_version_obj.model_dump().items() if k not in {"created_at", "id"}
        })
        create_response = mock_servicer.CreateSetupVersion(request_obj, FakeContext())
        create_rpc.terminate(create_response, (), grpc.StatusCode.OK, "")
        create_future.result()

        # Get the created version
        created_version = mock_servicer.setup_versions[generate_setup_version_obj.setup_id][
            generate_setup_version_obj.version
        ]

        # Delete the setup version
        delete_future = thread_pool.submit(asyncio.run, client.delete({"setup_version_id": created_version.id}))
        _, delete_request, delete_rpc = test_channel.take_unary_unary(delete_method_desc)

        assert delete_request.setup_version_id == created_version.id

        delete_context = FakeContext()
        delete_response = mock_servicer.DeleteSetupVersion(delete_request, delete_context)
        delete_rpc.send_initial_metadata(())
        delete_rpc.terminate(delete_response, (), grpc.StatusCode.OK, "")

        result = delete_future.result()
        assert result is True

        # Verify deletion in mock servicer
        assert generate_setup_version_obj.setup_id not in mock_servicer.setup_versions

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_delete_setup_version_not_found(
            self,
            client: GrpcSetupVersion,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockSetupVersionServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test deleting a non-existent setup version returns False.

        Verifies that attempting to delete a non-existent setup version returns False.
        """
        service_desc = setup_version_service_pb2.DESCRIPTOR.services_by_name["SetupVersionService"]
        delete_method_desc = service_desc.methods_by_name["DeleteSetupVersion"]

        delete_future = thread_pool.submit(asyncio.run, client.delete({"setup_version_id": "nonexistent_version_id"}))
        _, delete_request, delete_rpc = test_channel.take_unary_unary(delete_method_desc)

        delete_context = FakeContext()
        delete_response = mock_servicer.DeleteSetupVersion(delete_request, delete_context)
        delete_rpc.send_initial_metadata(())
        # When setup version doesn't exist, return OK status with success=False
        delete_rpc.terminate(delete_response, (), grpc.StatusCode.OK, "")

        result = delete_future.result()
        assert result is False

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
