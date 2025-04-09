"""Test the grpc service."""

import datetime
import secrets
import string

import grpc
import grpc_testing
import pytest
from digitalkin_proto.digitalkin.setup.v2 import (
    setup_pb2,
    setup_service_pb2,
    setup_service_pb2_grpc,
)
from freezegun import freeze_time
from grpc.framework.foundation import logging_pool
from mock_setup_servicer import FakeContext, MockSetupServicer

from digitalkin.grpc_servers.utils.models import SecurityMode, ServerConfig, ServerMode
from digitalkin.services.setup.grpc_setup import GrpcSetup
from digitalkin.services.setup.setup_strategy import SetupData, SetupVersionData

service_instance = MockSetupServicer()
service_name = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]

alphabet = string.ascii_letters + string.digits
client_execution_thread_pool = logging_pool.pool(1)


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
    channel = test_channel
    # Create a dummy ServerConfig; its values are not used since we override _init_channel.
    dummy_config = ServerConfig(
        host="[::]",
        port=50151,
        mode=ServerMode.ASYNC,
        security=SecurityMode.INSECURE,
        max_workers=10,
        credentials=None,
    )
    client = GrpcSetup()
    # emulate real instance
    client.__post_init__(dummy_config)

    # Override the channel and stub to use our test channel
    client.stub = setup_service_pb2_grpc.SetupServiceStub(channel)
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
        creation_date=datetime.datetime.now(),
    )


@pytest.fixture
def generate_setup_obj(generate_setup_version_obj: SetupVersionData) -> SetupData:
    # Create registration request with test setup data
    return SetupData(
        id=generate_setup_version_obj.setup_id,
        name=random_string(),
        organisation_id=random_string(),
        owner_id=random_string(),
        module_id=random_string(),
        current_setup_version=generate_setup_version_obj,
    )


@freeze_time("2025-04-01 12:00:01")
def test_create_setup_request_creation_success(
    client: GrpcSetup,
    test_channel: grpc_testing.Channel,
    generate_setup_obj: SetupData,
    generate_setup_version_obj: SetupVersionData,
) -> None:
    """Test successful create_setup with a good request.

    Verifies that create_setup create the good request.

    Args:
        grpc_test_server: Mock gRPC server for testing.
    """
    # Start the client call (this call will block until the response is simulated).
    future = client_execution_thread_pool.submit(client.create_setup, generate_setup_obj.model_dump())

    # Get the service and method descriptor.
    service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
    method_desc = service_desc.methods_by_name["CreateSetup"]

    # Intercept the pending unary-unary call.
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    # Use grpc_testing to send the response back to the client.
    rpc.send_initial_metadata(())
    rpc.terminate(
        # use the servicer to emulate a real request handling from a server
        setup_pb2.CreateSetupResponse(success=True),
        (),
        grpc.StatusCode.OK,
        "",
    )

    # Verify that the client call returns success.
    result = future.result()
    assert result.success is True

    # Verify the request correspond to the setup data
    assert request.name == generate_setup_obj.name
    assert request.organisation_id == generate_setup_obj.organisation_id
    assert request.owner_id == generate_setup_obj.owner_id
    assert request.current_setup_version.setup_id == generate_setup_obj.current_setup_version.setup_id
    assert request.current_setup_version.version == generate_setup_obj.current_setup_version.version
    assert (
        request.current_setup_version.creation_date.ToDatetime()
        == generate_setup_obj.current_setup_version.creation_date
    )
    assert dict(request.current_setup_version.content) == generate_setup_obj.current_setup_version.content


@freeze_time("2025-04-01 12:00:01")
def test_create_setup_success(
    client: GrpcSetup,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockSetupServicer,
    generate_setup_obj: SetupData,
    generate_setup_version_obj: SetupVersionData,
) -> None:
    """Test successful create_setup.

    Verifies that create_setup RPC call with a valid request using the fake servicer.

    Args:
        grpc_test_server: Mock gRPC server for testing.
    """
    # Start the client call (this call will block until the response is simulated).
    future = client_execution_thread_pool.submit(client.create_setup, generate_setup_obj.model_dump())

    # Get the service and method descriptor.
    service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
    method_desc = service_desc.methods_by_name["CreateSetup"]

    # Intercept the pending unary-unary call.
    _, _request, rpc = test_channel.take_unary_unary(method_desc)

    # Use grpc_testing to send the response back to the client.
    rpc.send_initial_metadata(())
    request_obj = setup_pb2.CreateSetupRequest(**{
        k: v for (k, v) in generate_setup_obj.model_dump().items() if k not in ("id")
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
    assert result.success is True

    setup = next(
        filter(
            lambda obj: getattr(obj, "name", None) == generate_setup_obj.name,
            mock_servicer.setups.values(),
        )
    )

    assert isinstance(setup, SetupData)
    assert setup.name == generate_setup_obj.name
    assert setup.organisation_id == generate_setup_obj.organisation_id
    assert setup.owner_id == generate_setup_obj.owner_id
    assert setup.current_setup_version.setup_id == generate_setup_obj.current_setup_version.setup_id
    assert setup.current_setup_version.version == generate_setup_obj.current_setup_version.version
    assert setup.current_setup_version.creation_date == generate_setup_obj.current_setup_version.creation_date
    assert setup.current_setup_version.content == generate_setup_obj.current_setup_version.content


# Test RegisterModule
def test_create_setup_validation_error(
    client: GrpcSetup,
    generate_setup_version_obj: SetupVersionData,
    generate_setup_obj: SetupData,
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
    future = client_execution_thread_pool.submit(client.create_setup, generate_setup_obj.model_dump())
    with pytest.raises(ValueError, match="Invalid data for Setup Creation"):
        future.result()


@freeze_time("2025-04-01 12:00:01")
def test_create_setup_version_request_creation_success(
    client: GrpcSetup,
    test_channel: grpc_testing.Channel,
    generate_setup_version_obj: SetupVersionData,
) -> None:
    """Test successful create_setup_version with a good request.

    Verifies that create_setup create the good request.

    Args:
        grpc_test_server: Mock gRPC server for testing.
    """
    # Start the client call (this call will block until the response is simulated).
    future = client_execution_thread_pool.submit(client.create_setup_version, generate_setup_version_obj.model_dump())

    # Get the service and method descriptor.
    service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
    method_desc = service_desc.methods_by_name["CreateSetupVersion"]

    # Intercept the pending unary-unary call.
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    # Use grpc_testing to send the response back to the client.
    rpc.send_initial_metadata(())
    rpc.terminate(
        # use the servicer to emulate a real request handling from a server
        setup_pb2.CreateSetupVersionResponse(success=True),
        (),
        grpc.StatusCode.OK,
        "",
    )

    # Verify that the client call returns success.
    result = future.result()
    assert result.success is True

    # Verify the request correspond to the setup data
    assert request.setup_id == generate_setup_version_obj.setup_id
    assert request.version == generate_setup_version_obj.version
    assert dict(request.content) == generate_setup_version_obj.content


@freeze_time("2025-04-01 12:00:01")
def test_create_setup_version_success(
    client: GrpcSetup,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockSetupServicer,
    generate_setup_version_obj: SetupVersionData,
) -> None:
    """Test successful create_setup.

    Verifies that create_setup RPC call with a valid request using the fake servicer.

    Args:
        grpc_test_server: Mock gRPC server for testing.
    """
    # Start the client call (this call will block until the response is simulated).
    future = client_execution_thread_pool.submit(client.create_setup_version, generate_setup_version_obj.model_dump())

    # Get the service and method descriptor.
    service_desc = setup_service_pb2.DESCRIPTOR.services_by_name["SetupService"]
    method_desc = service_desc.methods_by_name["CreateSetupVersion"]

    # Intercept the pending unary-unary call.
    _, _request, rpc = test_channel.take_unary_unary(method_desc)

    # Use grpc_testing to send the response back to the client.
    rpc.send_initial_metadata(())
    request_obj = setup_pb2.CreateSetupVersionRequest(**{
        k: v for (k, v) in generate_setup_version_obj.model_dump().items() if k not in {"creation_date", "id"}
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
    assert result.success is True

    setup_version = mock_servicer.setup_versions[generate_setup_version_obj.setup_id][
        generate_setup_version_obj.version
    ]

    assert isinstance(setup_version, SetupVersionData)
    # Verify the request correspond to the setup data
    assert setup_version.setup_id == generate_setup_version_obj.setup_id
    assert setup_version.version == generate_setup_version_obj.version
    assert setup_version.creation_date == generate_setup_version_obj.creation_date
    assert setup_version.content == generate_setup_version_obj.content


# Test RegisterModule
def test_create_setup_version_validation_error(
    client: GrpcSetup,
    generate_setup_version_obj: SetupVersionData,
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
    generate_setup_version_obj.creation_date = []
    generate_setup_version_obj.content = ""

    # Start the client call (this call will block until the response is simulated).
    future = client_execution_thread_pool.submit(client.create_setup_version, generate_setup_version_obj.model_dump())
    with pytest.raises(ValueError, match="Invalid data for Setup Version Creation"):
        future.result()
