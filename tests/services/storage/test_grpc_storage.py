"""Comprehensive tests for GrpcStorage service.

This test suite validates the GrpcStorage service implementation, including:
- Storing records with schema validation
- Reading records by collection and record_id
- Updating existing records
- Removing records
- Listing records in a collection
- Removing entire collections
- Error handling and edge cases
"""

from concurrent import futures

import grpc_testing
import pytest
from digitalkin_proto.agentic_mesh_protocol.storage.v1 import storage_service_pb2_grpc
from pydantic import BaseModel, Field
from tests.services.storage.mock_storage_servicer import FakeContext, MockStorageServicer

from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.storage.grpc_storage import GrpcStorage
from digitalkin.services.storage.storage_strategy import DataType, StorageServiceError

# Set timeout for all tests in this file (20 seconds)
pytestmark = pytest.mark.timeout(20)

# --- Test Constants ---
MISSION_ID = "missions:test_mission"
SETUP_ID = "setups:test_setup"
SETUP_VERSION_ID = "setup_versions:test_version"

# Thread pool for client execution
client_execution_thread_pool = futures.ThreadPoolExecutor(max_workers=1)


# --- Test Models ---
class TestDataModel(BaseModel):
    """Test data model for storage tests."""

    mission_id: str = Field(..., description="Mission ID")
    name: str = Field(..., description="Name field")
    value: int = Field(..., description="Value field")
    description: str | None = Field(None, description="Optional description")


class OutputDataModel(BaseModel):
    """Output data model for storage tests."""

    mission_id: str = Field(..., description="Mission ID")
    result: str = Field(..., description="Result field")
    score: float = Field(..., description="Score field")


class LogDataModel(BaseModel):
    """Log data model for storage tests."""

    mission_id: str = Field(..., description="Mission ID")
    level: str = Field(..., description="Log level")
    message: str = Field(..., description="Log message")
    timestamp: str = Field(..., description="Timestamp")


# --- Fixtures ---
@pytest.fixture
def storage_config() -> dict[str, type[BaseModel]]:
    """Provide test storage configuration with schema mappings.

    Returns:
        Dictionary mapping collection names to Pydantic models
    """
    return {
        "test_collection": TestDataModel,
        "outputs": OutputDataModel,
        "logs": LogDataModel,
    }


@pytest.fixture
def test_channel() -> grpc_testing.Channel:
    """Create a test gRPC channel.

    Returns:
        A testing channel for intercepting gRPC calls
    """
    return grpc_testing.channel(
        service_descriptors=[storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"]],
        time=grpc_testing.strict_real_time(),
    )


@pytest.fixture
def mock_servicer(storage_config: dict[str, type[BaseModel]]) -> MockStorageServicer:
    """Create a mock storage servicer.

    Args:
        storage_config: Schema configuration

    Returns:
        Mock servicer instance with schema configuration
    """
    return MockStorageServicer(schema_config=storage_config)


@pytest.fixture
def dummy_client_config() -> ClientConfig:
    """Create a dummy ClientConfig for testing.

    Returns:
        ClientConfig instance with test values
    """
    return ClientConfig(host="localhost", port=50051, secure=False)


@pytest.fixture
def client(
    test_channel: grpc_testing.Channel,
    storage_config: dict[str, type[BaseModel]],
    dummy_client_config: ClientConfig,
) -> GrpcStorage:
    """Create a GrpcStorage client with test channel.

    Args:
        test_channel: Test gRPC channel
        storage_config: Schema configuration
        dummy_client_config: Dummy client configuration

    Returns:
        GrpcStorage client configured for testing
    """
    client = GrpcStorage(MISSION_ID, SETUP_ID, SETUP_VERSION_ID, storage_config, dummy_client_config)
    client.stub = storage_service_pb2_grpc.StorageServiceStub(test_channel)
    return client


# ============================================================================
# _store() Tests
# ============================================================================


def test_store_record_success(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test successfully storing a new record.

    Verifies:
    - Record is stored with correct data
    - StorageRecord is returned with all fields
    - Collection and record_id are set correctly
    """
    collection = "test_collection"
    record_id = "record_001"
    data = {"mission_id": MISSION_ID, "name": "Test Record", "value": 42, "description": "A test record"}

    # Get the method descriptor
    method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

    # Execute client call in thread pool
    future = client_execution_thread_pool.submit(client.store, collection, record_id, data)

    # Intercept the call
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    # Verify request
    assert request.mission_id == MISSION_ID
    assert request.collection == collection
    assert request.record_id == record_id
    assert request.data_type == DataType.OUTPUT.name

    # Mock servicer processes the request
    context = FakeContext()
    response = mock_servicer.StoreRecord(request, context)

    # Terminate the RPC
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    # Get result
    result = future.result(timeout=5.0)

    # Verify result
    assert result is not None
    assert result.mission_id == MISSION_ID
    assert result.collection == collection
    assert result.record_id == record_id
    assert result.data_type == DataType.OUTPUT
    assert result.data.name == "Test Record"
    assert result.data.value == 42
    assert result.creation_date is not None
    assert result.update_date is not None


def test_store_record_invalid_schema(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test storing a record with invalid schema raises error.

    Verifies:
    - Invalid data is rejected by schema validation
    - StorageServiceError is raised
    """
    collection = "test_collection"
    record_id = "record_002"
    # Missing required 'value' field
    data = {"mission_id": MISSION_ID, "name": "Invalid Record"}

    method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

    future = client_execution_thread_pool.submit(client.store, collection, record_id, data)

    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.StoreRecord(request, context)

    # Terminate with error
    rpc.terminate(response, (), context._code, context._details)

    # Verify error is raised
    with pytest.raises(StorageServiceError):
        future.result(timeout=5.0)


def test_store_record_duplicate(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test storing a duplicate record raises error.

    Verifies:
    - Attempting to store a record with existing record_id fails
    - StorageServiceError is raised
    """
    collection = "test_collection"
    record_id = "record_003"
    data = {"mission_id": MISSION_ID, "name": "First Record", "value": 10}

    method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

    # Store first record
    future1 = client_execution_thread_pool.submit(client.store, collection, record_id, data)
    _, request1, rpc1 = test_channel.take_unary_unary(method_desc)
    context1 = FakeContext()
    response1 = mock_servicer.StoreRecord(request1, context1)
    rpc1.terminate(response1, (), grpc.StatusCode.OK, "")
    result1 = future1.result(timeout=5.0)
    assert result1 is not None

    # Attempt to store duplicate
    future2 = client_execution_thread_pool.submit(client.store, collection, record_id, data)
    _, request2, rpc2 = test_channel.take_unary_unary(method_desc)
    context2 = FakeContext()
    response2 = mock_servicer.StoreRecord(request2, context2)
    rpc2.terminate(response2, (), context2._code, context2._details)

    # Verify error is raised
    with pytest.raises(StorageServiceError):
        future2.result(timeout=5.0)


def test_store_record_with_output_type(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test storing a record with OUTPUT data type.

    Verifies:
    - OUTPUT data type is correctly set
    - Record is stored successfully
    """
    collection = "outputs"
    record_id = "output_001"
    data = {"mission_id": MISSION_ID, "result": "Success", "score": 0.95}

    method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

    future = client_execution_thread_pool.submit(client.store, collection, record_id, data, data_type="OUTPUT")

    _, request, rpc = test_channel.take_unary_unary(method_desc)

    assert request.data_type == DataType.OUTPUT.name

    context = FakeContext()
    response = mock_servicer.StoreRecord(request, context)
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    assert result.data_type == DataType.OUTPUT


def test_store_record_with_logs_type(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test storing a record with LOGS data type.

    Verifies:
    - LOGS data type is correctly set
    - Record is stored successfully
    """
    collection = "logs"
    record_id = "log_001"
    data = {
        "mission_id": MISSION_ID,
        "level": "INFO",
        "message": "Test log message",
        "timestamp": "2024-01-01T00:00:00Z",
    }

    method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

    future = client_execution_thread_pool.submit(client.store, collection, record_id, data, data_type="LOGS")

    _, request, rpc = test_channel.take_unary_unary(method_desc)

    assert request.data_type == DataType.LOGS.name

    context = FakeContext()
    response = mock_servicer.StoreRecord(request, context)
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    assert result.data_type == DataType.LOGS


def test_store_record_with_view_type(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test storing a record with VIEW data type.

    Verifies:
    - VIEW data type is correctly set
    - Record is stored successfully
    """
    collection = "test_collection"
    record_id = "view_001"
    data = {"mission_id": MISSION_ID, "name": "View Data", "value": 100}

    method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

    future = client_execution_thread_pool.submit(client.store, collection, record_id, data, data_type="VIEW")

    _, request, rpc = test_channel.take_unary_unary(method_desc)

    assert request.data_type == DataType.VIEW.name

    context = FakeContext()
    response = mock_servicer.StoreRecord(request, context)
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    assert result.data_type == DataType.VIEW


def test_store_record_with_other_type(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test storing a record with OTHER data type.

    Verifies:
    - OTHER data type is correctly set
    - Record is stored successfully
    """
    collection = "test_collection"
    record_id = "other_001"
    data = {"mission_id": MISSION_ID, "name": "Other Data", "value": 50}

    method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

    future = client_execution_thread_pool.submit(client.store, collection, record_id, data, data_type="OTHER")

    _, request, rpc = test_channel.take_unary_unary(method_desc)

    assert request.data_type == DataType.OTHER.name

    context = FakeContext()
    response = mock_servicer.StoreRecord(request, context)
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    assert result.data_type == DataType.OTHER


def test_store_record_auto_generated_id(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test storing a record with auto-generated ID.

    Verifies:
    - record_id is auto-generated when None is provided
    - Record is stored successfully
    """
    collection = "test_collection"
    data = {"mission_id": MISSION_ID, "name": "Auto ID Record", "value": 999}

    method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

    # Pass None for record_id to trigger auto-generation
    future = client_execution_thread_pool.submit(client.store, collection, None, data)

    _, request, rpc = test_channel.take_unary_unary(method_desc)

    # Verify record_id was generated (should be a UUID hex string)
    assert request.record_id is not None
    assert len(request.record_id) > 0

    context = FakeContext()
    response = mock_servicer.StoreRecord(request, context)
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    assert result.record_id is not None


# ============================================================================
# _read() Tests
# ============================================================================


def test_read_record_success(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test successfully reading an existing record.

    Verifies:
    - Record can be read after storing
    - All data fields are preserved
    """
    collection = "test_collection"
    record_id = "record_read_001"
    data = {"mission_id": MISSION_ID, "name": "Read Test", "value": 123}

    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]
    read_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "ReadRecord"
    ]

    # Store the record first
    store_future = client_execution_thread_pool.submit(client.store, collection, record_id, data)
    _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
    store_context = FakeContext()
    store_response = mock_servicer.StoreRecord(store_request, store_context)
    store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
    store_future.result(timeout=5.0)

    # Read the record
    read_future = client_execution_thread_pool.submit(client.read, collection, record_id)
    _, read_request, read_rpc = test_channel.take_unary_unary(read_method_desc)

    assert read_request.mission_id == MISSION_ID
    assert read_request.collection == collection
    assert read_request.record_id == record_id

    read_context = FakeContext()
    read_response = mock_servicer.ReadRecord(read_request, read_context)
    read_rpc.terminate(read_response, (), grpc.StatusCode.OK, "")

    result = read_future.result(timeout=5.0)
    assert result is not None
    assert result.record_id == record_id
    assert result.data.name == "Read Test"
    assert result.data.value == 123


def test_read_record_not_found(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test reading a non-existent record returns None.

    Verifies:
    - Reading non-existent record returns None
    - No exception is raised
    """
    collection = "test_collection"
    record_id = "nonexistent_record"

    read_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "ReadRecord"
    ]

    read_future = client_execution_thread_pool.submit(client.read, collection, record_id)
    _, read_request, read_rpc = test_channel.take_unary_unary(read_method_desc)

    read_context = FakeContext()
    read_response = mock_servicer.ReadRecord(read_request, read_context)
    read_rpc.terminate(read_response, (), read_context._code, read_context._details)

    result = read_future.result(timeout=5.0)
    assert result is None


def test_read_record_from_different_collections(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test reading records from different collections.

    Verifies:
    - Records with same record_id in different collections are independent
    - Each collection maintains separate records
    """
    record_id = "shared_id"
    data1 = {"mission_id": MISSION_ID, "name": "Collection 1", "value": 100}
    data2 = {"mission_id": MISSION_ID, "result": "Collection 2", "score": 0.8}

    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]
    read_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "ReadRecord"
    ]

    # Store in collection 1
    store_future1 = client_execution_thread_pool.submit(client.store, "test_collection", record_id, data1)
    _, store_request1, store_rpc1 = test_channel.take_unary_unary(store_method_desc)
    store_context1 = FakeContext()
    store_response1 = mock_servicer.StoreRecord(store_request1, store_context1)
    store_rpc1.terminate(store_response1, (), grpc.StatusCode.OK, "")
    store_future1.result(timeout=5.0)

    # Store in collection 2
    store_future2 = client_execution_thread_pool.submit(client.store, "outputs", record_id, data2)
    _, store_request2, store_rpc2 = test_channel.take_unary_unary(store_method_desc)
    store_context2 = FakeContext()
    store_response2 = mock_servicer.StoreRecord(store_request2, store_context2)
    store_rpc2.terminate(store_response2, (), grpc.StatusCode.OK, "")
    store_future2.result(timeout=5.0)

    # Read from collection 1
    read_future1 = client_execution_thread_pool.submit(client.read, "test_collection", record_id)
    _, read_request1, read_rpc1 = test_channel.take_unary_unary(read_method_desc)
    read_context1 = FakeContext()
    read_response1 = mock_servicer.ReadRecord(read_request1, read_context1)
    read_rpc1.terminate(read_response1, (), grpc.StatusCode.OK, "")
    result1 = read_future1.result(timeout=5.0)

    # Read from collection 2
    read_future2 = client_execution_thread_pool.submit(client.read, "outputs", record_id)
    _, read_request2, read_rpc2 = test_channel.take_unary_unary(read_method_desc)
    read_context2 = FakeContext()
    read_response2 = mock_servicer.ReadRecord(read_request2, read_context2)
    read_rpc2.terminate(read_response2, (), grpc.StatusCode.OK, "")
    result2 = read_future2.result(timeout=5.0)

    # Verify both records exist and are different
    assert result1 is not None
    assert result2 is not None
    assert result1.data.name == "Collection 1"
    assert result2.data.result == "Collection 2"


# ============================================================================
# _update() Tests
# ============================================================================


def test_update_record_success(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test successfully updating an existing record.

    Verifies:
    - Record data is updated correctly
    - update_date is refreshed
    """
    collection = "test_collection"
    record_id = "record_update_001"
    original_data = {"mission_id": MISSION_ID, "name": "Original", "value": 10}
    updated_data = {"mission_id": MISSION_ID, "name": "Updated", "value": 20, "description": "Updated description"}

    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]
    update_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "UpdateRecord"
    ]

    # Store original record
    store_future = client_execution_thread_pool.submit(client.store, collection, record_id, original_data)
    _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
    store_context = FakeContext()
    store_response = mock_servicer.StoreRecord(store_request, store_context)
    store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
    stored = store_future.result(timeout=5.0)

    # Update the record
    update_future = client_execution_thread_pool.submit(client.update, collection, record_id, updated_data)
    _, update_request, update_rpc = test_channel.take_unary_unary(update_method_desc)

    assert update_request.mission_id == MISSION_ID
    assert update_request.collection == collection
    assert update_request.record_id == record_id

    update_context = FakeContext()
    update_response = mock_servicer.UpdateRecord(update_request, update_context)
    update_rpc.terminate(update_response, (), grpc.StatusCode.OK, "")

    result = update_future.result(timeout=5.0)
    assert result is not None
    assert result.data.name == "Updated"
    assert result.data.value == 20
    assert result.data.description == "Updated description"
    assert result.update_date != stored.update_date


def test_update_record_not_found(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test updating a non-existent record returns None.

    Verifies:
    - Updating non-existent record returns None
    - No exception is raised
    """
    collection = "test_collection"
    record_id = "nonexistent_update"
    data = {"mission_id": MISSION_ID, "name": "Update", "value": 99}

    update_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "UpdateRecord"
    ]

    update_future = client_execution_thread_pool.submit(client.update, collection, record_id, data)
    _, update_request, update_rpc = test_channel.take_unary_unary(update_method_desc)

    update_context = FakeContext()
    update_response = mock_servicer.UpdateRecord(update_request, update_context)
    update_rpc.terminate(update_response, (), update_context._code, update_context._details)

    result = update_future.result(timeout=5.0)
    assert result is None


def test_update_record_validation_error(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test updating a record with invalid data returns None.

    Verifies:
    - Invalid update data is rejected
    - None is returned on validation failure
    """
    collection = "test_collection"
    record_id = "record_update_002"
    original_data = {"mission_id": MISSION_ID, "name": "Original", "value": 10}
    # Invalid data - missing required 'value' field
    invalid_data = {"mission_id": MISSION_ID, "name": "Invalid Update"}

    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]
    update_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "UpdateRecord"
    ]

    # Store original record
    store_future = client_execution_thread_pool.submit(client.store, collection, record_id, original_data)
    _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
    store_context = FakeContext()
    store_response = mock_servicer.StoreRecord(store_request, store_context)
    store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
    store_future.result(timeout=5.0)

    # Attempt invalid update
    update_future = client_execution_thread_pool.submit(client.update, collection, record_id, invalid_data)
    _, update_request, update_rpc = test_channel.take_unary_unary(update_method_desc)

    update_context = FakeContext()
    update_response = mock_servicer.UpdateRecord(update_request, update_context)
    update_rpc.terminate(update_response, (), update_context._code, update_context._details)

    result = update_future.result(timeout=5.0)
    assert result is None


# ============================================================================
# _remove() Tests
# ============================================================================


def test_remove_record_success(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test successfully removing a record.

    Verifies:
    - Record is removed from storage
    - Returns True on success
    """
    collection = "test_collection"
    record_id = "record_remove_001"
    data = {"mission_id": MISSION_ID, "name": "To Remove", "value": 99}

    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]
    remove_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "RemoveRecord"
    ]

    # Store the record
    store_future = client_execution_thread_pool.submit(client.store, collection, record_id, data)
    _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
    store_context = FakeContext()
    store_response = mock_servicer.StoreRecord(store_request, store_context)
    store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
    store_future.result(timeout=5.0)

    # Remove the record
    remove_future = client_execution_thread_pool.submit(client.remove, collection, record_id)
    _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_method_desc)

    assert remove_request.mission_id == MISSION_ID
    assert remove_request.collection == collection
    assert remove_request.record_id == record_id

    remove_context = FakeContext()
    remove_response = mock_servicer.RemoveRecord(remove_request, remove_context)
    remove_rpc.terminate(remove_response, (), grpc.StatusCode.OK, "")

    result = remove_future.result(timeout=5.0)
    assert result is True


def test_remove_record_not_found_idempotent(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test removing a non-existent record is idempotent.

    Verifies:
    - Removing non-existent record returns True
    - Operation is idempotent
    """
    collection = "test_collection"
    record_id = "nonexistent_remove"

    remove_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "RemoveRecord"
    ]

    remove_future = client_execution_thread_pool.submit(client.remove, collection, record_id)
    _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_method_desc)

    remove_context = FakeContext()
    remove_response = mock_servicer.RemoveRecord(remove_request, remove_context)
    remove_rpc.terminate(remove_response, (), grpc.StatusCode.OK, "")

    result = remove_future.result(timeout=5.0)
    # Idempotent delete - should still return True
    assert result is True


def test_remove_record_multiple_times(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test removing the same record multiple times is idempotent.

    Verifies:
    - First removal succeeds
    - Subsequent removals are idempotent
    """
    collection = "test_collection"
    record_id = "record_remove_002"
    data = {"mission_id": MISSION_ID, "name": "Multi Remove", "value": 88}

    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]
    remove_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "RemoveRecord"
    ]

    # Store the record
    store_future = client_execution_thread_pool.submit(client.store, collection, record_id, data)
    _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
    store_context = FakeContext()
    store_response = mock_servicer.StoreRecord(store_request, store_context)
    store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
    store_future.result(timeout=5.0)

    # First removal
    remove_future1 = client_execution_thread_pool.submit(client.remove, collection, record_id)
    _, remove_request1, remove_rpc1 = test_channel.take_unary_unary(remove_method_desc)
    remove_context1 = FakeContext()
    remove_response1 = mock_servicer.RemoveRecord(remove_request1, remove_context1)
    remove_rpc1.terminate(remove_response1, (), grpc.StatusCode.OK, "")
    result1 = remove_future1.result(timeout=5.0)

    # Second removal (idempotent)
    remove_future2 = client_execution_thread_pool.submit(client.remove, collection, record_id)
    _, remove_request2, remove_rpc2 = test_channel.take_unary_unary(remove_method_desc)
    remove_context2 = FakeContext()
    remove_response2 = mock_servicer.RemoveRecord(remove_request2, remove_context2)
    remove_rpc2.terminate(remove_response2, (), grpc.StatusCode.OK, "")
    result2 = remove_future2.result(timeout=5.0)

    assert result1 is True
    assert result2 is True


# ============================================================================
# _list() Tests
# ============================================================================


def test_list_records_with_data(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test listing records in a collection with data.

    Verifies:
    - All records in collection are returned
    - Record data is correct
    """
    collection = "test_collection"

    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]
    list_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "ListRecords"
    ]

    # Store multiple records
    records_data = [
        ("rec_1", {"mission_id": MISSION_ID, "name": "Record 1", "value": 1}),
        ("rec_2", {"mission_id": MISSION_ID, "name": "Record 2", "value": 2}),
        ("rec_3", {"mission_id": MISSION_ID, "name": "Record 3", "value": 3}),
    ]

    for record_id, data in records_data:
        store_future = client_execution_thread_pool.submit(client.store, collection, record_id, data)
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.StoreRecord(store_request, store_context)
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
        store_future.result(timeout=5.0)

    # List all records
    list_future = client_execution_thread_pool.submit(client.list, collection)
    _, list_request, list_rpc = test_channel.take_unary_unary(list_method_desc)

    assert list_request.mission_id == MISSION_ID
    assert list_request.collection == collection

    list_context = FakeContext()
    list_response = mock_servicer.ListRecords(list_request, list_context)
    list_rpc.terminate(list_response, (), grpc.StatusCode.OK, "")

    result = list_future.result(timeout=5.0)
    assert len(result) == 3
    record_ids = {r.record_id for r in result}
    assert record_ids == {"rec_1", "rec_2", "rec_3"}


def test_list_records_empty_collection(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test listing records in an empty collection.

    Verifies:
    - Empty list is returned for empty collection
    - No error is raised
    """
    collection = "empty_collection"

    list_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "ListRecords"
    ]

    list_future = client_execution_thread_pool.submit(client.list, collection)
    _, list_request, list_rpc = test_channel.take_unary_unary(list_method_desc)

    list_context = FakeContext()
    list_response = mock_servicer.ListRecords(list_request, list_context)
    list_rpc.terminate(list_response, (), grpc.StatusCode.OK, "")

    result = list_future.result(timeout=5.0)
    assert len(result) == 0


def test_list_records_multiple_collections_isolated(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test listing records shows only records from specified collection.

    Verifies:
    - Records from different collections are isolated
    - List only returns records from requested collection
    """
    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]
    list_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "ListRecords"
    ]

    # Store in collection 1
    store_future1 = client_execution_thread_pool.submit(
        client.store, "test_collection", "rec1", {"mission_id": MISSION_ID, "name": "Coll 1", "value": 1}
    )
    _, store_request1, store_rpc1 = test_channel.take_unary_unary(store_method_desc)
    store_context1 = FakeContext()
    store_response1 = mock_servicer.StoreRecord(store_request1, store_context1)
    store_rpc1.terminate(store_response1, (), grpc.StatusCode.OK, "")
    store_future1.result(timeout=5.0)

    # Store in collection 2
    store_future2 = client_execution_thread_pool.submit(
        client.store, "outputs", "rec2", {"mission_id": MISSION_ID, "result": "Coll 2", "score": 0.5}
    )
    _, store_request2, store_rpc2 = test_channel.take_unary_unary(store_method_desc)
    store_context2 = FakeContext()
    store_response2 = mock_servicer.StoreRecord(store_request2, store_context2)
    store_rpc2.terminate(store_response2, (), grpc.StatusCode.OK, "")
    store_future2.result(timeout=5.0)

    # List collection 1
    list_future1 = client_execution_thread_pool.submit(client.list, "test_collection")
    _, list_request1, list_rpc1 = test_channel.take_unary_unary(list_method_desc)
    list_context1 = FakeContext()
    list_response1 = mock_servicer.ListRecords(list_request1, list_context1)
    list_rpc1.terminate(list_response1, (), grpc.StatusCode.OK, "")
    result1 = list_future1.result(timeout=5.0)

    # List collection 2
    list_future2 = client_execution_thread_pool.submit(client.list, "outputs")
    _, list_request2, list_rpc2 = test_channel.take_unary_unary(list_method_desc)
    list_context2 = FakeContext()
    list_response2 = mock_servicer.ListRecords(list_request2, list_context2)
    list_rpc2.terminate(list_response2, (), grpc.StatusCode.OK, "")
    result2 = list_future2.result(timeout=5.0)

    # Verify isolation
    assert len(result1) == 1
    assert len(result2) == 1
    assert result1[0].record_id == "rec1"
    assert result2[0].record_id == "rec2"


# ============================================================================
# _remove_collection() Tests
# ============================================================================


def test_remove_collection_success(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test successfully removing an entire collection.

    Verifies:
    - All records in collection are removed
    - Returns True on success
    """
    collection = "test_collection"

    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]
    remove_coll_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "RemoveCollection"
    ]
    list_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "ListRecords"
    ]

    # Store some records
    for i in range(3):
        store_future = client_execution_thread_pool.submit(
            client.store, collection, f"rec_{i}", {"mission_id": MISSION_ID, "name": f"Record {i}", "value": i}
        )
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.StoreRecord(store_request, store_context)
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
        store_future.result(timeout=5.0)

    # Remove collection
    remove_future = client_execution_thread_pool.submit(client.remove_collection, collection)
    _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_coll_method_desc)

    assert remove_request.mission_id == MISSION_ID
    assert remove_request.collection == collection

    remove_context = FakeContext()
    remove_response = mock_servicer.RemoveCollection(remove_request, remove_context)
    remove_rpc.terminate(remove_response, (), grpc.StatusCode.OK, "")

    result = remove_future.result(timeout=5.0)
    assert result is True

    # Verify collection is empty
    list_future = client_execution_thread_pool.submit(client.list, collection)
    _, list_request, list_rpc = test_channel.take_unary_unary(list_method_desc)
    list_context = FakeContext()
    list_response = mock_servicer.ListRecords(list_request, list_context)
    list_rpc.terminate(list_response, (), grpc.StatusCode.OK, "")
    list_result = list_future.result(timeout=5.0)

    assert len(list_result) == 0


def test_remove_collection_empty_idempotent(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test removing an empty collection is idempotent.

    Verifies:
    - Removing empty collection returns True
    - Operation is idempotent
    """
    collection = "empty_collection_remove"

    remove_coll_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "RemoveCollection"
    ]

    remove_future = client_execution_thread_pool.submit(client.remove_collection, collection)
    _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_coll_method_desc)

    remove_context = FakeContext()
    remove_response = mock_servicer.RemoveCollection(remove_request, remove_context)
    remove_rpc.terminate(remove_response, (), grpc.StatusCode.OK, "")

    result = remove_future.result(timeout=5.0)
    assert result is True


def test_remove_collection_does_not_affect_other_collections(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test removing a collection does not affect other collections.

    Verifies:
    - Other collections remain intact after removal
    - Only specified collection is removed
    """
    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]
    remove_coll_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "RemoveCollection"
    ]
    list_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "ListRecords"
    ]

    # Store in collection 1
    store_future1 = client_execution_thread_pool.submit(
        client.store, "test_collection", "rec1", {"mission_id": MISSION_ID, "name": "Coll 1", "value": 1}
    )
    _, store_request1, store_rpc1 = test_channel.take_unary_unary(store_method_desc)
    store_context1 = FakeContext()
    store_response1 = mock_servicer.StoreRecord(store_request1, store_context1)
    store_rpc1.terminate(store_response1, (), grpc.StatusCode.OK, "")
    store_future1.result(timeout=5.0)

    # Store in collection 2
    store_future2 = client_execution_thread_pool.submit(
        client.store, "outputs", "rec2", {"mission_id": MISSION_ID, "result": "Coll 2", "score": 0.5}
    )
    _, store_request2, store_rpc2 = test_channel.take_unary_unary(store_method_desc)
    store_context2 = FakeContext()
    store_response2 = mock_servicer.StoreRecord(store_request2, store_context2)
    store_rpc2.terminate(store_response2, (), grpc.StatusCode.OK, "")
    store_future2.result(timeout=5.0)

    # Remove collection 1
    remove_future = client_execution_thread_pool.submit(client.remove_collection, "test_collection")
    _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_coll_method_desc)
    remove_context = FakeContext()
    remove_response = mock_servicer.RemoveCollection(remove_request, remove_context)
    remove_rpc.terminate(remove_response, (), grpc.StatusCode.OK, "")
    remove_future.result(timeout=5.0)

    # Verify collection 1 is empty
    list_future1 = client_execution_thread_pool.submit(client.list, "test_collection")
    _, list_request1, list_rpc1 = test_channel.take_unary_unary(list_method_desc)
    list_context1 = FakeContext()
    list_response1 = mock_servicer.ListRecords(list_request1, list_context1)
    list_rpc1.terminate(list_response1, (), grpc.StatusCode.OK, "")
    result1 = list_future1.result(timeout=5.0)

    # Verify collection 2 is intact
    list_future2 = client_execution_thread_pool.submit(client.list, "outputs")
    _, list_request2, list_rpc2 = test_channel.take_unary_unary(list_method_desc)
    list_context2 = FakeContext()
    list_response2 = mock_servicer.ListRecords(list_request2, list_context2)
    list_rpc2.terminate(list_response2, (), grpc.StatusCode.OK, "")
    result2 = list_future2.result(timeout=5.0)

    assert len(result1) == 0
    assert len(result2) == 1
    assert result2[0].record_id == "rec2"


# ============================================================================
# Edge Cases
# ============================================================================


def test_store_record_with_special_characters(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test storing a record with special characters in data.

    Verifies:
    - Special characters are handled correctly
    - Record can be stored and retrieved
    """
    collection = "test_collection"
    record_id = "special_chars"
    data = {
        "mission_id": MISSION_ID,
        "name": "Test with special: @#$%^&*()",
        "value": 42,
        "description": "Unicode: 你好世界 🌍",
    }

    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]

    store_future = client_execution_thread_pool.submit(client.store, collection, record_id, data)
    _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
    store_context = FakeContext()
    store_response = mock_servicer.StoreRecord(store_request, store_context)
    store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")

    result = store_future.result(timeout=5.0)
    assert result is not None
    assert result.data.name == "Test with special: @#$%^&*()"
    assert result.data.description == "Unicode: 你好世界 🌍"


def test_store_record_with_large_data(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockStorageServicer,
) -> None:
    """Test storing a record with large data payload.

    Verifies:
    - Large data payloads are handled correctly
    - Record can be stored and retrieved
    """
    collection = "test_collection"
    record_id = "large_data"
    large_description = "A" * 10000  # 10KB string
    data = {"mission_id": MISSION_ID, "name": "Large Data Record", "value": 999, "description": large_description}

    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]

    store_future = client_execution_thread_pool.submit(client.store, collection, record_id, data)
    _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
    store_context = FakeContext()
    store_response = mock_servicer.StoreRecord(store_request, store_context)
    store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")

    result = store_future.result(timeout=5.0)
    assert result is not None
    assert len(result.data.description) == 10000


def test_mission_isolation(
    test_channel: grpc_testing.Channel,
    storage_config: dict[str, type[BaseModel]],
    mock_servicer: MockStorageServicer,
    dummy_client_config: ClientConfig,
) -> None:
    """Test that records from different missions are isolated.

    Verifies:
    - Records are isolated by mission_id
    - One mission cannot access another mission's records
    """
    # Create two clients with different mission IDs
    mission1_id = "missions:mission_1"
    mission2_id = "missions:mission_2"

    client1 = GrpcStorage(mission1_id, SETUP_ID, SETUP_VERSION_ID, storage_config, dummy_client_config)
    client1.stub = storage_service_pb2_grpc.StorageServiceStub(test_channel)

    client2 = GrpcStorage(mission2_id, SETUP_ID, SETUP_VERSION_ID, storage_config, dummy_client_config)
    client2.stub = storage_service_pb2_grpc.StorageServiceStub(test_channel)

    collection = "test_collection"
    record_id = "shared_record_id"

    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]
    read_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "ReadRecord"
    ]

    # Store with client1
    data1 = {"mission_id": mission1_id, "name": "Mission 1 Data", "value": 100}
    store_future1 = client_execution_thread_pool.submit(client1.store, collection, record_id, data1)
    _, store_request1, store_rpc1 = test_channel.take_unary_unary(store_method_desc)
    store_context1 = FakeContext()
    store_response1 = mock_servicer.StoreRecord(store_request1, store_context1)
    store_rpc1.terminate(store_response1, (), grpc.StatusCode.OK, "")
    result1 = store_future1.result(timeout=5.0)

    # Store with client2
    data2 = {"mission_id": mission2_id, "name": "Mission 2 Data", "value": 200}
    store_future2 = client_execution_thread_pool.submit(client2.store, collection, record_id, data2)
    _, store_request2, store_rpc2 = test_channel.take_unary_unary(store_method_desc)
    store_context2 = FakeContext()
    store_response2 = mock_servicer.StoreRecord(store_request2, store_context2)
    store_rpc2.terminate(store_response2, (), grpc.StatusCode.OK, "")
    result2 = store_future2.result(timeout=5.0)

    # Read with client1
    read_future1 = client_execution_thread_pool.submit(client1.read, collection, record_id)
    _, read_request1, read_rpc1 = test_channel.take_unary_unary(read_method_desc)
    read_context1 = FakeContext()
    read_response1 = mock_servicer.ReadRecord(read_request1, read_context1)
    read_rpc1.terminate(read_response1, (), grpc.StatusCode.OK, "")
    read_result1 = read_future1.result(timeout=5.0)

    # Read with client2
    read_future2 = client_execution_thread_pool.submit(client2.read, collection, record_id)
    _, read_request2, read_rpc2 = test_channel.take_unary_unary(read_method_desc)
    read_context2 = FakeContext()
    read_response2 = mock_servicer.ReadRecord(read_request2, read_context2)
    read_rpc2.terminate(read_response2, (), grpc.StatusCode.OK, "")
    read_result2 = read_future2.result(timeout=5.0)

    # Verify isolation
    assert result1.data.value == 100
    assert result2.data.value == 200
    assert read_result1.data.name == "Mission 1 Data"
    assert read_result2.data.name == "Mission 2 Data"


def test_store_with_no_schema_configured(
    client: GrpcStorage,
    test_channel: grpc_testing.Channel,
) -> None:
    """Test storing to a collection with no schema configured.

    Verifies:
    - Error is raised when no schema is registered
    - ValueError is raised with appropriate message
    """
    # Create mock servicer WITHOUT schema configuration
    mock_servicer_no_schema = MockStorageServicer(schema_config={})

    collection = "unconfigured_collection"
    record_id = "record_001"
    data = {"mission_id": MISSION_ID, "some_field": "some_value"}

    store_method_desc = storage_service_pb2_grpc.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
        "StoreRecord"
    ]

    store_future = client_execution_thread_pool.submit(client.store, collection, record_id, data)
    _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
    store_context = FakeContext()
    store_response = mock_servicer_no_schema.StoreRecord(store_request, store_context)
    store_rpc.terminate(store_response, (), store_context._code, store_context._details)

    # Verify error is raised
    with pytest.raises(StorageServiceError):
        store_future.result(timeout=5.0)
