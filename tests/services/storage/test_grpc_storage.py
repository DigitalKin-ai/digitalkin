"""Comprehensive tests for GrpcStorage service.

This test suite validates the GrpcStorage service implementation, including:
- Storing records with schema validation
- Reading records by collection and record_id
- Removing records
- Error handling and edge cases
"""

import asyncio
from concurrent import futures

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.storage.v1 import storage_service_pb2, storage_service_pb2_grpc
from pydantic import BaseModel, Field

from digitalkin.exception.storage import StorageServiceError
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.storage import DataType
from digitalkin.services.storage import GrpcStorage
from tests.fixtures.grpc_fixtures import AsyncStubWrapper, FakeContext
from tests.services.storage.mock_storage_servicer import MockStorageServicer

# Set timeout for all tests in this file (20 seconds)
pytestmark = pytest.mark.timeout(20)

# --- Test Constants ---
MISSION_ID = "missions:test_mission"
SETUP_ID = "setups:test_setup"
SETUP_VERSION_ID = "setup_versions:test_version"


# --- Test Models ---
class MockDataModel(BaseModel):
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
def thread_pool():
    """Create thread pool and ensure cleanup.

    Returns:
        ThreadPoolExecutor instance
    """
    pool = futures.ThreadPoolExecutor(max_workers=1)
    yield pool
    pool.shutdown(wait=True, cancel_futures=True)


@pytest.fixture
def storage_config() -> dict[str, type[BaseModel]]:
    """Provide test storage configuration with schema mappings.

    Returns:
        Dictionary mapping collection names to Pydantic models
    """
    return {
        "test_collection": MockDataModel,
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
        service_descriptors=[storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"]],
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
    from digitalkin.models.grpc_servers.models import SecurityMode, ServerMode

    return ClientConfig(
        host="localhost",
        port=50051,
        mode=ServerMode.ASYNC,
        security=SecurityMode.INSECURE,
        credentials=None,
    )


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
    client.stub = AsyncStubWrapper(storage_service_pb2_grpc.StorageServiceStub(test_channel))
    return client


# ============================================================================
# Test Classes
# ============================================================================


class TestCreateData:
    """Tests for the store() method.

    This test class validates the storage of records with different data types,
    schema validation, duplicate handling, and auto-generated IDs.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_create_record_success(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
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
        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["CreateRecord"]

        # Execute client call in thread pool
        future = thread_pool.submit(asyncio.run, client.create(collection, record_id, data))

        # Intercept the call
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        # Verify request
        assert request.mission_id == MISSION_ID
        assert request.collection == collection
        assert request.record_id == record_id
        # data_type is now a protobuf enum integer value
        assert DataType.from_proto(request.data_type) == DataType.OUTPUT

        # Mock servicer processes the request
        context = FakeContext()
        response = mock_servicer.CreateRecord(request, context)

        # Terminate the RPC
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        # Get result
        result = future.result(timeout=1.0)

        # Verify result
        assert result is not None
        assert result.mission_id == MISSION_ID
        assert result.collection == collection
        assert result.record_id == record_id
        assert result.data_type == DataType.OUTPUT
        assert result.data.name == "Test Record"
        assert result.data.value == 42
        assert result.created_at is not None
        assert result.updated_at is not None

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    async def test_create_record_invalid_schema(
        self,
        client: GrpcStorage,
    ) -> None:
        """Test storing a record with invalid schema raises error.

        Verifies:
        - Invalid data is rejected by schema validation
        - ValueError is raised client-side during validation
        """
        collection = "test_collection"
        record_id = "record_002"
        # Missing required 'value' field
        data = {"mission_id": MISSION_ID, "name": "Invalid Record"}

        # ValueError is raised client-side during validation, before any gRPC call
        with pytest.raises(ValueError, match="Validation failed"):
            await client.create(collection, record_id, data)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_create_record_duplicate(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test storing a duplicate record raises error.

        Verifies:
        - Attempting to store a record with existing record_id fails
        - StorageServiceError is raised
        """
        collection = "test_collection"
        record_id = "record_003"
        data = {"mission_id": MISSION_ID, "name": "First Record", "value": 10}

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["CreateRecord"]

        # Store first record
        future1 = thread_pool.submit(asyncio.run, client.create(collection, record_id, data))
        _, request1, rpc1 = test_channel.take_unary_unary(method_desc)
        context1 = FakeContext()
        response1 = mock_servicer.CreateRecord(request1, context1)
        rpc1.send_initial_metadata(())
        rpc1.terminate(response1, (), grpc.StatusCode.OK, "")
        result1 = future1.result(timeout=1.0)
        assert result1 is not None

        # Attempt to store duplicate
        future2 = thread_pool.submit(asyncio.run, client.create(collection, record_id, data))
        _, request2, rpc2 = test_channel.take_unary_unary(method_desc)
        context2 = FakeContext()
        response2 = mock_servicer.CreateRecord(request2, context2)
        rpc2.send_initial_metadata(())
        rpc2.terminate(response2, (), context2._code, context2._details)

        # Verify error is raised
        with pytest.raises(StorageServiceError):
            future2.result(timeout=1.0)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_create_record_with_output_type(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test storing a record with OUTPUT data type.

        Verifies:
        - OUTPUT data type is correctly set
        - Record is stored successfully
        """
        collection = "outputs"
        record_id = "output_001"
        data = {"mission_id": MISSION_ID, "result": "Success", "score": 0.95}

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["CreateRecord"]

        future = thread_pool.submit(asyncio.run, client.create(collection, record_id, data, data_type="OUTPUT"))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert DataType.from_proto(request.data_type) == DataType.OUTPUT

        context = FakeContext()
        response = mock_servicer.CreateRecord(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)
        assert result.data_type == DataType.OUTPUT

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_create_record_with_logs_type(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
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

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["CreateRecord"]

        future = thread_pool.submit(asyncio.run, client.create(collection, record_id, data, data_type="LOGS"))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert DataType.from_proto(request.data_type) == DataType.LOGS

        context = FakeContext()
        response = mock_servicer.CreateRecord(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)
        assert result.data_type == DataType.LOGS

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_create_record_with_view_type(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test storing a record with VIEW data type.

        Verifies:
        - VIEW data type is correctly set
        - Record is stored successfully
        """
        collection = "test_collection"
        record_id = "view_001"
        data = {"mission_id": MISSION_ID, "name": "View Data", "value": 100}

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["CreateRecord"]

        future = thread_pool.submit(asyncio.run, client.create(collection, record_id, data, data_type="VIEW"))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert DataType.from_proto(request.data_type) == DataType.VIEW

        context = FakeContext()
        response = mock_servicer.CreateRecord(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)
        assert result.data_type == DataType.VIEW

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_create_record_with_other_type(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test storing a record with OTHER data type.

        Verifies:
        - OTHER data type is correctly set
        - Record is stored successfully
        """
        collection = "test_collection"
        record_id = "other_001"
        data = {"mission_id": MISSION_ID, "name": "Other Data", "value": 50}

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["CreateRecord"]

        future = thread_pool.submit(asyncio.run, client.create(collection, record_id, data, data_type="OTHER"))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert DataType.from_proto(request.data_type) == DataType.OTHER

        context = FakeContext()
        response = mock_servicer.CreateRecord(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)
        assert result.data_type == DataType.OTHER

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_create_record_auto_generated_id(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test storing a record with auto-generated ID.

        Verifies:
        - record_id is auto-generated when None is provided
        - Record is stored successfully
        """
        collection = "test_collection"
        data = {"mission_id": MISSION_ID, "name": "Auto ID Record", "value": 999}

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["CreateRecord"]

        # Pass None for record_id to trigger auto-generation
        future = thread_pool.submit(asyncio.run, client.create(collection, None, data))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        # Verify record_id was generated (should be a UUID hex string)
        assert request.record_id is not None
        assert len(request.record_id) > 0

        context = FakeContext()
        response = mock_servicer.CreateRecord(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)
        assert result.record_id is not None


class TestGetData:
    """Tests for the retrieve/read() method.

    This test class validates reading records from storage, handling non-existent
    records, and reading from different collections.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_record_success(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully reading an existing record.

        Verifies:
        - Record can be read after storing
        - All data fields are preserved
        """
        collection = "test_collection"
        record_id = "record_read_001"
        data = {"mission_id": MISSION_ID, "name": "Read Test", "value": 123}

        store_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "CreateRecord"
        ]
        read_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "GetRecord"
        ]

        # Store the record first
        store_future = thread_pool.submit(asyncio.run, client.create(collection, record_id, data))
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.CreateRecord(store_request, store_context)
        store_rpc.send_initial_metadata(())
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
        store_future.result(timeout=1.0)

        # Read the record
        read_future = thread_pool.submit(asyncio.run, client.get(collection, record_id))
        _, read_request, read_rpc = test_channel.take_unary_unary(read_method_desc)

        assert read_request.mission_id == MISSION_ID
        assert read_request.collection == collection
        assert read_request.record_id == record_id

        read_context = FakeContext()
        read_response = mock_servicer.GetRecord(read_request, read_context)
        read_rpc.send_initial_metadata(())
        read_rpc.terminate(read_response, (), grpc.StatusCode.OK, "")

        result = read_future.result(timeout=1.0)
        assert result is not None
        assert result.record_id == record_id
        assert result.data.name == "Read Test"
        assert result.data.value == 123

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_get_record_not_found(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test reading a non-existent record returns None.

        Verifies:
        - Reading non-existent record returns None
        - No exception is raised
        """
        collection = "test_collection"
        record_id = "nonexistent_record"

        read_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "GetRecord"
        ]

        read_future = thread_pool.submit(asyncio.run, client.get(collection, record_id))
        _, read_request, read_rpc = test_channel.take_unary_unary(read_method_desc)

        read_context = FakeContext()
        read_response = mock_servicer.GetRecord(read_request, read_context)
        read_rpc.send_initial_metadata(())
        read_rpc.terminate(read_response, (), read_context._code, read_context._details)

        result = read_future.result(timeout=1.0)
        assert result is None

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_get_record_from_different_collections(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test reading records from different collections.

        Verifies:
        - Records with same record_id in different collections are independent
        - Each collection maintains separate records
        """
        record_id = "shared_id"
        data1 = {"mission_id": MISSION_ID, "name": "Collection 1", "value": 100}
        data2 = {"mission_id": MISSION_ID, "result": "Collection 2", "score": 0.8}

        store_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "CreateRecord"
        ]
        read_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "GetRecord"
        ]

        # Store in collection 1
        store_future1 = thread_pool.submit(asyncio.run, client.create("test_collection", record_id, data1))
        _, store_request1, store_rpc1 = test_channel.take_unary_unary(store_method_desc)
        store_context1 = FakeContext()
        store_response1 = mock_servicer.CreateRecord(store_request1, store_context1)
        store_rpc1.send_initial_metadata(())
        store_rpc1.terminate(store_response1, (), grpc.StatusCode.OK, "")
        store_future1.result(timeout=1.0)

        # Store in collection 2
        store_future2 = thread_pool.submit(asyncio.run, client.create("outputs", record_id, data2))
        _, store_request2, store_rpc2 = test_channel.take_unary_unary(store_method_desc)
        store_context2 = FakeContext()
        store_response2 = mock_servicer.CreateRecord(store_request2, store_context2)
        store_rpc2.send_initial_metadata(())
        store_rpc2.terminate(store_response2, (), grpc.StatusCode.OK, "")
        store_future2.result(timeout=1.0)

        # Read from collection 1
        read_future1 = thread_pool.submit(asyncio.run, client.get("test_collection", record_id))
        _, read_request1, read_rpc1 = test_channel.take_unary_unary(read_method_desc)
        read_context1 = FakeContext()
        read_response1 = mock_servicer.GetRecord(read_request1, read_context1)
        read_rpc1.send_initial_metadata(())
        read_rpc1.terminate(read_response1, (), grpc.StatusCode.OK, "")
        result1 = read_future1.result(timeout=1.0)

        # Read from collection 2
        read_future2 = thread_pool.submit(asyncio.run, client.get("outputs", record_id))
        _, read_request2, read_rpc2 = test_channel.take_unary_unary(read_method_desc)
        read_context2 = FakeContext()
        read_response2 = mock_servicer.GetRecord(read_request2, read_context2)
        read_rpc2.send_initial_metadata(())
        read_rpc2.terminate(read_response2, (), grpc.StatusCode.OK, "")
        result2 = read_future2.result(timeout=1.0)

        # Verify both records exist and are different
        assert result1 is not None
        assert result2 is not None
        assert result1.data.name == "Collection 1"
        assert result2.data.result == "Collection 2"


class TestUpdateData:
    """Tests for the update() method.

    This test class validates updating existing records, handling non-existent
    records, and schema validation during updates.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_update_record_success(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully updating an existing record.

        Verifies:
        - Record can be updated after storing
        - Updated data is reflected in the response
        - Update timestamp is updated
        """
        collection = "test_collection"
        record_id = "record_update_001"
        original_data = {"mission_id": MISSION_ID, "name": "Original", "value": 100}
        updated_data = {"mission_id": MISSION_ID, "name": "Updated", "value": 200}

        store_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "CreateRecord"
        ]
        update_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "UpdateRecord"
        ]

        # Store the record first
        store_future = thread_pool.submit(asyncio.run, client.create(collection, record_id, original_data))
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.CreateRecord(store_request, store_context)
        store_rpc.send_initial_metadata(())
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
        store_result = store_future.result(timeout=1.0)

        # Update the record
        update_future = thread_pool.submit(asyncio.run, client.update(collection, record_id, updated_data))
        _, update_request, update_rpc = test_channel.take_unary_unary(update_method_desc)

        assert update_request.mission_id == MISSION_ID
        assert update_request.collection == collection
        assert update_request.record_id == record_id

        update_context = FakeContext()
        update_response = mock_servicer.UpdateRecord(update_request, update_context)
        update_rpc.send_initial_metadata(())
        update_rpc.terminate(update_response, (), grpc.StatusCode.OK, "")

        result = update_future.result(timeout=1.0)
        assert result is not None
        assert result.record_id == record_id
        assert result.data.name == "Updated"
        assert result.data.value == 200
        # Update timestamp should be later than creation timestamp
        assert result.updated_at != store_result.updated_at

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_update_record_not_found(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test updating a non-existent record returns None.

        Verifies:
        - Updating non-existent record returns None
        - No exception is raised
        """
        collection = "test_collection"
        record_id = "nonexistent_update"
        data = {"mission_id": MISSION_ID, "name": "Should Fail", "value": 999}

        update_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "UpdateRecord"
        ]

        update_future = thread_pool.submit(asyncio.run, client.update(collection, record_id, data))
        _, update_request, update_rpc = test_channel.take_unary_unary(update_method_desc)

        update_context = FakeContext()
        update_response = mock_servicer.UpdateRecord(update_request, update_context)
        update_rpc.send_initial_metadata(())
        update_rpc.terminate(update_response, (), update_context._code, update_context._details)

        result = update_future.result(timeout=1.0)
        assert result is None

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    async def test_update_record_with_validation_error(
        self,
        client: GrpcStorage,
    ) -> None:
        """Test updating a record with invalid data raises error.

        Verifies:
        - Invalid data is rejected by schema validation
        - ValueError is raised client-side during validation
        """
        collection = "test_collection"
        record_id = "record_002"
        # Missing required 'value' field
        data = {"mission_id": MISSION_ID, "name": "Invalid Update"}

        # ValueError is raised client-side during validation, before any gRPC call
        with pytest.raises(ValueError, match="Validation failed"):
            await client.update(collection, record_id, data)


class TestDeleteData:
    """Tests for the delete/remove() method.

    This test class validates removing individual records, handling non-existent
    records, and idempotent delete operations.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_delete_record_success(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully removing an existing record.

        Verifies:
        - Record can be removed after storing
        - Remove returns True on success
        - Removed record cannot be read
        """
        collection = "test_collection"
        record_id = "record_remove_001"
        data = {"mission_id": MISSION_ID, "name": "To be removed", "value": 999}

        store_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "CreateRecord"
        ]
        remove_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "DeleteRecord"
        ]
        read_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "GetRecord"
        ]

        # Store the record first
        store_future = thread_pool.submit(asyncio.run, client.create(collection, record_id, data))
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.CreateRecord(store_request, store_context)
        store_rpc.send_initial_metadata(())
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
        store_future.result(timeout=1.0)

        # Remove the record
        remove_future = thread_pool.submit(asyncio.run, client.delete(collection, record_id))
        _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_method_desc)

        assert remove_request.mission_id == MISSION_ID
        assert remove_request.collection == collection
        assert remove_request.record_id == record_id

        remove_context = FakeContext()
        remove_response = mock_servicer.DeleteRecord(remove_request, remove_context)
        remove_rpc.send_initial_metadata(())
        remove_rpc.terminate(remove_response, (), grpc.StatusCode.OK, "")

        result = remove_future.result(timeout=1.0)
        assert result is True

        # Try to read the removed record
        read_future = thread_pool.submit(asyncio.run, client.get(collection, record_id))
        _, read_request, read_rpc = test_channel.take_unary_unary(read_method_desc)
        read_context = FakeContext()
        read_response = mock_servicer.GetRecord(read_request, read_context)
        read_rpc.send_initial_metadata(())
        read_rpc.terminate(read_response, (), grpc.StatusCode.NOT_FOUND, "Record not found")

        # Should return None for non-existent record
        read_result = read_future.result(timeout=1.0)
        assert read_result is None

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_delete_record_not_found(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test removing a non-existent record returns True (idempotent).

        Verifies:
        - Removing non-existent record is idempotent
        - Returns True even if record didn't exist
        """
        collection = "test_collection"
        record_id = "nonexistent_remove"

        remove_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "DeleteRecord"
        ]

        remove_future = thread_pool.submit(asyncio.run, client.delete(collection, record_id))
        _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_method_desc)

        remove_context = FakeContext()
        # Mock servicer should return success even if record doesn't exist (idempotent)
        remove_response = mock_servicer.DeleteRecord(remove_request, remove_context)
        remove_rpc.send_initial_metadata(())
        remove_rpc.terminate(remove_response, (), grpc.StatusCode.OK, "")

        result = remove_future.result(timeout=1.0)
        assert result is True

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_delete_record_twice(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test removing a record twice is idempotent.

        Verifies:
        - Record can be removed multiple times without error
        - Second removal still returns True
        """
        collection = "test_collection"
        record_id = "record_remove_twice"
        data = {"mission_id": MISSION_ID, "name": "Remove twice", "value": 888}

        store_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "CreateRecord"
        ]
        remove_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "DeleteRecord"
        ]

        # Store the record
        store_future = thread_pool.submit(asyncio.run, client.create(collection, record_id, data))
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.CreateRecord(store_request, store_context)
        store_rpc.send_initial_metadata(())
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
        store_future.result(timeout=1.0)

        # Remove the record first time
        remove_future1 = thread_pool.submit(asyncio.run, client.delete(collection, record_id))
        _, remove_request1, remove_rpc1 = test_channel.take_unary_unary(remove_method_desc)
        remove_context1 = FakeContext()
        remove_response1 = mock_servicer.DeleteRecord(remove_request1, remove_context1)
        remove_rpc1.send_initial_metadata(())
        remove_rpc1.terminate(remove_response1, (), grpc.StatusCode.OK, "")
        result1 = remove_future1.result(timeout=1.0)

        # Remove the record second time
        remove_future2 = thread_pool.submit(asyncio.run, client.delete(collection, record_id))
        _, remove_request2, remove_rpc2 = test_channel.take_unary_unary(remove_method_desc)
        remove_context2 = FakeContext()
        remove_response2 = mock_servicer.DeleteRecord(remove_request2, remove_context2)
        remove_rpc2.send_initial_metadata(())
        remove_rpc2.terminate(remove_response2, (), grpc.StatusCode.OK, "")
        result2 = remove_future2.result(timeout=1.0)

        assert result1 is True
        assert result2 is True

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_delete_collection_success(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully removing an entire collection.

        Verifies:
        - Collection can be removed after storing records
        - Remove returns True on success
        - All records in collection are deleted
        """
        collection = "test_collection"
        records_data = [
            {"mission_id": MISSION_ID, "name": "Record 1", "value": 100},
            {"mission_id": MISSION_ID, "name": "Record 2", "value": 200},
        ]

        store_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "CreateRecord"
        ]
        remove_coll_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "DeleteCollection"
        ]
        list_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "ListRecords"
        ]

        # Store multiple records
        for idx, data in enumerate(records_data):
            record_id = f"record_coll_{idx}"
            store_future = thread_pool.submit(asyncio.run, client.create(collection, record_id, data))
            _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
            store_context = FakeContext()
            store_response = mock_servicer.CreateRecord(store_request, store_context)
            store_rpc.send_initial_metadata(())
            store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
            store_future.result(timeout=1.0)

        # Remove the collection
        remove_future = thread_pool.submit(asyncio.run, client.delete_collection(collection))
        _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_coll_method_desc)

        assert remove_request.mission_id == MISSION_ID
        assert remove_request.collection == collection

        remove_context = FakeContext()
        remove_response = mock_servicer.DeleteCollection(remove_request, remove_context)
        remove_rpc.send_initial_metadata(())
        remove_rpc.terminate(remove_response, (), grpc.StatusCode.OK, "")

        result = remove_future.result(timeout=1.0)
        assert result is True

        # Verify collection is empty
        list_future = thread_pool.submit(asyncio.run, client.list(collection))
        _, list_request, list_rpc = test_channel.take_unary_unary(list_method_desc)
        list_context = FakeContext()
        list_response = mock_servicer.ListRecords(list_request, list_context)
        list_rpc.send_initial_metadata(())
        list_rpc.terminate(list_response, (), grpc.StatusCode.OK, "")

        list_results = list_future.result(timeout=1.0)
        assert list_results == []

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_delete_collection_not_found(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test removing a non-existent collection returns True (idempotent).

        Verifies:
        - Removing non-existent collection is idempotent
        - Returns True even if collection didn't exist
        """
        collection = "nonexistent_collection"

        remove_coll_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "DeleteCollection"
        ]

        remove_future = thread_pool.submit(asyncio.run, client.delete_collection(collection))
        _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_coll_method_desc)

        remove_context = FakeContext()
        remove_response = mock_servicer.DeleteCollection(remove_request, remove_context)
        remove_rpc.send_initial_metadata(())
        remove_rpc.terminate(remove_response, (), grpc.StatusCode.OK, "")

        result = remove_future.result(timeout=1.0)
        assert result is True

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_delete_collection_isolation(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test that removing one collection doesn't affect other collections.

        Verifies:
        - Collections are isolated
        - Removing one collection leaves others intact
        """
        data1 = {"mission_id": MISSION_ID, "name": "Collection 1", "value": 111}
        data2 = {"mission_id": MISSION_ID, "result": "Collection 2", "score": 0.9}

        store_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "CreateRecord"
        ]
        remove_coll_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "DeleteCollection"
        ]
        list_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "ListRecords"
        ]

        # Store in both collections
        store_future1 = thread_pool.submit(asyncio.run, client.create("test_collection", "rec1", data1))
        _, store_request1, store_rpc1 = test_channel.take_unary_unary(store_method_desc)
        store_context1 = FakeContext()
        store_response1 = mock_servicer.CreateRecord(store_request1, store_context1)
        store_rpc1.send_initial_metadata(())
        store_rpc1.terminate(store_response1, (), grpc.StatusCode.OK, "")
        store_future1.result(timeout=1.0)

        store_future2 = thread_pool.submit(asyncio.run, client.create("outputs", "rec2", data2))
        _, store_request2, store_rpc2 = test_channel.take_unary_unary(store_method_desc)
        store_context2 = FakeContext()
        store_response2 = mock_servicer.CreateRecord(store_request2, store_context2)
        store_rpc2.send_initial_metadata(())
        store_rpc2.terminate(store_response2, (), grpc.StatusCode.OK, "")
        store_future2.result(timeout=1.0)

        # Remove collection 1
        remove_future = thread_pool.submit(asyncio.run, client.delete_collection("test_collection"))
        _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_coll_method_desc)
        remove_context = FakeContext()
        remove_response = mock_servicer.DeleteCollection(remove_request, remove_context)
        remove_rpc.send_initial_metadata(())
        remove_rpc.terminate(remove_response, (), grpc.StatusCode.OK, "")
        remove_future.result(timeout=1.0)

        # Verify collection 2 still has records
        list_future = thread_pool.submit(asyncio.run, client.list("outputs"))
        _, list_request, list_rpc = test_channel.take_unary_unary(list_method_desc)
        list_context = FakeContext()
        list_response = mock_servicer.ListRecords(list_request, list_context)
        list_rpc.send_initial_metadata(())
        list_rpc.terminate(list_response, (), grpc.StatusCode.OK, "")

        results = list_future.result(timeout=1.0)
        assert len(results) == 1
        assert results[0].collection == "outputs"


class TestListData:
    """Tests for the list() method.

    This test class validates listing all records in a collection, handling empty
    collections, and filtering by collection.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_list_records_success(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully listing all records in a collection.

        Verifies:
        - All records in a collection can be listed
        - List returns correct number of records
        - All record data is preserved
        """
        collection = "test_collection"
        records_data = [
            {"mission_id": MISSION_ID, "name": "Record 1", "value": 100},
            {"mission_id": MISSION_ID, "name": "Record 2", "value": 200},
            {"mission_id": MISSION_ID, "name": "Record 3", "value": 300},
        ]

        store_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "CreateRecord"
        ]
        list_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "ListRecords"
        ]

        # Store multiple records
        for idx, data in enumerate(records_data):
            record_id = f"record_list_{idx}"
            store_future = thread_pool.submit(asyncio.run, client.create(collection, record_id, data))
            _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
            store_context = FakeContext()
            store_response = mock_servicer.CreateRecord(store_request, store_context)
            store_rpc.send_initial_metadata(())
            store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
            store_future.result(timeout=1.0)

        # List all records
        list_future = thread_pool.submit(asyncio.run, client.list(collection))
        _, list_request, list_rpc = test_channel.take_unary_unary(list_method_desc)

        assert list_request.mission_id == MISSION_ID
        assert list_request.collection == collection

        list_context = FakeContext()
        list_response = mock_servicer.ListRecords(list_request, list_context)
        list_rpc.send_initial_metadata(())
        list_rpc.terminate(list_response, (), grpc.StatusCode.OK, "")

        results = list_future.result(timeout=1.0)
        assert len(results) == 3
        assert all(r.collection == collection for r in results)
        values = sorted([r.data.value for r in results])
        assert values == [100, 200, 300]

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_list_records_empty_collection(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test listing records from an empty collection.

        Verifies:
        - Empty collection returns empty list
        - No exception is raised
        """
        collection = "empty_collection"

        list_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "ListRecords"
        ]

        list_future = thread_pool.submit(asyncio.run, client.list(collection))
        _, list_request, list_rpc = test_channel.take_unary_unary(list_method_desc)

        list_context = FakeContext()
        list_response = mock_servicer.ListRecords(list_request, list_context)
        list_rpc.send_initial_metadata(())
        list_rpc.terminate(list_response, (), grpc.StatusCode.OK, "")

        results = list_future.result(timeout=1.0)
        assert results == []

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_list_records_multiple_collections(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test that list only returns records from the specified collection.

        Verifies:
        - List is filtered by collection
        - Records from other collections are not included
        """
        data1 = {"mission_id": MISSION_ID, "name": "Collection 1", "value": 111}
        data2 = {"mission_id": MISSION_ID, "result": "Collection 2", "score": 0.75}

        store_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "CreateRecord"
        ]
        list_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "ListRecords"
        ]

        # Store in collection 1
        store_future1 = thread_pool.submit(asyncio.run, client.create("test_collection", "rec1", data1))
        _, store_request1, store_rpc1 = test_channel.take_unary_unary(store_method_desc)
        store_context1 = FakeContext()
        store_response1 = mock_servicer.CreateRecord(store_request1, store_context1)
        store_rpc1.send_initial_metadata(())
        store_rpc1.terminate(store_response1, (), grpc.StatusCode.OK, "")
        store_future1.result(timeout=1.0)

        # Store in collection 2
        store_future2 = thread_pool.submit(asyncio.run, client.create("outputs", "rec2", data2))
        _, store_request2, store_rpc2 = test_channel.take_unary_unary(store_method_desc)
        store_context2 = FakeContext()
        store_response2 = mock_servicer.CreateRecord(store_request2, store_context2)
        store_rpc2.send_initial_metadata(())
        store_rpc2.terminate(store_response2, (), grpc.StatusCode.OK, "")
        store_future2.result(timeout=1.0)

        # List from collection 1
        list_future = thread_pool.submit(asyncio.run, client.list("test_collection"))
        _, list_request, list_rpc = test_channel.take_unary_unary(list_method_desc)
        list_context = FakeContext()
        list_response = mock_servicer.ListRecords(list_request, list_context)
        list_rpc.send_initial_metadata(())
        list_rpc.terminate(list_response, (), grpc.StatusCode.OK, "")

        results = list_future.result(timeout=1.0)
        assert len(results) == 1
        assert results[0].collection == "test_collection"
        assert results[0].data.name == "Collection 1"


class TestStorageEdgeCases:
    """Tests for edge cases and error handling.

    This test class validates special character handling, large data payloads,
    mission isolation, and schema configuration validation.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_create_record_with_special_characters(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
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

        store_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "CreateRecord"
        ]

        store_future = thread_pool.submit(asyncio.run, client.create(collection, record_id, data))
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.CreateRecord(store_request, store_context)
        store_rpc.send_initial_metadata(())
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")

        result = store_future.result(timeout=1.0)
        assert result is not None
        assert result.data.name == "Test with special: @#$%^&*()"
        assert result.data.description == "Unicode: 你好世界 🌍"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_create_record_with_large_data(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
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

        store_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "CreateRecord"
        ]

        store_future = thread_pool.submit(asyncio.run, client.create(collection, record_id, data))
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.CreateRecord(store_request, store_context)
        store_rpc.send_initial_metadata(())
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")

        result = store_future.result(timeout=1.0)
        assert result is not None
        assert len(result.data.description) == 10000

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_mission_isolation(
        self,
        test_channel: grpc_testing.Channel,
        storage_config: dict[str, type[BaseModel]],
        mock_servicer: MockStorageServicer,
        dummy_client_config: ClientConfig,
        thread_pool: futures.ThreadPoolExecutor,
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
        client1.stub = AsyncStubWrapper(storage_service_pb2_grpc.StorageServiceStub(test_channel))

        client2 = GrpcStorage(mission2_id, SETUP_ID, SETUP_VERSION_ID, storage_config, dummy_client_config)
        client2.stub = AsyncStubWrapper(storage_service_pb2_grpc.StorageServiceStub(test_channel))

        collection = "test_collection"
        record_id = "shared_record_id"

        store_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "CreateRecord"
        ]
        read_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "GetRecord"
        ]

        # Store with client1
        data1 = {"mission_id": mission1_id, "name": "Mission 1 Data", "value": 100}
        store_future1 = thread_pool.submit(asyncio.run, client1.create(collection, record_id, data1))
        _, store_request1, store_rpc1 = test_channel.take_unary_unary(store_method_desc)
        store_context1 = FakeContext()
        store_response1 = mock_servicer.CreateRecord(store_request1, store_context1)
        store_rpc1.send_initial_metadata(())
        store_rpc1.terminate(store_response1, (), grpc.StatusCode.OK, "")
        result1 = store_future1.result(timeout=1.0)

        # Store with client2
        data2 = {"mission_id": mission2_id, "name": "Mission 2 Data", "value": 200}
        store_future2 = thread_pool.submit(asyncio.run, client2.create(collection, record_id, data2))
        _, store_request2, store_rpc2 = test_channel.take_unary_unary(store_method_desc)
        store_context2 = FakeContext()
        store_response2 = mock_servicer.CreateRecord(store_request2, store_context2)
        store_rpc2.send_initial_metadata(())
        store_rpc2.terminate(store_response2, (), grpc.StatusCode.OK, "")
        result2 = store_future2.result(timeout=1.0)

        # Read with client1
        read_future1 = thread_pool.submit(asyncio.run, client1.get(collection, record_id))
        _, read_request1, read_rpc1 = test_channel.take_unary_unary(read_method_desc)
        read_context1 = FakeContext()
        read_response1 = mock_servicer.GetRecord(read_request1, read_context1)
        read_rpc1.send_initial_metadata(())
        read_rpc1.terminate(read_response1, (), grpc.StatusCode.OK, "")
        read_result1 = read_future1.result(timeout=1.0)

        # Read with client2
        read_future2 = thread_pool.submit(asyncio.run, client2.get(collection, record_id))
        _, read_request2, read_rpc2 = test_channel.take_unary_unary(read_method_desc)
        read_context2 = FakeContext()
        read_response2 = mock_servicer.GetRecord(read_request2, read_context2)
        read_rpc2.send_initial_metadata(())
        read_rpc2.terminate(read_response2, (), grpc.StatusCode.OK, "")
        read_result2 = read_future2.result(timeout=1.0)

        # Verify isolation
        assert result1.data.value == 100
        assert result2.data.value == 200
        assert read_result1.data.name == "Mission 1 Data"
        assert read_result2.data.name == "Mission 2 Data"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    async def test_create_record_invalid_schema(
        self,
        client: GrpcStorage,
    ) -> None:
        """Test storing to a collection with no schema configured.

        Verifies:
        - Error is raised when no schema is registered
        - ValueError is raised with appropriate message
        """
        collection = "unconfigured_collection"
        record_id = "record_001"
        data = {"mission_id": MISSION_ID, "some_field": "some_value"}

        # ValueError is raised client-side during validation, before any gRPC call
        # So we don't need to intercept the gRPC channel
        with pytest.raises(ValueError, match="No schema registered for collection"):
            await client.create(collection, record_id, data)


# Note: TestSearchData is intentionally not included as the current implementation
# does not have search/query operations beyond basic list functionality.


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
