"""Comprehensive tests for GrpcStorage service.

This test suite validates the GrpcStorage service implementation, including:
- Storing records with schema validation
- Reading records by collection and record_id
- Removing records
- Error handling and edge cases
"""

import asyncio
import logging
from collections.abc import Iterator
from concurrent import futures
from unittest.mock import AsyncMock, Mock

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.storage.v1 import data_pb2, storage_service_pb2, storage_service_pb2_grpc
from pydantic import BaseModel, Field
from tests.fixtures.grpc_fixtures import AsyncStubWrapper, FakeContext
from tests.services.storage.mock_storage_servicer import MockStorageServicer

from digitalkin.grpc_servers.exceptions import CircuitOpenError, PermissionDeniedError, ServerError
from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
from digitalkin.models.grpc_servers.circuit_breaker import CBState
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.storage import ContextStorage, DataType, Visibility
from digitalkin.models.settings.grpc_client import get_circuit_breaker_settings, get_grpc_client_settings
from digitalkin.services.storage.exceptions import StorageServiceError
from digitalkin.services.storage.grpc_storage import GrpcStorage

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
    from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode

    return ClientConfig(
        host="localhost",
        port=50051,
        mode=ControlFlow.ASYNC,
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


class TestStoreData:
    """Tests for the store() method.

    This test class validates the storage of records with different data types,
    schema validation, duplicate handling, and auto-generated IDs.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_store_record_success(
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
        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

        # Execute client call in thread pool
        future = thread_pool.submit(asyncio.run, client.store(collection, record_id, data))

        # Intercept the call
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        # Verify request
        assert request.context == data_pb2.CONTEXT_MISSIONS
        assert request.collection == collection
        assert request.record_id == record_id
        assert request.data_type == data_pb2.OUTPUT

        # Mock servicer processes the request
        context = FakeContext()
        response = mock_servicer.StoreRecord(request, context)

        # Terminate the RPC
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        # Get result
        result = future.result(timeout=1.0)

        # Verify result
        assert result is not None
        assert result.context == MISSION_ID
        assert result.collection == collection
        assert result.record_id == record_id
        assert result.data_type == DataType.OUTPUT
        assert result.data.name == "Test Record"
        assert result.data.value == 42
        assert result.creation_date is not None
        assert result.update_date is not None

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    async def test_store_record_invalid_schema(
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
            await client.store(collection, record_id, data)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_store_record_duplicate(
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

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

        # Store first record
        future1 = thread_pool.submit(asyncio.run, client.store(collection, record_id, data))
        _, request1, rpc1 = test_channel.take_unary_unary(method_desc)
        context1 = FakeContext()
        response1 = mock_servicer.StoreRecord(request1, context1)
        rpc1.send_initial_metadata(())
        rpc1.terminate(response1, (), grpc.StatusCode.OK, "")
        result1 = future1.result(timeout=1.0)
        assert result1 is not None

        # Attempt to store duplicate
        future2 = thread_pool.submit(asyncio.run, client.store(collection, record_id, data))
        _, request2, rpc2 = test_channel.take_unary_unary(method_desc)
        context2 = FakeContext()
        response2 = mock_servicer.StoreRecord(request2, context2)
        rpc2.send_initial_metadata(())
        rpc2.terminate(response2, (), context2._code, context2._details)

        # Verify error is raised
        with pytest.raises(StorageServiceError):
            future2.result(timeout=1.0)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_store_record_with_output_type(
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

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

        future = thread_pool.submit(asyncio.run, client.store(collection, record_id, data, data_type=DataType.OUTPUT))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert request.data_type == data_pb2.OUTPUT

        context = FakeContext()
        response = mock_servicer.StoreRecord(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)
        assert result.data_type == DataType.OUTPUT

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_store_record_with_logs_type(
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

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

        future = thread_pool.submit(asyncio.run, client.store(collection, record_id, data, data_type=DataType.LOGS))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert request.data_type == data_pb2.LOGS

        context = FakeContext()
        response = mock_servicer.StoreRecord(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)
        assert result.data_type == DataType.LOGS

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_store_record_with_view_type(
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

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

        future = thread_pool.submit(asyncio.run, client.store(collection, record_id, data, data_type=DataType.VIEW))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert request.data_type == data_pb2.VIEW

        context = FakeContext()
        response = mock_servicer.StoreRecord(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)
        assert result.data_type == DataType.VIEW

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_store_record_with_other_type(
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

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

        future = thread_pool.submit(asyncio.run, client.store(collection, record_id, data, data_type=DataType.OTHER))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert request.data_type == data_pb2.OTHER

        context = FakeContext()
        response = mock_servicer.StoreRecord(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)
        assert result.data_type == DataType.OTHER

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_store_record_auto_generated_id(
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

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["StoreRecord"]

        # Pass None for record_id to trigger auto-generation
        future = thread_pool.submit(asyncio.run, client.store(collection, None, data))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        # Verify record_id was generated (should be a UUID hex string)
        assert request.record_id is not None
        assert len(request.record_id) > 0

        context = FakeContext()
        response = mock_servicer.StoreRecord(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)
        assert result.record_id is not None


class TestRetrieveData:
    """Tests for the retrieve/read() method.

    This test class validates reading records from storage, handling non-existent
    records, and reading from different collections.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_read_record_success(
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
            "StoreRecord"
        ]
        read_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "ReadRecord"
        ]

        # Store the record first
        store_future = thread_pool.submit(asyncio.run, client.store(collection, record_id, data))
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.StoreRecord(store_request, store_context)
        store_rpc.send_initial_metadata(())
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
        store_future.result(timeout=1.0)

        # Read the record
        read_future = thread_pool.submit(asyncio.run, client.read(collection, record_id))
        _, read_request, read_rpc = test_channel.take_unary_unary(read_method_desc)

        assert read_request.context == data_pb2.CONTEXT_MISSIONS
        assert read_request.collection == collection
        assert read_request.record_id == record_id

        read_context = FakeContext()
        read_response = mock_servicer.ReadRecord(read_request, read_context)
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
    def test_read_record_not_found(
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
            "ReadRecord"
        ]

        read_future = thread_pool.submit(asyncio.run, client.read(collection, record_id))
        _, read_request, read_rpc = test_channel.take_unary_unary(read_method_desc)

        read_context = FakeContext()
        read_response = mock_servicer.ReadRecord(read_request, read_context)
        read_rpc.send_initial_metadata(())
        read_rpc.terminate(read_response, (), read_context._code, read_context._details)

        result = read_future.result(timeout=1.0)
        assert result is None

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_read_record_from_different_collections(
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
            "StoreRecord"
        ]
        read_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "ReadRecord"
        ]

        # Store in collection 1
        store_future1 = thread_pool.submit(asyncio.run, client.store("test_collection", record_id, data1))
        _, store_request1, store_rpc1 = test_channel.take_unary_unary(store_method_desc)
        store_context1 = FakeContext()
        store_response1 = mock_servicer.StoreRecord(store_request1, store_context1)
        store_rpc1.send_initial_metadata(())
        store_rpc1.terminate(store_response1, (), grpc.StatusCode.OK, "")
        store_future1.result(timeout=1.0)

        # Store in collection 2
        store_future2 = thread_pool.submit(asyncio.run, client.store("outputs", record_id, data2))
        _, store_request2, store_rpc2 = test_channel.take_unary_unary(store_method_desc)
        store_context2 = FakeContext()
        store_response2 = mock_servicer.StoreRecord(store_request2, store_context2)
        store_rpc2.send_initial_metadata(())
        store_rpc2.terminate(store_response2, (), grpc.StatusCode.OK, "")
        store_future2.result(timeout=1.0)

        # Read from collection 1
        read_future1 = thread_pool.submit(asyncio.run, client.read("test_collection", record_id))
        _, read_request1, read_rpc1 = test_channel.take_unary_unary(read_method_desc)
        read_context1 = FakeContext()
        read_response1 = mock_servicer.ReadRecord(read_request1, read_context1)
        read_rpc1.send_initial_metadata(())
        read_rpc1.terminate(read_response1, (), grpc.StatusCode.OK, "")
        result1 = read_future1.result(timeout=1.0)

        # Read from collection 2
        read_future2 = thread_pool.submit(asyncio.run, client.read("outputs", record_id))
        _, read_request2, read_rpc2 = test_channel.take_unary_unary(read_method_desc)
        read_context2 = FakeContext()
        read_response2 = mock_servicer.ReadRecord(read_request2, read_context2)
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
            "StoreRecord"
        ]
        update_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "UpdateRecord"
        ]

        # Store the record first
        store_future = thread_pool.submit(asyncio.run, client.store(collection, record_id, original_data))
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.StoreRecord(store_request, store_context)
        store_rpc.send_initial_metadata(())
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
        store_result = store_future.result(timeout=1.0)

        # Update the record
        update_future = thread_pool.submit(asyncio.run, client.update(collection, record_id, updated_data))
        _, update_request, update_rpc = test_channel.take_unary_unary(update_method_desc)

        assert update_request.context == data_pb2.CONTEXT_MISSIONS
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
        assert result.update_date != store_result.update_date

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
    def test_remove_record_success(
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
            "StoreRecord"
        ]
        remove_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "RemoveRecord"
        ]
        read_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "ReadRecord"
        ]

        # Store the record first
        store_future = thread_pool.submit(asyncio.run, client.store(collection, record_id, data))
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.StoreRecord(store_request, store_context)
        store_rpc.send_initial_metadata(())
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
        store_future.result(timeout=1.0)

        # Remove the record
        remove_future = thread_pool.submit(asyncio.run, client.remove(collection, record_id))
        _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_method_desc)

        assert remove_request.context == data_pb2.CONTEXT_MISSIONS
        assert remove_request.collection == collection
        assert remove_request.record_id == record_id

        remove_context = FakeContext()
        remove_response = mock_servicer.RemoveRecord(remove_request, remove_context)
        remove_rpc.send_initial_metadata(())
        remove_rpc.terminate(remove_response, (), grpc.StatusCode.OK, "")

        result = remove_future.result(timeout=1.0)
        assert result is True

        # Try to read the removed record
        read_future = thread_pool.submit(asyncio.run, client.read(collection, record_id))
        _, read_request, read_rpc = test_channel.take_unary_unary(read_method_desc)
        read_context = FakeContext()
        read_response = mock_servicer.ReadRecord(read_request, read_context)
        read_rpc.send_initial_metadata(())
        read_rpc.terminate(read_response, (), grpc.StatusCode.NOT_FOUND, "Record not found")

        # Should return None for non-existent record
        read_result = read_future.result(timeout=1.0)
        assert read_result is None

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_remove_record_not_found(
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
            "RemoveRecord"
        ]

        remove_future = thread_pool.submit(asyncio.run, client.remove(collection, record_id))
        _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_method_desc)

        remove_context = FakeContext()
        # Mock servicer should return success even if record doesn't exist (idempotent)
        remove_response = mock_servicer.RemoveRecord(remove_request, remove_context)
        remove_rpc.send_initial_metadata(())
        remove_rpc.terminate(remove_response, (), grpc.StatusCode.OK, "")

        result = remove_future.result(timeout=1.0)
        assert result is True

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_remove_record_twice(
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
            "StoreRecord"
        ]
        remove_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "RemoveRecord"
        ]

        # Store the record
        store_future = thread_pool.submit(asyncio.run, client.store(collection, record_id, data))
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.StoreRecord(store_request, store_context)
        store_rpc.send_initial_metadata(())
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
        store_future.result(timeout=1.0)

        # Remove the record first time
        remove_future1 = thread_pool.submit(asyncio.run, client.remove(collection, record_id))
        _, remove_request1, remove_rpc1 = test_channel.take_unary_unary(remove_method_desc)
        remove_context1 = FakeContext()
        remove_response1 = mock_servicer.RemoveRecord(remove_request1, remove_context1)
        remove_rpc1.send_initial_metadata(())
        remove_rpc1.terminate(remove_response1, (), grpc.StatusCode.OK, "")
        result1 = remove_future1.result(timeout=1.0)

        # Remove the record second time
        remove_future2 = thread_pool.submit(asyncio.run, client.remove(collection, record_id))
        _, remove_request2, remove_rpc2 = test_channel.take_unary_unary(remove_method_desc)
        remove_context2 = FakeContext()
        remove_response2 = mock_servicer.RemoveRecord(remove_request2, remove_context2)
        remove_rpc2.send_initial_metadata(())
        remove_rpc2.terminate(remove_response2, (), grpc.StatusCode.OK, "")
        result2 = remove_future2.result(timeout=1.0)

        assert result1 is True
        assert result2 is True

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_remove_collection_success(
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
            "StoreRecord"
        ]
        remove_coll_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "RemoveCollection"
        ]
        list_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "ListRecords"
        ]

        # Store multiple records
        for idx, data in enumerate(records_data):
            record_id = f"record_coll_{idx}"
            store_future = thread_pool.submit(asyncio.run, client.store(collection, record_id, data))
            _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
            store_context = FakeContext()
            store_response = mock_servicer.StoreRecord(store_request, store_context)
            store_rpc.send_initial_metadata(())
            store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
            store_future.result(timeout=1.0)

        # Remove the collection
        remove_future = thread_pool.submit(asyncio.run, client.remove_collection(collection))
        _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_coll_method_desc)

        assert remove_request.context == data_pb2.CONTEXT_MISSIONS
        assert remove_request.collection == collection

        remove_context = FakeContext()
        remove_response = mock_servicer.RemoveCollection(remove_request, remove_context)
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
    def test_remove_collection_not_found(
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
            "RemoveCollection"
        ]

        remove_future = thread_pool.submit(asyncio.run, client.remove_collection(collection))
        _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_coll_method_desc)

        remove_context = FakeContext()
        remove_response = mock_servicer.RemoveCollection(remove_request, remove_context)
        remove_rpc.send_initial_metadata(())
        remove_rpc.terminate(remove_response, (), grpc.StatusCode.OK, "")

        result = remove_future.result(timeout=1.0)
        assert result is True

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_remove_collection_isolation(
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
            "StoreRecord"
        ]
        remove_coll_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "RemoveCollection"
        ]
        list_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "ListRecords"
        ]

        # Store in both collections
        store_future1 = thread_pool.submit(asyncio.run, client.store("test_collection", "rec1", data1))
        _, store_request1, store_rpc1 = test_channel.take_unary_unary(store_method_desc)
        store_context1 = FakeContext()
        store_response1 = mock_servicer.StoreRecord(store_request1, store_context1)
        store_rpc1.send_initial_metadata(())
        store_rpc1.terminate(store_response1, (), grpc.StatusCode.OK, "")
        store_future1.result(timeout=1.0)

        store_future2 = thread_pool.submit(asyncio.run, client.store("outputs", "rec2", data2))
        _, store_request2, store_rpc2 = test_channel.take_unary_unary(store_method_desc)
        store_context2 = FakeContext()
        store_response2 = mock_servicer.StoreRecord(store_request2, store_context2)
        store_rpc2.send_initial_metadata(())
        store_rpc2.terminate(store_response2, (), grpc.StatusCode.OK, "")
        store_future2.result(timeout=1.0)

        # Remove collection 1
        remove_future = thread_pool.submit(asyncio.run, client.remove_collection("test_collection"))
        _, remove_request, remove_rpc = test_channel.take_unary_unary(remove_coll_method_desc)
        remove_context = FakeContext()
        remove_response = mock_servicer.RemoveCollection(remove_request, remove_context)
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
            "StoreRecord"
        ]
        list_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "ListRecords"
        ]

        # Store multiple records
        for idx, data in enumerate(records_data):
            record_id = f"record_list_{idx}"
            store_future = thread_pool.submit(asyncio.run, client.store(collection, record_id, data))
            _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
            store_context = FakeContext()
            store_response = mock_servicer.StoreRecord(store_request, store_context)
            store_rpc.send_initial_metadata(())
            store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")
            store_future.result(timeout=1.0)

        # List all records
        list_future = thread_pool.submit(asyncio.run, client.list(collection))
        _, list_request, list_rpc = test_channel.take_unary_unary(list_method_desc)

        assert list_request.context == data_pb2.CONTEXT_MISSIONS
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
    @pytest.mark.smoke
    def test_list_cross_owner_context_and_visibilities(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockStorageServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """List under USERS/ORGANIZATIONS maps to the cross-owner wire enum.

        Verifies:
        - context=USERS -> CONTEXT_USERS, context=ORGANIZATIONS -> CONTEXT_ORGANIZATIONS
        - the visibilities filter is forwarded on the wire
        """
        collection = "test_collection"
        list_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "ListRecords"
        ]

        for scope_context, wire in (
            (ContextStorage.USERS, data_pb2.CONTEXT_USERS),
            (ContextStorage.ORGANIZATIONS, data_pb2.CONTEXT_ORGANIZATIONS),
            (ContextStorage.UNSPECIFIED, data_pb2.CONTEXT_UNSPECIFIED),
        ):
            list_future = thread_pool.submit(
                asyncio.run,
                client.list(collection, context=scope_context, visibilities=[Visibility.PUBLIC, Visibility.INTERNAL]),
            )
            _, list_request, list_rpc = test_channel.take_unary_unary(list_method_desc)

            assert list_request.context == wire
            assert list_request.collection == collection
            assert list(list_request.visibilities) == [data_pb2.VISIBILITY_PUBLIC, data_pb2.VISIBILITY_INTERNAL]

            list_context = FakeContext()
            list_response = mock_servicer.ListRecords(list_request, list_context)
            list_rpc.send_initial_metadata(())
            list_rpc.terminate(list_response, (), grpc.StatusCode.OK, "")
            assert isinstance(list_future.result(timeout=1.0), list)

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
            "StoreRecord"
        ]
        list_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "ListRecords"
        ]

        # Store in collection 1
        store_future1 = thread_pool.submit(asyncio.run, client.store("test_collection", "rec1", data1))
        _, store_request1, store_rpc1 = test_channel.take_unary_unary(store_method_desc)
        store_context1 = FakeContext()
        store_response1 = mock_servicer.StoreRecord(store_request1, store_context1)
        store_rpc1.send_initial_metadata(())
        store_rpc1.terminate(store_response1, (), grpc.StatusCode.OK, "")
        store_future1.result(timeout=1.0)

        # Store in collection 2
        store_future2 = thread_pool.submit(asyncio.run, client.store("outputs", "rec2", data2))
        _, store_request2, store_rpc2 = test_channel.take_unary_unary(store_method_desc)
        store_context2 = FakeContext()
        store_response2 = mock_servicer.StoreRecord(store_request2, store_context2)
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

    @pytest.mark.grpc
    @pytest.mark.edge_case
    async def test_list_skips_invalid_records(self, client: GrpcStorage) -> None:
        """Test that a record failing schema validation is skipped, not the whole list.

        Verifies:
        - Records written by other modules with a foreign shape do not empty the list
        - Valid records in the same collection are still returned
        """
        from unittest.mock import AsyncMock

        from google.protobuf.struct_pb2 import Struct

        def _record(record_id: str, data: dict) -> data_pb2.StorageRecord:
            struct = Struct()
            struct.update(data)
            return data_pb2.StorageRecord(
                context=MISSION_ID,
                collection="test_collection",
                record_id=record_id,
                data=struct,
                data_type=data_pb2.DataType.Value("OUTPUT"),
            )

        valid = _record("valid", {"mission_id": MISSION_ID, "name": "ok", "value": 1})
        invalid = _record("foreign", {"unexpected": "shape"})
        client.exec_grpc_query = AsyncMock(  # type: ignore[method-assign]
            return_value=data_pb2.ListRecordsResponse(records=[invalid, valid])
        )

        results = await client._list("test_collection", MISSION_ID)
        assert [r.record_id for r in results] == ["valid"]


class TestStorageEdgeCases:
    """Tests for edge cases and error handling.

    This test class validates special character handling, large data payloads,
    mission isolation, and schema configuration validation.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_store_record_with_special_characters(
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
            "StoreRecord"
        ]

        store_future = thread_pool.submit(asyncio.run, client.store(collection, record_id, data))
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.StoreRecord(store_request, store_context)
        store_rpc.send_initial_metadata(())
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")

        result = store_future.result(timeout=1.0)
        assert result is not None
        assert result.data.name == "Test with special: @#$%^&*()"
        assert result.data.description == "Unicode: 你好世界 🌍"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_store_record_with_large_data(
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
            "StoreRecord"
        ]

        store_future = thread_pool.submit(asyncio.run, client.store(collection, record_id, data))
        _, store_request, store_rpc = test_channel.take_unary_unary(store_method_desc)
        store_context = FakeContext()
        store_response = mock_servicer.StoreRecord(store_request, store_context)
        store_rpc.send_initial_metadata(())
        store_rpc.terminate(store_response, (), grpc.StatusCode.OK, "")

        result = store_future.result(timeout=1.0)
        assert result is not None
        assert len(result.data.description) == 10000

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_mission_context_kind_only(
        self,
        test_channel: grpc_testing.Channel,
        storage_config: dict[str, type[BaseModel]],
        mock_servicer: MockStorageServicer,
        dummy_client_config: ClientConfig,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Requests carry only the context KIND — never the concrete mission id.

        Since dev4 the concrete id travels via x-mission-id task metadata and
        isolation is enforced server-side; two clients with different mission ids
        must emit byte-identical context fields.
        """
        mission1_id = "missions:mission_1"
        mission2_id = "missions:mission_2"

        client1 = GrpcStorage(mission1_id, SETUP_ID, SETUP_VERSION_ID, storage_config, dummy_client_config)
        client1.stub = AsyncStubWrapper(storage_service_pb2_grpc.StorageServiceStub(test_channel))

        client2 = GrpcStorage(mission2_id, SETUP_ID, SETUP_VERSION_ID, storage_config, dummy_client_config)
        client2.stub = AsyncStubWrapper(storage_service_pb2_grpc.StorageServiceStub(test_channel))

        collection = "test_collection"

        store_method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name[
            "StoreRecord"
        ]

        data1 = {"mission_id": mission1_id, "name": "Mission 1 Data", "value": 100}
        store_future1 = thread_pool.submit(asyncio.run, client1.store(collection, "record_1", data1))
        _, store_request1, store_rpc1 = test_channel.take_unary_unary(store_method_desc)
        store_response1 = mock_servicer.StoreRecord(store_request1, FakeContext())
        store_rpc1.send_initial_metadata(())
        store_rpc1.terminate(store_response1, (), grpc.StatusCode.OK, "")
        result1 = store_future1.result(timeout=1.0)

        data2 = {"mission_id": mission2_id, "name": "Mission 2 Data", "value": 200}
        store_future2 = thread_pool.submit(asyncio.run, client2.store(collection, "record_2", data2))
        _, store_request2, store_rpc2 = test_channel.take_unary_unary(store_method_desc)
        store_response2 = mock_servicer.StoreRecord(store_request2, FakeContext())
        store_rpc2.send_initial_metadata(())
        store_rpc2.terminate(store_response2, (), grpc.StatusCode.OK, "")
        result2 = store_future2.result(timeout=1.0)

        assert store_request1.context == data_pb2.CONTEXT_MISSIONS
        assert store_request2.context == data_pb2.CONTEXT_MISSIONS
        assert result1.data.value == 100
        assert result2.data.value == 200

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    async def test_store_with_no_schema_configured(
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
            await client.store(collection, record_id, data)


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


class TestCircuitBreakerInteraction:
    """GrpcStorage behavior around the per-service circuit breaker.

    Regression: a burst of new-session reads (each NOT_FOUND) opened the
    StorageService breaker in production; every read/store then fast-failed
    for ~30s and flooded logs (Railway dropped 2373 lines). Fix: application
    codes (NOT_FOUND) must not trip the breaker, and expected circuit-open
    rejections must log quietly.
    """

    @pytest.fixture(autouse=True)
    def _clear_breaker(self) -> Iterator[None]:
        """Isolate the StorageService breaker singleton between tests.

        Yields:
            Control to the test with a cleared breaker registry.
        """
        CircuitBreaker._instances.clear()
        yield
        CircuitBreaker._instances.clear()

    @staticmethod
    def _open_storage_breaker(monkeypatch: pytest.MonkeyPatch) -> None:
        """Force the StorageService breaker OPEN (fail_max=1, one failure)."""
        monkeypatch.setenv("DIGITALKIN_CB_FAIL_MAX", "1")
        get_circuit_breaker_settings.cache_clear()
        cb = CircuitBreaker.get_or_create("StorageService")
        cb.record_failure()
        assert cb.state == CBState.OPEN

    @pytest.mark.grpc
    @pytest.mark.unit
    async def test_store_logs_quietly_when_circuit_open(
        self, client: GrpcStorage, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Open-circuit StoreRecord raises but logs at DEBUG (no stack trace)."""
        self._open_storage_breaker(monkeypatch)
        data = {"mission_id": MISSION_ID, "name": "x", "value": 1}

        monkeypatch.setattr(logging.getLogger("digitalkin"), "propagate", True)
        with (
            caplog.at_level(logging.DEBUG, logger="digitalkin"),
            pytest.raises(StorageServiceError) as exc_info,
        ):
            await client.store("test_collection", "rec_open", data)

        # Cause chain preserved down to CircuitOpenError.
        assert isinstance(exc_info.value.__cause__, ServerError)
        assert isinstance(exc_info.value.__cause__.__cause__, CircuitOpenError)
        # Quiet: a DEBUG "circuit open" line, and no ERROR/exception record.
        assert any(r.levelno == logging.DEBUG and "circuit open" in r.getMessage() for r in caplog.records)
        assert [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR] == []

    @pytest.mark.grpc
    @pytest.mark.unit
    async def test_read_logs_quietly_when_circuit_open(
        self, client: GrpcStorage, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Open-circuit ReadRecord returns None and logs at DEBUG only."""
        self._open_storage_breaker(monkeypatch)

        monkeypatch.setattr(logging.getLogger("digitalkin"), "propagate", True)
        with caplog.at_level(logging.DEBUG, logger="digitalkin"):
            result = await client.read("test_collection", "rec_missing")

        assert result is None
        assert any(r.levelno == logging.DEBUG and "circuit open" in r.getMessage() for r in caplog.records)
        assert [r.getMessage() for r in caplog.records if r.levelno >= logging.INFO] == []

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_not_found_keeps_breaker_closed(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        thread_pool: futures.ThreadPoolExecutor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Real NOT_FOUND from the storage server must not open the breaker.

        With fail_max=1 a single tick would open it under the old code; the
        service responded, so it must stay CLOSED.
        """
        monkeypatch.setenv("DIGITALKIN_CB_FAIL_MAX", "1")
        monkeypatch.setenv("DIGITALKIN_GRPC_QUERY_MAX_RETRIES", "0")
        get_circuit_breaker_settings.cache_clear()
        get_grpc_client_settings.cache_clear()

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["ReadRecord"]
        future = thread_pool.submit(asyncio.run, client.read("test_collection", "missing"))
        _meta, _req, rpc = test_channel.take_unary_unary(method_desc)
        rpc.send_initial_metadata(())
        rpc.terminate(data_pb2.ReadRecordResponse(), (), grpc.StatusCode.NOT_FOUND, "not found")
        result = future.result(timeout=2.0)

        assert result is None
        assert CircuitBreaker.get_or_create("StorageService").state == CBState.CLOSED

    @pytest.mark.grpc
    @pytest.mark.edge_case
    @pytest.mark.chaos
    async def test_permission_denied_propagates_and_keeps_breaker_closed(self, client: GrpcStorage) -> None:
        """A permission error from the channel middleware is re-raised (not swallowed to None); breaker untouched."""
        CircuitBreaker.remove("StorageService")
        client.stub = Mock()
        client.stub.ReadRecord = AsyncMock(side_effect=PermissionDeniedError("[/StorageService/ReadRecord] denied"))

        with pytest.raises(PermissionDeniedError):
            await client.read("test_collection", "denied")
        assert CircuitBreaker.get_or_create("StorageService").state == CBState.CLOSED

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.chaos
    def test_unavailable_opens_breaker(
        self,
        client: GrpcStorage,
        test_channel: grpc_testing.Channel,
        thread_pool: futures.ThreadPoolExecutor,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Real UNAVAILABLE from the storage server still opens the breaker."""
        monkeypatch.setenv("DIGITALKIN_CB_FAIL_MAX", "1")
        monkeypatch.setenv("DIGITALKIN_GRPC_QUERY_MAX_RETRIES", "0")
        get_circuit_breaker_settings.cache_clear()
        get_grpc_client_settings.cache_clear()

        method_desc = storage_service_pb2.DESCRIPTOR.services_by_name["StorageService"].methods_by_name["ReadRecord"]
        future = thread_pool.submit(asyncio.run, client.read("test_collection", "any"))
        _meta, _req, rpc = test_channel.take_unary_unary(method_desc)
        rpc.send_initial_metadata(())
        rpc.terminate(data_pb2.ReadRecordResponse(), (), grpc.StatusCode.UNAVAILABLE, "down")
        result = future.result(timeout=2.0)

        assert result is None
        assert CircuitBreaker.get_or_create("StorageService").state == CBState.OPEN
