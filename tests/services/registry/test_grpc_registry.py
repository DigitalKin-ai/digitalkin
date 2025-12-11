"""Comprehensive tests for GrpcRegistry service.

This test suite validates the GrpcRegistry service implementation, including:
- Discovering modules by ID
- Searching modules by criteria
- Registering modules
- Getting module status
- Sending heartbeats
"""

from concurrent import futures

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.registry.v1 import (
    registry_enums_pb2,
    registry_service_pb2,
    registry_service_pb2_grpc,
)
from tests.fixtures.grpc_fixtures import FakeContext
from tests.services.registry.mock_registry_servicer import MockRegistryServicer

from digitalkin.models.grpc_servers.models import ClientConfig, SecurityMode, ServerMode
from digitalkin.models.services.registry import RegistryModuleStatus, RegistryModuleType
from digitalkin.services.registry.exceptions import (
    RegistryServiceError,
)
from digitalkin.services.registry.grpc_registry import GrpcRegistry

# Set timeout for all tests in this file (20 seconds)
pytestmark = pytest.mark.timeout(20)

# --- Test Constants ---
MISSION_ID = "missions:test_mission"
SETUP_ID = "setups:test_setup"
SETUP_VERSION_ID = "setup_versions:test_version"


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
def test_channel() -> grpc_testing.Channel:
    """Create a test gRPC channel.

    Returns:
        A testing channel for intercepting gRPC calls
    """
    return grpc_testing.channel(
        service_descriptors=[registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"]],
        time=grpc_testing.strict_real_time(),
    )


@pytest.fixture
def mock_servicer() -> MockRegistryServicer:
    """Create a mock registry servicer.

    Returns:
        Mock servicer instance
    """
    return MockRegistryServicer()


@pytest.fixture
def dummy_client_config() -> ClientConfig:
    """Create a dummy ClientConfig for testing.

    Returns:
        ClientConfig instance with test values
    """
    return ClientConfig(
        host="localhost",
        port=50052,
        mode=ServerMode.ASYNC,
        security=SecurityMode.INSECURE,
        credentials=None,
    )


@pytest.fixture
def client(
    test_channel: grpc_testing.Channel,
    dummy_client_config: ClientConfig,
) -> GrpcRegistry:
    """Create a GrpcRegistry client with test channel.

    Args:
        test_channel: Test gRPC channel
        dummy_client_config: Dummy client configuration

    Returns:
        GrpcRegistry client configured for testing
    """
    registry_client = GrpcRegistry(MISSION_ID, SETUP_ID, SETUP_VERSION_ID, dummy_client_config)
    registry_client.stub = registry_service_pb2_grpc.RegistryServiceStub(test_channel)
    return registry_client


# ============================================================================
# Test Classes
# ============================================================================


class TestDiscoverById:
    """Tests for the discover_by_id() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_discover_by_id_success(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully discovering a module by ID."""
        module_id = "module_001"

        # Pre-register a module
        mock_servicer.registered_modules[module_id] = {
            "module_id": module_id,
            "module_type": "tool",
            "name": "TestModule",
            "address": "localhost",
            "port": 50051,
            "version": "1.0.0",
            "status": registry_enums_pb2.MODULE_STATUS_READY,
        }

        # Get the method descriptor
        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name["GetModule"]

        # Execute client call in thread pool
        future = thread_pool.submit(client.discover_by_id, module_id)

        # Intercept the call
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        # Verify request
        assert request.module_id == module_id

        # Mock servicer processes the request
        context = FakeContext()
        response = mock_servicer.GetModule(request, context)

        # Terminate the RPC
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        # Get result
        result = future.result(timeout=1.0)

        # Verify result
        assert result is not None
        assert result.module_id == module_id
        assert result.module_type == RegistryModuleType.TOOL
        assert result.address == "localhost"
        assert result.port == 50051
        assert result.name == "TestModule"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_discover_by_id_not_found(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test discovering a non-existent module raises error."""
        module_id = "nonexistent_module"

        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name["GetModule"]

        future = thread_pool.submit(client.discover_by_id, module_id)

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.GetModule(request, context)

        # Return empty response with OK status - the client detects empty id
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        # The error handler wraps RegistryModuleNotFoundError in RegistryServiceError
        with pytest.raises(RegistryServiceError) as exc_info:
            future.result(timeout=1.0)
        assert module_id in str(exc_info.value)


class TestSearch:
    """Tests for the search() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_search_by_name(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test searching modules by name."""
        # Pre-register modules
        mock_servicer.registered_modules["mod1"] = {
            "module_id": "mod1",
            "module_type": "tool",
            "name": "SearchableModule",
            "address": "localhost",
            "port": 50051,
            "version": "1.0.0",
            "status": registry_enums_pb2.MODULE_STATUS_READY,
        }
        mock_servicer.registered_modules["mod2"] = {
            "module_id": "mod2",
            "module_type": "archetype",
            "name": "OtherModule",
            "address": "localhost",
            "port": 50052,
            "version": "1.0.0",
            "status": registry_enums_pb2.MODULE_STATUS_READY,
        }

        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name[
            "DiscoverModules"
        ]

        future = thread_pool.submit(client.search, name="Searchable")

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.DiscoverModules(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        results = future.result(timeout=1.0)

        assert len(results) == 1
        assert results[0].module_id == "mod1"
        assert results[0].name == "SearchableModule"

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_search_by_type(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test searching modules by type."""
        mock_servicer.registered_modules["mod1"] = {
            "module_id": "mod1",
            "module_type": "tool",
            "name": "Tool1",
            "address": "localhost",
            "port": 50051,
            "version": "1.0.0",
            "status": registry_enums_pb2.MODULE_STATUS_READY,
        }
        mock_servicer.registered_modules["mod2"] = {
            "module_id": "mod2",
            "module_type": "archetype",
            "name": "Archetype1",
            "address": "localhost",
            "port": 50052,
            "version": "1.0.0",
            "status": registry_enums_pb2.MODULE_STATUS_READY,
        }

        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name[
            "DiscoverModules"
        ]

        future = thread_pool.submit(client.search, module_type="tool")

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.DiscoverModules(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        results = future.result(timeout=1.0)

        assert len(results) == 1
        assert results[0].module_type == RegistryModuleType.TOOL

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_search_no_results(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test search with no matching results."""
        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name[
            "DiscoverModules"
        ]

        future = thread_pool.submit(client.search, name="NonExistent")

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.DiscoverModules(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        results = future.result(timeout=1.0)

        assert len(results) == 0


class TestRegister:
    """Tests for the register() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_register_success(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully registering a module (updates existing)."""
        module_id = "existing_module"

        # Pre-register module (new proto requires module to exist)
        mock_servicer.registered_modules[module_id] = {
            "module_id": module_id,
            "module_type": "tool",
            "name": "ExistingModule",
            "address": "old-host",
            "port": 50050,
            "version": "0.9.0",
            "status": registry_enums_pb2.MODULE_STATUS_READY,
        }

        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name[
            "RegisterModule"
        ]

        future = thread_pool.submit(
            client.register,
            module_id=module_id,
            address="localhost",
            port=50053,
            version="1.0.0",
        )

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert request.module_id == module_id
        assert request.address == "localhost"
        assert request.port == 50053
        assert request.version == "1.0.0"

        context = FakeContext()
        response = mock_servicer.RegisterModule(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)

        assert result is not None
        assert result.module_id == module_id
        assert result.address == "localhost"
        assert result.port == 50053

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_register_not_found(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test registering a non-existent module returns None."""
        module_id = "new_module"

        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name[
            "RegisterModule"
        ]

        future = thread_pool.submit(
            client.register,
            module_id=module_id,
            address="localhost",
            port=50053,
            version="1.0.0",
        )

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.RegisterModule(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)

        # New proto returns None if module doesn't exist
        assert result is None


class TestGetStatus:
    """Tests for the get_status() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_status_success(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully getting module status."""
        module_id = "module_001"

        mock_servicer.registered_modules[module_id] = {
            "module_id": module_id,
            "module_type": "tool",
            "name": "TestModule",
            "address": "localhost",
            "port": 50051,
            "version": "1.0.0",
            "status": registry_enums_pb2.MODULE_STATUS_READY,
        }

        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name["GetModule"]

        future = thread_pool.submit(client.get_status, module_id)

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.GetModule(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)

        assert result.module_id == module_id
        assert result.status == RegistryModuleStatus.READY


class TestHeartbeat:
    """Tests for the heartbeat() method."""

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_heartbeat_success(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successfully sending a heartbeat."""
        module_id = "module_001"

        mock_servicer.registered_modules[module_id] = {
            "module_id": module_id,
            "module_type": "tool",
            "name": "TestModule",
            "address": "localhost",
            "port": 50051,
            "version": "1.0.0",
            "status": registry_enums_pb2.MODULE_STATUS_READY,
        }

        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name["Heartbeat"]

        future = thread_pool.submit(client.heartbeat, module_id)

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert request.module_id == module_id

        context = FakeContext()
        response = mock_servicer.Heartbeat(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)

        assert result == RegistryModuleStatus.ACTIVE

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_heartbeat_not_found(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test heartbeat for non-existent module."""
        module_id = "nonexistent_module"

        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name["Heartbeat"]

        future = thread_pool.submit(client.heartbeat, module_id)

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.Heartbeat(request, context)

        # Return response - module not found but client gets UNSPECIFIED status
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)

        # Returns UNSPECIFIED status when module not found
        assert result == RegistryModuleStatus.UNSPECIFIED
