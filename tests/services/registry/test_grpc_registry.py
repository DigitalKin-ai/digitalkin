"""Comprehensive tests for GrpcRegistry service.

This test suite validates the GrpcRegistry service implementation, including:
- Discovering modules by ID
- Searching modules by criteria
- Registering modules
- Getting module status
- Sending heartbeats
"""

import asyncio
import types
from concurrent import futures

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.registry.v1 import (
    registry_enums_pb2,
    registry_service_pb2,
    registry_service_pb2_grpc,
)

from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.registry import (
    RegistryModuleStatus,
    RegistryModuleType,
    RegistrySetupStatus,
    RegistryVisibility,
)
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode
from digitalkin.services.registry.exceptions import (
    RegistryServiceError,
)
from digitalkin.services.registry.grpc_registry import GrpcRegistry
from tests.fixtures.grpc_fixtures import AsyncStubWrapper, FakeContext
from tests.services.registry.mock_registry_servicer import MockRegistryServicer

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
        mode=ControlFlow.ASYNC,
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
    registry_client.stub = AsyncStubWrapper(registry_service_pb2_grpc.RegistryServiceStub(test_channel))

    async def _test_exec_grpc_query(self, query_endpoint, request, timeout=None, metadata=None):
        response = getattr(self.stub, query_endpoint)(request)
        return await response if asyncio.iscoroutine(response) else response

    registry_client.exec_grpc_query = types.MethodType(_test_exec_grpc_query, registry_client)

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
            "module_type": "tool_module",
            "name": "TestModule",
            "address": "localhost",
            "port": 50051,
            "version": "1.0.0",
            "status": registry_enums_pb2.MODULE_STATUS_READY,
        }

        # Get the method descriptor
        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name["GetModule"]

        # Execute client call in thread pool
        future = thread_pool.submit(asyncio.run, client.discover_by_id(module_id))

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
        assert result.module_type == RegistryModuleType.TOOL_MODULE
        assert result.address == "localhost"
        assert result.port == 50051
        assert result.module_name == "TestModule"

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

        future = thread_pool.submit(asyncio.run, client.discover_by_id(module_id))

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
            "module_type": "tool_module",
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
            "SearchModules"
        ]

        future = thread_pool.submit(asyncio.run, client.search(name="Searchable"))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.SearchModules(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        results = future.result(timeout=1.0)

        assert len(results) == 1
        assert results[0].module_id == "mod1"
        assert results[0].module_name == "SearchableModule"

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
            "module_type": "tool_module",
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
            "SearchModules"
        ]

        future = thread_pool.submit(asyncio.run, client.search(module_type="tool_module"))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.SearchModules(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        results = future.result(timeout=1.0)

        assert len(results) == 1
        assert results[0].module_type == RegistryModuleType.TOOL_MODULE

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_search_returns_trimmed_summaries(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test search sends limit on the wire and never populates address/port."""
        mock_servicer.registered_modules["mod1"] = {
            "module_id": "mod1",
            "module_type": "tool_module",
            "name": "Tool1",
            "address": "localhost",
            "port": 50051,
            "version": "1.0.0",
            "status": registry_enums_pb2.MODULE_STATUS_READY,
        }

        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name[
            "SearchModules"
        ]

        future = thread_pool.submit(asyncio.run, client.search(name="Tool", limit=5))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert request.limit == 5

        context = FakeContext()
        response = mock_servicer.SearchModules(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        results = future.result(timeout=1.0)

        assert len(results) == 1
        # ModuleSummary is trimmed: network location never crosses the search surface
        assert results[0].address == ""
        assert results[0].port == 0
        assert results[0].status == RegistryModuleStatus.READY
        assert results[0].version == "1.0.0"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.parametrize(
        ("view", "expected_type", "expected_id"),
        [
            ("search_tools", RegistryModuleType.TOOL_MODULE, "mod1"),
            ("search_kins", RegistryModuleType.ARCHETYPE, "mod2"),
        ],
    )
    def test_typed_views(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
        view: str,
        expected_type: RegistryModuleType,
        expected_id: str,
    ) -> None:
        """Test search_tools/search_kins return only the matching module type."""
        mock_servicer.registered_modules["mod1"] = {
            "module_id": "mod1",
            "module_type": "tool_module",
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
            "SearchModules"
        ]

        search_view = client.search_tools if view == "search_tools" else client.search_kins
        future = thread_pool.submit(asyncio.run, search_view())

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.SearchModules(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        results = future.result(timeout=1.0)

        assert len(results) == 1
        assert results[0].module_id == expected_id
        assert results[0].module_type == expected_type

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
            "SearchModules"
        ]

        future = thread_pool.submit(asyncio.run, client.search(name="NonExistent"))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.SearchModules(request, context)

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
            "module_type": "tool_module",
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
            asyncio.run,
            client.register(
                module_id=module_id,
                address="localhost",
                port=50053,
                version="1.0.0",
            ),
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
    def test_register_declares_module_type(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test registration sends the declared module type on the wire."""
        module_id = "existing_module"

        # Pre-register module with no type — registration declares it
        mock_servicer.registered_modules[module_id] = {
            "module_id": module_id,
            "module_type": "",
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
            asyncio.run,
            client.register(
                module_id=module_id,
                address="localhost",
                port=50053,
                version="1.0.0",
                module_type=RegistryModuleType.TOOL_MODULE,
            ),
        )

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        assert request.module_type == registry_enums_pb2.MODULE_TYPE_TOOL_MODULE

        context = FakeContext()
        response = mock_servicer.RegisterModule(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)

        assert result is not None
        assert result.module_type == RegistryModuleType.TOOL_MODULE

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
            asyncio.run,
            client.register(
                module_id=module_id,
                address="localhost",
                port=50053,
                version="1.0.0",
            ),
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
            "module_type": "tool_module",
            "name": "TestModule",
            "address": "localhost",
            "port": 50051,
            "version": "1.0.0",
            "status": registry_enums_pb2.MODULE_STATUS_READY,
        }

        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name["GetModule"]

        future = thread_pool.submit(asyncio.run, client.get_status(module_id))

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
            "module_type": "tool_module",
            "name": "TestModule",
            "address": "localhost",
            "port": 50051,
            "version": "1.0.0",
            "status": registry_enums_pb2.MODULE_STATUS_READY,
        }

        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name["Heartbeat"]

        future = thread_pool.submit(asyncio.run, client.heartbeat(module_id))

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

        future = thread_pool.submit(asyncio.run, client.heartbeat(module_id))

        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.Heartbeat(request, context)

        # Return response - module not found but client gets UNSPECIFIED status
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=1.0)

        # Returns UNSPECIFIED status when module not found
        assert result == RegistryModuleStatus.UNSPECIFIED


class TestSearchSetups:
    """Tests for the search_setups() method."""

    def _seed_setups(self, mock_servicer: MockRegistryServicer) -> None:
        """Seed the mock servicer with two setups."""
        mock_servicer.setups["setups:duda"] = {
            "setup_id": "setups:duda",
            "name": "Duda Builder",
            "documentation": "Builds websites on the Duda platform",
            "status": registry_enums_pb2.SETUP_STATUS_READY,
            "visibility": registry_enums_pb2.VISIBILITY_PUBLIC,
            "organization_id": "organizations:dk",
            "module_id": "modules:duda",
            "module_name": "tool-duda",
            "module_type": "tool_module",
            "setup_version_id": "setup_versions:v1",
            "setup_version": "1.0.0",
        }
        mock_servicer.setups["setups:isaac"] = {
            "setup_id": "setups:isaac",
            "name": "Isaac",
            "documentation": "Multi-agent orchestration kin",
            "status": registry_enums_pb2.SETUP_STATUS_DRAFT,
            "visibility": registry_enums_pb2.VISIBILITY_PRIVATE,
            "organization_id": "organizations:dk",
            "module_id": "modules:isaac",
            "module_name": "archetype-isaac",
            "module_type": "archetype",
            "setup_version_id": "setup_versions:v2",
            "setup_version": "2.0.0",
        }

    def _run_search(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
        **kwargs: object,
    ) -> tuple[object, list]:
        """Run a search_setups call through the test channel, returning (request, results)."""
        method_desc = registry_service_pb2.DESCRIPTOR.services_by_name["RegistryService"].methods_by_name[
            "SearchSetups"
        ]
        future = thread_pool.submit(asyncio.run, client.search_setups(**kwargs))
        _, request, rpc = test_channel.take_unary_unary(method_desc)
        response = mock_servicer.SearchSetups(request, FakeContext())
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")
        return request, future.result(timeout=1.0)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_search_setups_maps_summary(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Proto SetupSummary maps into the search-safe SetupSummary with enums and no config field."""
        self._seed_setups(mock_servicer)

        _, results = self._run_search(client, test_channel, mock_servicer, thread_pool, query="duda")

        assert len(results) == 1
        setup = results[0]
        assert setup.setup_id == "setups:duda"
        assert setup.name == "Duda Builder"
        assert setup.status == RegistrySetupStatus.READY
        assert setup.visibility == RegistryVisibility.PUBLIC
        assert setup.module_id == "modules:duda"
        assert setup.module_name == "tool-duda"
        assert setup.module_type == RegistryModuleType.TOOL_MODULE
        assert setup.setup_version == "1.0.0"
        assert "config" not in type(setup).model_fields

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_search_setups_query_matches_documentation(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test the query filter matches documentation, not just name."""
        self._seed_setups(mock_servicer)

        _, results = self._run_search(client, test_channel, mock_servicer, thread_pool, query="orchestration")

        assert len(results) == 1
        assert results[0].setup_id == "setups:isaac"

    @pytest.mark.grpc
    @pytest.mark.integration
    def test_search_setups_statuses_filter_on_wire(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test the statuses filter is sent on the wire and applied."""
        self._seed_setups(mock_servicer)

        request, results = self._run_search(
            client,
            test_channel,
            mock_servicer,
            thread_pool,
            statuses=[RegistrySetupStatus.READY],
        )

        assert list(request.statuses) == [registry_enums_pb2.SETUP_STATUS_READY]
        assert len(results) == 1
        assert results[0].setup_id == "setups:duda"

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_search_setups_no_results(
        self,
        client: GrpcRegistry,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockRegistryServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test search with no matches returns an empty list."""
        _, results = self._run_search(client, test_channel, mock_servicer, thread_pool, query="nothing")

        assert results == []
