"""Comprehensive tests for the GrpcCost service.

Tests all Cost service methods with success cases, validation errors,
edge cases, and various cost types.
"""

import asyncio
import logging
import secrets
from collections.abc import Callable
from concurrent import futures
from typing import Any
from unittest.mock import patch

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.cost.v1 import (
    cost_dto_pb2,
    cost_enums_pb2,
    cost_messages_pb2,
    cost_service_pb2,
    cost_service_pb2_grpc,
)
from agentic_mesh_protocol.pagination.v1 import bulk_pb2, pagination_pb2
from mock_cost_servicer import MockCostServicer
from tests.fixtures.grpc_fixtures import AsyncStubWrapper, FakeContext

from digitalkin.grpc_servers.exceptions import ServerError
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.services.cost import CostType
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode
from digitalkin.services.cost.cost_strategy import CostConfig, CostData
from digitalkin.services.cost.exceptions import CostServiceError
from digitalkin.services.cost.grpc_cost import GrpcCost

service_instance = MockCostServicer()
service_name = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]

test_logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def thread_pool():
    """Create thread pool and ensure cleanup.

    Yields:
        ThreadPoolExecutor instance
    """
    test_logger.info("Creating thread pool...")
    pool = futures.ThreadPoolExecutor(max_workers=10)
    yield pool
    test_logger.info("Shutting down thread pool...")
    pool.shutdown(wait=True, cancel_futures=True)
    test_logger.info("Thread pool shut down")


@pytest.fixture
def test_channel() -> grpc_testing.Channel:
    """Mock a gRPC channel.

    Returns:
        Mock gRPC Channel
    """
    test_logger.info("Creating test channel...")
    test_clock = grpc_testing.strict_real_time()
    channel = grpc_testing.channel([service_name], test_clock)
    test_logger.info("Test channel created")
    return channel


@pytest.fixture
def mock_servicer() -> MockCostServicer:
    """Return an instance of the mock servicer.

    Returns:
        Mock Cost Servicer
    """
    test_logger.info("Creating mock servicer...")
    servicer = MockCostServicer()
    test_logger.info("Mock servicer created")
    return servicer


@pytest.fixture
def cost_config() -> dict[str, CostConfig]:
    """Create sample cost configuration.

    Returns:
        dict: Cost configuration mapping
    """
    return {
        "gpt4_input": CostConfig(
            cost_name="gpt4_input",
            cost_type="TOKEN_INPUT",
            description="GPT-4 input tokens",
            unit="tokens",
            rate=0.00003,  # $0.03 per 1k tokens
        ),
        "gpt4_output": CostConfig(
            cost_name="gpt4_output",
            cost_type="TOKEN_OUTPUT",
            description="GPT-4 output tokens",
            unit="tokens",
            rate=0.00006,  # $0.06 per 1k tokens
        ),
        "api_call": CostConfig(
            cost_name="api_call",
            cost_type="API_CALL",
            description="API call",
            unit="calls",
            rate=0.001,  # $0.001 per call
        ),
        "storage": CostConfig(
            cost_name="storage",
            cost_type="STORAGE",
            description="Storage",
            unit="GB",
            rate=0.02,  # $0.02 per GB
        ),
        "compute_time": CostConfig(
            cost_name="compute_time",
            cost_type="TIME",
            description="Compute time",
            unit="hours",
            rate=0.05,  # $0.05 per hour
        ),
        "other_cost": CostConfig(
            cost_name="other_cost",
            cost_type="OTHER",
            description="Other costs",
            unit="units",
            rate=0.01,
        ),
    }


@pytest.fixture
def client(test_channel: grpc_testing.Channel, cost_config: dict[str, CostConfig]) -> GrpcCost:
    """Instantiate a GrpcCost client that uses the test channel.

    Returns:
        gRPC client as GrpcCost
    """
    test_logger.info("Creating client...")
    dummy_config = ClientConfig(
        host="[::]",
        port=50051,
        mode=ControlFlow.ASYNC,
        security=SecurityMode.INSECURE,
        credentials=None,
    )

    mission_id = "missions:test"
    setup_id = "setups:1"
    setup_version_id = "setup_versions:1"
    client = GrpcCost(mission_id, setup_id, setup_version_id, cost_config, dummy_config)

    # Override the channel and stub to use our test channel
    client.stub = AsyncStubWrapper(cost_service_pb2_grpc.CostServiceStub(test_channel))
    test_logger.info("Client created")
    return client


@pytest.fixture
def serve(test_channel: grpc_testing.Channel) -> Callable[[str, Callable[[Any], Any]], Any]:
    """Answer the next pending call of an RPC with the response built by a handler.

    Returns:
        Callable taking the RPC name and a request-to-response handler, returning the request.
    """

    def _serve(rpc_name: str, handler: Callable[[Any], Any]) -> Any:
        method_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"].methods_by_name[rpc_name]
        _, request, rpc = test_channel.take_unary_unary(method_desc)
        rpc.send_initial_metadata(())
        rpc.terminate(handler(request), (), grpc.StatusCode.OK, "")
        return request

    return _serve


# ============================================================================
# Test: add() Method
# ============================================================================


class TestAddCost:
    """Tests for the add() method of GrpcCost service.

    Covers success cases, validation errors, various cost types,
    cost calculations, and edge cases for quantity values.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_add_cost_success(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successful cost addition with valid configuration.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        # Add a cost
        name = f"test_cost_{secrets.token_hex(4)}"
        quantity = 1000.0

        # Start the client call in a separate thread
        future = thread_pool.submit(asyncio.run, client.add(name, "gpt4_input", quantity))

        # Get the method descriptor
        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["CreateCost"]

        # Intercept the pending unary-unary call
        _invocation_metadata, request, rpc = test_channel.take_unary_unary(method_desc)

        # Process with mock servicer
        context = FakeContext()
        response = mock_servicer.CreateCost(request, context)

        # Send response back to client
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        # Verify the client call completes without error
        result = future.result(timeout=5.0)
        assert result is None  # add() returns None on success

        # Verify the cost was stored
        assert client.mission_id in mock_servicer.costs
        stored_costs = mock_servicer.costs[client.mission_id]
        assert len(stored_costs) == 1
        assert stored_costs[0]["name"] == name
        assert stored_costs[0]["quantity"] == quantity
        assert stored_costs[0]["cost"] == pytest.approx(0.00003 * quantity)  # rate * quantity

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    async def test_add_cost_invalid_config_name(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test add with non-existent cost configuration name.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
        """
        name = f"test_cost_{secrets.token_hex(4)}"
        quantity = 100.0

        # Try to add cost with invalid config name
        with pytest.raises(CostServiceError, match=r"Cost config .* not found"):
            await client.add(name, "nonexistent_config", quantity)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_add_cost_various_types(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test adding costs with various cost types.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        configs = [
            ("gpt4_input", "TOKEN_INPUT", 1000.0),
            ("gpt4_output", "TOKEN_OUTPUT", 500.0),
            ("api_call", "API_CALL", 10.0),
            ("storage", "STORAGE", 5.0),
            ("compute_time", "TIME", 2.0),
            ("other_cost", "OTHER", 7.0),
        ]

        for config_name, expected_type, quantity in configs:
            name = f"test_{config_name}_{secrets.token_hex(4)}"

            # Start client call
            future = thread_pool.submit(asyncio.run, client.add(name, config_name, quantity))

            # Intercept and process
            service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
            method_desc = service_desc.methods_by_name["CreateCost"]
            _, request, rpc = test_channel.take_unary_unary(method_desc)

            context = FakeContext()
            response = mock_servicer.CreateCost(request, context)

            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")

            result = future.result(timeout=5.0)
            assert result is None

        # Verify all costs were stored
        stored_costs = mock_servicer.costs[client.mission_id]
        assert len(stored_costs) == len(configs)

        # Verify cost types
        cost_types = [cost["cost_type"].name for cost in stored_costs]
        expected_types = [ct for _, ct, _ in configs]
        assert sorted(cost_types) == sorted(expected_types)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_add_cost_calculation(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test that cost calculation (rate * quantity) is correct.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        test_cases = [
            ("gpt4_input", 1000.0, 0.00003 * 1000),  # $0.03
            ("gpt4_output", 500.0, 0.00006 * 500),  # $0.03
            ("api_call", 25.0, 0.001 * 25),  # $0.025
            ("storage", 100.0, 0.02 * 100),  # $2.00
        ]

        for config_name, quantity, expected_cost in test_cases:
            name = f"test_{config_name}_{secrets.token_hex(4)}"

            future = thread_pool.submit(asyncio.run, client.add(name, config_name, quantity))

            service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
            method_desc = service_desc.methods_by_name["CreateCost"]
            _, request, rpc = test_channel.take_unary_unary(method_desc)

            context = FakeContext()
            response = mock_servicer.CreateCost(request, context)

            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")

            future.result(timeout=5.0)

        # Verify calculations
        stored_costs = mock_servicer.costs[client.mission_id]
        for i, (_, _, expected_cost) in enumerate(test_cases):
            assert abs(stored_costs[i]["cost"] - expected_cost) < 0.0001

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_add_cost_zero_quantity(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test adding cost with zero quantity: a free usage is valid in the protocol.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        name = f"test_zero_{secrets.token_hex(4)}"

        future = thread_pool.submit(asyncio.run, client.add(name, "gpt4_input", 0.0))

        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["CreateCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.CreateCost(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), context._code, context._details)

        assert future.result(timeout=5.0) is None
        assert context._code == grpc.StatusCode.OK
        stored = mock_servicer.costs[client.mission_id]
        assert stored[0]["quantity"] == pytest.approx(0.0)
        assert stored[0]["cost"] == pytest.approx(0.0)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.validation
    def test_add_cost_negative_quantity(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test adding cost with negative quantity (invalid).

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        name = f"test_negative_{secrets.token_hex(4)}"

        future = thread_pool.submit(asyncio.run, client.add(name, "gpt4_input", -100.0))

        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["CreateCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.CreateCost(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), context._code, context._details)

        # Should fail with validation error - negative quantity not allowed
        assert context._code == grpc.StatusCode.INVALID_ARGUMENT
        with pytest.raises(ServerError, match="INVALID_ARGUMENT"):
            future.result(timeout=5.0)
        assert client.mission_id not in mock_servicer.costs

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_cost_with_special_characters_in_name(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test cost with special characters in name.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        name = "test-cost_123.special@chars"

        future = thread_pool.submit(asyncio.run, client.add(name, "gpt4_input", 100.0))

        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["CreateCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.CreateCost(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        assert result is None

        # Verify stored
        stored_costs = mock_servicer.costs[client.mission_id]
        assert any(c["name"] == name for c in stored_costs)


# ============================================================================
# Test: get() Method
# ============================================================================


class TestGetCost:
    """Tests for the get() method of GrpcCost service.

    Covers retrieving costs by name, handling non-existent costs,
    and retrieving multiple costs with the same name.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_cost_success(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test successful retrieval of costs by name.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        # First, add a cost
        name = f"test_get_{secrets.token_hex(4)}"
        quantity = 1000.0

        # Add cost
        future_add = thread_pool.submit(asyncio.run, client.add(name, "gpt4_input", quantity))
        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["CreateCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)
        context = FakeContext()
        response = mock_servicer.CreateCost(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")
        future_add.result(timeout=5.0)

        # Now get the cost
        future_get = thread_pool.submit(asyncio.run, client.get(name))

        method_desc = service_desc.methods_by_name["ListCosts"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)
        assert request.mission_id == client.mission_id
        assert list(request.filter.names) == [name]
        assert request.pagination.limit == 100
        assert request.pagination.offset == 0

        context = FakeContext()
        response = mock_servicer.ListCosts(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future_get.result(timeout=5.0)
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], CostData)
        assert result[0].name == name
        assert result[0].quantity == quantity

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_cost_not_found(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test getting a cost that doesn't exist.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        name = f"nonexistent_{secrets.token_hex(4)}"

        future = thread_pool.submit(asyncio.run, client.get(name))

        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["ListCosts"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.ListCosts(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_cost_multiple_with_same_name(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test getting multiple costs with the same name.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        name = f"test_multi_{secrets.token_hex(4)}"

        # Add multiple costs with the same name
        quantities = [100.0, 200.0, 300.0]
        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]

        for quantity in quantities:
            future_add = thread_pool.submit(asyncio.run, client.add(name, "gpt4_input", quantity))
            method_desc = service_desc.methods_by_name["CreateCost"]
            _, request, rpc = test_channel.take_unary_unary(method_desc)
            context = FakeContext()
            response = mock_servicer.CreateCost(request, context)
            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")
            future_add.result(timeout=5.0)

        # Get all costs with this name
        future_get = thread_pool.submit(asyncio.run, client.get(name))

        method_desc = service_desc.methods_by_name["ListCosts"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.ListCosts(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future_get.result(timeout=5.0)
        assert isinstance(result, list)
        assert len(result) == 3
        assert all(isinstance(c, CostData) for c in result)
        assert all(c.name == name for c in result)

        # Verify quantities
        result_quantities = sorted([c.quantity for c in result])
        assert result_quantities == sorted(quantities)


# ============================================================================
# Test: get_filtered() Method
# ============================================================================


class TestGetFilteredCost:
    """Tests for the get_filtered() method of GrpcCost service.

    Covers filtering by names, cost types, combinations of both,
    empty results, and no filters (return all).
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_filtered_by_names(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test filtering costs by names.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        # Add multiple costs
        names = [f"cost_{i}_{secrets.token_hex(4)}" for i in range(5)]
        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]

        for name in names:
            future_add = thread_pool.submit(asyncio.run, client.add(name, "gpt4_input", 100.0))
            method_desc = service_desc.methods_by_name["CreateCost"]
            _, request, rpc = test_channel.take_unary_unary(method_desc)
            context = FakeContext()
            response = mock_servicer.CreateCost(request, context)
            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")
            future_add.result(timeout=5.0)

        # Filter by subset of names
        filter_names = names[:3]
        future_get = thread_pool.submit(asyncio.run, client.get_filtered(names=filter_names))

        method_desc = service_desc.methods_by_name["ListCosts"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.ListCosts(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future_get.result(timeout=5.0)
        assert isinstance(result, list)
        assert len(result) == 3
        assert all(c.name in filter_names for c in result)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_filtered_by_cost_types(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test filtering costs by cost types.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        # Add costs with different types
        configs = [
            ("gpt4_input", "TOKEN_INPUT"),
            ("gpt4_output", "TOKEN_OUTPUT"),
            ("api_call", "API_CALL"),
            ("storage", "STORAGE"),
        ]
        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]

        for config_name, _ in configs:
            name = f"test_{config_name}_{secrets.token_hex(4)}"
            future_add = thread_pool.submit(asyncio.run, client.add(name, config_name, 100.0))
            method_desc = service_desc.methods_by_name["CreateCost"]
            _, request, rpc = test_channel.take_unary_unary(method_desc)
            context = FakeContext()
            response = mock_servicer.CreateCost(request, context)
            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")
            future_add.result(timeout=5.0)

        # Filter by token types only
        future_get = thread_pool.submit(asyncio.run, client.get_filtered(cost_types=["TOKEN_INPUT", "TOKEN_OUTPUT"]))

        method_desc = service_desc.methods_by_name["ListCosts"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)
        assert list(request.filter.types) == [cost_enums_pb2.TOKEN_INPUT, cost_enums_pb2.TOKEN_OUTPUT]

        context = FakeContext()
        response = mock_servicer.ListCosts(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future_get.result(timeout=5.0)
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(c.cost_type in {CostType.TOKEN_INPUT, CostType.TOKEN_OUTPUT} for c in result)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_filtered_by_names_and_types(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test filtering costs by both names and types.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        # Add various costs
        test_data = [
            ("cost_a", "gpt4_input", "TOKEN_INPUT"),
            ("cost_b", "gpt4_output", "TOKEN_OUTPUT"),
            ("cost_c", "api_call", "API_CALL"),
            ("cost_d", "gpt4_input", "TOKEN_INPUT"),
        ]
        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]

        for name, config, _ in test_data:
            future_add = thread_pool.submit(asyncio.run, client.add(name, config, 100.0))
            method_desc = service_desc.methods_by_name["CreateCost"]
            _, request, rpc = test_channel.take_unary_unary(method_desc)
            context = FakeContext()
            response = mock_servicer.CreateCost(request, context)
            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")
            future_add.result(timeout=5.0)

        # Filter by names and token input type
        future_get = thread_pool.submit(
            asyncio.run, client.get_filtered(names=["cost_a", "cost_d"], cost_types=["TOKEN_INPUT"])
        )

        method_desc = service_desc.methods_by_name["ListCosts"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.ListCosts(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future_get.result(timeout=5.0)
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(c.name in {"cost_a", "cost_d"} for c in result)
        assert all(c.cost_type == CostType.TOKEN_INPUT for c in result)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_get_filtered_empty_results(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test filtering with no matching results.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        # Filter with non-existent names
        future = thread_pool.submit(asyncio.run, client.get_filtered(names=["nonexistent"]))

        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["ListCosts"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.ListCosts(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        assert isinstance(result, list)
        assert len(result) == 0

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.smoke
    def test_get_filtered_no_filters(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test get_filtered with no filters (returns all costs).

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        # Add some costs
        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]

        for i in range(3):
            name = f"cost_{i}"
            future_add = thread_pool.submit(asyncio.run, client.add(name, "gpt4_input", 100.0))
            method_desc = service_desc.methods_by_name["CreateCost"]
            _, request, rpc = test_channel.take_unary_unary(method_desc)
            context = FakeContext()
            response = mock_servicer.CreateCost(request, context)
            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")
            future_add.result(timeout=5.0)

        # Get all costs (no filter)
        future_get = thread_pool.submit(asyncio.run, client.get_filtered())

        method_desc = service_desc.methods_by_name["ListCosts"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.ListCosts(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future_get.result(timeout=5.0)
        assert isinstance(result, list)
        assert len(result) == 3


# ============================================================================
# Test: Edge Cases and Error Handling
# ============================================================================


class TestCostEdgeCases:
    """Tests for edge cases in GrpcCost service.

    Covers very large quantities, fractional quantities,
    and isolation between different missions.
    """

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_cost_with_very_large_quantity(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test cost with very large quantity.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        name = "large_quantity_test"
        quantity = 1_000_000_000.0  # 1 billion

        future = thread_pool.submit(asyncio.run, client.add(name, "gpt4_input", quantity))

        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["CreateCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.CreateCost(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        assert result is None

        # Verify calculation
        stored_costs = mock_servicer.costs[client.mission_id]
        assert stored_costs[0]["quantity"] == quantity
        assert stored_costs[0]["cost"] == pytest.approx(0.00003 * quantity)

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_cost_with_fractional_quantity(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test cost with fractional quantity.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        name = "fractional_test"
        quantity = 123.456

        future = thread_pool.submit(asyncio.run, client.add(name, "gpt4_input", quantity))

        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["CreateCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.CreateCost(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future.result(timeout=5.0)
        assert result is None

        # Verify stored correctly
        stored_costs = mock_servicer.costs[client.mission_id]
        assert abs(stored_costs[0]["quantity"] - quantity) < 0.0001

    @pytest.mark.grpc
    @pytest.mark.integration
    @pytest.mark.edge_case
    def test_multiple_missions_isolation(
        self,
        client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test that costs for different missions are isolated.

        Args:
            client: GrpcCost client for testing
            test_channel: Mock gRPC channel
            mock_servicer: Mock cost servicer
        """
        # Add costs for the test client's mission
        name1 = "mission1_cost"
        future = thread_pool.submit(asyncio.run, client.add(name1, "gpt4_input", 100.0))

        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["CreateCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.CreateCost(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")
        future.result(timeout=5.0)

        # Manually add cost for a different mission
        different_mission_cost = {
            "cost": 50.0,
            "name": "mission2_cost",
            "unit": "tokens",
            "cost_type": CostType.TOKEN_INPUT,
            "mission_id": "missions:different",
            "rate": 0.00003,
            "quantity": 1000.0,
            "setup_version_id": "setup_versions:2",
        }
        mock_servicer._validate_and_store_cost(different_mission_cost)

        # Get costs for original mission
        future_get = thread_pool.submit(asyncio.run, client.get_filtered())

        method_desc = service_desc.methods_by_name["ListCosts"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.ListCosts(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        result = future_get.result(timeout=5.0)

        # Should only see costs from original mission
        assert len(result) == 1
        assert result[0].name == name1
        assert result[0].mission_id == client.mission_id


# ============================================================================
# Test: OperationError outcomes and pagination
# ============================================================================


class TestResultOutcomes:
    """Tests for ``CostResult`` outcomes holding an ``OperationError`` and for listing pagination."""

    @pytest.mark.grpc
    @pytest.mark.validation
    def test_add_cost_operation_error_raises(
            self,
            client: GrpcCost,
            serve: Callable[[str, Callable[[Any], Any]], Any],
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """A CreateCost result holding an OperationError raises CostServiceError."""
        future = thread_pool.submit(asyncio.run, client.add("dup_cost", "gpt4_input", 10.0))

        serve(
            "CreateCost",
            lambda request: cost_dto_pb2.CreateCostResponse(
                result=cost_messages_pb2.CostResult(
                    identifier=request.name,
                    error=bulk_pb2.OperationError(code="ALREADY_EXISTS", message="cost already recorded"),
                )
            ),
        )

        with pytest.raises(CostServiceError, match="dup_cost: ALREADY_EXISTS cost already recorded"):
            future.result(timeout=5.0)

    @pytest.mark.grpc
    @pytest.mark.edge_case
    def test_get_filtered_drops_error_results(
            self,
            client: GrpcCost,
            serve: Callable[[str, Callable[[Any], Any]], Any],
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """A ListCosts result holding an OperationError is dropped and logged, the others are kept."""
        kept = cost_messages_pb2.Cost(
            mission_id=client.mission_id,
            setup_version_id=client.setup_version_id,
            name="kept",
            cost=0.3,
            quantity=10.0,
            rate=0.03,
            unit="tokens",
            type=cost_enums_pb2.TOKEN_INPUT,
        )
        response = cost_dto_pb2.ListCostsResponse(
            results=[
                cost_messages_pb2.CostResult(identifier="kept", cost=kept),
                cost_messages_pb2.CostResult(
                    identifier="broken",
                    error=bulk_pb2.OperationError(code="INTERNAL", message="corrupted row"),
                ),
            ],
            bulk=bulk_pb2.BulkResponse(
                total_processed=2,
                total_failed=1,
                pagination=pagination_pb2.PaginationResponse(total_count=2),
            ),
            total_cost=0.3,
        )

        with patch.object(logger, "warning") as warning:
            future = thread_pool.submit(asyncio.run, client.get_filtered())
            serve("ListCosts", lambda _request: response)
            result = future.result(timeout=5.0)

        assert [cost.name for cost in result] == ["kept"]
        assert result[0].cost_type == CostType.TOKEN_INPUT
        assert result[0].quantity == pytest.approx(10.0)
        assert any("broken" in call.args and "corrupted row" in call.args for call in warning.call_args_list)

    @pytest.mark.grpc
    @pytest.mark.edge_case
    def test_get_filtered_reads_every_page(
            self,
            client: GrpcCost,
            serve: Callable[[str, Callable[[Any], Any]], Any],
            mock_servicer: MockCostServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Listings over 100 items are read page by page until total_count is reached."""
        for i in range(150):
            mock_servicer._validate_and_store_cost({
                "cost": 0.03,
                "name": f"page_cost_{i}",
                "unit": "tokens",
                "cost_type": CostType.TOKEN_INPUT,
                "mission_id": client.mission_id,
                "rate": 0.00003,
                "quantity": 1000.0,
                "setup_version_id": client.setup_version_id,
            })

        pages: list[tuple[int, int]] = []

        def list_page(request: cost_dto_pb2.ListCostsRequest) -> cost_dto_pb2.ListCostsResponse:
            pages.append((request.pagination.limit, request.pagination.offset))
            return mock_servicer.ListCosts(request, FakeContext())

        future = thread_pool.submit(asyncio.run, client.get_filtered())
        serve("ListCosts", list_page)
        serve("ListCosts", list_page)
        result = future.result(timeout=5.0)

        assert pages == [(100, 0), (100, 100)]
        assert [cost.name for cost in result] == [f"page_cost_{i}" for i in range(150)]

    @pytest.mark.grpc
    @pytest.mark.validation
    def test_get_empty_name_rejected(
            self,
            client: GrpcCost,
            test_channel: grpc_testing.Channel,
            mock_servicer: MockCostServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """An empty name breaks the CostFilter rules and surfaces INVALID_ARGUMENT."""
        future = thread_pool.submit(asyncio.run, client.get(""))

        method_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"].methods_by_name["ListCosts"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)
        context = FakeContext()
        response = mock_servicer.ListCosts(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), context._code, context._details)

        assert context._code == grpc.StatusCode.INVALID_ARGUMENT
        with pytest.raises(ServerError, match="INVALID_ARGUMENT"):
            future.result(timeout=5.0)


# ============================================================================
# Test: get_cost_config() / set_cost_config() Methods
# ============================================================================


class TestCostConfig:
    """Tests for ListCostConfigs and SetCostConfig through GrpcCost."""

    @pytest.mark.grpc
    @pytest.mark.smoke
    def test_set_cost_config_success(
            self,
            client: GrpcCost,
            cost_config: dict[str, CostConfig],
            serve: Callable[[str, Callable[[Any], Any]], Any],
            mock_servicer: MockCostServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Every configuration stored returns True and sends the types by name."""
        future = thread_pool.submit(asyncio.run, client.set_cost_config(list(cost_config.values())))
        request = serve("SetCostConfig", lambda request: mock_servicer.SetCostConfig(request, FakeContext()))

        assert future.result(timeout=5.0) is True
        assert request.setup_version_id == client.setup_version_id
        assert [(config.name, cost_enums_pb2.CostType.Name(config.type)) for config in request.configs] == [
            (config.cost_name, config.cost_type) for config in cost_config.values()
        ]
        assert len(mock_servicer.configs[client.setup_version_id]) == len(cost_config)

    @pytest.mark.grpc
    @pytest.mark.edge_case
    def test_set_cost_config_failed_item_returns_false(
            self,
            client: GrpcCost,
            cost_config: dict[str, CostConfig],
            serve: Callable[[str, Callable[[Any], Any]], Any],
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """A configuration ending in an OperationError makes set_cost_config return False."""
        response = cost_dto_pb2.SetCostConfigResponse(
            results=[
                cost_messages_pb2.CostResult(
                    identifier="gpt4_input",
                    config=cost_messages_pb2.CostConfig(
                        name="gpt4_input", type=cost_enums_pb2.TOKEN_INPUT, unit="tokens", rate=0.00003
                    ),
                ),
                cost_messages_pb2.CostResult(
                    identifier="gpt4_output",
                    error=bulk_pb2.OperationError(code="FAILED_PRECONDITION", message="rate locked"),
                ),
            ],
            bulk=bulk_pb2.BulkResponse(total_processed=2, total_failed=1),
        )
        configs = [cost_config["gpt4_input"], cost_config["gpt4_output"]]

        with patch.object(logger, "warning") as warning:
            future = thread_pool.submit(asyncio.run, client.set_cost_config(configs))
            serve("SetCostConfig", lambda _request: response)
            stored = future.result(timeout=5.0)

        assert stored is False
        assert any("gpt4_output" in call.args and "rate locked" in call.args for call in warning.call_args_list)

    @pytest.mark.grpc
    @pytest.mark.smoke
    def test_get_cost_config_success(
            self,
            client: GrpcCost,
            serve: Callable[[str, Callable[[Any], Any]], Any],
            mock_servicer: MockCostServicer,
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Stored configurations come back as SDK CostConfig models."""
        mock_servicer.configs[client.setup_version_id] = [
            cost_messages_pb2.CostConfig(
                name="gpt4_input",
                type=cost_enums_pb2.TOKEN_INPUT,
                description="GPT-4 input tokens",
                unit="tokens",
                rate=0.00003,
            )
        ]

        future = thread_pool.submit(asyncio.run, client.get_cost_config())
        request = serve("ListCostConfigs", lambda request: mock_servicer.ListCostConfigs(request, FakeContext()))

        assert request.setup_version_id == client.setup_version_id
        assert (request.pagination.limit, request.pagination.offset) == (100, 0)
        assert future.result(timeout=5.0) == [
            CostConfig(
                cost_name="gpt4_input",
                cost_type="TOKEN_INPUT",
                description="GPT-4 input tokens",
                unit="tokens",
                rate=0.00003,
            )
        ]

    @pytest.mark.grpc
    @pytest.mark.edge_case
    def test_get_cost_config_drops_error_results(
            self,
            client: GrpcCost,
            serve: Callable[[str, Callable[[Any], Any]], Any],
            thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """A ListCostConfigs result holding an OperationError is dropped."""
        response = cost_dto_pb2.ListCostConfigsResponse(
            results=[
                cost_messages_pb2.CostResult(
                    identifier="api_call",
                    config=cost_messages_pb2.CostConfig(
                        name="api_call", type=cost_enums_pb2.API_CALL, unit="calls", rate=0.001
                    ),
                ),
                cost_messages_pb2.CostResult(
                    identifier="legacy",
                    error=bulk_pb2.OperationError(code="DATA_LOSS", message="unreadable config"),
                ),
            ],
            bulk=bulk_pb2.BulkResponse(
                total_processed=2,
                total_failed=1,
                pagination=pagination_pb2.PaginationResponse(total_count=2),
            ),
        )

        future = thread_pool.submit(asyncio.run, client.get_cost_config())
        serve("ListCostConfigs", lambda _request: response)
        result = future.result(timeout=5.0)

        assert [(config.cost_name, config.cost_type, config.rate) for config in result] == [
            ("api_call", "API_CALL", 0.001)
        ]


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
