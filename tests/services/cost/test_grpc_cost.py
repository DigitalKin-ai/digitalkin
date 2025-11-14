"""Comprehensive tests for the GrpcCost service.

Tests all Cost service methods with success cases, validation errors,
edge cases, and various cost types.
"""

import logging
import secrets

import grpc
import grpc_testing
import pytest
from digitalkin_proto.agentic_mesh_protocol.cost.v1 import cost_service_pb2, cost_service_pb2_grpc
from grpc.framework.foundation import logging_pool
from mock_cost_servicer import FakeContext, MockCostServicer

from digitalkin.models.grpc_servers.models import ClientConfig, SecurityMode, ServerMode
from digitalkin.services.cost.cost_strategy import CostConfig, CostData, CostServiceError, CostType
from digitalkin.services.cost.grpc_cost import GrpcCost

service_instance = MockCostServicer()
service_name = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]

test_logger = logging.getLogger(__name__)
client_execution_thread_pool = logging_pool.pool(max_workers=10)


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
        mode=ServerMode.ASYNC,
        security=SecurityMode.INSECURE,
        credentials=None,
    )

    mission_id = "mission_test"
    setup_id = "setup:1"
    setup_version_id = "setup_version:1"
    client = GrpcCost(mission_id, setup_id, setup_version_id, cost_config, dummy_config)

    # Override the channel and stub to use our test channel
    client.stub = cost_service_pb2_grpc.CostServiceStub(test_channel)
    test_logger.info("Client created")
    return client


# ============================================================================
# Test: add() Method
# ============================================================================


def test_add_cost_success(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
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
    future = client_execution_thread_pool.submit(client.add, name, "gpt4_input", quantity)

    # Get the method descriptor
    service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
    method_desc = service_desc.methods_by_name["AddCost"]

    # Intercept the pending unary-unary call
    _invocation_metadata, request, rpc = test_channel.take_unary_unary(method_desc)

    # Process with mock servicer
    context = FakeContext()
    response = mock_servicer.AddCost(request, context)

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
    assert stored_costs[0]["cost"] == 0.00003 * quantity  # rate * quantity


def test_add_cost_invalid_config_name(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
) -> None:
    """Test add with non-existent cost configuration name.

    Args:
        client: GrpcCost client for testing
        test_channel: Mock gRPC channel
    """
    name = f"test_cost_{secrets.token_hex(4)}"
    quantity = 100.0

    # Try to add cost with invalid config name
    with pytest.raises(CostServiceError, match="Cost config .* not found"):
        client.add(name, "nonexistent_config", quantity)


def test_add_cost_various_types(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
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
        future = client_execution_thread_pool.submit(client.add, name, config_name, quantity)

        # Intercept and process
        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["AddCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.AddCost(request, context)

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


def test_add_cost_calculation(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
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

        future = client_execution_thread_pool.submit(client.add, name, config_name, quantity)

        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["AddCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)

        context = FakeContext()
        response = mock_servicer.AddCost(request, context)

        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")

        future.result(timeout=5.0)

    # Verify calculations
    stored_costs = mock_servicer.costs[client.mission_id]
    for i, (_, _, expected_cost) in enumerate(test_cases):
        assert abs(stored_costs[i]["cost"] - expected_cost) < 0.0001


def test_add_cost_zero_quantity(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
) -> None:
    """Test adding cost with zero quantity (edge case).

    Args:
        client: GrpcCost client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock cost servicer
    """
    name = f"test_zero_{secrets.token_hex(4)}"

    future = client_execution_thread_pool.submit(client.add, name, "gpt4_input", 0.0)

    service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
    method_desc = service_desc.methods_by_name["AddCost"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.AddCost(request, context)

    # Zero quantity should be rejected
    rpc.send_initial_metadata(())
    rpc.terminate(response, (), context._code, context._details)

    # Should fail
    with pytest.raises(Exception):  # Will raise gRPC error
        future.result(timeout=5.0)


def test_add_cost_negative_quantity(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
) -> None:
    """Test adding cost with negative quantity (invalid).

    Args:
        client: GrpcCost client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock cost servicer
    """
    name = f"test_negative_{secrets.token_hex(4)}"

    future = client_execution_thread_pool.submit(client.add, name, "gpt4_input", -100.0)

    service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
    method_desc = service_desc.methods_by_name["AddCost"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.AddCost(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), context._code, context._details)

    # Should fail with validation error
    with pytest.raises(Exception):
        future.result(timeout=5.0)


# ============================================================================
# Test: get() Method
# ============================================================================


def test_get_cost_success(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
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
    future_add = client_execution_thread_pool.submit(client.add, name, "gpt4_input", quantity)
    service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
    method_desc = service_desc.methods_by_name["AddCost"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)
    context = FakeContext()
    response = mock_servicer.AddCost(request, context)
    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")
    future_add.result(timeout=5.0)

    # Now get the cost
    future_get = client_execution_thread_pool.submit(client.get, name)

    method_desc = service_desc.methods_by_name["GetCost"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetCost(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future_get.result(timeout=5.0)
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], CostData)
    assert result[0].name == name
    assert result[0].quantity == quantity


def test_get_cost_not_found(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
) -> None:
    """Test getting a cost that doesn't exist.

    Args:
        client: GrpcCost client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock cost servicer
    """
    name = f"nonexistent_{secrets.token_hex(4)}"

    future = client_execution_thread_pool.submit(client.get, name)

    service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
    method_desc = service_desc.methods_by_name["GetCost"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetCost(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    assert isinstance(result, list)
    assert len(result) == 0


def test_get_cost_multiple_with_same_name(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
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
        future_add = client_execution_thread_pool.submit(client.add, name, "gpt4_input", quantity)
        method_desc = service_desc.methods_by_name["AddCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)
        context = FakeContext()
        response = mock_servicer.AddCost(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")
        future_add.result(timeout=5.0)

    # Get all costs with this name
    client_execution_thread_pool.submit(client.get, name)

    method_desc = service_desc.methods_by_name["GetCost"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetCost(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
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


def test_get_filtered_by_names(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
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
        future_add = client_execution_thread_pool.submit(client.add, name, "gpt4_input", 100.0)
        method_desc = service_desc.methods_by_name["AddCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)
        context = FakeContext()
        response = mock_servicer.AddCost(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")
        future_add.result(timeout=5.0)

    # Filter by subset of names
    filter_names = names[:3]
    future_get = client_execution_thread_pool.submit(client.get_filtered, names=filter_names)

    method_desc = service_desc.methods_by_name["GetCosts"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetCosts(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future_get.result(timeout=5.0)
    assert isinstance(result, list)
    assert len(result) == 3
    assert all(c.name in filter_names for c in result)


def test_get_filtered_by_cost_types(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
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
        future_add = client_execution_thread_pool.submit(client.add, name, config_name, 100.0)
        method_desc = service_desc.methods_by_name["AddCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)
        context = FakeContext()
        response = mock_servicer.AddCost(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")
        future_add.result(timeout=5.0)

    # Filter by token types only
    future_get = client_execution_thread_pool.submit(client.get_filtered, cost_types=["TOKEN_INPUT", "TOKEN_OUTPUT"])

    method_desc = service_desc.methods_by_name["GetCosts"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetCosts(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future_get.result(timeout=5.0)
    assert isinstance(result, list)
    assert len(result) == 2
    assert all(c.cost_type in {CostType.TOKEN_INPUT, CostType.TOKEN_OUTPUT} for c in result)


def test_get_filtered_by_names_and_types(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
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
        future_add = client_execution_thread_pool.submit(client.add, name, config, 100.0)
        method_desc = service_desc.methods_by_name["AddCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)
        context = FakeContext()
        response = mock_servicer.AddCost(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")
        future_add.result(timeout=5.0)

    # Filter by names and token input type
    future_get = client_execution_thread_pool.submit(
        client.get_filtered, names=["cost_a", "cost_d"], cost_types=["TOKEN_INPUT"]
    )

    method_desc = service_desc.methods_by_name["GetCosts"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetCosts(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future_get.result(timeout=5.0)
    assert isinstance(result, list)
    assert len(result) == 2
    assert all(c.name in {"cost_a", "cost_d"} for c in result)
    assert all(c.cost_type == CostType.TOKEN_INPUT for c in result)


def test_get_filtered_empty_results(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
) -> None:
    """Test filtering with no matching results.

    Args:
        client: GrpcCost client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock cost servicer
    """
    # Filter with non-existent names
    future = client_execution_thread_pool.submit(client.get_filtered, names=["nonexistent"])

    service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
    method_desc = service_desc.methods_by_name["GetCosts"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetCosts(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    assert isinstance(result, list)
    assert len(result) == 0


def test_get_filtered_no_filters(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
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
        future_add = client_execution_thread_pool.submit(client.add, name, "gpt4_input", 100.0)
        method_desc = service_desc.methods_by_name["AddCost"]
        _, request, rpc = test_channel.take_unary_unary(method_desc)
        context = FakeContext()
        response = mock_servicer.AddCost(request, context)
        rpc.send_initial_metadata(())
        rpc.terminate(response, (), grpc.StatusCode.OK, "")
        future_add.result(timeout=5.0)

    # Get all costs (no filter)
    future_get = client_execution_thread_pool.submit(client.get_filtered)

    method_desc = service_desc.methods_by_name["GetCosts"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetCosts(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future_get.result(timeout=5.0)
    assert isinstance(result, list)
    assert len(result) == 3


# ============================================================================
# Test: Edge Cases and Error Handling
# ============================================================================


def test_cost_with_special_characters_in_name(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
) -> None:
    """Test cost with special characters in name.

    Args:
        client: GrpcCost client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock cost servicer
    """
    name = "test-cost_123.special@chars"

    future = client_execution_thread_pool.submit(client.add, name, "gpt4_input", 100.0)

    service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
    method_desc = service_desc.methods_by_name["AddCost"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.AddCost(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    assert result is None

    # Verify stored
    stored_costs = mock_servicer.costs[client.mission_id]
    assert any(c["name"] == name for c in stored_costs)


def test_cost_with_very_large_quantity(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
) -> None:
    """Test cost with very large quantity.

    Args:
        client: GrpcCost client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock cost servicer
    """
    name = "large_quantity_test"
    quantity = 1_000_000_000.0  # 1 billion

    future = client_execution_thread_pool.submit(client.add, name, "gpt4_input", quantity)

    service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
    method_desc = service_desc.methods_by_name["AddCost"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.AddCost(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    assert result is None

    # Verify calculation
    stored_costs = mock_servicer.costs[client.mission_id]
    assert stored_costs[0]["quantity"] == quantity
    assert stored_costs[0]["cost"] == 0.00003 * quantity


def test_cost_with_fractional_quantity(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
) -> None:
    """Test cost with fractional quantity.

    Args:
        client: GrpcCost client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock cost servicer
    """
    name = "fractional_test"
    quantity = 123.456

    future = client_execution_thread_pool.submit(client.add, name, "gpt4_input", quantity)

    service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
    method_desc = service_desc.methods_by_name["AddCost"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.AddCost(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future.result(timeout=5.0)
    assert result is None

    # Verify stored correctly
    stored_costs = mock_servicer.costs[client.mission_id]
    assert abs(stored_costs[0]["quantity"] - quantity) < 0.0001


def test_multiple_missions_isolation(
    client: GrpcCost,
    test_channel: grpc_testing.Channel,
    mock_servicer: MockCostServicer,
) -> None:
    """Test that costs for different missions are isolated.

    Args:
        client: GrpcCost client for testing
        test_channel: Mock gRPC channel
        mock_servicer: Mock cost servicer
    """
    # Add costs for the test client's mission
    name1 = "mission1_cost"
    future = client_execution_thread_pool.submit(client.add, name1, "gpt4_input", 100.0)

    service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
    method_desc = service_desc.methods_by_name["AddCost"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.AddCost(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")
    future.result(timeout=5.0)

    # Manually add cost for a different mission
    different_mission_cost = {
        "cost": 50.0,
        "name": "mission2_cost",
        "unit": "tokens",
        "cost_type": CostType.TOKEN_INPUT,
        "mission_id": "different_mission",
        "rate": 0.00003,
        "quantity": 1000.0,
        "setup_version_id": "setup:2",
    }
    mock_servicer._validate_and_store_cost(different_mission_cost)

    # Get costs for original mission
    future_get = client_execution_thread_pool.submit(client.get_filtered)

    method_desc = service_desc.methods_by_name["GetCosts"]
    _, request, rpc = test_channel.take_unary_unary(method_desc)

    context = FakeContext()
    response = mock_servicer.GetCosts(request, context)

    rpc.send_initial_metadata(())
    rpc.terminate(response, (), grpc.StatusCode.OK, "")

    result = future_get.result(timeout=5.0)

    # Should only see costs from original mission
    assert len(result) == 1
    assert result[0].name == name1
    assert result[0].mission_id == client.mission_id
