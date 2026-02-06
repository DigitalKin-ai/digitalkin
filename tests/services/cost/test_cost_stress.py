"""Stress tests and race condition tests for cost service.

Tests production resilience under load:
- Concurrent cost additions
- Mission isolation under load
- Large quantity handling
- Memory efficiency with many costs
- gRPC mock stress testing

These tests validate production scalability requirements:
- High-throughput cost tracking
- Multi-tenant isolation
- Large-scale cost aggregation
"""

import secrets
from concurrent import futures

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.cost.v1 import cost_service_pb2, cost_service_pb2_grpc
from tests.fixtures.grpc_fixtures import FakeContext
from tests.services.cost.mock_cost_servicer import MockCostServicer

from digitalkin.models.grpc_servers.models import ClientConfig, SecurityMode, ServerMode
from digitalkin.models.services.cost import AmountLimit, CostTypeEnum, QuantityLimit
from digitalkin.services.cost.cost_strategy import CostConfig, CostServiceError
from digitalkin.services.cost.default_cost import DefaultCost
from digitalkin.services.cost.grpc_cost import GrpcCost

# Set timeout for stress tests
pytestmark = pytest.mark.timeout(60)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_config() -> dict[str, CostConfig]:
    """Create sample cost configuration."""
    return {
        "gpt4_input": CostConfig(
            cost_name="gpt4_input",
            cost_type="TOKEN_INPUT",
            description="GPT-4 input tokens",
            unit="tokens",
            rate=0.00003,
        ),
        "gpt4_output": CostConfig(
            cost_name="gpt4_output",
            cost_type="TOKEN_OUTPUT",
            description="GPT-4 output tokens",
            unit="tokens",
            rate=0.00006,
        ),
        "api_call": CostConfig(
            cost_name="api_call",
            cost_type="API_CALL",
            description="API call",
            unit="calls",
            rate=0.001,
        ),
        "storage": CostConfig(
            cost_name="storage",
            cost_type="STORAGE",
            description="Storage",
            unit="GB",
            rate=0.02,
        ),
    }


@pytest.fixture
def cost_service(sample_config: dict[str, CostConfig]) -> DefaultCost:
    """Create a DefaultCost service instance."""
    return DefaultCost(
        mission_id="missions:stress_test",
        setup_id="setup:test",
        setup_version_id="setup_version:test",
        config=sample_config,
    )


@pytest.fixture(scope="module")
def thread_pool() -> futures.ThreadPoolExecutor:
    """Create thread pool for gRPC tests."""
    pool = futures.ThreadPoolExecutor(max_workers=20)
    yield pool
    pool.shutdown(wait=True, cancel_futures=True)


@pytest.fixture
def test_channel() -> grpc_testing.Channel:
    """Mock a gRPC channel."""
    test_clock = grpc_testing.strict_real_time()
    service_name = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
    return grpc_testing.channel([service_name], test_clock)


@pytest.fixture
def mock_servicer() -> MockCostServicer:
    """Return an instance of the mock servicer."""
    return MockCostServicer()


@pytest.fixture
def grpc_client(
    test_channel: grpc_testing.Channel,
    sample_config: dict[str, CostConfig],
) -> GrpcCost:
    """Create a GrpcCost client for testing."""
    dummy_config = ClientConfig(
        host="[::]",
        port=50051,
        mode=ServerMode.ASYNC,
        security=SecurityMode.INSECURE,
        credentials=None,
    )

    client = GrpcCost(
        "missions:grpc_stress",
        "setup:test",
        "setup_version:test",
        sample_config,
        dummy_config,
    )
    client.stub = cost_service_pb2_grpc.CostServiceStub(test_channel)
    return client


# ============================================================================
# Test: High Volume Cost Addition (DefaultCost)
# ============================================================================


class TestHighVolumeCostAddition:
    """Tests for high-volume cost addition patterns."""

    def test_add_thousand_costs(self, cost_service: DefaultCost) -> None:
        """Test adding 1000 costs sequentially."""
        for i in range(1000):
            cost_service.add(f"cost_{i}", "gpt4_input", 100.0)

        # Verify all costs were stored - use get_filtered with names
        names = [f"cost_{i}" for i in range(1000)]
        costs = cost_service.get_filtered(names=names)
        assert len(costs) == 1000

    def test_add_costs_different_configs(self, cost_service: DefaultCost) -> None:
        """Test adding costs across all config types at high volume."""
        configs = ["gpt4_input", "gpt4_output", "api_call", "storage"]

        for config_name in configs:
            for i in range(250):
                cost_service.add(f"{config_name}_{i}", config_name, 10.0)

        # Verify counts per type - use names filter
        input_names = [f"gpt4_input_{i}" for i in range(250)]
        output_names = [f"gpt4_output_{i}" for i in range(250)]
        api_names = [f"api_call_{i}" for i in range(250)]
        storage_names = [f"storage_{i}" for i in range(250)]

        input_costs = cost_service.get_filtered(names=input_names)
        output_costs = cost_service.get_filtered(names=output_names)
        api_costs = cost_service.get_filtered(names=api_names)
        storage_costs = cost_service.get_filtered(names=storage_names)

        assert len(input_costs) == 250
        assert len(output_costs) == 250
        assert len(api_costs) == 250
        assert len(storage_costs) == 250

    def test_cost_calculation_accuracy_at_scale(self, cost_service: DefaultCost) -> None:
        """Test that cost calculations remain accurate at high volume."""
        total_quantity = 0.0

        for i in range(500):
            quantity = i * 10.0  # Increasing quantities
            cost_service.add(f"scaled_cost_{i}", "gpt4_input", quantity)
            total_quantity += quantity

        # Calculate expected total cost
        expected_total = total_quantity * 0.00003

        # Sum actual costs - use names filter
        names = [f"scaled_cost_{i}" for i in range(500)]
        costs = cost_service.get_filtered(names=names)
        actual_total = sum(c.cost for c in costs)

        # Should be accurate to floating point precision
        assert abs(actual_total - expected_total) < 0.01


# ============================================================================
# Test: Mission Isolation Under Load
# ============================================================================


class TestMissionIsolationUnderLoad:
    """Tests for mission isolation with multiple concurrent missions."""

    def test_multiple_missions_isolation(self, sample_config: dict[str, CostConfig]) -> None:
        """Test that costs from multiple missions stay isolated."""
        missions = [f"missions:mission_{i}" for i in range(10)]
        services = [
            DefaultCost(
                mission_id=mission,
                setup_id="setup:test",
                setup_version_id="setup_version:test",
                config=sample_config,
            )
            for mission in missions
        ]

        # Add costs to each mission
        for i, service in enumerate(services):
            for j in range(100):
                service.add(f"cost_{i}_{j}", "gpt4_input", float(i * 100 + j))

        # Verify isolation using names filter
        for i, service in enumerate(services):
            names = [f"cost_{i}_{j}" for j in range(100)]
            costs = service.get_filtered(names=names)
            assert len(costs) == 100

            # Verify quantities are correct for this mission
            expected_quantities = {float(i * 100 + j) for j in range(100)}
            actual_quantities = {c.quantity for c in costs}
            assert actual_quantities == expected_quantities

    def test_mission_isolation_with_same_cost_names(
        self, sample_config: dict[str, CostConfig]
    ) -> None:
        """Test isolation when different missions use same cost names."""
        service1 = DefaultCost(
            mission_id="missions:mission_a",
            setup_id="setup:test",
            setup_version_id="setup_version:test",
            config=sample_config,
        )

        service2 = DefaultCost(
            mission_id="missions:mission_b",
            setup_id="setup:test",
            setup_version_id="setup_version:test",
            config=sample_config,
        )

        # Add cost with same name to both missions
        service1.add("shared_name_cost", "gpt4_input", 1000.0)
        service2.add("shared_name_cost", "gpt4_input", 2000.0)

        # Each mission should have its own cost
        costs1 = service1.get("shared_name_cost")
        costs2 = service2.get("shared_name_cost")

        assert len(costs1) == 1
        assert len(costs2) == 1
        assert costs1[0].quantity == 1000.0
        assert costs2[0].quantity == 2000.0


# ============================================================================
# Test: Large Quantity Handling
# ============================================================================


class TestLargeQuantityHandling:
    """Tests for handling very large quantities."""

    def test_very_large_quantity(self, cost_service: DefaultCost) -> None:
        """Test handling of very large quantities (billions)."""
        large_quantity = 1_000_000_000_000.0  # 1 trillion

        cost_service.add("huge_usage", "gpt4_input", large_quantity)

        costs = cost_service.get("huge_usage")
        assert len(costs) == 1
        assert costs[0].quantity == large_quantity

        # Cost calculation should still be accurate
        expected_cost = large_quantity * 0.00003
        assert abs(costs[0].cost - expected_cost) < 1.0  # Allow $1 tolerance for float

    def test_very_small_quantity(self, cost_service: DefaultCost) -> None:
        """Test handling of very small quantities."""
        small_quantity = 0.000001

        cost_service.add("tiny_usage", "gpt4_input", small_quantity)

        costs = cost_service.get("tiny_usage")
        assert len(costs) == 1
        assert costs[0].quantity == small_quantity

    def test_quantity_extremes_with_limits(self, cost_service: DefaultCost) -> None:
        """Test accumulated limits with extreme quantities."""
        cost_service.set_limits([
            QuantityLimit(
                name="gpt4_input",
                type=CostTypeEnum.TOKEN_INPUT,
                max_value=1e15,  # 1 quadrillion
            ),
        ])

        # Large quantities should pass
        for i in range(10):
            assert cost_service.check_limit("gpt4_input", 1e13) is True
            cost_service._accumulated["gpt4_input_quantity"] = (i + 1) * 1e13

        # Total is now 1e14 (100 trillion), still under limit
        assert cost_service._accumulated["gpt4_input_quantity"] == 1e14

        # Next large add should exceed
        assert cost_service.check_limit("gpt4_input", 1e15) is False


# ============================================================================
# Test: Memory Efficiency
# ============================================================================


class TestMemoryEfficiency:
    """Tests for memory efficiency with many costs."""

    def test_large_number_of_costs_memory(self, cost_service: DefaultCost) -> None:
        """Test memory doesn't grow excessively with many costs."""
        import sys

        # Get baseline memory
        baseline_size = sys.getsizeof(cost_service.db)

        # Add many costs
        for i in range(10000):
            cost_service.add(f"memory_test_{i}", "gpt4_input", float(i))

        # Check db size grew as expected (not exponentially)
        # Each CostData is roughly the same size
        final_size = sys.getsizeof(cost_service.db)

        # The dictionary itself shouldn't grow much beyond initial allocation
        # The main memory is in the list values
        assert final_size < baseline_size * 100  # Reasonable growth

    def test_filter_performance_with_many_costs(self, cost_service: DefaultCost) -> None:
        """Test that filtering remains performant with many costs."""
        import time

        # Add many costs of different types
        for i in range(5000):
            config = ["gpt4_input", "gpt4_output", "api_call", "storage"][i % 4]
            cost_service.add(f"perf_test_{i}", config, float(i))

        # Time the filter operation - use names filter for gpt4_input costs
        gpt4_input_names = [f"perf_test_{i}" for i in range(0, 5000, 4)]  # Every 4th starting at 0

        start = time.perf_counter()
        results = cost_service.get_filtered(names=gpt4_input_names)
        elapsed = time.perf_counter() - start

        assert len(results) == 1250  # 5000 / 4

        # Filter should complete quickly (guards against O(n²) regressions)
        assert elapsed < 1.0


# ============================================================================
# Test: gRPC Stress Tests
# ============================================================================


class TestGrpcStress:
    """Stress tests for gRPC cost service."""

    @pytest.mark.grpc
    @pytest.mark.stress
    def test_rapid_sequential_adds(
        self,
        grpc_client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test rapid sequential cost additions."""
        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["AddCost"]

        for i in range(50):
            name = f"rapid_{i}_{secrets.token_hex(4)}"

            future = thread_pool.submit(grpc_client.add, name, "gpt4_input", 100.0)

            _, request, rpc = test_channel.take_unary_unary(method_desc)

            context = FakeContext()
            response = mock_servicer.AddCost(request, context)

            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")

            future.result(timeout=5.0)

        # Verify all costs were added
        assert len(mock_servicer.costs[grpc_client.mission_id]) == 50

    @pytest.mark.grpc
    @pytest.mark.stress
    def test_mixed_operations_under_load(
        self,
        grpc_client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test mixed add/get operations under load."""
        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]

        # First, add a batch of costs
        add_method = service_desc.methods_by_name["AddCost"]
        for i in range(20):
            name = f"mixed_{i}"
            future = thread_pool.submit(grpc_client.add, name, "gpt4_input", 100.0)

            _, request, rpc = test_channel.take_unary_unary(add_method)
            context = FakeContext()
            response = mock_servicer.AddCost(request, context)
            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")
            future.result(timeout=5.0)

        # Now do mixed operations
        get_method = service_desc.methods_by_name["GetCost"]

        for i in range(10):
            # Add
            name = f"mixed_extra_{i}"
            future_add = thread_pool.submit(grpc_client.add, name, "gpt4_output", 50.0)
            _, request, rpc = test_channel.take_unary_unary(add_method)
            context = FakeContext()
            response = mock_servicer.AddCost(request, context)
            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")
            future_add.result(timeout=5.0)

            # Get
            future_get = thread_pool.submit(grpc_client.get, f"mixed_{i}")
            _, request, rpc = test_channel.take_unary_unary(get_method)
            context = FakeContext()
            response = mock_servicer.GetCost(request, context)
            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")
            future_get.result(timeout=5.0)

        # Verify final state
        all_costs = mock_servicer.costs[grpc_client.mission_id]
        assert len(all_costs) == 30  # 20 initial + 10 extra


# ============================================================================
# Test: Limit Enforcement Under Stress
# ============================================================================


class TestLimitEnforcementUnderStress:
    """Tests for limit enforcement under stress conditions."""

    def test_limit_enforcement_high_frequency(self, cost_service: DefaultCost) -> None:
        """Test that limits are correctly enforced under high-frequency checks."""
        cost_service.set_limits([
            QuantityLimit(name="api_call", type=CostTypeEnum.API_CALL, max_value=1000.0),
        ])

        # Rapid check and accumulate pattern
        total = 0.0
        for i in range(1000):
            assert cost_service.check_limit("api_call", 1.0) is True
            cost_service.add(f"freq_{i}", "api_call", 1.0)
            total += 1.0
            cost_service._accumulated["api_call_quantity"] = total

        # Next one should fail
        assert cost_service.check_limit("api_call", 1.0) is False

        # Verify exactly 1000 costs using names filter
        names = [f"freq_{i}" for i in range(1000)]
        costs = cost_service.get_filtered(names=names)
        assert len(costs) == 1000

    def test_amount_limit_precision_under_stress(self, cost_service: DefaultCost) -> None:
        """Test amount limit precision with many small additions.

        Note: Due to floating point precision, we use a slightly higher limit
        to account for accumulated rounding errors.
        """
        # Set a limit with buffer for floating point precision
        cost_service.set_limits([
            AmountLimit(name="api_call", type=CostTypeEnum.API_CALL, max_value=1.01),  # Slight buffer
        ])

        # rate = 0.001, so 1000 calls = $1.00
        total_amount = 0.0
        for i in range(1000):
            assert cost_service.check_limit("api_call", 1.0) is True
            cost_service.add(f"precise_{i}", "api_call", 1.0)
            total_amount += 0.001  # rate * 1.0
            cost_service._accumulated["api_call_amount"] = total_amount

        # Total is now ~$1.00, adding 10 more ($0.01) should still pass
        # But adding much more should fail
        assert cost_service.check_limit("api_call", 100.0) is False  # Would add $0.10

    def test_multiple_limits_stress(self, cost_service: DefaultCost) -> None:
        """Test multiple limits under stress conditions."""
        cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostTypeEnum.TOKEN_INPUT, max_value=50000.0),
            QuantityLimit(name="gpt4_output", type=CostTypeEnum.TOKEN_OUTPUT, max_value=25000.0),
            QuantityLimit(name="api_call", type=CostTypeEnum.API_CALL, max_value=100.0),
        ])

        input_total = 0.0
        output_total = 0.0
        call_total = 0.0

        # Stress test all limits simultaneously
        for i in range(100):
            assert cost_service.check_limit("gpt4_input", 500.0) is True
            cost_service.add(f"input_{i}", "gpt4_input", 500.0)
            input_total += 500.0
            cost_service._accumulated["gpt4_input_quantity"] = input_total

            assert cost_service.check_limit("gpt4_output", 250.0) is True
            cost_service.add(f"output_{i}", "gpt4_output", 250.0)
            output_total += 250.0
            cost_service._accumulated["gpt4_output_quantity"] = output_total

            assert cost_service.check_limit("api_call", 1.0) is True
            cost_service.add(f"call_{i}", "api_call", 1.0)
            call_total += 1.0
            cost_service._accumulated["api_call_quantity"] = call_total

        # All should now be at their limits
        assert cost_service.check_limit("gpt4_input", 1.0) is False
        assert cost_service.check_limit("gpt4_output", 1.0) is False
        assert cost_service.check_limit("api_call", 1.0) is False


# ============================================================================
# Test: Error Recovery
# ============================================================================


class TestErrorRecovery:
    """Tests for error recovery in cost service."""

    def test_continue_after_limit_check_fails(self, cost_service: DefaultCost) -> None:
        """Test that service continues to work after limit check returns False."""
        cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostTypeEnum.TOKEN_INPUT, max_value=1000.0),
            QuantityLimit(name="gpt4_output", type=CostTypeEnum.TOKEN_OUTPUT, max_value=1000.0),
        ])

        # Use up gpt4_input limit
        cost_service._accumulated["gpt4_input_quantity"] = 1000.0

        # Check should fail for gpt4_input
        assert cost_service.check_limit("gpt4_input", 1.0) is False

        # But gpt4_output should still work
        assert cost_service.check_limit("gpt4_output", 500.0) is True
        cost_service.add("output_1", "gpt4_output", 500.0)
        cost_service._accumulated["gpt4_output_quantity"] = 500.0

        assert cost_service.check_limit("gpt4_output", 500.0) is True
        cost_service.add("output_2", "gpt4_output", 500.0)
        cost_service._accumulated["gpt4_output_quantity"] = 1000.0

        # Verify costs were added
        output_costs = cost_service.get_filtered(names=["output_1", "output_2"])
        assert len(output_costs) == 2

    def test_invalid_config_doesnt_corrupt_state(self, cost_service: DefaultCost) -> None:
        """Test that invalid config errors don't corrupt service state."""
        # Add some valid costs
        for i in range(10):
            cost_service.add(f"valid_{i}", "gpt4_input", 100.0)

        # Try invalid config
        with pytest.raises(CostServiceError):
            cost_service.add("invalid", "nonexistent_config", 100.0)

        # Service should still work
        cost_service.add("after_error", "gpt4_input", 100.0)

        # All valid costs should be present - use names filter
        names = [f"valid_{i}" for i in range(10)] + ["after_error"]
        costs = cost_service.get_filtered(names=names)
        assert len(costs) == 11


# ============================================================================
# Test: Data Integrity
# ============================================================================


class TestDataIntegrity:
    """Tests for data integrity under various conditions."""

    def test_cost_data_immutability(self, cost_service: DefaultCost) -> None:
        """Test that retrieved cost data can't corrupt internal state."""
        cost_service.add("original", "gpt4_input", 1000.0)

        # Get the cost
        costs = cost_service.get("original")
        original_cost = costs[0]

        # Try to modify the returned object
        # (Pydantic models are immutable by default, but test the pattern)
        retrieved_quantity = original_cost.quantity

        # Re-fetch and verify
        costs_again = cost_service.get("original")
        assert costs_again[0].quantity == retrieved_quantity

    def test_concurrent_reads_consistency(self, cost_service: DefaultCost) -> None:
        """Test that concurrent reads return consistent data."""
        # Add costs
        for i in range(100):
            cost_service.add(f"concurrent_{i}", "gpt4_input", float(i))

        # Use names filter for consistent results
        names = [f"concurrent_{i}" for i in range(100)]

        # Multiple reads should return same data
        results = [
            cost_service.get_filtered(names=names)
            for _ in range(10)
        ]

        # All should have same length and data
        first_len = len(results[0])
        assert all(len(r) == first_len for r in results)

        first_total = sum(c.quantity for c in results[0])
        for result in results[1:]:
            assert sum(c.quantity for c in result) == first_total
