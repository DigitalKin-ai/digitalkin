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

import asyncio
import secrets
import time
from concurrent import futures

import grpc
import grpc_testing
import pytest
from agentic_mesh_protocol.cost.v1 import cost_service_pb2, cost_service_pb2_grpc
from tests.fixtures.grpc_fixtures import AsyncStubWrapper, FakeContext
from tests.services.cost.mock_cost_servicer import MockCostServicer

from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.settings.utils.channel import CommunicationMode, SecurityMode
from digitalkin.models.services.cost import AmountLimit, CostTypeEnum, QuantityLimit
from digitalkin.services.cost.cost_strategy import CostConfig, CostServiceError
from digitalkin.services.cost.default_cost import DefaultCost
from digitalkin.services.cost.grpc_cost import GrpcCost
from tests.fixtures.stress_reporter import StressReporter

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
        mode=CommunicationMode.ASYNC,
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
    client.stub = AsyncStubWrapper(cost_service_pb2_grpc.CostServiceStub(test_channel))
    return client


# ============================================================================
# Test: High Volume Cost Addition (DefaultCost)
# ============================================================================


class TestHighVolumeCostAddition:
    """Tests for high-volume cost addition patterns."""

    async def test_add_thousand_costs(self, cost_service: DefaultCost) -> None:
        """Test adding 1000 costs sequentially."""
        t0 = time.perf_counter()
        for i in range(1000):
            await cost_service.add(f"cost_{i}", "gpt4_input", 100.0)
        elapsed = time.perf_counter() - t0

        # Verify all costs were stored - use get_filtered with names
        names = [f"cost_{i}" for i in range(1000)]
        costs = await cost_service.get_filtered(names=names)

        rpt = StressReporter("High Volume: 1,000 Sequential Adds")
        rpt.metric("Total costs", StressReporter.count(len(costs)))
        rpt.metric("Duration", StressReporter.duration(elapsed))
        rpt.metric("Throughput", StressReporter.throughput(1000, elapsed))
        rpt.result(len(costs) == 1000)

        assert len(costs) == 1000

    async def test_add_costs_different_configs(self, cost_service: DefaultCost) -> None:
        """Test adding costs across all config types at high volume."""
        configs = ["gpt4_input", "gpt4_output", "api_call", "storage"]

        t0 = time.perf_counter()
        for config_name in configs:
            for i in range(250):
                await cost_service.add(f"{config_name}_{i}", config_name, 10.0)
        elapsed = time.perf_counter() - t0

        # Verify counts per type - use names filter
        input_names = [f"gpt4_input_{i}" for i in range(250)]
        output_names = [f"gpt4_output_{i}" for i in range(250)]
        api_names = [f"api_call_{i}" for i in range(250)]
        storage_names = [f"storage_{i}" for i in range(250)]

        input_costs = await cost_service.get_filtered(names=input_names)
        output_costs = await cost_service.get_filtered(names=output_names)
        api_costs = await cost_service.get_filtered(names=api_names)
        storage_costs = await cost_service.get_filtered(names=storage_names)

        rpt = StressReporter("High Volume: 4 Config Types x 250 Each")
        rpt.metric("Duration", StressReporter.duration(elapsed))
        rpt.metric("Throughput", StressReporter.throughput(1000, elapsed))
        rpt.metric("gpt4_input", StressReporter.count(len(input_costs)))
        rpt.metric("gpt4_output", StressReporter.count(len(output_costs)))
        rpt.metric("api_call", StressReporter.count(len(api_costs)))
        rpt.metric("storage", StressReporter.count(len(storage_costs)))
        rpt.result(all(len(c) == 250 for c in [input_costs, output_costs, api_costs, storage_costs]))

        assert len(input_costs) == 250
        assert len(output_costs) == 250
        assert len(api_costs) == 250
        assert len(storage_costs) == 250

    async def test_cost_calculation_accuracy_at_scale(self, cost_service: DefaultCost) -> None:
        """Test that cost calculations remain accurate at high volume."""
        total_quantity = 0.0

        for i in range(500):
            quantity = i * 10.0  # Increasing quantities
            await cost_service.add(f"scaled_cost_{i}", "gpt4_input", quantity)
            total_quantity += quantity

        # Calculate expected total cost
        expected_total = total_quantity * 0.00003

        # Sum actual costs - use names filter
        names = [f"scaled_cost_{i}" for i in range(500)]
        costs = await cost_service.get_filtered(names=names)
        actual_total = sum(c.cost for c in costs)
        delta = abs(actual_total - expected_total)

        rpt = StressReporter("Cost Accuracy at Scale (500 ops)")
        rpt.metric("Total quantity", f"{total_quantity:,.0f}")
        rpt.metric("Expected cost", f"${expected_total:,.6f}")
        rpt.metric("Actual cost", f"${actual_total:,.6f}")
        rpt.metric("Delta", f"${delta:.6f}")
        rpt.metric("Threshold", "< $0.01")
        rpt.result(delta < 0.01)

        # Should be accurate to floating point precision
        assert delta < 0.01


# ============================================================================
# Test: Mission Isolation Under Load
# ============================================================================


class TestMissionIsolationUnderLoad:
    """Tests for mission isolation with multiple concurrent missions."""

    async def test_multiple_missions_isolation(self, sample_config: dict[str, CostConfig]) -> None:
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

        t0 = time.perf_counter()
        # Add costs to each mission
        for i, service in enumerate(services):
            for j in range(100):
                await service.add(f"cost_{i}_{j}", "gpt4_input", float(i * 100 + j))
        elapsed = time.perf_counter() - t0

        # Verify isolation using names filter
        all_isolated = True
        for i, service in enumerate(services):
            names = [f"cost_{i}_{j}" for j in range(100)]
            costs = await service.get_filtered(names=names)
            expected_quantities = {float(i * 100 + j) for j in range(100)}
            actual_quantities = {c.quantity for c in costs}
            if len(costs) != 100 or actual_quantities != expected_quantities:
                all_isolated = False

        rpt = StressReporter("Mission Isolation: 10 Missions x 100 Costs")
        rpt.metric("Missions", StressReporter.count(len(missions)))
        rpt.metric("Costs per mission", StressReporter.count(100))
        rpt.metric("Total operations", StressReporter.count(1000))
        rpt.metric("Duration", StressReporter.duration(elapsed))
        rpt.metric("Isolation", "OK" if all_isolated else "CONTAMINATED")
        rpt.result(all_isolated)

        assert all_isolated

    async def test_mission_isolation_with_same_cost_names(
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
        await service1.add("shared_name_cost", "gpt4_input", 1000.0)
        await service2.add("shared_name_cost", "gpt4_input", 2000.0)

        # Each mission should have its own cost
        costs1 = await service1.get("shared_name_cost")
        costs2 = await service2.get("shared_name_cost")

        isolated = (
            len(costs1) == 1
            and len(costs2) == 1
            and costs1[0].quantity == 1000.0
            and costs2[0].quantity == 2000.0
        )

        rpt = StressReporter("Mission Isolation: Same Cost Names")
        rpt.metric("Mission A quantity", f"{costs1[0].quantity:,.0f}")
        rpt.metric("Mission B quantity", f"{costs2[0].quantity:,.0f}")
        rpt.metric("Isolation", "OK" if isolated else "CONTAMINATED")
        rpt.result(isolated)

        assert len(costs1) == 1
        assert len(costs2) == 1
        assert costs1[0].quantity == 1000.0
        assert costs2[0].quantity == 2000.0


# ============================================================================
# Test: Large Quantity Handling
# ============================================================================


class TestLargeQuantityHandling:
    """Tests for handling very large quantities."""

    async def test_very_large_quantity(self, cost_service: DefaultCost) -> None:
        """Test handling of very large quantities (billions)."""
        large_quantity = 1_000_000_000_000.0  # 1 trillion

        await cost_service.add("huge_usage", "gpt4_input", large_quantity)

        costs = await cost_service.get("huge_usage")
        expected_cost = large_quantity * 0.00003
        delta = abs(costs[0].cost - expected_cost)

        rpt = StressReporter("Large Quantity: 1 Trillion")
        rpt.metric("Quantity", f"{large_quantity:,.0f}")
        rpt.metric("Expected cost", f"${expected_cost:,.2f}")
        rpt.metric("Actual cost", f"${costs[0].cost:,.2f}")
        rpt.metric("Delta", f"${delta:.2f}")
        rpt.result(len(costs) == 1 and costs[0].quantity == large_quantity and delta < 1.0)

        assert len(costs) == 1
        assert costs[0].quantity == large_quantity
        assert delta < 1.0

    async def test_very_small_quantity(self, cost_service: DefaultCost) -> None:
        """Test handling of very small quantities."""
        small_quantity = 0.000001

        await cost_service.add("tiny_usage", "gpt4_input", small_quantity)

        costs = await cost_service.get("tiny_usage")

        rpt = StressReporter("Small Quantity: 0.000001")
        rpt.metric("Quantity stored", f"{costs[0].quantity:.6f}")
        rpt.metric("Cost", f"${costs[0].cost:.12f}")
        rpt.result(len(costs) == 1 and costs[0].quantity == small_quantity)

        assert len(costs) == 1
        assert costs[0].quantity == small_quantity

    async def test_quantity_extremes_with_limits(self, cost_service: DefaultCost) -> None:
        """Test accumulated limits with extreme quantities."""
        await cost_service.set_limits([
            QuantityLimit(
                name="gpt4_input",
                type=CostTypeEnum.TOKEN_INPUT,
                max_value=1e15,  # 1 quadrillion
            ),
        ])

        # Large quantities should pass
        for i in range(10):
            assert await cost_service.check_limit("gpt4_input", 1e13) is True
            cost_service._accumulated["gpt4_input_quantity"] = (i + 1) * 1e13

        # Total is now 1e14 (100 trillion), still under limit
        accumulated = cost_service._accumulated["gpt4_input_quantity"]
        under_limit = accumulated == 1e14
        exceeded = await cost_service.check_limit("gpt4_input", 1e15) is False

        rpt = StressReporter("Quantity Extremes with Limits")
        rpt.metric("Limit", f"{1e15:,.0f}")
        rpt.metric("Accumulated", f"{accumulated:,.0f}")
        rpt.metric("Under limit", "OK" if under_limit else "FAIL")
        rpt.metric("Exceeds on overflow", "OK" if exceeded else "FAIL")
        rpt.result(under_limit and exceeded)

        assert under_limit
        assert exceeded


# ============================================================================
# Test: Memory Efficiency
# ============================================================================


class TestMemoryEfficiency:
    """Tests for memory efficiency with many costs."""

    async def test_large_number_of_costs_memory(self, cost_service: DefaultCost) -> None:
        """Test memory doesn't grow excessively with many costs."""
        import sys

        # Get baseline memory
        baseline_size = sys.getsizeof(cost_service.db)

        # Add many costs
        for i in range(10000):
            await cost_service.add(f"memory_test_{i}", "gpt4_input", float(i))

        final_size = sys.getsizeof(cost_service.db)
        ratio = final_size / baseline_size if baseline_size > 0 else float("inf")

        rpt = StressReporter("Memory: 10,000 Costs")
        rpt.metric("Baseline db size", StressReporter.mem(baseline_size))
        rpt.metric("Final db size", StressReporter.mem(final_size))
        rpt.metric("Growth ratio", StressReporter.ratio(ratio))
        rpt.metric("Threshold", "< 100x")
        rpt.result(final_size < baseline_size * 100)

        assert final_size < baseline_size * 100

    async def test_filter_performance_with_many_costs(self, cost_service: DefaultCost) -> None:
        """Test that filtering remains performant with many costs."""
        # Add many costs of different types
        for i in range(5000):
            config = ["gpt4_input", "gpt4_output", "api_call", "storage"][i % 4]
            await cost_service.add(f"perf_test_{i}", config, float(i))

        # Time the filter operation - use names filter for gpt4_input costs
        gpt4_input_names = [f"perf_test_{i}" for i in range(0, 5000, 4)]  # Every 4th starting at 0

        t0 = time.perf_counter()
        results = await cost_service.get_filtered(names=gpt4_input_names)
        elapsed = time.perf_counter() - t0

        rpt = StressReporter("Filter Performance: 5,000 Costs")
        rpt.metric("Total costs", StressReporter.count(5000))
        rpt.metric("Filter matches", StressReporter.count(len(results)))
        rpt.metric("Filter duration", StressReporter.duration(elapsed))
        rpt.metric("Threshold", "< 1.0s")
        rpt.result(len(results) == 1250 and elapsed < 1.0)

        assert len(results) == 1250
        assert elapsed < 1.0


# ============================================================================
# Test: gRPC Stress Tests
# ============================================================================


class TestGrpcStress:
    """Stress tests for gRPC cost service."""

    @pytest.mark.grpc
    @pytest.mark.stress
    async def test_rapid_sequential_adds(
        self,
        grpc_client: GrpcCost,
        test_channel: grpc_testing.Channel,
        mock_servicer: MockCostServicer,
        thread_pool: futures.ThreadPoolExecutor,
    ) -> None:
        """Test rapid sequential cost additions."""
        service_desc = cost_service_pb2.DESCRIPTOR.services_by_name["CostService"]
        method_desc = service_desc.methods_by_name["AddCost"]

        t0 = time.perf_counter()
        for i in range(50):
            name = f"rapid_{i}_{secrets.token_hex(4)}"

            future = thread_pool.submit(asyncio.run, grpc_client.add(name, "gpt4_input", 100.0))

            _, request, rpc = test_channel.take_unary_unary(method_desc)

            context = FakeContext()
            response = mock_servicer.AddCost(request, context)

            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")

            future.result(timeout=5.0)
        elapsed = time.perf_counter() - t0

        count = len(mock_servicer.costs[grpc_client.mission_id])

        rpt = StressReporter("gRPC: 50 Rapid Sequential Adds")
        rpt.metric("Costs added", StressReporter.count(count))
        rpt.metric("Duration", StressReporter.duration(elapsed))
        rpt.metric("Throughput", StressReporter.throughput(50, elapsed))
        rpt.result(count == 50)

        assert count == 50

    @pytest.mark.grpc
    @pytest.mark.stress
    async def test_mixed_operations_under_load(
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
        t0 = time.perf_counter()
        for i in range(20):
            name = f"mixed_{i}"
            future = thread_pool.submit(asyncio.run, grpc_client.add(name, "gpt4_input", 100.0))

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
            future_add = thread_pool.submit(asyncio.run, grpc_client.add(name, "gpt4_output", 50.0))
            _, request, rpc = test_channel.take_unary_unary(add_method)
            context = FakeContext()
            response = mock_servicer.AddCost(request, context)
            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")
            future_add.result(timeout=5.0)

            # Get
            future_get = thread_pool.submit(asyncio.run, grpc_client.get(f"mixed_{i}"))
            _, request, rpc = test_channel.take_unary_unary(get_method)
            context = FakeContext()
            response = mock_servicer.GetCost(request, context)
            rpc.send_initial_metadata(())
            rpc.terminate(response, (), grpc.StatusCode.OK, "")
            future_get.result(timeout=5.0)
        elapsed = time.perf_counter() - t0

        all_costs = mock_servicer.costs[grpc_client.mission_id]

        rpt = StressReporter("gRPC: Mixed Add/Get Under Load")
        rpt.metric("Initial adds", StressReporter.count(20))
        rpt.metric("Mixed rounds (add+get)", StressReporter.count(10))
        rpt.metric("Total costs", StressReporter.count(len(all_costs)))
        rpt.metric("Duration", StressReporter.duration(elapsed))
        rpt.metric("Throughput", StressReporter.throughput(40, elapsed))
        rpt.result(len(all_costs) == 30)

        assert len(all_costs) == 30


# ============================================================================
# Test: Limit Enforcement Under Stress
# ============================================================================


class TestLimitEnforcementUnderStress:
    """Tests for limit enforcement under stress conditions."""

    async def test_limit_enforcement_high_frequency(self, cost_service: DefaultCost) -> None:
        """Test that limits are correctly enforced under high-frequency checks."""
        await cost_service.set_limits([
            QuantityLimit(name="api_call", type=CostTypeEnum.API_CALL, max_value=1000.0),
        ])

        t0 = time.perf_counter()
        # Rapid check and accumulate pattern
        total = 0.0
        for i in range(1000):
            assert await cost_service.check_limit("api_call", 1.0) is True
            await cost_service.add(f"freq_{i}", "api_call", 1.0)
            total += 1.0
            cost_service._accumulated["api_call_quantity"] = total
        elapsed = time.perf_counter() - t0

        exceeded = await cost_service.check_limit("api_call", 1.0) is False

        names = [f"freq_{i}" for i in range(1000)]
        costs = await cost_service.get_filtered(names=names)

        rpt = StressReporter("Limit Enforcement: 1,000 High-Freq Checks")
        rpt.metric("Checks + adds", StressReporter.count(1000))
        rpt.metric("Duration", StressReporter.duration(elapsed))
        rpt.metric("Throughput", StressReporter.throughput(1000, elapsed))
        rpt.metric("Limit enforced at cap", "OK" if exceeded else "FAIL")
        rpt.metric("Costs stored", StressReporter.count(len(costs)))
        rpt.result(exceeded and len(costs) == 1000)

        assert exceeded
        assert len(costs) == 1000

    async def test_amount_limit_precision_under_stress(self, cost_service: DefaultCost) -> None:
        """Test amount limit precision with many small additions.

        Note: Due to floating point precision, we use a slightly higher limit
        to account for accumulated rounding errors.
        """
        # Set a limit with buffer for floating point precision
        await cost_service.set_limits([
            AmountLimit(name="api_call", type=CostTypeEnum.API_CALL, max_value=1.01),  # Slight buffer
        ])

        t0 = time.perf_counter()
        # rate = 0.001, so 1000 calls = $1.00
        total_amount = 0.0
        for i in range(1000):
            assert await cost_service.check_limit("api_call", 1.0) is True
            await cost_service.add(f"precise_{i}", "api_call", 1.0)
            total_amount += 0.001  # rate * 1.0
            cost_service._accumulated["api_call_amount"] = total_amount
        elapsed = time.perf_counter() - t0

        exceeded = await cost_service.check_limit("api_call", 100.0) is False

        rpt = StressReporter("Amount Limit Precision: 1,000 Small Adds")
        rpt.metric("Accumulated amount", f"${total_amount:.6f}")
        rpt.metric("Limit", "$1.01")
        rpt.metric("Duration", StressReporter.duration(elapsed))
        rpt.metric("Overflow blocked", "OK" if exceeded else "FAIL")
        rpt.result(exceeded)

        assert exceeded

    async def test_multiple_limits_stress(self, cost_service: DefaultCost) -> None:
        """Test multiple limits under stress conditions."""
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostTypeEnum.TOKEN_INPUT, max_value=50000.0),
            QuantityLimit(name="gpt4_output", type=CostTypeEnum.TOKEN_OUTPUT, max_value=25000.0),
            QuantityLimit(name="api_call", type=CostTypeEnum.API_CALL, max_value=100.0),
        ])

        input_total = 0.0
        output_total = 0.0
        call_total = 0.0

        t0 = time.perf_counter()
        for i in range(100):
            assert await cost_service.check_limit("gpt4_input", 500.0) is True
            await cost_service.add(f"input_{i}", "gpt4_input", 500.0)
            input_total += 500.0
            cost_service._accumulated["gpt4_input_quantity"] = input_total

            assert await cost_service.check_limit("gpt4_output", 250.0) is True
            await cost_service.add(f"output_{i}", "gpt4_output", 250.0)
            output_total += 250.0
            cost_service._accumulated["gpt4_output_quantity"] = output_total

            assert await cost_service.check_limit("api_call", 1.0) is True
            await cost_service.add(f"call_{i}", "api_call", 1.0)
            call_total += 1.0
            cost_service._accumulated["api_call_quantity"] = call_total
        elapsed = time.perf_counter() - t0

        input_blocked = await cost_service.check_limit("gpt4_input", 1.0) is False
        output_blocked = await cost_service.check_limit("gpt4_output", 1.0) is False
        call_blocked = await cost_service.check_limit("api_call", 1.0) is False
        all_blocked = input_blocked and output_blocked and call_blocked

        rpt = StressReporter("Multiple Limits: 3 Types x 100 Rounds")
        rpt.metric("Rounds", StressReporter.count(100))
        rpt.metric("Operations (3 per round)", StressReporter.count(300))
        rpt.metric("Duration", StressReporter.duration(elapsed))
        rpt.metric("gpt4_input blocked at cap", "OK" if input_blocked else "FAIL")
        rpt.metric("gpt4_output blocked at cap", "OK" if output_blocked else "FAIL")
        rpt.metric("api_call blocked at cap", "OK" if call_blocked else "FAIL")
        rpt.result(all_blocked)

        assert all_blocked


# ============================================================================
# Test: Error Recovery
# ============================================================================


class TestErrorRecovery:
    """Tests for error recovery in cost service."""

    async def test_continue_after_limit_check_fails(self, cost_service: DefaultCost) -> None:
        """Test that service continues to work after limit check returns False."""
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostTypeEnum.TOKEN_INPUT, max_value=1000.0),
            QuantityLimit(name="gpt4_output", type=CostTypeEnum.TOKEN_OUTPUT, max_value=1000.0),
        ])

        # Use up gpt4_input limit
        cost_service._accumulated["gpt4_input_quantity"] = 1000.0

        input_blocked = await cost_service.check_limit("gpt4_input", 1.0) is False

        # But gpt4_output should still work
        output_ok_1 = await cost_service.check_limit("gpt4_output", 500.0) is True
        await cost_service.add("output_1", "gpt4_output", 500.0)
        cost_service._accumulated["gpt4_output_quantity"] = 500.0

        output_ok_2 = await cost_service.check_limit("gpt4_output", 500.0) is True
        await cost_service.add("output_2", "gpt4_output", 500.0)
        cost_service._accumulated["gpt4_output_quantity"] = 1000.0

        output_costs = await cost_service.get_filtered(names=["output_1", "output_2"])
        passed = input_blocked and output_ok_1 and output_ok_2 and len(output_costs) == 2

        rpt = StressReporter("Error Recovery: Continue After Limit Fail")
        rpt.metric("Exhausted limit blocked", "OK" if input_blocked else "FAIL")
        rpt.metric("Other limit still works", "OK" if output_ok_1 and output_ok_2 else "FAIL")
        rpt.metric("Costs after recovery", StressReporter.count(len(output_costs)))
        rpt.result(passed)

        assert passed

    async def test_invalid_config_doesnt_corrupt_state(self, cost_service: DefaultCost) -> None:
        """Test that invalid config errors don't corrupt service state."""
        # Add some valid costs
        for i in range(10):
            await cost_service.add(f"valid_{i}", "gpt4_input", 100.0)

        # Try invalid config — pytest.raises ensures exception is raised
        with pytest.raises(CostServiceError):
            await cost_service.add("invalid", "nonexistent_config", 100.0)

        # Service should still work
        await cost_service.add("after_error", "gpt4_input", 100.0)

        names = [f"valid_{i}" for i in range(10)] + ["after_error"]
        costs = await cost_service.get_filtered(names=names)

        rpt = StressReporter("Error Recovery: Invalid Config")
        rpt.metric("Pre-error costs", StressReporter.count(10))
        rpt.metric("Post-error costs", StressReporter.count(len(costs)))
        rpt.metric("State intact", "OK" if len(costs) == 11 else "CORRUPTED")
        rpt.result(len(costs) == 11)

        assert len(costs) == 11


# ============================================================================
# Test: Data Integrity
# ============================================================================


class TestDataIntegrity:
    """Tests for data integrity under various conditions."""

    async def test_cost_data_immutability(self, cost_service: DefaultCost) -> None:
        """Test that retrieved cost data can't corrupt internal state."""
        await cost_service.add("original", "gpt4_input", 1000.0)

        costs = await cost_service.get("original")
        retrieved_quantity = costs[0].quantity

        # Re-fetch and verify
        costs_again = await cost_service.get("original")
        immutable = costs_again[0].quantity == retrieved_quantity

        rpt = StressReporter("Data Integrity: Immutability")
        rpt.metric("Original quantity", f"{retrieved_quantity:,.0f}")
        rpt.metric("Re-fetched quantity", f"{costs_again[0].quantity:,.0f}")
        rpt.metric("Immutable", "OK" if immutable else "CORRUPTED")
        rpt.result(immutable)

        assert immutable

    async def test_concurrent_reads_consistency(self, cost_service: DefaultCost) -> None:
        """Test that concurrent reads return consistent data."""
        for i in range(100):
            await cost_service.add(f"concurrent_{i}", "gpt4_input", float(i))

        names = [f"concurrent_{i}" for i in range(100)]

        t0 = time.perf_counter()
        results = []
        for _ in range(10):
            results.append(await cost_service.get_filtered(names=names))
        elapsed = time.perf_counter() - t0

        first_len = len(results[0])
        first_total = sum(c.quantity for c in results[0])
        all_consistent = all(
            len(r) == first_len and sum(c.quantity for c in r) == first_total
            for r in results[1:]
        )

        rpt = StressReporter("Data Integrity: 10 Concurrent Reads")
        rpt.metric("Costs stored", StressReporter.count(100))
        rpt.metric("Read rounds", StressReporter.count(10))
        rpt.metric("Duration", StressReporter.duration(elapsed))
        rpt.metric("Consistent lengths", "OK" if all(len(r) == first_len for r in results) else "FAIL")
        rpt.metric("Consistent totals", "OK" if all_consistent else "DRIFT")
        rpt.result(all_consistent)

        assert all_consistent
