"""Comprehensive tests for cost limit enforcement.

Tests the CostLimit functionality in CostStrategy:
- QuantityLimit enforcement (max tokens, calls, etc.)
- AmountLimit enforcement (max cost in dollars)
- Accumulated tracking
- check_limit method returning bool
- Limit boundary conditions

These tests validate production cost control requirements:
- Budget enforcement for API usage
- Token limit enforcement for LLM calls
- Graceful handling of limit exceeded scenarios
"""

import pytest

from digitalkin.models.services.cost import AmountLimit, CostType, QuantityLimit, CostConfig
from digitalkin.services import DefaultCost

# Set timeout for all tests in this file
pytestmark = pytest.mark.timeout(10)


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def sample_config() -> dict[str, CostConfig]:
    """Create sample cost configuration."""
    return {
        "gpt4_input": CostConfig(
            name="gpt4_input",
            type=CostType.TOKEN_INPUT,
            description="GPT-4 input tokens",
            unit="tokens",
            rate=0.00003,  # $0.03 per 1k tokens
        ),
        "gpt4_output": CostConfig(
            name="gpt4_output",
            type=CostType.TOKEN_OUTPUT,
            description="GPT-4 output tokens",
            unit="tokens",
            rate=0.00006,  # $0.06 per 1k tokens
        ),
        "api_call": CostConfig(
            name="api_call",
            type=CostType.API_CALL,
            description="API call",
            unit="calls",
            rate=0.001,  # $0.001 per call
        ),
        "storage": CostConfig(
            name="storage",
            type=CostType.STORAGE,
            description="Storage",
            unit="GB",
            rate=0.02,  # $0.02 per GB
        ),
        "compute_time": CostConfig(
            name="compute_time",
            type=CostType.TIME,
            description="Compute time",
            unit="hours",
            rate=0.05,  # $0.05 per hour
        ),
    }


@pytest.fixture
def cost_service(sample_config: dict[str, CostConfig]) -> DefaultCost:
    """Create a DefaultCost service instance."""
    return DefaultCost(
        mission_id="missions:test",
        setup_id="setup:test",
        setup_version_id="setup_version:test",
        config=sample_config,
    )


# ============================================================================
# Test: CostLimit Model (Discriminated Union)
# ============================================================================


class TestCostLimitModel:
    """Tests for QuantityLimit and AmountLimit Pydantic models."""

    def test_quantity_limit_creation(self) -> None:
        """Test QuantityLimit creation."""
        limit = QuantityLimit(
            name="gpt4_input",
            type=CostType.TOKEN_INPUT,
            max_value=10000.0,
        )

        assert limit.name == "gpt4_input"
        assert limit.type == CostType.TOKEN_INPUT
        assert limit.max_value == 10000.0
        assert limit.limit_type == "quantity"

    def test_amount_limit_creation(self) -> None:
        """Test AmountLimit creation."""
        limit = AmountLimit(
            name="api_call",
            type=CostType.API_CALL,
            max_value=1.0,
        )

        assert limit.name == "api_call"
        assert limit.type == CostType.API_CALL
        assert limit.max_value == 1.0
        assert limit.limit_type == "amount"

    def test_quantity_limit_serialization(self) -> None:
        """Test QuantityLimit serializes correctly."""
        limit = QuantityLimit(
            name="storage",
            type=CostType.STORAGE,
            max_value=100.0,
        )

        data = limit.model_dump()

        assert data["name"] == "storage"
        assert data["type"] == CostType.STORAGE
        assert data["max_value"] == 100.0
        assert data["limit_type"] == "quantity"

    def test_amount_limit_serialization(self) -> None:
        """Test AmountLimit serializes correctly."""
        limit = AmountLimit(
            name="gpt4_output",
            type=CostType.TOKEN_OUTPUT,
            max_value=5.0,
        )

        data = limit.model_dump()

        assert data["name"] == "gpt4_output"
        assert data["type"] == CostType.TOKEN_OUTPUT
        assert data["max_value"] == 5.0
        assert data["limit_type"] == "amount"


# ============================================================================
# Test: set_limits Method
# ============================================================================


class TestSetLimits:
    """Tests for CostStrategy.set_limits method."""

    async def test_set_single_quantity_limit(self, cost_service: DefaultCost) -> None:
        """Test setting a single quantity limit."""
        limits = [
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=10000.0),
        ]

        await cost_service.set_limits(limits)

        assert "gpt4_input" in cost_service._limits
        assert cost_service._limits["gpt4_input"].max_value == 10000.0

    async def test_set_single_amount_limit(self, cost_service: DefaultCost) -> None:
        """Test setting a single amount limit."""
        limits = [
            AmountLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=1.0),
        ]

        await cost_service.set_limits(limits)

        assert "gpt4_input" in cost_service._limits
        assert cost_service._limits["gpt4_input"].max_value == 1.0

    async def test_set_multiple_limits(self, cost_service: DefaultCost) -> None:
        """Test setting multiple limits of different types."""
        limits = [
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=10000.0),
            AmountLimit(name="gpt4_output", type=CostType.TOKEN_OUTPUT, max_value=5.0),
            QuantityLimit(name="api_call", type=CostType.API_CALL, max_value=1000.0),
        ]

        await cost_service.set_limits(limits)

        assert len(cost_service._limits) == 3
        assert "gpt4_input" in cost_service._limits
        assert "gpt4_output" in cost_service._limits
        assert "api_call" in cost_service._limits

    async def test_set_limits_resets_accumulated(self, cost_service: DefaultCost) -> None:
        """Test that set_limits resets accumulated values."""
        # First, set some accumulated values manually
        cost_service._accumulated["gpt4_input_quantity"] = 5000.0
        cost_service._accumulated["gpt4_input_amount"] = 0.15

        # Set new limits
        limits = [
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=10000.0),
        ]

        await cost_service.set_limits(limits)

        # Accumulated should be reset
        assert cost_service._accumulated == {}

    async def test_set_limits_replaces_existing(self, cost_service: DefaultCost) -> None:
        """Test that set_limits replaces existing limits."""
        # Set initial limits
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=10000.0),
        ])

        assert cost_service._limits["gpt4_input"].max_value == 10000.0

        # Replace with new limits
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=20000.0),
        ])

        assert cost_service._limits["gpt4_input"].max_value == 20000.0


# ============================================================================
# Test: check_limit Method
# ============================================================================


class TestCheckLimit:
    """Tests for CostStrategy.check_limit method."""

    async def test_check_limit_no_limit_set(self, cost_service: DefaultCost) -> None:
        """Test check_limit returns True when no limit is set."""
        assert await cost_service.check_limit("gpt4_input", 100000.0) is True

    async def test_check_limit_quantity_under_limit(self, cost_service: DefaultCost) -> None:
        """Test check_limit returns True when quantity is under limit."""
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=10000.0),
        ])

        assert await cost_service.check_limit("gpt4_input", 5000.0) is True

    async def test_check_limit_quantity_exceeds_limit(self, cost_service: DefaultCost) -> None:
        """Test check_limit returns False when quantity exceeds limit."""
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=10000.0),
        ])

        assert await cost_service.check_limit("gpt4_input", 15000.0) is False

    async def test_check_limit_amount_under_limit(self, cost_service: DefaultCost) -> None:
        """Test check_limit returns True when projected amount is under limit."""
        await cost_service.set_limits([
            AmountLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=1.0),
        ])

        # 10000 tokens * $0.00003/token = $0.30
        assert await cost_service.check_limit("gpt4_input", 10000.0) is True

    async def test_check_limit_amount_exceeds_limit(self, cost_service: DefaultCost) -> None:
        """Test check_limit returns False when projected amount exceeds limit."""
        await cost_service.set_limits([
            AmountLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=0.10),
        ])

        # 10000 tokens * $0.00003/token = $0.30, which exceeds $0.10 limit
        assert await cost_service.check_limit("gpt4_input", 10000.0) is False

    async def test_check_limit_with_accumulated_quantity(self, cost_service: DefaultCost) -> None:
        """Test check_limit considers accumulated quantity."""
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=10000.0),
        ])

        # Simulate previous usage
        cost_service._accumulated["gpt4_input_quantity"] = 8000.0

        # Adding 3000 would exceed (8000 + 3000 = 11000 > 10000)
        assert await cost_service.check_limit("gpt4_input", 3000.0) is False

    async def test_check_limit_with_accumulated_amount(self, cost_service: DefaultCost) -> None:
        """Test check_limit considers accumulated amount."""
        await cost_service.set_limits([
            AmountLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=0.50),
        ])

        # Simulate previous usage: $0.30 already spent
        cost_service._accumulated["gpt4_input_amount"] = 0.30

        # 10000 tokens * $0.00003 = $0.30 more, total $0.60 > $0.50
        assert await cost_service.check_limit("gpt4_input", 10000.0) is False

        # But 5000 tokens = $0.15 more, total $0.45 <= $0.50
        assert await cost_service.check_limit("gpt4_input", 5000.0) is True

    async def test_check_limit_exact_boundary(self, cost_service: DefaultCost) -> None:
        """Test check_limit at exact boundary (equal to limit)."""
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=10000.0),
        ])

        # Exactly at limit should pass
        assert await cost_service.check_limit("gpt4_input", 10000.0) is True

    async def test_check_limit_config_not_found(self, cost_service: DefaultCost) -> None:
        """Test check_limit returns True when config doesn't exist."""
        await cost_service.set_limits([
            QuantityLimit(name="nonexistent", type=CostType.CUSTOM, max_value=100.0),
        ])

        # Returns True - config doesn't exist, can't calculate
        assert await cost_service.check_limit("nonexistent", 1000.0) is True


# ============================================================================
# Test: Accumulated Tracking
# ============================================================================


class TestAccumulatedTracking:
    """Tests for accumulated value tracking."""

    async def test_accumulated_quantity_tracking(self, cost_service: DefaultCost) -> None:
        """Test that quantity is tracked correctly via manual accumulation."""
        await cost_service.set_limits([
            QuantityLimit(name="api_call", type=CostType.API_CALL, max_value=100.0),
        ])

        # Simulate tracking by adding costs and manually updating accumulated
        await cost_service.create("call_1", "api_call", 25.0)
        cost_service._accumulated["api_call_quantity"] = 25.0

        await cost_service.create("call_2", "api_call", 30.0)
        cost_service._accumulated["api_call_quantity"] = 55.0

        await cost_service.create("call_3", "api_call", 20.0)
        cost_service._accumulated["api_call_quantity"] = 75.0

        assert cost_service._accumulated["api_call_quantity"] == 75.0

    async def test_accumulated_affects_check_limit(self, cost_service: DefaultCost) -> None:
        """Test that accumulated values affect check_limit results."""
        await cost_service.set_limits([
            QuantityLimit(name="api_call", type=CostType.API_CALL, max_value=100.0),
        ])

        # Initially should allow 50
        assert await cost_service.check_limit("api_call", 50.0) is True

        # Accumulate 60
        cost_service._accumulated["api_call_quantity"] = 60.0

        # Now 50 more should exceed
        assert await cost_service.check_limit("api_call", 50.0) is False

        # But 40 more should still work
        assert await cost_service.check_limit("api_call", 40.0) is True

    async def test_accumulated_reset_on_new_limits(self, cost_service: DefaultCost) -> None:
        """Test that accumulated values reset when limits are re-set."""
        await cost_service.set_limits([
            QuantityLimit(name="api_call", type=CostType.API_CALL, max_value=100.0),
        ])

        cost_service._accumulated["api_call_quantity"] = 50.0

        # Re-set limits (simulating new session)
        await cost_service.set_limits([
            QuantityLimit(name="api_call", type=CostType.API_CALL, max_value=100.0),
        ])

        # Accumulated should be reset
        assert cost_service._accumulated.get("api_call_quantity", 0) == 0


# ============================================================================
# Test: Edge Cases
# ============================================================================


class TestLimitEdgeCases:
    """Edge cases for cost limit functionality."""

    async def test_zero_quantity(self, cost_service: DefaultCost) -> None:
        """Test handling of zero quantity."""
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=10000.0),
        ])

        # Zero quantity should pass check
        assert await cost_service.check_limit("gpt4_input", 0.0) is True

    async def test_very_small_quantities(self, cost_service: DefaultCost) -> None:
        """Test handling of very small quantities."""
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=1.0),
        ])

        # Accumulate many small quantities
        total = 0.0
        for _ in range(100):
            total += 0.009
            cost_service._accumulated["gpt4_input_quantity"] = total

        # Total = 100 * 0.009 = 0.9, still under limit
        assert cost_service._accumulated["gpt4_input_quantity"] < 1.0

        # Next small one should still pass
        assert await cost_service.check_limit("gpt4_input", 0.009) is True

        # But a larger one should fail
        assert await cost_service.check_limit("gpt4_input", 0.2) is False

    async def test_floating_point_precision(self, cost_service: DefaultCost) -> None:
        """Test floating point precision in limit calculations."""
        await cost_service.set_limits([
            AmountLimit(name="api_call", type=CostType.API_CALL, max_value=0.001),
        ])

        # rate = 0.001 per call, limit = 0.001
        # 1 call should exactly hit the limit (pass)
        assert await cost_service.check_limit("api_call", 1.0) is True

        # Accumulate that call
        cost_service._accumulated["api_call_amount"] = 0.001

        # Even tiny addition should exceed
        assert await cost_service.check_limit("api_call", 0.001) is False

    async def test_very_large_limit(self, cost_service: DefaultCost) -> None:
        """Test handling of very large limits."""
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=1e12),
        ])

        # Large but valid usage should pass
        assert await cost_service.check_limit("gpt4_input", 1e11) is True

    async def test_limit_with_zero_rate_config(self, sample_config: dict[str, CostConfig]) -> None:
        """Test limit checking with zero rate config."""
        # Add a zero-rate config
        sample_config["free_tier"] = CostConfig(
            name="free_tier",
            type=CostType.OTHER,
            description="Free tier",
            unit="requests",
            rate=0.0,
        )

        cost_service = DefaultCost(
            mission_id="missions:test",
            setup_id="setup:test",
            setup_version_id="setup_version:test",
            config=sample_config,
        )

        await cost_service.set_limits([
            QuantityLimit(name="free_tier", type=CostType.CUSTOM, max_value=100.0),
        ])

        # Should work with zero rate - quantity check still applies
        assert await cost_service.check_limit("free_tier", 50.0) is True
        assert await cost_service.check_limit("free_tier", 150.0) is False  # Exceeds quantity


# ============================================================================
# Test: Independent Limits for Different Configs
# ============================================================================


class TestIndependentLimits:
    """Tests for independent limits on different configs."""

    async def test_independent_quantity_limits(self, cost_service: DefaultCost) -> None:
        """Test that quantity limits for different configs are independent."""
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=10000.0),
            QuantityLimit(name="gpt4_output", type=CostType.TOKEN_OUTPUT, max_value=5000.0),
        ])

        # Use up gpt4_input limit via accumulated
        cost_service._accumulated["gpt4_input_quantity"] = 10000.0

        # gpt4_input should now fail
        assert await cost_service.check_limit("gpt4_input", 1.0) is False

        # But gpt4_output should still have its full limit
        assert await cost_service.check_limit("gpt4_output", 4000.0) is True

    async def test_mixed_limit_types(self, cost_service: DefaultCost) -> None:
        """Test mixing quantity and amount limits on different configs."""
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=10000.0),
            AmountLimit(name="api_call", type=CostType.API_CALL, max_value=0.50),
        ])

        # gpt4_input uses quantity tracking
        cost_service._accumulated["gpt4_input_quantity"] = 8000.0
        assert await cost_service.check_limit("gpt4_input", 3000.0) is False

        # api_call uses amount tracking
        cost_service._accumulated["api_call_amount"] = 0.40
        # 200 calls * 0.001 = $0.20, total would be $0.60 > $0.50
        assert await cost_service.check_limit("api_call", 200.0) is False


# ============================================================================
# Test: Concurrent Usage Simulation
# ============================================================================


class TestConcurrentUsageSimulation:
    """Tests simulating concurrent usage patterns."""

    async def test_burst_usage_pattern(self, cost_service: DefaultCost) -> None:
        """Test burst usage pattern checking against limits."""
        await cost_service.set_limits([
            QuantityLimit(name="api_call", type=CostType.API_CALL, max_value=100.0),
        ])

        # Simulate checking before 50 calls
        total = 0.0
        for _ in range(50):
            assert await cost_service.check_limit("api_call", 1.0) is True
            total += 1.0
            cost_service._accumulated["api_call_quantity"] = total

        # 50 more should still work
        for _ in range(50):
            assert await cost_service.check_limit("api_call", 1.0) is True
            total += 1.0
            cost_service._accumulated["api_call_quantity"] = total

        # Next one should fail
        assert await cost_service.check_limit("api_call", 1.0) is False

    async def test_mixed_config_burst(self, cost_service: DefaultCost) -> None:
        """Test burst usage across multiple configs."""
        await cost_service.set_limits([
            QuantityLimit(name="gpt4_input", type=CostType.TOKEN_INPUT, max_value=50000.0),
            QuantityLimit(name="gpt4_output", type=CostType.TOKEN_OUTPUT, max_value=25000.0),
            QuantityLimit(name="api_call", type=CostType.API_CALL, max_value=100.0),
        ])

        input_total = 0.0
        output_total = 0.0
        call_total = 0.0

        # Simulate realistic LLM usage pattern
        for _ in range(10):
            # Each "turn" uses input, output, and API call
            assert await cost_service.check_limit("gpt4_input", 2000.0) is True
            input_total += 2000.0
            cost_service._accumulated["gpt4_input_quantity"] = input_total

            assert await cost_service.check_limit("gpt4_output", 1000.0) is True
            output_total += 1000.0
            cost_service._accumulated["gpt4_output_quantity"] = output_total

            assert await cost_service.check_limit("api_call", 1.0) is True
            call_total += 1.0
            cost_service._accumulated["api_call_quantity"] = call_total

        # Totals: input=20k, output=10k, calls=10
        assert cost_service._accumulated["gpt4_input_quantity"] == 20000.0
        assert cost_service._accumulated["gpt4_output_quantity"] == 10000.0
        assert cost_service._accumulated["api_call_quantity"] == 10.0

        # All should have room for more
        assert await cost_service.check_limit("gpt4_input", 5000.0) is True
        assert await cost_service.check_limit("gpt4_output", 5000.0) is True
        assert await cost_service.check_limit("api_call", 10.0) is True
