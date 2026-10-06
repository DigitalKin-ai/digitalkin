"""DefaultCost reads follow the ListCosts contract of GrpcCost: AND-combined filters, no filter = everything."""

import pytest

from digitalkin.services.cost.cost_strategy import CostConfig
from digitalkin.services.cost.default_cost import DefaultCost

pytestmark = [pytest.mark.timeout(10), pytest.mark.regression]


@pytest.fixture
def cost() -> DefaultCost:
    """A DefaultCost with an input-token and an API-call config."""
    return DefaultCost(
        "missions:m1",
        "setups:s1",
        "setup_versions:v1",
        {
            "llm_in": CostConfig(cost_name="llm_in", cost_type="TOKEN_INPUT", unit="tokens", rate=0.001),
            "search": CostConfig(cost_name="search", cost_type="API_CALL", unit="calls", rate=0.5),
        },
    )


class TestDefaultCostFilters:
    """Same answers as the ListCosts mock the GrpcCost tests run against."""

    async def test_no_filter_returns_every_cost(self, cost: DefaultCost) -> None:
        await cost.add("a", "llm_in", 10)
        await cost.add("b", "search", 1)

        assert [c.name for c in await cost.get_filtered()] == ["a", "b"]

    async def test_filters_combine_with_and(self, cost: DefaultCost) -> None:
        await cost.add("a", "llm_in", 10)
        await cost.add("b", "search", 1)

        assert [c.name for c in await cost.get_filtered(names=["a", "b"], cost_types=["API_CALL"])] == ["b"]
        assert await cost.get_filtered(names=["a"], cost_types=["API_CALL"]) == []

    async def test_type_filter_matches_by_name(self, cost: DefaultCost) -> None:
        """Regression: the type filter compared an enum member to strings and never matched."""
        await cost.add("a", "llm_in", 10)
        await cost.add("b", "search", 1)

        assert [c.name for c in await cost.get_filtered(cost_types=["TOKEN_INPUT"])] == ["a"]

    async def test_mission_without_costs_reads_empty(self, cost: DefaultCost) -> None:
        """Regression: reading before the first add raised instead of answering empty."""
        assert await cost.get_filtered(names=["a"]) == []
        assert await cost.get("a") == []

    async def test_get_returns_the_named_costs_only(self, cost: DefaultCost) -> None:
        await cost.add("a", "llm_in", 10)
        await cost.add("b", "search", 1)

        assert [c.name for c in await cost.get("b")] == ["b"]
