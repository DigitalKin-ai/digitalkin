"""LiteLLM cost-tracking callback.

LiteLLM's usage-detail wrappers vary by version and provider, and reading an undefined field on the
pydantic model raises ``AttributeError``: ``_book`` must read token categories so a missing field
(e.g. ``cache_creation_tokens`` on older builds) yields 0 instead of crashing the callback.
"""

from collections.abc import Iterator
import logging
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import litellm
import pytest
from pydantic import BaseModel

from digitalkin.community.litellm import (
    LLM_COST_CONFIG,
    CostInfo,
    CostRegistry,
    CostTrackingLogger,
    LiteLLMConfig,
    LiteLLMRegistry,
    ModelParser,
)


class FakeCost:
    """Captures cost-service ``add`` calls."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, float]] = []

    async def add(self, name: str, category: str, amount: float) -> None:
        self.calls.append((name, category, amount))


class _OldPromptDetails(BaseModel):
    """An older LiteLLM ``PromptTokensDetailsWrapper`` lacking cache-creation."""

    cached_tokens: int | None = None


class _CompletionDetails(BaseModel):
    reasoning_tokens: int | None = None


def _spec() -> SimpleNamespace:
    """``_book`` only reads ``spec.name`` and ``spec.costs``."""
    return SimpleNamespace(
        name="test-model",
        costs=CostInfo(input_cost_per_token=Decimal("1.0"), output_cost_per_token=Decimal("2.0")),
    )


@pytest.fixture
def mission() -> Iterator[FakeCost]:
    cost = FakeCost()
    CostRegistry.register("mission-1", cost, 2)  # type: ignore[arg-type]
    yield cost
    CostRegistry.unregister("mission-1", cost)  # type: ignore[arg-type]


class TestAsDict:
    def test_none_returns_empty(self):
        assert CostTrackingLogger._as_dict(None) == {}

    def test_dict_passthrough(self):
        assert CostTrackingLogger._as_dict({"cached_tokens": 5}) == {"cached_tokens": 5}

    def test_model_without_field_omits_it(self):
        details = CostTrackingLogger._as_dict(_OldPromptDetails(cached_tokens=3))
        assert details.get("cached_tokens") == 3
        assert details.get("cache_creation_tokens") is None


class TestBook:
    async def test_missing_cache_creation_field_does_not_crash(self):
        cost = FakeCost()
        usage = SimpleNamespace(
            prompt_tokens=100,
            completion_tokens=40,
            prompt_tokens_details=_OldPromptDetails(cached_tokens=10),
            completion_tokens_details=_CompletionDetails(reasoning_tokens=5),
        )

        await CostTrackingLogger._book(usage, _spec(), cost, Decimal("1.0"), "mission-1")  # type: ignore[arg-type]

        assert dict((name, amount) for name, _, amount in cost.calls) == {
            "llm_input_tokens": 90 / 20_000,
            "llm_output_tokens": 35 * 2 / 20_000,
            "llm_reasoning_tokens": 5 * 2 / 20_000,
            "llm_cached_tokens": 10 / 20_000,
        }

    async def test_none_details_books_only_plain_io(self):
        cost = FakeCost()
        usage = SimpleNamespace(
            prompt_tokens=50, completion_tokens=20, prompt_tokens_details=None, completion_tokens_details=None
        )

        await CostTrackingLogger._book(usage, _spec(), cost, Decimal("1.0"), "mission-1")  # type: ignore[arg-type]

        assert [(name, category) for name, category, _ in cost.calls] == [
            ("llm_input_tokens", "chatbot_input"),
            ("llm_output_tokens", "chatbot_output"),
        ]

    async def test_cache_creation_is_priced_at_its_own_rate(self):
        cost = FakeCost()
        spec = SimpleNamespace(
            name="m",
            costs=CostInfo(
                input_cost_per_token=Decimal(1),
                output_cost_per_token=Decimal(1),
                cache_creation_input_token_cost=Decimal("1.25"),
            ),
        )
        usage = SimpleNamespace(
            prompt_tokens=0,
            completion_tokens=0,
            prompt_tokens_details={"cache_creation_tokens": 8},
            completion_tokens_details=None,
        )

        await CostTrackingLogger._book(usage, spec, cost, Decimal(2), "m")  # type: ignore[arg-type]

        assert cost.calls == [("llm_cache_creation_tokens", "chatbot_cache_creation_input", 8 * 1.25 / 20_000 * 2)]


class TestCredit:
    def test_token_to_dk_credit(self):
        assert CostTrackingLogger.token_to_dk_credit(20_000, Decimal(1)) == 1.0
        assert CostTrackingLogger.token_to_dk_credit(20_000, Decimal(3), Decimal(2)) == 6.0


class TestCostRegistry:
    def test_multiplier_below_one_is_clamped(self):
        cost = FakeCost()
        CostRegistry.register("m-low", cost, 0.5)  # type: ignore[arg-type]
        try:
            entry = CostRegistry.get("m-low")
            assert entry is not None
            assert entry[1] == Decimal("1.0")
        finally:
            CostRegistry.unregister("m-low", cost)  # type: ignore[arg-type]
        assert CostRegistry.get("m-low") is None

    def test_a_turn_ending_keeps_the_other_turn_of_its_mission_billed(self):
        """Regression: the first turn to end unregistered the mission, so a concurrent turn went unbilled."""
        first, second = FakeCost(), FakeCost()
        CostRegistry.register("m-both", first, 1)  # type: ignore[arg-type]
        CostRegistry.register("m-both", second, 3)  # type: ignore[arg-type]

        CostRegistry.unregister("m-both", second)  # type: ignore[arg-type]
        assert CostRegistry.get("m-both") == (first, Decimal(1))

        CostRegistry.unregister("m-both", first)  # type: ignore[arg-type]
        assert CostRegistry.get("m-both") is None


class TestRegister:
    def test_register_is_idempotent(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(litellm, "callbacks", ["other"])

        CostTrackingLogger.register()
        CostTrackingLogger.register()

        assert litellm.callbacks[0] == "other"
        assert sum(isinstance(cb, CostTrackingLogger) for cb in litellm.callbacks) == 1


class TestSuccessEvent:
    @pytest.fixture(autouse=True)
    def _registry(self) -> Iterator[None]:
        registry = LiteLLMRegistry(LiteLLMConfig(base_url="http://proxy", bearer_token="t"))
        spec = ModelParser({
            "model_name": "gpt-5",
            "model_info": {"input_cost_per_token": 1e-06, "output_cost_per_token": 2e-06},
        }).to_model_spec()
        registry._models[spec.name] = spec
        LiteLLMRegistry._shared = registry
        yield
        LiteLLMRegistry._shared = None

    @staticmethod
    def _call(metadata: dict[str, Any], model: str = "litellm_proxy/gpt-5") -> dict[str, Any]:
        return {"model": model, "litellm_params": {"metadata": metadata}}

    @staticmethod
    def _response() -> SimpleNamespace:
        return SimpleNamespace(
            usage=SimpleNamespace(
                prompt_tokens=20_000, completion_tokens=0, prompt_tokens_details=None, completion_tokens_details=None
            )
        )

    async def test_books_to_the_mission_named_in_the_metadata(self, mission: FakeCost):
        await CostTrackingLogger().async_log_success_event(
            self._call({CostTrackingLogger.MISSION_METADATA_KEY: "mission-1"}), self._response(), None, None
        )

        assert mission.calls == [("llm_input_tokens", "chatbot_input", 2.0)]

    async def test_reads_the_standard_logging_object(self, mission: FakeCost):
        kwargs = {
            "model": "gpt-5",
            "standard_logging_object": {"metadata": {CostTrackingLogger.MISSION_METADATA_KEY: "mission-1"}},
        }

        await CostTrackingLogger().async_log_success_event(kwargs, self._response(), None, None)

        assert len(mission.calls) == 1

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"model": "gpt-5", "litellm_params": {"metadata": {}}},
            {"model": "gpt-5", "litellm_params": {"metadata": {"dk_mission_id": "unknown"}}},
            {"model": "unpriced", "litellm_params": {"metadata": {"dk_mission_id": "mission-1"}}},
        ],
    )
    async def test_unroutable_calls_book_nothing(self, mission: FakeCost, kwargs: dict[str, Any]):
        await CostTrackingLogger().async_log_success_event(kwargs, self._response(), None, None)

        assert mission.calls == []

    async def test_an_unregistered_mission_is_logged(
        self, mission: FakeCost, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ):
        kwargs = {"model": "gpt-5", "litellm_params": {"metadata": {"dk_mission_id": "unknown"}}}
        monkeypatch.setattr(logging.getLogger("digitalkin"), "propagate", True)
        with caplog.at_level(logging.WARNING, logger="digitalkin"):
            await CostTrackingLogger().async_log_success_event(kwargs, self._response(), None, None)

        assert any("LLM-COST-UNREGISTERED" in record.getMessage() for record in caplog.records)

    async def test_a_failure_never_escapes(self, mission: FakeCost):
        async def boom(*_: Any) -> None:
            raise RuntimeError

        mission.add = boom  # type: ignore[method-assign]

        await CostTrackingLogger().async_log_success_event(
            self._call({CostTrackingLogger.MISSION_METADATA_KEY: "mission-1"}), self._response(), None, None
        )


class TestCostConfig:
    def test_every_booked_category_is_declared(self):
        assert set(LLM_COST_CONFIG) == {
            "chatbot_input",
            "chatbot_cached_input",
            "chatbot_output",
            "chatbot_reasoning",
            "chatbot_cache_creation_input",
        }
        assert all(name == config.cost_name for name, config in LLM_COST_CONFIG.items())


def test_every_llm_token_category_is_billed_at_1_3() -> None:
    assert {name: config.rate for name, config in LLM_COST_CONFIG.items()} == dict.fromkeys(LLM_COST_CONFIG, 1.3)
