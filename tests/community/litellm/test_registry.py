"""LiteLLM proxy catalogue: parsing, lazy loading, default pick and the model picker."""

import copy
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest

from digitalkin.community.litellm import CapacityInfo, LiteLLMConfig, LiteLLMRegistry, ModelParser, Provider
from digitalkin.utils.dynamic_schema import DynamicField


def _registry(rows: list[dict[str, Any]]) -> LiteLLMRegistry:
    registry = LiteLLMRegistry(LiteLLMConfig(base_url="http://proxy", bearer_token="t"))
    registry._client.fetch_raw_models = AsyncMock(return_value=rows)  # type: ignore[method-assign]
    return registry


def _names(names: list[str]) -> LiteLLMRegistry:
    return _registry([{"model_name": n} for n in names])


def _row(name: str, **model_info: Any) -> dict[str, Any]:
    return {
        "model_name": name,
        "litellm_params": {"model": "vertex_ai/claude"},
        "model_info": {
            "key": f"{name}-key",
            "input_cost_per_token": 3e-06,
            "output_cost_per_token": 1.5e-05,
            **model_info,
        },
    }


class TestFetchDefaultModelName:
    async def test_first_candidate_present_wins(self) -> None:
        """The candidate order is the preference order, not the proxy's order."""
        first, second = LiteLLMRegistry.DEFAULT_MODEL_CANDIDATES[:2]
        assert await _names(["zzz", second, first]).fetch_default_model_name() == first

    async def test_later_candidate_when_first_is_gone(self) -> None:
        second = LiteLLMRegistry.DEFAULT_MODEL_CANDIDATES[1]
        assert await _names(["zzz", second]).fetch_default_model_name() == second

    async def test_falls_back_to_first_served_model(self) -> None:
        """No candidate served: the first priority-sorted name is still a valid enum member."""
        assert await _names(["some-model", "another-model"]).fetch_default_model_name() == "another-model"

    async def test_empty_catalogue_raises(self) -> None:
        with pytest.raises(LookupError):
            await _names([]).fetch_default_model_name()


class TestFetchModelNames:
    async def test_priority_models_first_then_the_rest_sorted(self) -> None:
        names = await _names(["zeta", " gpt-5 ", "alpha", "claude-sonnet-5", "", "gpt-5"]).fetch_model_names()
        assert names == ["claude-sonnet-5", "gpt-5", "alpha", "zeta"]


class TestModelParser:
    def test_full_spec(self) -> None:
        spec = ModelParser(
            _row(
                "claude-sonnet-5",
                max_input_tokens=1_000_000,
                max_output_tokens=64_000,
                supports_vision="true",
                supports_reasoning=True,
                cache_read_input_token_cost=3e-07,
            )
        ).to_model_spec()

        assert spec.provider == Provider.GOOGLE
        assert spec.model_id == "claude-sonnet-5-key"
        assert spec.capacity.context_window == 1_000_000
        assert spec.capacity.max_output == 64_000
        assert spec.costs.input_cost_per_token == Decimal("3.000000000")
        assert spec.costs.cache_read_input_token_cost == Decimal("0.300000000")
        assert spec.support.supports_vision is True
        assert spec.multimodal is True
        assert spec.thought_capacity is not None
        assert spec.thought_capacity.value == "medium"
        assert spec.has_native_reasoning is True
        assert spec.model_dump()["provider"] == "Google"

    def test_context_falls_back_through_the_known_keys(self) -> None:
        capacity = ModelParser(_row("m", context_window=8000, max_completion_tokens=100)).parse_capacity()
        assert (capacity.context_window, capacity.max_output) == (8000, 100)

    def test_invalid_capacity_becomes_zero(self) -> None:
        capacity = ModelParser(_row("m", max_input_tokens="lots")).parse_capacity()
        assert (capacity.context_window, capacity.max_output) == (0, 0)

    def test_negative_or_invalid_prices_are_dropped(self) -> None:
        costs = ModelParser(_row("m", output_cost_per_reasoning_token=-1, input_cost_per_query="abc")).parse_costs()
        assert costs.output_cost_per_reasoning_token is None
        assert costs.input_cost_per_query is None

    @pytest.mark.parametrize(
        ("info", "expected"),
        [
            ({}, "none"),
            ({"supports_reasoning": True, "output_cost_per_reasoning_token": 1e-5}, "high"),
            ({"supports_reasoning": True, "reasoning_level": "basic"}, "low"),
            ({"supports_reasoning": True, "reasoning_level": "weird"}, "none"),
        ],
    )
    def test_thought_capacity(self, info: dict[str, Any], expected: str) -> None:
        spec = ModelParser(_row("m", **info)).to_model_spec()
        assert spec.thought_capacity is not None
        assert spec.thought_capacity.value == expected
        assert spec.has_native_reasoning is (expected != "none")

    def test_missing_name_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="Failed to parse model spec"):
            ModelParser({"model_info": {}}).to_model_spec()


class TestLoading:
    async def test_initialize_many_loads_in_one_round_trip(self) -> None:
        registry = _registry([_row("a"), _row("b"), _row("c")])

        await registry.initialize_many(["a", "b", "", "a"])
        await registry.initialize_many(["a"])

        assert registry._client.fetch_raw_models.await_count == 1  # type: ignore[attr-defined]
        assert len(registry) == 2
        assert registry.get("a").name == "a"
        assert registry.try_get("c") is None

    async def test_initialize_many_raises_on_unknown_names(self) -> None:
        with pytest.raises(ValueError, match="missing"):
            await _registry([_row("a")]).initialize_many(["a", "missing"])

    async def test_initialize_all_skips_unparseable_rows(self) -> None:
        registry = _registry([_row("a"), {"model_info": {}}])

        await registry.initialize()

        assert len(registry) == 1

    def test_get_unknown_model_raises(self) -> None:
        with pytest.raises(ValueError, match="not in registry"):
            _names([]).get("nope")

    def test_shared_is_one_instance_built_from_the_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LITELLM_BASE_URL", "https://shared.test/")

        assert LiteLLMRegistry.shared() is LiteLLMRegistry.shared()
        assert LiteLLMRegistry.shared().config.base_url == "https://shared.test/"


class TestModelPicker:
    async def test_groups_by_creator_with_labels_and_variant_order(self) -> None:
        rows = [
            {"model_name": n, "model_info": {"metadata": {"model_creator": c}}}
            for n, c in [
                ("claude-opus-4-6-thinking-high", "anthropic"),
                ("claude-opus-4-6-thinking-low", "anthropic"),
                ("claude-opus-4-6", "anthropic"),
                ("gpt-5-mini", "OpenAI"),
                ("gemini-pro", "Google DeepMind"),
                ("orphan", None),
            ]
        ]

        groups = await _registry(rows).build_model_groups()

        assert [g["label"] for g in groups] == ["Anthropic", "Google DeepMind", "OpenAI"]
        anthropic = groups[0]["options"]
        assert [o["label"] for o in anthropic] == [
            "Claude Opus 4.6",
            "Claude Opus 4.6 Thinking Low",
            "Claude Opus 4.6 Thinking High",
        ]
        assert anthropic[0]["image"].endswith("/models_creators_logo/anthropic.png")
        assert groups[1]["options"][0]["image"].endswith("/google-deepmind.png")
        assert groups[2]["options"] == [
            {
                "value": "gpt-5-mini",
                "label": "GPT 5 Mini",
                "image": "https://dk-hub-shared.s3.eu-north-1.amazonaws.com/public/models_creators_logo/openai.png",
            }
        ]

    @pytest.mark.parametrize(
        ("name", "label"),
        [
            ("gpt-5-mini", "GPT 5 Mini"),
            ("gpt-4o-api", "GPT 4o API"),
            ("claude-opus-4-5", "Claude Opus 4.5"),
            ("claude-sonnet-4.5", "Claude Sonnet 4.5"),
            ("gemini_2_5_pro", "Gemini 2.5 PRO"),
            ("claude-sonnet-4.5-1-2", "Claude Sonnet 4.5 1.2"),
            ("grok-4-fast", "Grok 4 Fast"),
            ("o3", "o3"),
            ("", ""),
        ],
    )
    def test_model_labels(self, name: str, label: str) -> None:
        assert LiteLLMRegistry._format_model_label(name) == label

    async def test_model_picker_fetches_from_the_shared_registry(self) -> None:
        LiteLLMRegistry._shared = _registry([
            {"model_name": "gpt-5", "model_info": {"metadata": {"model_creator": "openai"}}}
        ])
        try:
            picker = LiteLLMRegistry.model_picker()

            assert isinstance(picker, DynamicField)
            assert set(picker.fetchers) == {"enum", "default", "ui:options"}
            assert await picker.fetchers["enum"]() == ["gpt-5"]
            assert await picker.fetchers["default"]() == "gpt-5"
            options = await picker.fetchers["ui:options"]()
            assert options["groups"][0]["options"][0]["value"] == "gpt-5"
        finally:
            LiteLLMRegistry._shared = None


class TestModelPickerDeepcopy:
    async def test_picker_metadata_survives_deepcopy_after_the_lock_was_used(self) -> None:
        """Setup resolution deep-copies field metadata; the shared registry and its lock are never copied."""
        registry = _names(["claude-sonnet-5"])
        await registry.fetch_model_names()
        async with registry._lock:
            pass
        LiteLLMRegistry._shared = registry
        try:
            copied = copy.deepcopy(LiteLLMRegistry.model_picker())

            assert await copied.fetchers["enum"]() == ["claude-sonnet-5"]
            assert copy.deepcopy(registry) is registry
        finally:
            LiteLLMRegistry._shared = None

    async def test_the_picker_resolves_the_shared_registry_on_each_fetch(self) -> None:
        """Declared at import time, the field reads no proxy setting until the schema is resolved."""
        picker = LiteLLMRegistry.model_picker()
        LiteLLMRegistry._shared = _names(["gpt-5"])
        try:
            assert await picker.fetchers["enum"]() == ["gpt-5"]
        finally:
            LiteLLMRegistry._shared = None


class TestInputBudget:
    @pytest.mark.parametrize(
        ("context_window", "max_output", "budget"),
        [
            (200_000, 64_000, 136_000),  # shared window (Anthropic, OpenAI): the output is reserved
            (1_048_576, 1_048_576, 1_048_576),  # separate output budget (Gemini via the proxy)
            (0, 0, 0),  # unknown window
        ],
    )
    def test_reserves_a_shared_output_only(self, context_window: int, max_output: int, budget: int) -> None:
        assert CapacityInfo(context_window=context_window, max_output=max_output).input_budget == budget
