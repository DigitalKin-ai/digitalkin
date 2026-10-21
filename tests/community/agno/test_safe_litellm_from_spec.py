"""SafeLiteLLM.from_spec must not send parameters the target model rejects, and wires proxy and costs."""

import asyncio
from collections.abc import Iterator
from types import SimpleNamespace

import litellm
import pytest

from digitalkin.community.agno.safe_litellm import SafeLiteLLM
from digitalkin.community.litellm import CapacityInfo, CostTrackingLogger, LiteLLMConfig, LiteLLMRegistry


@pytest.fixture(autouse=True)
def registry(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(litellm, "callbacks", [])
    monkeypatch.setattr(litellm, "use_litellm_proxy", False)
    LiteLLMRegistry._shared = LiteLLMRegistry(LiteLLMConfig(base_url="http://proxy", bearer_token="token"))
    yield
    LiteLLMRegistry._shared = None


def _spec(name: str, context_window: int = 128_000) -> SimpleNamespace:
    return SimpleNamespace(
        name=name,
        capacity=CapacityInfo(context_window=context_window, max_output=4096),
        support=SimpleNamespace(),
    )


class TestTemperature:
    def test_claude_5_model_does_not_send_temperature(self) -> None:
        model = SafeLiteLLM.from_spec(_spec("claude-sonnet-5"), temperature=0.2)  # type: ignore[arg-type]
        assert model.send_temperature is False
        assert "temperature" not in model.get_request_params()

    def test_other_models_send_temperature(self) -> None:
        model = SafeLiteLLM.from_spec(_spec("gpt-5"), temperature=0.2)  # type: ignore[arg-type]
        assert model.send_temperature is True
        assert model.get_request_params()["temperature"] == 0.2

    def test_default_temperature(self) -> None:
        assert SafeLiteLLM.from_spec(_spec("gpt-5")).temperature == 0.7  # type: ignore[arg-type]


class TestPromptCaching:
    def test_claude_models_enable_caching(self) -> None:
        assert SafeLiteLLM.from_spec(_spec("claude-sonnet-5")).enable_prompt_caching is True  # type: ignore[arg-type]

    def test_non_anthropic_models_do_not(self) -> None:
        assert SafeLiteLLM.from_spec(_spec("gpt-5")).enable_prompt_caching is False  # type: ignore[arg-type]


class TestSummarizedThinking:
    @pytest.mark.parametrize("name", ["claude-opus-5", "claude-opus-5-thinking", "claude-fable-5"])
    def test_opus_and_fable_5_request_summarized_thinking(self, name: str) -> None:
        params = SafeLiteLLM.from_spec(_spec(name), temperature=0.2).get_request_params()  # type: ignore[arg-type]
        assert params["thinking"] == {"type": "adaptive", "display": "summarized"}
        assert "temperature" not in params

    @pytest.mark.parametrize("name", ["claude-sonnet-5", "claude-haiku-4.5", "claude-opus-50", "gpt-5"])
    def test_other_models_leave_thinking_alone(self, name: str) -> None:
        assert "thinking" not in SafeLiteLLM.from_spec(_spec(name)).get_request_params()  # type: ignore[arg-type]


class TestWiring:
    def test_routes_through_the_shared_proxy(self) -> None:
        event = asyncio.Event()
        compactor = object()

        model = SafeLiteLLM.from_spec(
            _spec("gpt-5"),  # type: ignore[arg-type]
            extra_headers={"langfuse_session_id": "m"},
            session_id="mission-1",
            mission_id="mission-1",
            cancel_event=event,
            compactor=compactor,  # type: ignore[arg-type]
        )

        assert litellm.use_litellm_proxy is True
        assert (model.id, model.api_base, model.api_key, model.max_tokens) == ("gpt-5", "http://proxy", "token", 4096)
        assert model.top_p is None
        assert model.extra_headers == {"langfuse_session_id": "m"}
        assert model.extra_body == {"litellm_session_id": "mission-1"}
        assert model.request_params == {
            "drop_params": True,
            "num_retries": 2,
            "metadata": {CostTrackingLogger.MISSION_METADATA_KEY: "mission-1"},
        }
        assert model.cancel_event is event
        assert model.compactor is compactor

    def test_without_ids_no_metadata_nor_session(self) -> None:
        model = SafeLiteLLM.from_spec(_spec("gpt-5"))  # type: ignore[arg-type]

        assert model.extra_headers == {}
        assert model.extra_body is None
        assert model.request_params == {"drop_params": True, "num_retries": 2}

    def test_cost_tracking_is_registered_once(self) -> None:
        SafeLiteLLM.from_spec(_spec("gpt-5"))  # type: ignore[arg-type]
        SafeLiteLLM.from_spec(_spec("claude-sonnet-5"))  # type: ignore[arg-type]

        assert sum(isinstance(cb, CostTrackingLogger) for cb in litellm.callbacks) == 1


class TestInputBudget:
    def test_reserves_the_output_in_the_window(self) -> None:
        """The provider counts max_tokens against the same window as the input."""
        assert SafeLiteLLM.from_spec(_spec("claude-sonnet-5", 200_000)).input_tokens == 200_000 - 4096  # type: ignore[arg-type]

    def test_an_output_as_large_as_the_window_is_a_separate_budget(self) -> None:
        """Gemini as the proxy reports it: max_output equals the window, so nothing is reserved."""
        spec = _spec("gemini-4-argon", 1_048_576)
        spec.capacity = CapacityInfo(context_window=1_048_576, max_output=1_048_576)

        assert SafeLiteLLM.from_spec(spec).input_tokens == 1_048_576  # type: ignore[arg-type]

    def test_unknown_window_leaves_no_budget(self) -> None:
        assert SafeLiteLLM.from_spec(_spec("gpt-5", 0)).input_tokens is None  # type: ignore[arg-type]
