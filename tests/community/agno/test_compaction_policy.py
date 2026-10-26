"""Compaction policy: the settings, the summary model, and the compactor a head model gets."""

from __future__ import annotations

from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest

pytest.importorskip("agno", reason="optional agno dependency not installed")

from digitalkin.community.agno import compaction as compaction_mod
from digitalkin.community.agno.compaction import HistoryCompactor, get_compaction_settings
from digitalkin.community.litellm import CapacityInfo

_VARS = ("ENABLED", "MAX_TOKENS", "WINDOW_RATIO", "UNCOMPACTED_RUNS", "SUMMARY_MODEL")


@pytest.fixture(autouse=True)
def _clean_settings(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in _VARS:
        monkeypatch.delenv(f"DIGITALKIN_COMPACTION_{name}", raising=False)
    get_compaction_settings.cache_clear()
    yield
    get_compaction_settings.cache_clear()


def _set(monkeypatch: pytest.MonkeyPatch, name: str, value: str) -> None:
    monkeypatch.setenv(f"DIGITALKIN_COMPACTION_{name}", value)
    get_compaction_settings.cache_clear()


class _Registry:
    """Stands in for ``LiteLLMRegistry.shared()``: serves only the names it was given."""

    def __init__(self, *served: str) -> None:
        self.served = {name: SimpleNamespace(name=name) for name in served}
        self.requested: list[list[str]] = []

    async def initialize_many(self, names: list[str]) -> None:
        self.requested.append(list(names))
        missing = [name for name in names if name not in self.served]
        if missing:
            msg = f"Models not found in LiteLLM: {missing}"
            raise ValueError(msg)

    def try_get(self, name: str) -> Any:
        return self.served.get(name)


def _use_registry(monkeypatch: pytest.MonkeyPatch, registry: _Registry) -> _Registry:
    monkeypatch.setattr(compaction_mod, "LiteLLMRegistry", SimpleNamespace(shared=lambda: registry))
    return registry


class TestSettings:
    def test_defaults(self) -> None:
        settings = get_compaction_settings()

        assert settings.enabled is True
        assert (settings.max_tokens, settings.window_ratio, settings.uncompacted_runs) == (600_000, 0.9, 5)
        assert settings.summary_model == "claude-haiku-5.5"

    def test_reads_the_prefixed_names(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set(monkeypatch, "MAX_TOKENS", "20000")

        assert get_compaction_settings().max_tokens == 20_000


class TestLoadSummaryModel:
    async def test_defaults_to_haiku_5_5(self, monkeypatch: pytest.MonkeyPatch) -> None:
        registry = _use_registry(monkeypatch, _Registry("claude-haiku-5.5"))

        spec = await HistoryCompactor.load_summary_model()

        assert spec is not None
        assert spec.name == "claude-haiku-5.5"
        assert registry.requested == [["claude-haiku-5.5"]]

    async def test_environment_picks_another_model(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set(monkeypatch, "SUMMARY_MODEL", "gpt-5.4-nano")
        _use_registry(monkeypatch, _Registry("gpt-5.4-nano"))

        spec = await HistoryCompactor.load_summary_model()

        assert spec is not None
        assert spec.name == "gpt-5.4-nano"

    async def test_a_model_the_proxy_does_not_serve_falls_back(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _use_registry(monkeypatch, _Registry())

        assert await HistoryCompactor.load_summary_model() is None

    async def test_empty_name_uses_the_agents_model(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set(monkeypatch, "SUMMARY_MODEL", "")
        registry = _use_registry(monkeypatch, _Registry("claude-haiku-5.5"))

        assert await HistoryCompactor.load_summary_model() is None
        assert registry.requested == []

    async def test_loaded_with_compaction_off_too(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An archetype may still have it write Agno's session summary."""
        _set(monkeypatch, "ENABLED", "false")
        _use_registry(monkeypatch, _Registry("claude-haiku-5.5"))

        spec = await HistoryCompactor.load_summary_model()

        assert spec is not None
        assert spec.name == "claude-haiku-5.5"


class TestForModel:
    @staticmethod
    def _spec(name: str = "claude-sonnet-5", context_window: int = 1_000_000, max_output: int = 0) -> Mock:
        spec = Mock()
        spec.name = name
        spec.capacity = CapacityInfo(context_window=context_window, max_output=max_output)
        return spec

    @staticmethod
    def _record_models(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, dict[str, Any]]]:
        built: list[tuple[Any, dict[str, Any]]] = []
        monkeypatch.setattr(
            compaction_mod.SafeLiteLLM, "from_spec", lambda spec, **k: built.append((spec, k)) or Mock()
        )
        return built

    def _compactor(self, model: Mock, summary: Mock | None = None, headers: dict[str, str] | None = None) -> Any:
        return HistoryCompactor.for_model(
            model, summary_spec=summary, db=Mock(), session_id="s1", mission_id="mission-1", extra_headers=headers or {}
        )

    def test_disabled_compaction_builds_no_summarizer(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set(monkeypatch, "ENABLED", "false")
        built = self._record_models(monkeypatch)

        assert self._compactor(self._spec()) is None
        assert built == []

    def test_summaries_are_written_by_the_summary_model(self, monkeypatch: pytest.MonkeyPatch) -> None:
        built = self._record_models(monkeypatch)
        haiku = self._spec("claude-haiku-5.5", context_window=200_000)

        compactor = self._compactor(self._spec(), haiku)

        assert built[0][0] is haiku
        assert compactor.compaction.compact_at_tokens == 600_000, "the threshold still follows the agent's model"
        assert compactor.compaction.summarize_char_budget == 400_000, "the read follows the summarizer's window"

    def test_without_a_summary_model_the_agents_model_writes_them(self, monkeypatch: pytest.MonkeyPatch) -> None:
        built = self._record_models(monkeypatch)
        model = self._spec()

        self._compactor(model)

        assert built[0][0] is model

    def test_summarizer_is_billed_and_traced_with_the_run(self, monkeypatch: pytest.MonkeyPatch) -> None:
        built = self._record_models(monkeypatch)

        self._compactor(self._spec(), headers={"langfuse_session_id": "s1"})

        kwargs = built[0][1]
        assert "compactor" not in kwargs, "a summary request is never itself compacted"
        assert kwargs["mission_id"] == "mission-1", "the summary is billed to the mission"
        assert kwargs["extra_headers"] == {"langfuse_session_id": "s1"}
        assert kwargs["temperature"] == 0.2

    def test_policy_comes_from_the_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set(monkeypatch, "MAX_TOKENS", "20000")
        _set(monkeypatch, "UNCOMPACTED_RUNS", "2")
        self._record_models(monkeypatch)

        compactor = self._compactor(self._spec())

        assert compactor.threshold_tokens == 20_000
        assert compactor.compaction.uncompacted_runs == 2
        assert compactor.compaction.store_compacted_messages is False

    def test_the_output_is_reserved_in_the_window(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Input plus max_tokens must fit the window: the threshold leaves the output room."""
        self._record_models(monkeypatch)

        compactor = self._compactor(self._spec(context_window=200_000, max_output=64_000))

        assert compactor.threshold_tokens == int((200_000 - 64_000) * 0.9)


    def test_a_separate_output_budget_is_not_reserved(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Gemini as the proxy reports it (max_output = window): the full window is the input budget."""
        self._record_models(monkeypatch)

        compactor = self._compactor(self._spec(context_window=200_000, max_output=200_000))

        assert compactor.threshold_tokens == int(200_000 * 0.9)


class TestHistoryKwargs:
    def test_compaction_owns_the_replay_window(self) -> None:
        kwargs = HistoryCompactor.history_kwargs()

        assert kwargs["num_history_messages"] is None
        assert kwargs["num_history_runs"] >= 10_000
        assert kwargs["max_tool_calls_from_history"] is None
        assert kwargs["enable_session_summaries"] is False, "no second summary of the same history"
