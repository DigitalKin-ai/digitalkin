"""Tests for SafeLiteLLM reporting which proxy model group answered a call.

The LiteLLM proxy may serve a call from a fallback group. It says so only in its response headers
(``x-litellm-model-group``, ``x-litellm-attempted-fallbacks``), which litellm keeps in
``_hidden_params["additional_headers"]`` with an ``llm_provider-`` prefix. On a stream the proxy
restamps every chunk's ``model`` with the requested group, so the headers are the only signal.
"""

import uuid

from litellm.types.utils import Delta, Message, ModelResponse, ModelResponseStream, StreamingChoices, Usage

from digitalkin.community.agno.safe_litellm import SafeLiteLLM
from digitalkin.community.litellm.registry import LiteLLMRegistry
from digitalkin.models.events import ModelInfo

_KEY = ModelInfo.PROVIDER_DATA_KEY


def _model(model_id: str = "claude-opus-4-5") -> SafeLiteLLM:
    """Build the model without __init__ (which needs live LiteLLM config)."""
    model = SafeLiteLLM.__new__(SafeLiteLLM)
    model.id = model_id
    return model


def _headers(group: str | None, fallbacks: int | str | None = None) -> dict:
    headers: dict = {}
    if group is not None:
        headers["llm_provider-x-litellm-model-group"] = group
    if fallbacks is not None:
        headers["llm_provider-x-litellm-attempted-fallbacks"] = fallbacks
    return {"additional_headers": headers}


def _chunk(response_id: str, headers: dict, content: str | None = "ans") -> ModelResponseStream:
    chunk = ModelResponseStream(
        id=response_id,
        model="claude-opus-4-5",  # what the proxy restamps, fallback or not
        choices=[StreamingChoices(index=0, delta=Delta(role="assistant", content=content))],
    )
    chunk._hidden_params = headers
    return chunk


def _completion(headers: dict) -> ModelResponse:
    response = ModelResponse(
        choices=[{"index": 0, "message": Message(role="assistant", content="answer"), "finish_reason": "stop"}],
        usage=Usage(prompt_tokens=1, completion_tokens=1, total_tokens=2),
    )
    response._hidden_params = headers
    return response


def _stream_id() -> str:
    # The once-per-call guard is a ContextVar shared by every test of this task: a fresh id each time.
    return f"chatcmpl-{uuid.uuid4()}"


class TestStreamedServedModel:
    def test_fallback_reports_served_and_requested_model(self):
        parsed = _model()._parse_provider_response_delta(_chunk(_stream_id(), _headers("claude-sonnet-4.5", "1")))

        assert parsed.provider_data == {_KEY: {"name": "claude-sonnet-4.5", "requested_name": "claude-opus-4-5"}}

    def test_requested_model_answering_reports_no_requested_name(self):
        parsed = _model()._parse_provider_response_delta(_chunk(_stream_id(), _headers("claude-opus-4-5", "0")))

        assert parsed.provider_data == {_KEY: {"name": "claude-opus-4-5", "requested_name": None}}

    def test_reported_once_per_call(self):
        model, response_id = _model(), _stream_id()
        headers = _headers("claude-sonnet-4.5", 1)

        first = model._parse_provider_response_delta(_chunk(response_id, headers))
        second = model._parse_provider_response_delta(_chunk(response_id, headers))
        next_call = model._parse_provider_response_delta(_chunk(_stream_id(), _headers("claude-opus-4-5")))

        assert first.provider_data is not None
        assert second.provider_data is None
        assert next_call.provider_data == {_KEY: {"name": "claude-opus-4-5", "requested_name": None}}

    def test_tool_only_delta_still_reports(self):
        """A call that only requests tools has no text delta; its first chunk still names the model."""
        parsed = _model()._parse_provider_response_delta(
            _chunk(_stream_id(), _headers("claude-sonnet-4.5", 1), content=None)
        )

        assert parsed.provider_data == {_KEY: {"name": "claude-sonnet-4.5", "requested_name": "claude-opus-4-5"}}

    def test_no_proxy_headers_reports_nothing(self):
        parsed = _model()._parse_provider_response_delta(_chunk(_stream_id(), {}))

        assert parsed.provider_data is None

    def test_fallback_detected_without_the_fallback_count(self):
        """A served group other than the requested one is a fallback, whatever the count header says."""
        parsed = _model()._parse_provider_response_delta(_chunk(_stream_id(), _headers("claude-sonnet-4.5", "n/a")))

        assert parsed.provider_data == {_KEY: {"name": "claude-sonnet-4.5", "requested_name": "claude-opus-4-5"}}

    def test_stream_without_group_header_resolves_the_deployment(self, monkeypatch):
        """Proxies before litellm 1.88 send only x-litellm-model-id on a stream."""
        registry = LiteLLMRegistry.__new__(LiteLLMRegistry)
        registry._deployment_groups = {}
        registry._remember_deployments(
            [
                {"model_name": "claude-opus-4-5", "model_info": {"id": "dep-opus"}},
                {"model_name": "claude-sonnet-4.5", "model_info": {"id": "dep-sonnet"}},
            ]
        )
        monkeypatch.setattr(LiteLLMRegistry, "shared", classmethod(lambda cls: registry))

        fell_back = _model()._parse_provider_response_delta(
            _chunk(_stream_id(), {"additional_headers": {"llm_provider-x-litellm-model-id": "dep-sonnet"}})
        )
        direct = _model()._parse_provider_response_delta(
            _chunk(_stream_id(), {"additional_headers": {"llm_provider-x-litellm-model-id": "dep-opus"}})
        )
        unknown = _model()._parse_provider_response_delta(
            _chunk(_stream_id(), {"additional_headers": {"llm_provider-x-litellm-model-id": "dep-unknown"}})
        )

        assert fell_back.provider_data == {_KEY: {"name": "claude-sonnet-4.5", "requested_name": "claude-opus-4-5"}}
        assert direct.provider_data == {_KEY: {"name": "claude-opus-4-5", "requested_name": None}}
        assert unknown.provider_data is None


class TestCompletionServedModel:
    def test_fallback_reports_served_and_requested_model(self):
        parsed = _model()._parse_provider_response(_completion(_headers("claude-sonnet-4.5", 2)))

        assert parsed.provider_data == {_KEY: {"name": "claude-sonnet-4.5", "requested_name": "claude-opus-4-5"}}
        assert parsed.content == "answer"

    def test_no_proxy_headers_reports_nothing(self):
        assert _model()._parse_provider_response(_completion({})).provider_data is None


class _StreamWithoutProperty(ModelResponseStream):
    """A chunk as litellm before the ``hidden_params`` property (1.88) builds it."""

    @property
    def hidden_params(self) -> dict:  # type: ignore[override]
        raise AttributeError("hidden_params")


def test_served_model_reads_chunks_of_litellm_without_the_hidden_params_property():
    """Regression: reading the public property crashed every streamed run on litellm 1.88."""
    chunk = _StreamWithoutProperty(
        id=_stream_id(),
        model="claude-opus-4-5",
        choices=[StreamingChoices(index=0, delta=Delta(role="assistant", content="ans"))],
    )
    chunk._hidden_params = _headers("claude-sonnet-4.5", "1")

    parsed = _model()._parse_provider_response_delta(chunk)

    assert parsed.provider_data == {_KEY: {"name": "claude-sonnet-4.5", "requested_name": "claude-opus-4-5"}}
