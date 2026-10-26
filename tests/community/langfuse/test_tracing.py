import logging

"""Langfuse/OTel tracing.

OTel caps a span at 128 attributes by default and evicts the *oldest* when full. openinference emits
several attributes per conversation message plus one per tool schema, so a long run blew past the cap
and the first casualties were ``input.value`` and ``llm.invocation_parameters``, exactly what Langfuse
renders as Input and modelParameters.
"""

from typing import Any
from unittest.mock import MagicMock

import pytest
from opentelemetry.sdk.trace import ReadableSpan, SpanLimits, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from digitalkin.community.langfuse import LangfuseTracing
from digitalkin.community.langfuse import tracing as tracing_module


def _openinference_attributes(message_count: int, tool_count: int) -> dict[str, Any]:
    """The attribute pattern and ordering openinference emits per LLM call."""
    attributes: dict[str, Any] = {
        "openinference.span.kind": "LLM",
        "input.mime_type": "application/json",
        "input.value": '{"messages": [...]}',
        "llm.invocation_parameters": '{"model": "claude-opus-5"}',
    }
    for i in range(message_count):
        for field in ("role", "content", "tool_calls.0.id"):
            attributes[f"llm.input_messages.{i}.message.{field}"] = "x"
    for i in range(tool_count):
        attributes[f"llm.tools.{i}.tool.json_schema"] = "{}"
    return attributes


def _record_generation(provider: TracerProvider) -> Any:
    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("LiteLLM.ainvoke_stream", attributes=_openinference_attributes(30, 40)) as span:
        span.set_attribute("llm.model_name", "claude-opus-5")
        span.set_attribute("output.value", "...")
    return span


@pytest.fixture
def installed(monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, MagicMock]:
    """Langfuse enabled, with the global provider install and the agno instrumentation captured."""
    instrumentor = MagicMock()
    set_provider = MagicMock()
    monkeypatch.setattr(tracing_module, "AgnoInstrumentor", MagicMock(return_value=instrumentor))
    monkeypatch.setattr(tracing_module.trace_api, "set_tracer_provider", set_provider)
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk")
    monkeypatch.setenv("LANGFUSE_SECRET_KEY", "sk")
    return set_provider, instrumentor


def test_default_span_limits_evict_the_input() -> None:
    """The bug: with OTel defaults a long conversation silently loses `input.value`."""
    span = _record_generation(TracerProvider())

    assert span.dropped_attributes > 0
    assert "input.value" not in span.attributes


def test_init_raises_the_cap_and_hides_per_message_attributes(installed: tuple[MagicMock, MagicMock]) -> None:
    set_provider, instrumentor = installed

    LangfuseTracing.init_tracer_provider()

    provider = set_provider.call_args.args[0]
    assert provider._span_limits.max_span_attributes == 512
    span = _record_generation(provider)
    assert span.dropped_attributes == 0
    assert span.attributes["input.value"] == '{"messages": [...]}'
    assert span.attributes["llm.invocation_parameters"] == '{"model": "claude-opus-5"}'
    kwargs = instrumentor.instrument.call_args.kwargs
    assert kwargs["tracer_provider"] is provider
    assert kwargs["config"].hide_input_messages
    assert kwargs["config"].hide_output_messages
    assert kwargs["config"].hide_llm_tools
    provider.shutdown()


def test_init_is_idempotent(installed: tuple[MagicMock, MagicMock]) -> None:
    set_provider, _ = installed

    LangfuseTracing.init_tracer_provider()
    LangfuseTracing.init_tracer_provider()

    assert set_provider.call_count == 1
    set_provider.call_args.args[0].shutdown()


def test_init_without_keys_installs_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    set_provider = MagicMock()
    monkeypatch.setattr(tracing_module.trace_api, "set_tracer_provider", set_provider)

    otel_logger = logging.getLogger("opentelemetry.context")
    filters_before = len(otel_logger.filters)

    LangfuseTracing.init_tracer_provider()
    LangfuseTracing.init_tracer_provider()

    set_provider.assert_not_called()
    # Disabled tracing still initializes once: a second call must not stack another log filter.
    assert len(otel_logger.filters) == filters_before + 1
    otel_logger.removeFilter(otel_logger.filters[-1])


def test_trace_headers_are_empty_outside_a_span() -> None:
    assert LangfuseTracing.trace_headers() == {}


def test_proxy_headers() -> None:
    headers = LangfuseTracing.proxy_headers(
        "mission-1", "user-1", "ArchetypeIsaac", ["isaac", "digitalkin"], {"langfuse_existing_trace_id": "t"}
    )

    assert headers == {
        "langfuse_session_id": "mission-1",
        "langfuse_trace_user_id": "user-1",
        "langfuse_trace_name": "ArchetypeIsaac",
        "langfuse_tags": "isaac,digitalkin",
        "langfuse_existing_trace_id": "t",
    }


def test_run_span_sets_the_langfuse_attributes(monkeypatch: pytest.MonkeyPatch) -> None:
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(tracing_module.trace_api, "get_tracer", lambda name: provider.get_tracer(name))
    monkeypatch.setenv("LANGFUSE_ENVIRONMENT", "production")

    with LangfuseTracing.run_span(
        trace_name="ArchetypeIsaac",
        tags=["isaac", "digitalkin"],
        user_id="user-1",
        session_id="mission-1",
        metadata={"mission_id": "mission-1", "setup_version_id": None, "mode": "agent"},
        input_text="x" * 2000,
    ) as headers:
        assert tracing_module.baggage.get_baggage("langfuse.user.id") == "user-1"
        assert tracing_module.baggage.get_baggage("langfuse.session.id") == "mission-1"

    assert tracing_module.baggage.get_baggage("langfuse.user.id") is None
    (span,) = exporter.get_finished_spans()
    assert isinstance(span, ReadableSpan)
    assert span.name == "kin.run"
    assert headers == {
        "langfuse_existing_trace_id": format(span.context.trace_id, "032x"),
        "langfuse_parent_observation_id": format(span.context.span_id, "016x"),
    }
    attributes = dict(span.attributes or {})
    assert attributes["langfuse.user.id"] == "user-1"
    assert attributes["langfuse.session.id"] == "mission-1"
    assert attributes["langfuse.trace.name"] == "ArchetypeIsaac"
    assert attributes["langfuse.trace.tags"] == ("isaac", "digitalkin", "env:production")
    assert attributes["langfuse.trace.metadata.mission_id"] == "mission-1"
    assert attributes["langfuse.trace.metadata.mode"] == "agent"
    assert attributes["langfuse.trace.metadata.env"] == "production"
    assert "langfuse.trace.metadata.setup_version_id" not in attributes
    assert attributes["langfuse.trace.input"] == "x" * 1000
    provider.shutdown()
