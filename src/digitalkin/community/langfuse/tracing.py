"""OpenTelemetry export to Langfuse for agno runs and LiteLLM proxy trace linking."""

import base64
import logging
from collections.abc import Generator, Iterable, Mapping
from contextlib import contextmanager
from typing import ClassVar

from openinference.instrumentation import TraceConfig
from openinference.instrumentation.agno import AgnoInstrumentor
from opentelemetry import baggage
from opentelemetry import trace as trace_api
from opentelemetry.context import attach, detach
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.processor.baggage import ALLOW_ALL_BAGGAGE_KEYS, BaggageSpanProcessor
from opentelemetry.sdk.trace import SpanLimits, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from digitalkin.community.langfuse.settings import get_langfuse_settings


class _OtelContextDetachFilter(logging.Filter):
    """Drop the cosmetic "Failed to detach context" errors of openinference's async generators.

    openinference-instrumentation-agno uses ``use_span`` inside async generators, whose context
    token fails to detach when the generator is closed in another context. Tracing is unaffected.
    """

    def filter(self, record: logging.LogRecord) -> bool:  # ruff: ignore[no-self-use]
        """Keep every record but the detach error.

        Args:
            record: The log record.

        Returns:
            False for the detach error.
        """
        return "Failed to detach context" not in record.getMessage()


class LangfuseTracing:
    """Process-wide Langfuse tracer provider, run span and LiteLLM proxy headers."""

    _initialized: ClassVar[bool] = False

    @classmethod
    def init_tracer_provider(cls) -> None:
        """Install the OTLP tracer provider exporting to Langfuse and instrument agno, once.

        A no-op until both Langfuse keys are set.
        """
        if cls._initialized:
            return
        logging.getLogger("opentelemetry.context").addFilter(_OtelContextDetachFilter())
        cls._initialized = True
        settings = get_langfuse_settings()
        if not settings.enabled:
            return

        credentials = base64.b64encode(
            f"{settings.public_key}:{settings.secret_key.get_secret_value()}".encode()
        ).decode()
        exporter = OTLPSpanExporter(
            endpoint=f"{settings.base_url}/api/public/otel/v1/traces",
            headers={"Authorization": f"Basic {credentials}"},
        )
        # OTel caps a span at 128 attributes and evicts the oldest first, which were `input.value`
        # and `llm.invocation_parameters`, what Langfuse renders as Input and modelParameters.
        tracer_provider = TracerProvider(span_limits=SpanLimits(max_span_attributes=512))
        tracer_provider.add_span_processor(BaggageSpanProcessor(ALLOW_ALL_BAGGAGE_KEYS))
        # `input.value` of a long conversation weighs hundreds of KB: a small queue keeps a slow
        # Langfuse from piling it up into an OOM, and a 2s flush bounds what a pod kill loses.
        tracer_provider.add_span_processor(
            BatchSpanProcessor(exporter, max_queue_size=512, max_export_batch_size=16, schedule_delay_millis=2000)
        )
        trace_api.set_tracer_provider(tracer_provider)
        # Drop the per-message and per-tool attribute fan-out: Langfuse renders `input.value` and
        # `output.value` (kept), never `llm.input_messages.*` or `llm.tools.*`.
        AgnoInstrumentor().instrument(
            tracer_provider=tracer_provider,
            config=TraceConfig(hide_input_messages=True, hide_output_messages=True, hide_llm_tools=True),
        )

    @staticmethod
    def trace_headers() -> dict[str, str]:
        """Headers linking LiteLLM proxy generations to the current OTEL span.

        Returns:
            ``langfuse_existing_trace_id`` and ``langfuse_parent_observation_id``, or an empty dict
            outside a valid span.
        """
        ctx = trace_api.get_current_span().get_span_context()
        if not ctx.is_valid:
            return {}
        return {
            "langfuse_existing_trace_id": format(ctx.trace_id, "032x"),
            "langfuse_parent_observation_id": format(ctx.span_id, "016x"),
        }

    @staticmethod
    def proxy_headers(
        session_id: str,
        user_id: str,
        trace_name: str,
        tags: Iterable[str],
        trace_headers: Mapping[str, str] | None = None,
    ) -> dict[str, str]:
        """Langfuse headers the LiteLLM proxy reads to attribute its generations.

        Args:
            session_id: Langfuse session (typically the mission id).
            user_id: Langfuse trace user.
            trace_name: Langfuse trace name.
            tags: Langfuse trace tags.
            trace_headers: :meth:`trace_headers` of the run span, linking the generations to it.

        Returns:
            The headers.
        """
        return {
            "langfuse_session_id": session_id,
            "langfuse_trace_user_id": user_id,
            "langfuse_trace_name": trace_name,
            "langfuse_tags": ",".join(tags),
            **(trace_headers or {}),
        }

    @classmethod
    @contextmanager
    def run_span(
        cls,
        *,
        trace_name: str,
        tags: Iterable[str],
        user_id: str,
        session_id: str,
        metadata: Mapping[str, str | None] | None = None,
        input_text: str | None = None,
    ) -> Generator[dict[str, str], None, None]:
        """Open the ``kin.run`` span of one run, with Langfuse user/session baggage for child spans.

        Initializes the tracer provider first. The deployment environment is appended to the tags
        (``env:<environment>``) and to the metadata (``env``).

        Args:
            trace_name: Langfuse trace name.
            tags: Langfuse trace tags.
            user_id: Langfuse trace user.
            session_id: Langfuse session (typically the mission id).
            metadata: Langfuse trace metadata; None values are skipped.
            input_text: Run input; its first 1000 characters become the trace input.

        Yields:
            :meth:`trace_headers` of the span, for :meth:`proxy_headers`.
        """
        cls.init_tracer_provider()
        environment = get_langfuse_settings().environment
        token = attach(
            baggage.set_baggage(
                "langfuse.session.id", session_id, context=baggage.set_baggage("langfuse.user.id", user_id)
            )
        )
        try:
            with trace_api.get_tracer("digitalkin.langfuse").start_as_current_span("kin.run") as span:
                span.set_attribute("langfuse.user.id", user_id)
                span.set_attribute("langfuse.session.id", session_id)
                span.set_attribute("langfuse.trace.name", trace_name)
                span.set_attribute("langfuse.trace.tags", [*tags, f"env:{environment}"])
                for key, value in (metadata or {}).items():
                    if value is not None:
                        span.set_attribute(f"langfuse.trace.metadata.{key}", value)
                span.set_attribute("langfuse.trace.metadata.env", environment)
                if input_text is not None:
                    span.set_attribute("langfuse.trace.input", input_text[:1000])
                yield cls.trace_headers()
        finally:
            detach(token)
