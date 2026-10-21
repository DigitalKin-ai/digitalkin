"""Tests for SafeLiteLLM keeping the model-native URL citations agno's LiteLLM parser drops.

litellm returns OpenAI-style ``url_citation`` annotations on the message (and on streamed deltas) when
the model cites web sources. Agno's own OpenAI parser maps them to ``ModelResponse.citations``; its
LiteLLM parser does not, so they never became ``source_citation`` events.
"""

from litellm.types.utils import Delta, Message, ModelResponse, ModelResponseStream, StreamingChoices, Usage

from digitalkin.community.agno.safe_litellm import SafeLiteLLM

_ANNOTATIONS = [
    {
        "type": "url_citation",
        "url_citation": {"url": "https://ec.europa.eu/ai-act", "title": "AI Act", "start_index": 0},
    },
    {"type": "url_citation", "url_citation": {"url": "https://example.org/news"}},
    {"type": "url_citation", "url_citation": {"title": "no url"}},
    {"type": "url_citation", "url_citation": {"url": "javascript:alert(1)", "title": "script"}},
    {"type": "url_citation", "url_citation": {"url": "data:text/html,hi"}},
    {"type": "url_citation", "url_citation": {"url": "https:///no-host"}},
]


def _model() -> SafeLiteLLM:
    """Build the model without __init__ (which needs live LiteLLM config)."""
    return SafeLiteLLM.__new__(SafeLiteLLM)


def _completion(annotations: list | None) -> ModelResponse:
    message = Message(role="assistant", content="answer", annotations=annotations)
    return ModelResponse(
        choices=[{"index": 0, "message": message, "finish_reason": "stop"}],
        usage=Usage(prompt_tokens=1, completion_tokens=1, total_tokens=2),
    )


def _chunk(annotations: list | None) -> ModelResponseStream:
    delta = Delta(role="assistant", content="ans", annotations=annotations)
    return ModelResponseStream(choices=[StreamingChoices(index=0, delta=delta)])


class TestCompletionCitations:
    def test_url_citations_become_agno_citations(self):
        parsed = _model()._parse_provider_response(_completion(_ANNOTATIONS))

        assert parsed.citations is not None
        assert [(c.url, c.title) for c in parsed.citations.urls] == [
            ("https://ec.europa.eu/ai-act", "AI Act"),
            ("https://example.org/news", None),
        ]
        assert parsed.content == "answer"

    def test_no_annotations_leaves_citations_unset(self):
        assert _model()._parse_provider_response(_completion(None)).citations is None


class TestStreamCitations:
    def test_delta_annotations_become_agno_citations(self):
        parsed = _model()._parse_provider_response_delta(_chunk(_ANNOTATIONS))

        assert parsed.citations is not None
        assert [c.url for c in parsed.citations.urls] == ["https://ec.europa.eu/ai-act", "https://example.org/news"]
        assert parsed.content == "ans"

    def test_delta_without_annotations_leaves_citations_unset(self):
        assert _model()._parse_provider_response_delta(_chunk(None)).citations is None

    def test_only_web_urls_are_kept(self):
        """``javascript:``/``data:`` URLs and host-less ones would render as unsafe or broken links."""
        parsed = _model()._parse_provider_response_delta(
            _chunk([{"type": "url_citation", "url_citation": {"url": "javascript:alert(1)"}}])
        )
        assert parsed.citations is None

    def test_annotations_without_any_url_leave_citations_unset(self):
        parsed = _model()._parse_provider_response_delta(_chunk([{"type": "url_citation", "url_citation": {}}]))
        assert parsed.citations is None
