"""Tests for SafeLiteLLM surfacing provider refusals instead of completing silently.

Anthropic ends a refused request with ``stop_reason: "refusal"`` and no content; litellm maps it to
``finish_reason: "content_filter"``. Agno's LiteLLM model ignores ``finish_reason``, so the run used
to complete with empty content: no text ever reached the front, which just showed nothing.
SafeLiteLLM turns that finish reason into an ``OutputCheckError`` so the run ends on a
``RunErrorEvent`` (forwarded to the front as ``RUN_ERROR``) — and is not retried, because the same
request is refused again.
"""

from collections.abc import AsyncIterator
from typing import Any

import pytest
from agno.agent import Agent, RunEvent
from agno.exceptions import OutputCheckError
from litellm.types.utils import Choices, Delta, Message, ModelResponse, ModelResponseStream, StreamingChoices, Usage

from digitalkin.community.agno.safe_litellm import SafeLiteLLM


def _chunk(*, content: str | None = None, finish_reason: str | None = None) -> ModelResponseStream:
    return ModelResponseStream(
        choices=[StreamingChoices(index=0, delta=Delta(content=content), finish_reason=finish_reason)],
    )


class _FakeClient:
    """Stands in for the litellm module: replays canned chunks and counts calls."""

    def __init__(self, chunks: list[ModelResponseStream]) -> None:
        self.chunks = chunks
        self.calls = 0

    async def acompletion(self, **_: Any) -> AsyncIterator[ModelResponseStream]:
        self.calls += 1

        async def stream() -> AsyncIterator[ModelResponseStream]:
            for chunk in self.chunks:
                yield chunk

        return stream()


async def _run(chunks: list[ModelResponseStream]) -> tuple[list[Any], _FakeClient]:
    client = _FakeClient(chunks)
    agent = Agent(
        model=SafeLiteLLM(id="claude-opus-5", client=client),
        retries=3,
        delay_between_retries=0,
        telemetry=False,
    )
    events = [event async for event in agent.arun("Bonjour", stream=True, stream_events=True)]
    return events, client


class TestStreamingRefusal:
    @pytest.mark.asyncio
    async def test_refusal_ends_the_run_on_an_error_event(self):
        events, _ = await _run([_chunk(content=""), _chunk(finish_reason="content_filter")])

        errors = [event for event in events if event.event == RunEvent.run_error]
        assert len(errors) == 1
        assert errors[0].error_type == "output_check_error"
        assert "declined" in errors[0].content

    @pytest.mark.asyncio
    async def test_refusal_is_not_retried(self):
        _, client = await _run([_chunk(content=""), _chunk(finish_reason="content_filter")])

        assert client.calls == 1

    @pytest.mark.asyncio
    async def test_refusal_after_partial_text_still_errors(self):
        events, _ = await _run([_chunk(content="Je vais"), _chunk(finish_reason="content_filter")])

        assert any(event.event == RunEvent.run_error for event in events)

    @pytest.mark.asyncio
    async def test_normal_completion_is_untouched(self):
        events, _ = await _run([_chunk(content="Bonjour !"), _chunk(finish_reason="stop")])

        assert not any(event.event == RunEvent.run_error for event in events)
        completed = [event for event in events if event.event == RunEvent.run_completed]
        assert completed[0].content == "Bonjour !"


class TestNonStreamingRefusal:
    def test_refused_response_raises(self):
        response = ModelResponse(
            choices=[Choices(index=0, message=Message(content=""), finish_reason="content_filter")],
            usage=Usage(prompt_tokens=10, completion_tokens=0, total_tokens=10),
        )

        with pytest.raises(OutputCheckError):
            SafeLiteLLM(id="claude-opus-5")._parse_provider_response(response)

    def test_normal_response_parses(self):
        response = ModelResponse(
            choices=[Choices(index=0, message=Message(content="Bonjour !"), finish_reason="stop")],
            usage=Usage(prompt_tokens=10, completion_tokens=3, total_tokens=13),
        )

        parsed = SafeLiteLLM(id="claude-opus-5")._parse_provider_response(response)

        assert parsed.content == "Bonjour !"
