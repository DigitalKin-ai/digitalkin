"""StreamProgress: what one streamed model call received, for diagnosing a slow or cut call."""

from collections.abc import Iterator

import pytest
from agno.models.response import ModelResponse

from digitalkin.community.agno.stream_progress import StreamProgress


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[float]]:
    """A settable monotonic clock: tests append the next readings."""
    readings: list[float] = [0.0]
    monkeypatch.setattr("digitalkin.community.agno.stream_progress.time.monotonic", lambda: readings[0])
    return readings


class TestStreamProgress:
    def test_a_call_that_received_nothing_says_so(self, clock: list[float]) -> None:
        progress = StreamProgress()
        clock[0] = 120.4

        assert progress.elapsed() == pytest.approx(120.4)
        assert progress.summary() == (
            "120.4s, first chunk never, longest silence 120.4s, 0 chunks, text 0 chars, reasoning 0 chars, "
            "tool calls none"
        )

    def test_text_reasoning_and_tool_call_arguments_are_counted(self, clock: list[float]) -> None:
        progress = StreamProgress()
        clock[0] = 2.0
        progress.see(ModelResponse(reasoning_content="think"))
        clock[0] = 3.0
        progress.see(ModelResponse(content="Hello"))
        clock[0] = 50.0
        progress.see(
            ModelResponse(tool_calls=[{"index": 0, "function": {"name": "services_manager", "arguments": '{"ac'}}])
        )
        clock[0] = 51.0
        progress.see(ModelResponse(tool_calls=[{"index": 0, "function": {"arguments": 'tion": 1}'}}]))
        clock[0] = 60.0

        assert progress.summary() == (
            "60.0s, first chunk at 2.0s, longest silence 47.0s, 4 chunks, text 5 chars, reasoning 5 chars, "
            "tool calls services_manager (13 chars of arguments)"
        )

    def test_the_silence_since_the_last_chunk_counts(self, clock: list[float]) -> None:
        progress = StreamProgress()
        clock[0] = 1.0
        progress.see(ModelResponse(content="x"))
        clock[0] = 121.0

        assert "longest silence 120.0s" in progress.summary()

    def test_parallel_tool_calls_are_told_apart(self, clock: list[float]) -> None:
        progress = StreamProgress()
        progress.see(
            ModelResponse(
                tool_calls=[
                    {"index": 0, "function": {"name": "load", "arguments": "{}"}},
                    {"index": 1, "function": {"name": "search", "arguments": "{}"}},
                ]
            )
        )

        assert progress.summary().endswith("tool calls load (2 chars of arguments), search (2 chars of arguments)")
