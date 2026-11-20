"""What one streamed model call received, to tell why a call was slow or cut by the proxy."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agno.models.response import ModelResponse


@dataclass
class StreamProgress:
    """Counts what a streamed call received and when, for a one-line diagnosis.

    The proxy cuts a call that sends nothing for 120 s: whether the call was silent from the start (a
    long prompt to read, unstreamed reasoning) or was writing text, reasoning or tool-call arguments
    tells which lever fixes it.

    Attributes:
        started: Monotonic time the call started.
        first: Seconds from the start to the first chunk; None while nothing arrived.
        last: Monotonic time of the latest chunk (the start before any).
        silence: Longest gap between two chunks seen so far.
        chunks: Chunks received.
        text: Characters of text received.
        reasoning: Characters of reasoning received.
        tools: Per tool-call index, its function name and the characters of arguments received.
    """

    started: float = field(init=False)
    first: float | None = None
    last: float | None = None
    silence: float = 0.0
    chunks: int = 0
    text: int = 0
    reasoning: int = 0
    tools: dict[int, list[str | int]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Start the clock."""
        self.started = time.monotonic()

    def see(self, chunk: ModelResponse) -> None:
        """Count one received chunk.

        Args:
            chunk: The parsed chunk.
        """
        now = time.monotonic()
        self.silence = max(self.silence, now - (self.started if self.last is None else self.last))
        self.last = now
        if self.first is None:
            self.first = now - self.started
        self.chunks += 1
        if isinstance(chunk.content, str):
            self.text += len(chunk.content)
        if isinstance(chunk.reasoning_content, str):
            self.reasoning += len(chunk.reasoning_content)
        for call in chunk.tool_calls or []:
            function = call.get("function") or {}
            entry = self.tools.setdefault(call.get("index", 0), ["?", 0])
            if function.get("name"):
                entry[0] = function["name"]
            entry[1] = int(entry[1]) + len(function.get("arguments") or "")

    def elapsed(self) -> float:
        """Seconds since the call started.

        Returns:
            The elapsed time.
        """
        return time.monotonic() - self.started

    def summary(self) -> str:
        """Describe what the call received.

        Returns:
            Elapsed time, first chunk, longest silence (the current one included), and what arrived.
        """
        now = time.monotonic()
        silence = max(self.silence, now - (self.started if self.last is None else self.last))
        calls = ", ".join(f"{name} ({size} chars of arguments)" for name, size in self.tools.values())
        return (
            f"{now - self.started:.1f}s, first chunk "
            + ("never" if self.first is None else f"at {self.first:.1f}s")
            + f", longest silence {silence:.1f}s, {self.chunks} chunks, text {self.text} chars, "
            f"reasoning {self.reasoning} chars, tool calls {calls or 'none'}"
        )
