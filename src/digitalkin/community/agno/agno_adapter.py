"""Adapter to convert Agno events to DigitalKin framework-agnostic events.

This adapter bridges Agno-specific events to the DigitalKin event model,
allowing the core DigitalKin SDK to remain independent of Agno.
"""

from __future__ import annotations

import logging
from typing import Any

from digitalkin.models.events import (
    AgentRunEvent,
    BaseAgentRunEvent,
    ReasoningCompletedEvent,
    ReasoningContentDeltaEvent,
    ReasoningStartedEvent,
    ReasoningStepEvent,
    RunCompletedEvent,
    RunContentEvent,
    RunErrorEvent,
    RunStartedEvent,
    ToolCallCompletedEvent,
    ToolCallErrorEvent,
    ToolCallStartedEvent,
    ToolInfo,
)

logger = logging.getLogger(__name__)


class AgnoStreamAdapter:
    """Stateful converter: Agno streaming events -> DigitalKin events.

    Tracks reasoning state so that ``reasoning_content`` arriving on
    ``RunEvent.run_content`` events is automatically wrapped in proper
    ReasoningStarted / ReasoningContentDelta / ReasoningCompleted events.

    Usage::

        adapter = AgnoStreamAdapter()
        async for raw_event in agent.arun(..., stream=True, stream_events=True):
            for event in adapter.to_digitalkin_events(raw_event):
                await send(event)
        for event in adapter.flush():
            await send(event)
    """

    def __init__(self) -> None:
        """Initialize the AgnoStreamAdapter.

        This adapter tracks reasoning state to properly handle reasoning_content
        that arrives on RunEvent.run_content events.
        """
        self._reasoning_active: bool = False

    def to_digitalkin_events(self, agno_event: Any) -> list[BaseAgentRunEvent]:  # noqa: C901, PLR0911, PLR0912
        """Convert one Agno event into one or more DigitalKin events.

        Args:
            agno_event: Event from Agno's streaming API.

        Returns:
            List of corresponding DigitalKin events (may be empty).

        Raises:
            ImportError: If the optional 'agno' dependency is not installed.
        """
        try:
            from agno.run.agent import RunEvent  # pylint: disable=C0415 # pyright: ignore[reportMissingImports]
        except ImportError as exc:
            message = "The 'agno' package is required to use AgnoStreamAdapter. Install it with: pip install agno"
            raise ImportError(message) from exc

        event_type = agno_event.event
        event_types = getattr(agno_event, "events", None)

        logger.info("[DK STREAM-DEBUG => agno_adapter] Converting Agno event type: %s", event_type)
        logger.info("[DK STREAM-DEBUG => agno_adapter event_types] Converting Agno event types: %s", event_types)

        timestamp = getattr(agno_event, "timestamp", None)

        # ── Run Lifecycle ────────────────────────────────────────────────

        if event_type == RunEvent.run_started:
            return [
                RunStartedEvent(
                    event=AgentRunEvent.RUN_STARTED,
                    run_id=getattr(agno_event, "run_id", None),
                    thread_id=getattr(agno_event, "thread_id", None),
                    timestamp=timestamp,
                    metadata=None,
                )
            ]

        if event_type == RunEvent.run_content:
            return self._handle_run_content(agno_event, timestamp)

        if event_type == RunEvent.run_completed:
            return [
                RunCompletedEvent(
                    event=AgentRunEvent.RUN_COMPLETED,
                    run_id=getattr(agno_event, "run_id", None),
                    final_content=str(agno_event.content) if getattr(agno_event, "content", None) else None,
                    usage=None,
                    timestamp=timestamp,
                    metadata=None,
                )
            ]

        if event_type == RunEvent.run_error:
            return [
                RunErrorEvent(
                    event=AgentRunEvent.RUN_ERROR,
                    error_type=getattr(agno_event, "error_type", None),
                    content=str(agno_event.content) if getattr(agno_event, "content", None) else None,
                    error_details=None,
                    timestamp=timestamp,
                    metadata=None,
                )
            ]

        # ── Explicit Reasoning Events (native Agno reasoning models) ────

        if event_type == RunEvent.reasoning_started:
            self._reasoning_active = True
            logger.info("[DK STREAM-DEBUG => agno_adapter] Reasoning started (explicit)")
            return [
                ReasoningStartedEvent(
                    event=AgentRunEvent.REASONING_STARTED,
                    timestamp=timestamp,
                    metadata=None,
                )
            ]

        if event_type == RunEvent.reasoning_content_delta:
            reasoning_content = getattr(agno_event, "reasoning_content", "")
            return [
                ReasoningContentDeltaEvent(
                    event=AgentRunEvent.REASONING_CONTENT_DELTA,
                    delta=reasoning_content,
                    timestamp=timestamp,
                    metadata=None,
                )
            ]

        if event_type == RunEvent.reasoning_step:
            reasoning_content = getattr(agno_event, "reasoning_content", "")
            return [
                ReasoningStepEvent(
                    event=AgentRunEvent.REASONING_STEP,
                    delta=reasoning_content,
                    timestamp=timestamp,
                    metadata=None,
                )
            ]

        if event_type == RunEvent.reasoning_completed:
            self._reasoning_active = False
            logger.info("[DK STREAM-DEBUG => agno_adapter] Reasoning completed (explicit)")
            return [
                ReasoningCompletedEvent(
                    event=AgentRunEvent.REASONING_COMPLETED,
                    timestamp=timestamp,
                    metadata=None,
                )
            ]

        # ── Tool Call Events ─────────────────────────────────────────────

        if event_type == RunEvent.tool_call_started:
            tool = getattr(agno_event, "tool", None)
            tool_info = None
            if tool:
                tool_info = ToolInfo(
                    tool_call_id=getattr(tool, "tool_call_id", None),
                    tool_name=getattr(tool, "tool_name", None),
                    tool_args=getattr(tool, "tool_args", None),
                    result=None,
                )
            return [
                ToolCallStartedEvent(
                    event=AgentRunEvent.TOOL_CALL_STARTED,
                    tool=tool_info,
                    timestamp=timestamp,
                    metadata=None,
                )
            ]

        if event_type == RunEvent.tool_call_completed:
            tool = getattr(agno_event, "tool", None)
            tool_info = None
            if tool:
                tool_info = ToolInfo(
                    tool_call_id=getattr(tool, "tool_call_id", None),
                    tool_name=getattr(tool, "tool_name", None),
                    tool_args=getattr(tool, "tool_args", None),
                    result=getattr(tool, "result", None),
                )
            return [
                ToolCallCompletedEvent(
                    event=AgentRunEvent.TOOL_CALL_COMPLETED,
                    tool=tool_info,
                    content=str(agno_event.content) if getattr(agno_event, "content", None) else None,
                    timestamp=timestamp,
                    metadata=None,
                )
            ]

        if event_type == RunEvent.tool_call_error:
            tool = getattr(agno_event, "tool", None)
            tool_info = None
            if tool:
                tool_info = ToolInfo(
                    tool_call_id=getattr(tool, "tool_call_id", None),
                    tool_name=getattr(tool, "tool_name", None),
                    tool_args=None,
                    result=None,
                )
            return [
                ToolCallErrorEvent(
                    event=AgentRunEvent.TOOL_CALL_ERROR,
                    tool=tool_info,
                    error_message=str(agno_event.content) if getattr(agno_event, "content", None) else None,
                    timestamp=timestamp,
                    metadata=None,
                )
            ]

        # Unknown event - skip
        logger.debug("Skipping unhandled Agno event type: %s", event_type)
        return []

    def flush(self) -> list[BaseAgentRunEvent]:
        """Emit closing events if reasoning is still active at end of stream.

        Returns:
            List of closing events (empty if no active reasoning).
        """
        events: list[BaseAgentRunEvent] = []
        if self._reasoning_active:
            logger.info("[DK STREAM-DEBUG => agno_adapter] Flushing: closing active reasoning")
            events.append(
                ReasoningCompletedEvent(
                    event=AgentRunEvent.REASONING_COMPLETED,
                    timestamp=None,
                    metadata=None,
                )
            )
            self._reasoning_active = False
        return events

    # ── Private Helpers ──────────────────────────────────────────────────

    def _handle_run_content(self, agno_event: Any, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.run_content with reasoning_content awareness.

        When reasoning_content is present on a run_content event (native model
        reasoning via LiteLLM/proxy), this method emits proper reasoning
        lifecycle events instead of plain RunContentEvent.

        Args:
            agno_event: The Agno run_content event.
            timestamp: The timestamp for the event.

        Returns:
            List of DigitalKin events (reasoning events or run content event).
        """
        events: list[BaseAgentRunEvent] = []

        reasoning_content = getattr(agno_event, "reasoning_content", None)
        content = agno_event.content

        # ── Reasoning content present ────────────────────────────────
        if reasoning_content:
            # Auto-open reasoning sequence on first reasoning chunk
            if not self._reasoning_active:
                logger.info("[DK STREAM-DEBUG => agno_adapter] Reasoning auto-started from run_content")
                events.append(
                    ReasoningStartedEvent(
                        event=AgentRunEvent.REASONING_STARTED,
                        timestamp=timestamp,
                        metadata=None,
                    )
                )
                self._reasoning_active = True

            logger.info(
                "[DK STREAM-DEBUG => agno_adapter] Reasoning content delta (len=%d)",
                len(reasoning_content),
            )
            events.append(
                ReasoningContentDeltaEvent(
                    event=AgentRunEvent.REASONING_CONTENT_DELTA,
                    delta=reasoning_content,
                    timestamp=timestamp,
                    metadata=None,
                )
            )

        # ── Text content present ─────────────────────────────────────
        if content:
            # Auto-close reasoning when text content starts
            if self._reasoning_active:
                logger.info("[DK STREAM-DEBUG => agno_adapter] Reasoning auto-completed (text content arrived)")
                events.append(
                    ReasoningCompletedEvent(
                        event=AgentRunEvent.REASONING_COMPLETED,
                        timestamp=timestamp,
                        metadata=None,
                    )
                )
                self._reasoning_active = False

            events.append(
                RunContentEvent(
                    event=AgentRunEvent.RUN_CONTENT,
                    content=str(content),
                    reasoning_content=None,
                    content_type=None,
                    timestamp=timestamp,
                    metadata=None,
                )
            )

        # Edge case: neither reasoning_content nor content
        if not reasoning_content and not content:
            logger.debug("[DK STREAM-DEBUG => agno_adapter] run_content with no content, skipping")

        return events
