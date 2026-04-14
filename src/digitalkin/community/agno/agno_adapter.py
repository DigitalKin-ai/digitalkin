"""Adapter to convert Agno events to DigitalKin framework-agnostic events.

This adapter bridges Agno-specific events to the DigitalKin event model,
allowing the core DigitalKin SDK to remain independent of Agno.

The adapter owns ALL state management: tracking reasoning/content lifecycle,
generating message_id and reasoning_id on each phase start, and emitting
proper start/completed events for text message and reasoning sequences.
"""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

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
    TextMessageCompletedEvent,
    TextMessageStartedEvent,
    ToolCallCompletedEvent,
    ToolCallErrorEvent,
    ToolCallStartedEvent,
    ToolInfo,
)

logger = logging.getLogger(__name__)


class AgnoStreamAdapter:
    """Stateful converter: Agno streaming events -> DigitalKin events.

    Tracks reasoning and content state so that events arriving on
    ``RunEvent.run_content`` are automatically wrapped in proper
    lifecycle events (TextMessageStarted/Completed, ReasoningStarted/Completed).

    Usage::

        adapter = AgnoStreamAdapter()
        async for raw_event in agent.arun(..., stream=True, stream_events=True):
            for event in adapter.to_digitalkin_events(raw_event):
                await send(event)
        for event in adapter.flush():
            await send(event)
    """

    def __init__(self) -> None:
        """Initialize the AgnoStreamAdapter."""
        self._reasoning_active: bool = False
        self._current_reasoning_id: str | None = None

        self._content_active: bool = False
        self._current_message_id: str | None = None

        self._closed_tool_call_ids: set[str] = set()

        self._active_run_id: str | None = None
        self._completed_run_ids: set[str] = set()

        # HITL pause state — populated when a RunPausedEvent is seen
        # (tools with external_execution=True). Callers can inspect these
        # after streaming to decide whether to persist and resume later.
        self._is_paused: bool = False
        self._paused_tool_executions: list[Any] = []
        self._paused_requirements: list[Any] = []

        self._dispatch: dict[Any, Callable[[Any, Any], list[BaseAgentRunEvent]]] | None = None

    @property
    def is_paused(self) -> bool:
        """Whether the last stream ended on a run_paused event (external tool HITL)."""
        return self._is_paused

    @property
    def paused_tool_executions(self) -> list[Any]:
        """Agno ``ToolExecution`` objects awaiting external execution (HITL)."""
        return list(self._paused_tool_executions)

    @property
    def paused_requirements(self) -> list[Any]:
        """Agno ``RunRequirement`` objects carried by the paused run."""
        return list(self._paused_requirements)

    def to_digitalkin_events(self, agno_event: Any) -> list[BaseAgentRunEvent]:
        """Convert one Agno event into one or more DigitalKin events.

        Args:
            agno_event: Event from Agno's streaming API.

        Returns:
            List of corresponding DigitalKin events (may be empty).

        Raises:
            ImportError: If the optional 'agno' dependency is not installed.
        """
        if self._dispatch is None:
            try:
                from agno.run.agent import RunEvent  # pylint: disable=C0415 # pyright: ignore[reportMissingImports]
            except ImportError as exc:
                message = "The 'agno' package is required to use AgnoStreamAdapter. Install it with: pip install agno"
                raise ImportError(message) from exc

            self._dispatch = {
                RunEvent.run_started: self._handle_run_started,
                RunEvent.run_content: self._handle_run_content,
                RunEvent.run_completed: self._handle_run_completed,
                RunEvent.run_error: self._handle_run_error,
                RunEvent.run_paused: self._handle_run_paused,
                RunEvent.reasoning_started: self._handle_reasoning_started,
                RunEvent.reasoning_content_delta: self._handle_reasoning_content_delta,
                RunEvent.reasoning_step: self._handle_reasoning_step,
                RunEvent.reasoning_completed: self._handle_reasoning_completed,
                RunEvent.tool_call_started: self._handle_tool_call_started,
                RunEvent.tool_call_completed: self._handle_tool_call_completed,
                RunEvent.tool_call_error: self._handle_tool_call_error,
            }

        event_type = agno_event.event
        logger.debug("Converting Agno event: %s", event_type)

        handler = self._dispatch.get(event_type)
        if handler is None:
            logger.debug("Skipping unhandled Agno event type: %s", event_type)
            return []

        return handler(agno_event, getattr(agno_event, "timestamp", None))

    # ── Run Lifecycle Handlers ───────────────────────────────────────────

    def _handle_run_started(self, agno_event: Any, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.run_started.

        Returns:
            List containing a RunStartedEvent, or empty for duplicates.
        """
        run_id = getattr(agno_event, "run_id", None)

        if run_id and run_id == self._active_run_id:
            logger.debug("Skipping duplicate RunStarted for run_id=%s", run_id)
            return []

        self._active_run_id = run_id
        return [
            RunStartedEvent(
                event=AgentRunEvent.RUN_STARTED,
                run_id=run_id,
                thread_id=getattr(agno_event, "thread_id", None),
                timestamp=timestamp,
                metadata=None,
            )
        ]

    def _handle_run_completed(self, agno_event: Any, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.run_completed.

        Returns:
            List of closing events followed by a RunCompletedEvent.
        """
        run_id = getattr(agno_event, "run_id", None)

        if run_id and run_id in self._completed_run_ids and run_id != self._active_run_id:
            logger.debug("Skipping duplicate RunCompleted for run_id=%s", run_id)
            return []

        events: list[BaseAgentRunEvent] = []

        if self._content_active:
            events.extend(self._close_content(timestamp))
        if self._reasoning_active:
            events.extend(self._close_reasoning(timestamp))

        if run_id:
            self._completed_run_ids.add(run_id)
        self._active_run_id = None

        events.append(
            RunCompletedEvent(
                event=AgentRunEvent.RUN_COMPLETED,
                run_id=run_id,
                final_content=str(agno_event.content) if getattr(agno_event, "content", None) else None,
                usage=None,
                message_id=None,
                timestamp=timestamp,
                metadata=None,
            )
        )
        return events

    def _handle_run_error(  # noqa: PLR6301
        self,
        agno_event: Any,
        timestamp: Any,
    ) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.run_error.

        Returns:
            List containing a RunErrorEvent.
        """
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

    # ── Reasoning Handlers (native Agno reasoning models) ───────────────

    def _handle_reasoning_started(self, agno_event: Any, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.reasoning_started.

        Returns:
            List with an optional TextMessageCompletedEvent and a ReasoningStartedEvent.
        """
        _ = agno_event
        events: list[BaseAgentRunEvent] = []

        if self._content_active:
            events.extend(self._close_content(timestamp))

        self._current_reasoning_id = str(uuid.uuid4())
        self._reasoning_active = True
        logger.debug("Reasoning started, id=%s", self._current_reasoning_id)
        events.append(
            ReasoningStartedEvent(
                event=AgentRunEvent.REASONING_STARTED,
                reasoning_id=self._current_reasoning_id,
                timestamp=timestamp,
                metadata=None,
            )
        )
        return events

    def _handle_reasoning_content_delta(self, agno_event: Any, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.reasoning_content_delta.

        Returns:
            List containing a ReasoningContentDeltaEvent.
        """
        return [
            ReasoningContentDeltaEvent(
                event=AgentRunEvent.REASONING_CONTENT_DELTA,
                delta=getattr(agno_event, "reasoning_content", ""),
                reasoning_id=self._current_reasoning_id,
                timestamp=timestamp,
                metadata=None,
            )
        ]

    def _handle_reasoning_step(self, agno_event: Any, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.reasoning_step.

        Returns:
            List containing a ReasoningStepEvent.
        """
        return [
            ReasoningStepEvent(
                event=AgentRunEvent.REASONING_STEP,
                delta=getattr(agno_event, "reasoning_content", ""),
                reasoning_id=self._current_reasoning_id,
                timestamp=timestamp,
                metadata=None,
            )
        ]

    def _handle_reasoning_completed(self, agno_event: Any, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.reasoning_completed.

        Returns:
            List containing a ReasoningCompletedEvent if reasoning was active.
        """
        _ = agno_event
        logger.debug("Reasoning completed")
        return self._close_reasoning(timestamp)

    # ── Tool Call Handlers ──────────────────────────────────────────────

    def _handle_tool_call_started(self, agno_event: Any, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.tool_call_started.

        Returns:
            List of any needed closing events and a ToolCallStartedEvent.
        """
        events: list[BaseAgentRunEvent] = []

        if self._reasoning_active:
            logger.debug("Reasoning auto-completed (tool call started)")
            events.extend(self._close_reasoning(timestamp))
        if self._content_active:
            events.extend(self._close_content(timestamp))

        tool = getattr(agno_event, "tool", None)
        tool_info = None
        if tool:
            tool_info = ToolInfo(
                tool_call_id=getattr(tool, "tool_call_id", None),
                tool_name=getattr(tool, "tool_name", None),
                tool_args=getattr(tool, "tool_args", None),
                result=None,
            )
        events.append(
            ToolCallStartedEvent(
                event=AgentRunEvent.TOOL_CALL_STARTED,
                tool=tool_info,
                timestamp=timestamp,
                metadata=None,
            )
        )
        return events

    def _handle_tool_call_completed(self, agno_event: Any, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.tool_call_completed.

        Returns:
            List containing a ToolCallCompletedEvent.
        """
        tool = getattr(agno_event, "tool", None)
        tool_info = None
        tool_call_id = None
        if tool:
            tool_call_id = getattr(tool, "tool_call_id", None)
            tool_info = ToolInfo(
                tool_call_id=tool_call_id,
                tool_name=getattr(tool, "tool_name", None),
                tool_args=getattr(tool, "tool_args", None),
                result=getattr(tool, "result", None),
            )

        if tool_call_id:
            self._closed_tool_call_ids.add(tool_call_id)

        return [
            ToolCallCompletedEvent(
                event=AgentRunEvent.TOOL_CALL_COMPLETED,
                tool=tool_info,
                content=str(agno_event.content) if getattr(agno_event, "content", None) else None,
                timestamp=timestamp,
                metadata=None,
            )
        ]

    def _handle_run_paused(self, agno_event: Any, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle ``RunEvent.run_paused`` — HITL pause on external tool execution.

        Agno does NOT emit ``tool_call_started`` / ``tool_call_completed`` for
        tools declared with ``external_execution=True`` (see
        ``agno/models/base.py`` where the emission is short-circuited). The
        front therefore never sees the corresponding AG-UI ``ToolCallStart``
        / ``ToolCallArgs`` / ``ToolCallEnd`` events unless we synthesize them.

        This handler:

        1. Closes any active reasoning / content sequence.
        2. Iterates ``RunPausedEvent.tools`` and emits one pair of
           ``ToolCallStartedEvent`` + ``ToolCallCompletedEvent`` per tool.
           The ``ToolCallCompletedEvent`` carries ``content=None`` and
           ``tool.result=None`` so the downstream AG-UI bridge emits
           ``ToolCallEnd`` *without* a ``ToolCallResult`` (guarded by the
           ``if result_content:`` check in ``AgUiMixin``).
        3. Records pause state on the adapter (``is_paused``,
           ``paused_tool_executions``, ``paused_requirements``) so callers
           can detect the pause after streaming and persist the run for
           later resumption.

        Returns:
            Synthesized tool-call events for the paused tools. The caller
            is responsible for subsequently emitting the AG-UI
            ``RunFinished`` with ``result.status = "awaiting_tool_result"``
            — this adapter stays protocol-agnostic.
        """
        events: list[BaseAgentRunEvent] = []

        if self._reasoning_active:
            events.extend(self._close_reasoning(timestamp))
        if self._content_active:
            events.extend(self._close_content(timestamp))

        tools = getattr(agno_event, "tools", None) or []
        requirements = getattr(agno_event, "requirements", None) or []

        self._is_paused = True
        self._paused_tool_executions = list(tools)
        self._paused_requirements = list(requirements)

        for tool_exec in tools:
            tool_call_id = getattr(tool_exec, "tool_call_id", None)
            tool_info = ToolInfo(
                tool_call_id=tool_call_id,
                tool_name=getattr(tool_exec, "tool_name", None),
                tool_args=getattr(tool_exec, "tool_args", None),
                result=None,
            )
            logger.debug(
                "Synthesizing tool-call events for external_execution tool %s (id=%s)",
                tool_info.tool_name,
                tool_call_id,
            )
            events.extend((
                ToolCallStartedEvent(
                    event=AgentRunEvent.TOOL_CALL_STARTED,
                    tool=tool_info,
                    timestamp=timestamp,
                    metadata=None,
                ),
                ToolCallCompletedEvent(
                    event=AgentRunEvent.TOOL_CALL_COMPLETED,
                    tool=tool_info,
                    content=None,
                    timestamp=timestamp,
                    metadata=None,
                ),
            ))
            if tool_call_id:
                self._closed_tool_call_ids.add(tool_call_id)

        return events

    def _handle_tool_call_error(self, agno_event: Any, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.tool_call_error.

        Returns:
            List containing a ToolCallErrorEvent, or empty if already closed.
        """
        tool = getattr(agno_event, "tool", None)
        tool_call_id = getattr(tool, "tool_call_id", None) if tool else None

        if tool_call_id and tool_call_id in self._closed_tool_call_ids:
            logger.debug("Skipping duplicate ToolCallError for tool %s", tool_call_id)
            return []

        tool_info = None
        if tool:
            tool_info = ToolInfo(
                tool_call_id=tool_call_id,
                tool_name=getattr(tool, "tool_name", None),
                tool_args=None,
                result=None,
            )

        if tool_call_id:
            self._closed_tool_call_ids.add(tool_call_id)

        return [
            ToolCallErrorEvent(
                event=AgentRunEvent.TOOL_CALL_ERROR,
                tool=tool_info,
                error_message=str(agno_event.content) if getattr(agno_event, "content", None) else None,
                timestamp=timestamp,
                metadata=None,
            )
        ]

    def flush(self) -> list[BaseAgentRunEvent]:
        """Emit closing events for any active sequences at end of stream.

        Returns:
            List of closing events (empty if nothing is active).
        """
        events: list[BaseAgentRunEvent] = []
        if self._content_active:
            logger.debug("Flushing active content sequence")
            events.extend(self._close_content(None))
        if self._reasoning_active:
            logger.debug("Flushing active reasoning sequence")
            events.extend(self._close_reasoning(None))
        return events

    # ── Private Helpers ──────────────────────────────────────────────────

    def _close_reasoning(self, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Close active reasoning sequence.

        Returns:
            List of closing events (empty if reasoning is not active).
        """
        if not self._reasoning_active:
            return []

        events: list[BaseAgentRunEvent] = [
            ReasoningCompletedEvent(
                event=AgentRunEvent.REASONING_COMPLETED,
                reasoning_id=self._current_reasoning_id,
                timestamp=timestamp,
                metadata=None,
            )
        ]
        self._reasoning_active = False
        self._current_reasoning_id = None
        return events

    def _close_content(self, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Close active text message sequence.

        Returns:
            List of closing events (empty if content is not active).
        """
        if not self._content_active:
            return []

        events: list[BaseAgentRunEvent] = [
            TextMessageCompletedEvent(
                event=AgentRunEvent.TEXT_MESSAGE_COMPLETED,
                message_id=self._current_message_id or "",
                timestamp=timestamp,
                metadata=None,
            )
        ]
        self._content_active = False
        self._current_message_id = None
        return events

    def _handle_run_content(self, agno_event: Any, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.run_content — the core state machine.

        Rules:
        - reasoning_content non-empty: reasoning data (close content if transitioning)
        - content non-empty: text data (close reasoning if transitioning)
        - reasoning_content == "": close reasoning if active
        - content == "": close content if active
        - None values: ignored

        Returns:
            List of DigitalKin events for this run_content chunk.
        """
        events: list[BaseAgentRunEvent] = []

        reasoning_content = getattr(agno_event, "reasoning_content", None)
        content = agno_event.content

        # ── Reasoning content handling ──
        if reasoning_content is not None:
            events.extend(self._process_reasoning_content(reasoning_content, timestamp))

        # ── Text content handling ──
        if content is not None:
            events.extend(self._process_text_content(content, timestamp))

        # Edge case: neither reasoning_content nor content
        if reasoning_content is None and content is None:
            logger.debug("run_content with no content, skipping")

        return events

    def _process_reasoning_content(self, reasoning_content: str, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Process reasoning_content from a run_content event.

        Returns:
            List of reasoning lifecycle and content events.
        """
        events: list[BaseAgentRunEvent] = []

        if not reasoning_content:
            # Empty string "" → signal to close reasoning
            if self._reasoning_active:
                events.extend(self._close_reasoning(timestamp))
            return events

        # Non-empty string → reasoning data
        # Close text message if transitioning from content to reasoning
        if self._content_active:
            events.extend(self._close_content(timestamp))

        # Auto-open reasoning on first chunk
        if not self._reasoning_active:
            self._current_reasoning_id = str(uuid.uuid4())
            logger.debug("Reasoning auto-started, id=%s", self._current_reasoning_id)
            events.append(
                ReasoningStartedEvent(
                    event=AgentRunEvent.REASONING_STARTED,
                    reasoning_id=self._current_reasoning_id,
                    timestamp=timestamp,
                    metadata=None,
                )
            )
            self._reasoning_active = True

        events.append(
            ReasoningContentDeltaEvent(
                event=AgentRunEvent.REASONING_CONTENT_DELTA,
                delta=reasoning_content,
                reasoning_id=self._current_reasoning_id,
                timestamp=timestamp,
                metadata=None,
            )
        )
        return events

    def _process_text_content(self, content: str, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Process text content from a run_content event.

        Returns:
            List of text message lifecycle and content events.
        """
        events: list[BaseAgentRunEvent] = []

        if not content:
            # Empty string "" → signal to close text message
            if self._content_active:
                events.extend(self._close_content(timestamp))
            return events

        # Non-empty string → text data
        # Close reasoning if transitioning from reasoning to content
        if self._reasoning_active:
            logger.debug("Reasoning auto-completed (text content arrived)")
            events.extend(self._close_reasoning(timestamp))

        # Auto-open text message on first chunk
        if not self._content_active:
            self._current_message_id = str(uuid.uuid4())
            events.append(
                TextMessageStartedEvent(
                    event=AgentRunEvent.TEXT_MESSAGE_STARTED,
                    message_id=self._current_message_id,
                    timestamp=timestamp,
                    metadata=None,
                )
            )
            self._content_active = True

        events.append(
            RunContentEvent(
                event=AgentRunEvent.RUN_CONTENT,
                content=str(content),
                message_id=self._current_message_id,
                reasoning_content=None,
                content_type=None,
                timestamp=timestamp,
                metadata=None,
            )
        )
        return events
