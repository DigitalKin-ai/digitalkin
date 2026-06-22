"""Convert Agno streaming events into framework-agnostic DigitalKin events."""

from __future__ import annotations

import logging
import uuid
from typing import TYPE_CHECKING, Any, TypeAlias

if TYPE_CHECKING:
    from collections.abc import Callable

    from agno.run.agent import BaseAgentRunEvent as _AgentBase
    from agno.run.agent import ReasoningCompletedEvent as _AgentReasoningCompleted
    from agno.run.agent import ReasoningContentDeltaEvent as _AgentReasoningContentDelta
    from agno.run.agent import ReasoningStartedEvent as _AgentReasoningStarted
    from agno.run.agent import ReasoningStepEvent as _AgentReasoningStep
    from agno.run.agent import RunCompletedEvent as _AgentRunCompleted
    from agno.run.agent import RunContentEvent as _AgentRunContent
    from agno.run.agent import RunErrorEvent as _AgentRunError
    from agno.run.agent import RunPausedEvent as _AgentRunPaused
    from agno.run.agent import RunStartedEvent as _AgentRunStarted
    from agno.run.agent import ToolCallCompletedEvent as _AgentToolCallCompleted
    from agno.run.agent import ToolCallErrorEvent as _AgentToolCallError
    from agno.run.agent import ToolCallStartedEvent as _AgentToolCallStarted
    from agno.run.team import BaseTeamRunEvent as _TeamBase
    from agno.run.team import ReasoningCompletedEvent as _TeamReasoningCompleted
    from agno.run.team import ReasoningContentDeltaEvent as _TeamReasoningContentDelta
    from agno.run.team import ReasoningStartedEvent as _TeamReasoningStarted
    from agno.run.team import ReasoningStepEvent as _TeamReasoningStep
    from agno.run.team import RunCompletedEvent as _TeamRunCompleted
    from agno.run.team import RunContentEvent as _TeamRunContent
    from agno.run.team import RunErrorEvent as _TeamRunError
    from agno.run.team import RunPausedEvent as _TeamRunPaused
    from agno.run.team import RunStartedEvent as _TeamRunStarted
    from agno.run.team import ToolCallCompletedEvent as _TeamToolCallCompleted
    from agno.run.team import ToolCallErrorEvent as _TeamToolCallError
    from agno.run.team import ToolCallStartedEvent as _TeamToolCallStarted

    AgnoRunEvent: TypeAlias = _AgentBase | _TeamBase
    AgnoRunStartedEvent: TypeAlias = _AgentRunStarted | _TeamRunStarted
    AgnoRunContentEvent: TypeAlias = _AgentRunContent | _TeamRunContent
    AgnoRunCompletedEvent: TypeAlias = _AgentRunCompleted | _TeamRunCompleted
    AgnoRunErrorEvent: TypeAlias = _AgentRunError | _TeamRunError
    AgnoRunPausedEvent: TypeAlias = _AgentRunPaused | _TeamRunPaused
    AgnoReasoningStartedEvent: TypeAlias = _AgentReasoningStarted | _TeamReasoningStarted
    AgnoReasoningContentDeltaEvent: TypeAlias = _AgentReasoningContentDelta | _TeamReasoningContentDelta
    AgnoReasoningStepEvent: TypeAlias = _AgentReasoningStep | _TeamReasoningStep
    AgnoReasoningCompletedEvent: TypeAlias = _AgentReasoningCompleted | _TeamReasoningCompleted
    AgnoToolCallStartedEvent: TypeAlias = _AgentToolCallStarted | _TeamToolCallStarted
    AgnoToolCallCompletedEvent: TypeAlias = _AgentToolCallCompleted | _TeamToolCallCompleted
    AgnoToolCallErrorEvent: TypeAlias = _AgentToolCallError | _TeamToolCallError

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
    """Stateful Agno→DigitalKin event converter.

    Auto-wraps ``run_content`` deltas in TextMessage/Reasoning lifecycle
    events and tracks HITL pause state.
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

        self._is_paused: bool = False
        self._paused_tool_executions: list[Any] = []
        self._paused_requirements: list[Any] = []

        self._dispatch: dict[Any, Callable[..., list[BaseAgentRunEvent]]] | None = None
        self._team_enum: type | None = None

        self._last_metadata: dict[str, Any] | None = None

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

    @staticmethod
    def _build_metadata(agno_event: AgnoRunEvent, *, is_team: bool) -> dict[str, Any]:
        """Extract identity info from a raw Agno event.

        Team leader events carry ``team_id``/``team_name``; agent events carry
        ``agent_id``/``agent_name``. Member events set ``parent_run_id`` to the
        team's run id so the client can group deltas by speaker.

        ``run_id`` is intentionally absent: it is already carried by the typed
        event fields (``RunStartedEvent.run_id`` etc.) and must not be
        duplicated in ``metadata``.

        Args:
            agno_event: Raw Agno event (Pydantic model or equivalent).
            is_team: Whether the event originates from a team-level context.

        Returns:
            Dict with ``source``, ``name``, ``id`` and ``parent_run_id`` —
            ready to hand to the ``metadata`` field of a DigitalKin event.
        """
        data = agno_event.__dict__
        if is_team:
            return {
                "source": "team",
                "name": data.get("team_name"),
                "id": data.get("team_id"),
                "parent_run_id": data.get("parent_run_id"),
            }
        return {
            "source": "agent",
            "name": data.get("agent_name"),
            "id": data.get("agent_id"),
            "parent_run_id": data.get("parent_run_id"),
        }

    def _build_dispatch(self) -> dict[Any, Callable[..., list[BaseAgentRunEvent]]]:
        """Import Agno enums lazily and populate the dispatch table.

        Also caches ``TeamRunEvent`` in ``self._team_enum`` for source detection.

        Returns:
            Dispatch table mapping Agno event enum members to handlers.

        Raises:
            ImportError: If the optional 'agno' dependency is not installed.
        """
        try:
            from agno.run.agent import RunEvent  # pylint: disable=C0415
            from agno.run.team import TeamRunEvent  # pylint: disable=C0415
        except ImportError as exc:
            message = "The 'agno' package is required to use AgnoStreamAdapter. Install it with: pip install agno"
            raise ImportError(message) from exc

        self._team_enum = TeamRunEvent

        handler_by_name: dict[str, Callable[..., list[BaseAgentRunEvent]]] = {
            "run_started": self._handle_run_started,
            "run_content": self._handle_run_content,
            "run_completed": self._handle_run_completed,
            "run_error": self._handle_run_error,
            "run_paused": self._handle_run_paused,
            "reasoning_started": self._handle_reasoning_started,
            "reasoning_content_delta": self._handle_reasoning_content_delta,
            "reasoning_step": self._handle_reasoning_step,
            "reasoning_completed": self._handle_reasoning_completed,
            "tool_call_started": self._handle_tool_call_started,
            "tool_call_completed": self._handle_tool_call_completed,
            "tool_call_error": self._handle_tool_call_error,
        }
        dispatch = {
            enum_cls[name]: handler
            for enum_cls in (RunEvent, TeamRunEvent)
            for name, handler in handler_by_name.items()
        }
        self._dispatch = dispatch
        return dispatch

    def to_digitalkin_events(self, agno_event: AgnoRunEvent) -> list[BaseAgentRunEvent]:
        """Convert one Agno event into one or more DigitalKin events.

        Args:
            agno_event: Event from Agno's streaming API.

        Returns:
            List of DigitalKin events (may be empty).

        Raises:
            ImportError: If the optional 'agno' dependency is not installed.
        """
        dispatch = self._dispatch if self._dispatch is not None else self._build_dispatch()

        event_type = agno_event.event
        logger.debug("Converting Agno event: %s", event_type)

        handler = dispatch.get(event_type)
        if handler is None:
            logger.debug("Skipping unhandled Agno event type: %s", event_type)
            return []

        is_team = self._team_enum is not None and isinstance(event_type, self._team_enum)
        self._last_metadata = self._build_metadata(agno_event, is_team=is_team)

        return handler(agno_event, agno_event.__dict__.get("timestamp"))

    # ── Run Lifecycle Handlers ───────────────────────────────────────────

    def _handle_run_started(self, agno_event: AgnoRunStartedEvent, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.run_started.

        Nested runs (a team member's own run, or a team invoked from a
        workflow) carry a non-empty ``parent_run_id``. The AG-UI protocol
        only accepts a single ``RUN_STARTED`` per stream, so we drop
        nested ones — content/tool events from members still propagate
        and carry ``metadata.parent_run_id`` for client-side routing.

        Returns:
            List containing a RunStartedEvent, or empty for duplicates / nested runs.
        """
        parent_run_id = getattr(agno_event, "parent_run_id", None)
        run_id = agno_event.run_id

        if parent_run_id:
            logger.info(
                "[agno-adapter] DROP nested run_started run_id=%s parent_run_id=%s agent=%s/%s",
                run_id,
                parent_run_id,
                getattr(agno_event, "agent_id", None),
                getattr(agno_event, "agent_name", None),
            )
            return []

        if run_id and run_id == self._active_run_id:
            logger.info("[agno-adapter] DROP duplicate run_started run_id=%s", run_id)
            return []

        logger.info(
            "[agno-adapter] EMIT run_started run_id=%s session_id=%s active_was=%s metadata=%s",
            run_id,
            getattr(agno_event, "session_id", None),
            self._active_run_id,
            self._last_metadata,
        )
        self._active_run_id = run_id
        return [
            RunStartedEvent(
                event=AgentRunEvent.RUN_STARTED,
                run_id=run_id,
                thread_id=agno_event.session_id,
                timestamp=timestamp,
                metadata=self._last_metadata,
            )
        ]

    def _handle_run_completed(self, agno_event: AgnoRunCompletedEvent, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.run_completed.

        Mirrors ``_handle_run_started``: nested runs are silently dropped
        so the outer run's ``RUN_COMPLETED`` stays the single top-level
        closure on the stream.

        Returns:
            List of closing events followed by a RunCompletedEvent,
            or empty for nested / duplicate events.
        """
        parent_run_id = getattr(agno_event, "parent_run_id", None)
        run_id = agno_event.run_id

        if parent_run_id:
            # Close the subagent's text/reasoning bubble so the main agent's
            # continuation gets a fresh message_id. Inject a "\n---\n" footer
            # on the same message before the TextMessageCompletedEvent so the
            # frontend can visually separate subagent content from the rest.
            events: list[BaseAgentRunEvent] = []
            if self._content_active:
                events.append(
                    RunContentEvent(
                        event=AgentRunEvent.RUN_CONTENT,
                        content=" \n\n --- \n\n ",
                        message_id=self._current_message_id,
                        reasoning_content=None,
                        content_type=None,
                        timestamp=timestamp,
                        metadata=self._last_metadata,
                    )
                )
                events.extend(self._close_content(timestamp))
            if self._reasoning_active:
                events.extend(self._close_reasoning(timestamp))
            logger.info(
                "[agno-adapter] DROP nested run_completed run_id=%s parent_run_id=%s closed=%d",
                run_id,
                parent_run_id,
                len(events),
            )
            return events

        if run_id and run_id in self._completed_run_ids and run_id != self._active_run_id:
            logger.info("[agno-adapter] DROP duplicate run_completed run_id=%s", run_id)
            return []

        logger.info(
            "[agno-adapter] EMIT run_completed run_id=%s active_run_id=%s",
            run_id,
            self._active_run_id,
        )

        events = []

        if self._content_active:
            events.extend(self._close_content(timestamp))
        if self._reasoning_active:
            events.extend(self._close_reasoning(timestamp))

        if run_id:
            self._completed_run_ids.add(run_id)
        self._active_run_id = None

        content = agno_event.content
        events.append(
            RunCompletedEvent(
                event=AgentRunEvent.RUN_COMPLETED,
                run_id=run_id,
                final_content=str(content) if content else None,
                usage=None,
                message_id=None,
                timestamp=timestamp,
                metadata=self._last_metadata,
            )
        )
        return events

    def _handle_run_error(self, agno_event: AgnoRunErrorEvent, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.run_error.

        Returns:
            List containing a RunErrorEvent.
        """
        content = agno_event.content
        return [
            RunErrorEvent(
                event=AgentRunEvent.RUN_ERROR,
                error_type=agno_event.error_type,
                content=str(content) if content else None,
                error_details=None,
                timestamp=timestamp,
                metadata=self._last_metadata,
            )
        ]

    # ── Reasoning Handlers (native Agno reasoning models) ───────────────

    def _handle_reasoning_started(
        self, agno_event: AgnoReasoningStartedEvent, timestamp: Any
    ) -> list[BaseAgentRunEvent]:
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
                metadata=self._last_metadata,
            )
        )
        return events

    def _handle_reasoning_content_delta(
        self, agno_event: AgnoReasoningContentDeltaEvent, timestamp: Any
    ) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.reasoning_content_delta.

        Returns:
            List containing a ReasoningContentDeltaEvent.
        """
        return [
            ReasoningContentDeltaEvent(
                event=AgentRunEvent.REASONING_CONTENT_DELTA,
                delta=agno_event.reasoning_content or "",
                reasoning_id=self._current_reasoning_id,
                timestamp=timestamp,
                metadata=self._last_metadata,
            )
        ]

    def _handle_reasoning_step(self, agno_event: AgnoReasoningStepEvent, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle ``RunEvent.reasoning_step`` — emitted by Agno's ``ReasoningTools``.

        Unlike the native reasoning events (``reasoning_started`` /
        ``reasoning_content_delta`` / ``reasoning_completed``), a
        ``reasoning_step`` may arrive without a preceding
        ``reasoning_started``. This happens when the LLM calls tool-based
        reasoning (``think`` / ``analyze`` from ``ReasoningTools``) rather
        than using the model's built-in extended thinking.

        To comply with the AG-UI protocol — which requires every
        ``REASONING_MESSAGE_CONTENT`` to be wrapped in a
        ``REASONING_START`` … ``REASONING_END`` lifecycle — we auto-open
        a reasoning sequence here if none is active. The sequence is
        auto-closed by the next non-reasoning event (``_handle_run_content``,
        ``_handle_tool_call_started``, etc.) or by ``flush()``.

        Returns:
            Optionally a ``ReasoningStartedEvent`` followed by the
            ``ReasoningStepEvent``.
        """
        events: list[BaseAgentRunEvent] = []

        content = getattr(agno_event, "reasoning_content", "")
        if not content:
            return events

        if self._content_active:
            events.extend(self._close_content(timestamp))

        if not self._reasoning_active:
            self._current_reasoning_id = str(uuid.uuid4())
            self._reasoning_active = True
            logger.debug("Reasoning auto-started (from reasoning_step), id=%s", self._current_reasoning_id)
            events.append(
                ReasoningStartedEvent(
                    event=AgentRunEvent.REASONING_STARTED,
                    reasoning_id=self._current_reasoning_id,
                    timestamp=timestamp,
                    metadata=None,
                )
            )

        events.append(
            ReasoningStepEvent(
                event=AgentRunEvent.REASONING_STEP,
                delta=content,
                reasoning_id=self._current_reasoning_id,
                timestamp=timestamp,
                metadata=self._last_metadata,
            )
        )
        return events

    def _handle_reasoning_completed(
        self, agno_event: AgnoReasoningCompletedEvent, timestamp: Any
    ) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.reasoning_completed.

        Returns:
            List containing a ReasoningCompletedEvent if reasoning was active.
        """
        _ = agno_event
        logger.debug("Reasoning completed")
        return self._close_reasoning(timestamp)

    # ── Tool Call Handlers ──────────────────────────────────────────────

    def _handle_tool_call_started(
        self, agno_event: AgnoToolCallStartedEvent, timestamp: Any
    ) -> list[BaseAgentRunEvent]:
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

        tool = agno_event.tool
        tool_info = None
        if tool:
            tool_info = ToolInfo(
                tool_call_id=tool.tool_call_id,
                tool_name=tool.tool_name,
                tool_args=tool.tool_args,
                result=None,
            )
        events.append(
            ToolCallStartedEvent(
                event=AgentRunEvent.TOOL_CALL_STARTED,
                tool=tool_info,
                timestamp=timestamp,
                metadata=self._last_metadata,
            )
        )
        return events

    def _handle_tool_call_completed(
        self, agno_event: AgnoToolCallCompletedEvent, timestamp: Any
    ) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.tool_call_completed.

        Returns:
            List containing a ToolCallCompletedEvent.
        """
        tool = agno_event.tool
        tool_info = None
        tool_call_id = None
        if tool:
            tool_call_id = tool.tool_call_id
            tool_info = ToolInfo(
                tool_call_id=tool_call_id,
                tool_name=tool.tool_name,
                tool_args=tool.tool_args,
                result=tool.result,
            )

        if tool_call_id:
            self._closed_tool_call_ids.add(tool_call_id)

        content = agno_event.content
        return [
            ToolCallCompletedEvent(
                event=AgentRunEvent.TOOL_CALL_COMPLETED,
                tool=tool_info,
                content=str(content) if content else None,
                timestamp=timestamp,
                metadata=self._last_metadata,
            )
        ]

    def _handle_run_paused(self, agno_event: AgnoRunPausedEvent, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle ``RunEvent.run_paused`` — HITL pause on external tool execution.

        Agno suppresses tool_call_started/completed for tools with
        ``external_execution=True``; we re-emit them so the front sees
        the call.

        Returns:
            Synthesized tool-call events for the paused external tools.
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

        # Only synthesize for external tools; server-side ones already streamed.
        seen_ids: set[str] = set()
        for tool_exec in tools:
            if not getattr(tool_exec, "external_execution_required", False):
                continue
            tool_call_id = getattr(tool_exec, "tool_call_id", None)
            if not tool_call_id or tool_call_id in seen_ids:
                continue
            seen_ids.add(tool_call_id)
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
            self._closed_tool_call_ids.add(tool_call_id)

        return events

    def _handle_tool_call_error(self, agno_event: AgnoToolCallErrorEvent, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.tool_call_error.

        Returns:
            List containing a ToolCallErrorEvent, or empty if already closed.
        """
        tool = agno_event.tool
        tool_call_id = tool.tool_call_id if tool else None

        if tool_call_id and tool_call_id in self._closed_tool_call_ids:
            logger.debug("Skipping duplicate ToolCallError for tool %s", tool_call_id)
            return []

        tool_info = None
        if tool:
            tool_info = ToolInfo(
                tool_call_id=tool_call_id,
                tool_name=tool.tool_name,
                tool_args=None,
                result=None,
            )

        if tool_call_id:
            self._closed_tool_call_ids.add(tool_call_id)

        content = agno_event.content
        return [
            ToolCallErrorEvent(
                event=AgentRunEvent.TOOL_CALL_ERROR,
                tool=tool_info,
                error_message=str(content) if content else None,
                timestamp=timestamp,
                metadata=self._last_metadata,
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
                metadata=self._last_metadata,
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
                metadata=self._last_metadata,
            )
        ]
        self._content_active = False
        self._current_message_id = None
        return events

    def _handle_run_content(self, agno_event: AgnoRunContentEvent, timestamp: Any) -> list[BaseAgentRunEvent]:
        """Handle RunEvent.run_content — the core state machine.

        Non-empty content opens or extends its sequence; empty strings close it.

        Returns:
            DigitalKin events for this chunk.
        """
        events: list[BaseAgentRunEvent] = []

        reasoning_content = agno_event.reasoning_content
        content = agno_event.content

        if reasoning_content is not None:
            events.extend(self._process_reasoning_content(reasoning_content, timestamp))

        if content is not None:
            events.extend(self._process_text_content(content, timestamp))

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
            if self._reasoning_active:
                events.extend(self._close_reasoning(timestamp))
            return events

        if self._content_active:
            events.extend(self._close_content(timestamp))

        if not self._reasoning_active:
            self._current_reasoning_id = str(uuid.uuid4())
            logger.debug("Reasoning auto-started, id=%s", self._current_reasoning_id)
            events.append(
                ReasoningStartedEvent(
                    event=AgentRunEvent.REASONING_STARTED,
                    reasoning_id=self._current_reasoning_id,
                    timestamp=timestamp,
                    metadata=self._last_metadata,
                )
            )
            self._reasoning_active = True

        events.append(
            ReasoningContentDeltaEvent(
                event=AgentRunEvent.REASONING_CONTENT_DELTA,
                delta=reasoning_content,
                reasoning_id=self._current_reasoning_id,
                timestamp=timestamp,
                metadata=self._last_metadata,
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
            if self._content_active:
                events.extend(self._close_content(timestamp))
            return events

        if self._reasoning_active:
            logger.debug("Reasoning auto-completed (text content arrived)")
            events.extend(self._close_reasoning(timestamp))

        if not self._content_active:
            self._current_message_id = str(uuid.uuid4())
            events.append(
                TextMessageStartedEvent(
                    event=AgentRunEvent.TEXT_MESSAGE_STARTED,
                    message_id=self._current_message_id,
                    timestamp=timestamp,
                    metadata=self._last_metadata,
                )
            )
            self._content_active = True

            # Inject "--- <name> ---" header when the newly-opened
            # bubble belongs to a team member (nested agent event).
            meta = self._last_metadata or {}
            if meta.get("parent_run_id") and meta.get("source") == "agent":
                name = meta.get("name") or "member"
                events.append(
                    RunContentEvent(
                        event=AgentRunEvent.RUN_CONTENT,
                        content=f"\n --- \n ### {name} \n\n",
                        message_id=self._current_message_id,
                        reasoning_content=None,
                        content_type=None,
                        timestamp=timestamp,
                        metadata=self._last_metadata,
                    )
                )

        events.append(
            RunContentEvent(
                event=AgentRunEvent.RUN_CONTENT,
                content=str(content),
                message_id=self._current_message_id,
                reasoning_content=None,
                content_type=None,
                timestamp=timestamp,
                metadata=self._last_metadata,
            )
        )
        return events
