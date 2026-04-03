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


def to_digitalkin_event(agno_event: Any) -> BaseAgentRunEvent | None:  # noqa: PLR0911, C901, PLR0912
    """Convert an Agno event to a DigitalKin framework-agnostic event.

    Args:
        agno_event: Event from Agno's streaming API.

    Returns:
        Corresponding DigitalKin BaseAgentRunEvent.

    Raises:
        ImportError: If the optional 'agno' dependency is not installed.
        ValueError: If the event type is not recognized.
    """
    try:
        from agno.run.agent import RunEvent  # pylint: disable=C0415 # pyright: ignore[reportMissingImports]
    except ImportError as exc:
        message = "The 'agno' package is required to use agno_to_digitalkin_event. \
Install it with: pip install agno"
        raise ImportError(message) from exc

    event_type = agno_event.event

    logger.info("[DK STREAM-DEBUG => agno_adapter] Converting Agno event type: %s", event_type)

    # Import timestamp if available
    timestamp = getattr(agno_event, "timestamp", None)

    if event_type == RunEvent.run_started:
        return RunStartedEvent(
            event=AgentRunEvent.RUN_STARTED,
            run_id=getattr(agno_event, "run_id", None),
            thread_id=getattr(agno_event, "thread_id", None),
            timestamp=timestamp,
            metadata=None,
        )

    if event_type == RunEvent.run_content:
        return RunContentEvent(
            event=AgentRunEvent.RUN_CONTENT,
            content=str(agno_event.content) if agno_event.content else None,
            reasoning_content=getattr(agno_event, "reasoning_content", None),
            content_type=None,
            timestamp=timestamp,
            metadata=None,
        )

    if event_type == RunEvent.run_completed:
        return RunCompletedEvent(
            event=AgentRunEvent.RUN_COMPLETED,
            run_id=getattr(agno_event, "run_id", None),
            final_content=str(agno_event.content) if getattr(agno_event, "content", None) else None,
            usage=None,
            timestamp=timestamp,
            metadata=None,
        )

    if event_type == RunEvent.run_error:
        return RunErrorEvent(
            event=AgentRunEvent.RUN_ERROR,
            error_type=getattr(agno_event, "error_type", None),
            content=str(agno_event.content) if getattr(agno_event, "content", None) else None,
            error_details=None,
            timestamp=timestamp,
            metadata=None,
        )

    if event_type == RunEvent.reasoning_started:
        return ReasoningStartedEvent(
            event=AgentRunEvent.REASONING_STARTED,
            timestamp=timestamp,
            metadata=None,
        )

    if event_type == RunEvent.reasoning_content_delta:
        reasoning_content = getattr(agno_event, "reasoning_content", "")
        return ReasoningContentDeltaEvent(
            event=AgentRunEvent.REASONING_CONTENT_DELTA,
            delta=reasoning_content,
            timestamp=timestamp,
            metadata=None,
        )

    if event_type == RunEvent.reasoning_step:
        reasoning_content = getattr(agno_event, "reasoning_content", "")
        return ReasoningStepEvent(
            event=AgentRunEvent.REASONING_STEP,
            delta=reasoning_content,
            timestamp=timestamp,
            metadata=None,
        )

    if event_type == RunEvent.reasoning_completed:
        return ReasoningCompletedEvent(
            event=AgentRunEvent.REASONING_COMPLETED,
            timestamp=timestamp,
            metadata=None,
        )

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
        return ToolCallStartedEvent(
            event=AgentRunEvent.TOOL_CALL_STARTED,
            tool=tool_info,
            timestamp=timestamp,
            metadata=None,
        )

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
        return ToolCallCompletedEvent(
            event=AgentRunEvent.TOOL_CALL_COMPLETED,
            tool=tool_info,
            content=str(agno_event.content) if getattr(agno_event, "content", None) else None,
            timestamp=timestamp,
            metadata=None,
        )

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
        return ToolCallErrorEvent(
            event=AgentRunEvent.TOOL_CALL_ERROR,
            tool=tool_info,
            error_message=str(agno_event.content) if getattr(agno_event, "content", None) else None,
            timestamp=timestamp,
            metadata=None,
        )

    # Unknown event - skip (internal framework events with no streamable content)
    logger.debug("Skipping unhandled Agno event type: %s", event_type)
    return None
