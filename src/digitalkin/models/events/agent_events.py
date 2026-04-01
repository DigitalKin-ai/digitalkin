"""Framework-agnostic agent run event models.

These models define a common interface for agent execution events that can be
used across different AI frameworks (Agno, LangChain, custom agents, etc.).
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class AgentRunEvent(str, Enum):
    """Agent run event types."""

    RUN_STARTED = "run_started"
    RUN_CONTENT = "run_content"
    RUN_COMPLETED = "run_completed"
    RUN_ERROR = "run_error"

    REASONING_STARTED = "reasoning_started"
    REASONING_CONTENT_DELTA = "reasoning_content_delta"
    REASONING_STEP = "reasoning_step"
    REASONING_COMPLETED = "reasoning_completed"

    TOOL_CALL_STARTED = "tool_call_started"
    TOOL_CALL_COMPLETED = "tool_call_completed"
    TOOL_CALL_ERROR = "tool_call_error"


class BaseAgentRunEvent(BaseModel):
    """Base class for all agent run events."""

    event: AgentRunEvent = Field(..., description="Type of the event")
    timestamp: float | None = Field(None, description="Event timestamp (Unix time)")
    metadata: dict[str, Any] | None = Field(None, description="Additional event metadata")

    class Config:
        """Pydantic configuration."""

        use_enum_values = True


class RunStartedEvent(BaseAgentRunEvent):
    """Event emitted when an agent run starts."""

    event: AgentRunEvent = Field(AgentRunEvent.RUN_STARTED, description="Event type")
    run_id: str | None = Field(None, description="Unique identifier for this run")
    thread_id: str | None = Field(None, description="Thread/conversation identifier")


class RunContentEvent(BaseAgentRunEvent):
    """Event emitted when the agent produces content (text, reasoning, etc.)."""

    event: AgentRunEvent = Field(AgentRunEvent.RUN_CONTENT, description="Event type")
    content: str | None = Field(None, description="Text content produced by the agent")
    reasoning_content: str | None = Field(None, description="Reasoning content (if extended thinking is enabled)")
    content_type: str | None = Field(None, description="Type of content (text, json, etc.)")


class RunCompletedEvent(BaseAgentRunEvent):
    """Event emitted when an agent run completes successfully."""

    event: AgentRunEvent = Field(AgentRunEvent.RUN_COMPLETED, description="Event type")
    run_id: str | None = Field(None, description="Unique identifier for this run")
    final_content: str | None = Field(None, description="Final accumulated content")
    usage: dict[str, Any] | None = Field(None, description="Token usage statistics")


class RunErrorEvent(BaseAgentRunEvent):
    """Event emitted when an agent run encounters an error."""

    event: AgentRunEvent = Field(AgentRunEvent.RUN_ERROR, description="Event type")
    error_type: str | None = Field(None, description="Type/category of error")
    content: str | None = Field(None, description="Error message")
    error_details: dict[str, Any] | None = Field(None, description="Additional error details")


class ReasoningStartedEvent(BaseAgentRunEvent):
    """Event emitted when a reasoning phase starts."""

    event: AgentRunEvent = Field(AgentRunEvent.REASONING_STARTED, description="Event type")


class ReasoningContentDeltaEvent(BaseAgentRunEvent):
    """Event emitted during extended thinking/reasoning phases."""

    event: AgentRunEvent = Field(AgentRunEvent.REASONING_CONTENT_DELTA, description="Event type")
    delta: str = Field(..., description="Delta of reasoning content")


class ReasoningStepEvent(BaseAgentRunEvent):
    """Event emitted for intermediate reasoning steps."""

    event: AgentRunEvent = Field(AgentRunEvent.REASONING_STEP, description="Event type")
    delta: str = Field(..., description="Reasoning step content")


class ReasoningCompletedEvent(BaseAgentRunEvent):
    """Event emitted when a reasoning phase completes."""

    event: AgentRunEvent = Field(AgentRunEvent.REASONING_COMPLETED, description="Event type")


class ToolInfo(BaseModel):
    """Information about a tool call."""

    tool_call_id: str | None = Field(None, description="Unique identifier for this tool call")
    tool_name: str | None = Field(None, description="Name of the tool being called")
    tool_args: dict[str, Any] | str | None = Field(None, description="Arguments passed to the tool")
    result: str | None = Field(None, description="Result returned by the tool")


class ToolCallStartedEvent(BaseAgentRunEvent):
    """Event emitted when a tool call starts."""

    event: AgentRunEvent = Field(AgentRunEvent.TOOL_CALL_STARTED, description="Event type")
    tool: ToolInfo | None = Field(None, description="Tool information")


class ToolCallCompletedEvent(BaseAgentRunEvent):
    """Event emitted when a tool call completes successfully."""

    event: AgentRunEvent = Field(AgentRunEvent.TOOL_CALL_COMPLETED, description="Event type")
    tool: ToolInfo | None = Field(None, description="Tool information including result")
    content: str | None = Field(None, description="Tool execution result content")


class ToolCallErrorEvent(BaseAgentRunEvent):
    """Event emitted when a tool call encounters an error."""

    event: AgentRunEvent = Field(AgentRunEvent.TOOL_CALL_ERROR, description="Event type")
    tool: ToolInfo | None = Field(None, description="Tool information")
    error_message: str | None = Field(None, description="Error message")
