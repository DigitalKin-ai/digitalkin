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

    TEXT_MESSAGE_STARTED = "text_message_started"
    TEXT_MESSAGE_COMPLETED = "text_message_completed"

    TOOL_CALL_STARTED = "tool_call_started"
    TOOL_CALL_COMPLETED = "tool_call_completed"
    TOOL_CALL_ERROR = "tool_call_error"

    CUSTOM = "custom"


class BaseAgentRunEvent(BaseModel):
    """Base class for all agent run events."""

    event: AgentRunEvent = Field(..., description="Type of the event")
    timestamp: float | None = Field(default=None, description="Event timestamp (Unix time)")
    metadata: dict[str, Any] | None = Field(default=None, description="Additional event metadata")

    class Config:
        """Pydantic configuration."""

        use_enum_values = True


class RunStartedEvent(BaseAgentRunEvent):
    """Event emitted when an agent run starts."""

    event: AgentRunEvent = Field(AgentRunEvent.RUN_STARTED, description="Event type")
    run_id: str | None = Field(default=None, description="Unique identifier for this run")
    thread_id: str | None = Field(default=None, description="Thread/conversation identifier")


class TextMessageStartedEvent(BaseAgentRunEvent):
    """Event emitted when a new text message sequence begins."""

    event: AgentRunEvent = Field(AgentRunEvent.TEXT_MESSAGE_STARTED, description="Event type")
    message_id: str = Field(..., description="Unique ID for this text message")


class TextMessageCompletedEvent(BaseAgentRunEvent):
    """Event emitted when a text message sequence ends."""

    event: AgentRunEvent = Field(AgentRunEvent.TEXT_MESSAGE_COMPLETED, description="Event type")
    message_id: str = Field(..., description="ID of the text message being closed")


class RunContentEvent(BaseAgentRunEvent):
    """Event emitted when the agent produces content (text, reasoning, etc.)."""

    event: AgentRunEvent = Field(AgentRunEvent.RUN_CONTENT, description="Event type")
    content: str | None = Field(default=None, description="Text content produced by the agent")
    reasoning_content: str | None = Field(default=None, description="Reasoning content (if extended thinking on)")
    content_type: str | None = Field(default=None, description="Type of content (text, json, etc.)")
    message_id: str | None = Field(default=None, description="ID of the parent text message")


class RunCompletedEvent(BaseAgentRunEvent):
    """Event emitted when an agent run completes successfully."""

    event: AgentRunEvent = Field(AgentRunEvent.RUN_COMPLETED, description="Event type")
    run_id: str | None = Field(default=None, description="Unique identifier for this run")
    final_content: str | None = Field(default=None, description="Final accumulated content")
    usage: dict[str, Any] | None = Field(default=None, description="Token usage statistics")
    message_id: str | None = Field(default=None, description="ID of the text message to close, if any")


class RunErrorEvent(BaseAgentRunEvent):
    """Event emitted when an agent run encounters an error."""

    event: AgentRunEvent = Field(AgentRunEvent.RUN_ERROR, description="Event type")
    error_type: str | None = Field(default=None, description="Type/category of error")
    content: str | None = Field(default=None, description="Error message")
    error_details: dict[str, Any] | None = Field(default=None, description="Additional error details")


class ReasoningStartedEvent(BaseAgentRunEvent):
    """Event emitted when a reasoning phase starts."""

    event: AgentRunEvent = Field(AgentRunEvent.REASONING_STARTED, description="Event type")
    reasoning_id: str | None = Field(default=None, description="Unique ID for this reasoning phase")


class ReasoningContentDeltaEvent(BaseAgentRunEvent):
    """Event emitted during extended thinking/reasoning phases."""

    event: AgentRunEvent = Field(AgentRunEvent.REASONING_CONTENT_DELTA, description="Event type")
    delta: str = Field(..., description="Delta of reasoning content")
    reasoning_id: str | None = Field(default=None, description="ID of the parent reasoning phase")


class ReasoningStepEvent(BaseAgentRunEvent):
    """Event emitted for intermediate reasoning steps."""

    event: AgentRunEvent = Field(AgentRunEvent.REASONING_STEP, description="Event type")
    delta: str = Field(..., description="Reasoning step content")
    reasoning_id: str | None = Field(default=None, description="ID of the parent reasoning phase")


class ReasoningCompletedEvent(BaseAgentRunEvent):
    """Event emitted when a reasoning phase completes."""

    event: AgentRunEvent = Field(AgentRunEvent.REASONING_COMPLETED, description="Event type")
    reasoning_id: str | None = Field(default=None, description="ID of the reasoning phase being closed")


class ToolInfo(BaseModel):
    """Information about a tool call."""

    tool_call_id: str | None = Field(default=None, description="Unique identifier for this tool call")
    tool_name: str | None = Field(default=None, description="Name of the tool being called")
    tool_args: dict[str, Any] | str | None = Field(default=None, description="Arguments passed to the tool")
    result: str | None = Field(default=None, description="Result returned by the tool")


class ToolCallStartedEvent(BaseAgentRunEvent):
    """Event emitted when a tool call starts."""

    event: AgentRunEvent = Field(AgentRunEvent.TOOL_CALL_STARTED, description="Event type")
    tool: ToolInfo | None = Field(default=None, description="Tool information")


class ToolCallCompletedEvent(BaseAgentRunEvent):
    """Event emitted when a tool call completes successfully."""

    event: AgentRunEvent = Field(AgentRunEvent.TOOL_CALL_COMPLETED, description="Event type")
    tool: ToolInfo | None = Field(default=None, description="Tool information including result")
    content: str | None = Field(default=None, description="Tool execution result content")


class ToolCallErrorEvent(BaseAgentRunEvent):
    """Event emitted when a tool call encounters an error."""

    event: AgentRunEvent = Field(AgentRunEvent.TOOL_CALL_ERROR, description="Event type")
    tool: ToolInfo | None = Field(None, description="Tool information")
    error_message: str | None = Field(None, description="Error message")


class CustomEvent(BaseAgentRunEvent):
    """Event emitted for application-defined custom events.

    Carries an application-specific ``name`` that discriminates the custom
    event subtype and a free-form ``value`` payload for metadata transfer.
    """

    event: AgentRunEvent = Field(AgentRunEvent.CUSTOM, description="Event type")
    name: str = Field(..., description="Application-defined event name (discriminator)")
    value: Any = Field(..., description="Application-defined payload")
