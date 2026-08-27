# pylint: disable=C0301
"""Output model for the Template module."""

from typing import Annotated, Literal, TypeAlias

from ag_ui.core.events import (
    ActivityDeltaEvent,
    ActivitySnapshotEvent,
    CustomEvent,
    MessagesSnapshotEvent,
    RawEvent,
    ReasoningEncryptedValueEvent,
    ReasoningEndEvent,
    ReasoningMessageChunkEvent,
    ReasoningMessageContentEvent,
    ReasoningMessageEndEvent,
    ReasoningMessageStartEvent,
    ReasoningStartEvent,
    RunErrorEvent,
    RunFinishedEvent,
    RunStartedEvent,
    StateDeltaEvent,
    StateSnapshotEvent,
    SubagentErrorEvent,
    SubagentFinishedEvent,
    SubagentStartedEvent,
    TextMessageChunkEvent,
    TextMessageContentEvent,
    TextMessageEndEvent,
    TextMessageStartEvent,
    ThinkingEndEvent,
    ThinkingStartEvent,
    ThinkingTextMessageContentEvent,
    ThinkingTextMessageEndEvent,
    ThinkingTextMessageStartEvent,
    ToolCallArgsEvent,
    ToolCallChunkEvent,
    ToolCallEndEvent,
    ToolCallResultEvent,
    ToolCallStartEvent,
)
from pydantic import ConfigDict, Field
from pydantic.alias_generators import to_camel

from digitalkin.models.module.module_types import DataModel, DataTrigger


class AgUiDataTrigger(DataTrigger):
    """DataTrigger subclass that serializes wrapper fields as camelCase.

    AG-UI events must be serialized with camelCase field names. Since the SDK
    calls ``model_dump(mode="json")`` without ``by_alias=True``, we define
    camelCase aliases here and activate them via ``TemplateOutput.model_dump``.
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class AgUiTextMessageStartOutput(AgUiDataTrigger):
    """AG-UI TextMessageStart event - signals start of a text message."""

    protocol: Literal["agui_text_message_start"] = "agui_text_message_start"
    event: TextMessageStartEvent = Field(..., description="AG-UI TextMessageStart event payload")


class AgUiTextMessageContentOutput(AgUiDataTrigger):
    """AG-UI TextMessageContent event - carries a text delta chunk."""

    protocol: Literal["agui_text_message_content"] = "agui_text_message_content"
    event: TextMessageContentEvent = Field(..., description="AG-UI TextMessageContent event payload")


class AgUiTextMessageEndOutput(AgUiDataTrigger):
    """AG-UI TextMessageEnd event - signals end of a text message."""

    protocol: Literal["agui_text_message_end"] = "agui_text_message_end"
    event: TextMessageEndEvent = Field(..., description="AG-UI TextMessageEnd event payload")


class AgUiTextMessageChunkOutput(AgUiDataTrigger):
    """AG-UI TextMessageChunk event - aggregated text message chunk."""

    protocol: Literal["agui_text_message_chunk"] = "agui_text_message_chunk"
    event: TextMessageChunkEvent = Field(..., description="AG-UI TextMessageChunk event payload")


class AgUiThinkingTextMessageStartOutput(AgUiDataTrigger):
    """AG-UI ThinkingTextMessageStart event - signals start of internal thinking."""

    protocol: Literal["agui_thinking_text_message_start"] = "agui_thinking_text_message_start"
    event: ThinkingTextMessageStartEvent = Field(..., description="AG-UI ThinkingTextMessageStart event payload")


class AgUiThinkingTextMessageContentOutput(AgUiDataTrigger):
    """AG-UI ThinkingTextMessageContent event - carries a thinking text delta chunk."""

    protocol: Literal["agui_thinking_text_message_content"] = "agui_thinking_text_message_content"
    event: ThinkingTextMessageContentEvent = Field(..., description="AG-UI ThinkingTextMessageContent event payload")


class AgUiThinkingTextMessageEndOutput(AgUiDataTrigger):
    """AG-UI ThinkingTextMessageEnd event - signals end of internal thinking."""

    protocol: Literal["agui_thinking_text_message_end"] = "agui_thinking_text_message_end"
    event: ThinkingTextMessageEndEvent = Field(..., description="AG-UI ThinkingTextMessageEnd event payload")


class AgUiToolCallStartOutput(AgUiDataTrigger):
    """AG-UI ToolCallStart event - signals start of a tool invocation."""

    protocol: Literal["agui_tool_call_start"] = "agui_tool_call_start"
    event: ToolCallStartEvent = Field(..., description="AG-UI ToolCallStart event payload")


class AgUiToolCallArgsOutput(AgUiDataTrigger):
    """AG-UI ToolCallArgs event - carries streamed tool call arguments delta."""

    protocol: Literal["agui_tool_call_args"] = "agui_tool_call_args"
    event: ToolCallArgsEvent = Field(..., description="AG-UI ToolCallArgs event payload")


class AgUiToolCallEndOutput(AgUiDataTrigger):
    """AG-UI ToolCallEnd event - signals end of tool call argument streaming."""

    protocol: Literal["agui_tool_call_end"] = "agui_tool_call_end"
    event: ToolCallEndEvent = Field(..., description="AG-UI ToolCallEnd event payload")


class AgUiToolCallChunkOutput(AgUiDataTrigger):
    """AG-UI ToolCallChunk event - aggregated tool call chunk."""

    protocol: Literal["agui_tool_call_chunk"] = "agui_tool_call_chunk"
    event: ToolCallChunkEvent = Field(..., description="AG-UI ToolCallChunk event payload")


class AgUiToolCallResultOutput(AgUiDataTrigger):
    """AG-UI ToolCallResult event - carries the result of a completed tool call."""

    protocol: Literal["agui_tool_call_result"] = "agui_tool_call_result"
    event: ToolCallResultEvent = Field(..., description="AG-UI ToolCallResult event payload")


class AgUiStateSnapshotOutput(AgUiDataTrigger):
    """AG-UI StateSnapshot event - full agent state snapshot."""

    protocol: Literal["agui_state_snapshot"] = "agui_state_snapshot"
    event: StateSnapshotEvent = Field(..., description="AG-UI StateSnapshot event payload")


class AgUiStateDeltaOutput(AgUiDataTrigger):
    """AG-UI StateDelta event - JSON Patch (RFC 6902) operations on agent state."""

    protocol: Literal["agui_state_delta"] = "agui_state_delta"
    event: StateDeltaEvent = Field(..., description="AG-UI StateDelta event payload")


class AgUiMessagesSnapshotOutput(AgUiDataTrigger):
    """AG-UI MessagesSnapshot event - full conversation messages snapshot."""

    protocol: Literal["agui_messages_snapshot"] = "agui_messages_snapshot"
    event: MessagesSnapshotEvent = Field(..., description="AG-UI MessagesSnapshot event payload")


class AgUiActivitySnapshotOutput(AgUiDataTrigger):
    """AG-UI ActivitySnapshot event - full activity message snapshot."""

    protocol: Literal["agui_activity_snapshot"] = "agui_activity_snapshot"
    event: ActivitySnapshotEvent = Field(..., description="AG-UI ActivitySnapshot event payload")


class AgUiActivityDeltaOutput(AgUiDataTrigger):
    """AG-UI ActivityDelta event - JSON Patch delta for an activity message."""

    protocol: Literal["agui_activity_delta"] = "agui_activity_delta"
    event: ActivityDeltaEvent = Field(..., description="AG-UI ActivityDelta event payload")


class AgUiRunStartedOutput(AgUiDataTrigger):
    """AG-UI RunStarted event - signals that an agent run has begun."""

    protocol: Literal["agui_run_started"] = "agui_run_started"
    event: RunStartedEvent = Field(..., description="AG-UI RunStarted event payload")


class AgUiRunFinishedOutput(AgUiDataTrigger):
    """AG-UI RunFinished event - signals that an agent run has completed."""

    protocol: Literal["agui_run_finished"] = "agui_run_finished"
    event: RunFinishedEvent = Field(..., description="AG-UI RunFinished event payload")


class AgUiRunErrorOutput(AgUiDataTrigger):
    """AG-UI RunError event - signals that a run encountered an error."""

    protocol: Literal["agui_run_error"] = "agui_run_error"
    event: RunErrorEvent = Field(..., description="AG-UI RunError event payload")


class AgUiSubagentStartedOutput(AgUiDataTrigger):
    """AG-UI SubagentStarted event - signals the agent delegated to a child agent."""

    protocol: Literal["agui_subagent_started"] = "agui_subagent_started"
    event: SubagentStartedEvent = Field(..., description="AG-UI SubagentStarted event payload")


class AgUiSubagentFinishedOutput(AgUiDataTrigger):
    """AG-UI SubagentFinished event - signals a delegated run completed."""

    protocol: Literal["agui_subagent_finished"] = "agui_subagent_finished"
    event: SubagentFinishedEvent = Field(..., description="AG-UI SubagentFinished event payload")


class AgUiSubagentErrorOutput(AgUiDataTrigger):
    """AG-UI SubagentError event - signals a delegated run failed, without ending the run."""

    protocol: Literal["agui_subagent_error"] = "agui_subagent_error"
    event: SubagentErrorEvent = Field(..., description="AG-UI SubagentError event payload")


class AgUiReasoningStartOutput(AgUiDataTrigger):
    """AG-UI ReasoningStart event - signals start of a reasoning phase."""

    protocol: Literal["agui_reasoning_start"] = "agui_reasoning_start"
    event: ReasoningStartEvent = Field(..., description="AG-UI ReasoningStart event payload")


class AgUiReasoningMessageStartOutput(AgUiDataTrigger):
    """AG-UI ReasoningMessageStart event - signals start of a reasoning message."""

    protocol: Literal["agui_reasoning_message_start"] = "agui_reasoning_message_start"
    event: ReasoningMessageStartEvent = Field(..., description="AG-UI ReasoningMessageStart event payload")


class AgUiReasoningMessageContentOutput(AgUiDataTrigger):
    """AG-UI ReasoningMessageContent event - carries a reasoning content delta."""

    protocol: Literal["agui_reasoning_message_content"] = "agui_reasoning_message_content"
    event: ReasoningMessageContentEvent = Field(..., description="AG-UI ReasoningMessageContent event payload")


class AgUiReasoningMessageEndOutput(AgUiDataTrigger):
    """AG-UI ReasoningMessageEnd event - signals end of a reasoning message."""

    protocol: Literal["agui_reasoning_message_end"] = "agui_reasoning_message_end"
    event: ReasoningMessageEndEvent = Field(..., description="AG-UI ReasoningMessageEnd event payload")


class AgUiReasoningMessageChunkOutput(AgUiDataTrigger):
    """AG-UI ReasoningMessageChunk event - aggregated reasoning message chunk."""

    protocol: Literal["agui_reasoning_message_chunk"] = "agui_reasoning_message_chunk"
    event: ReasoningMessageChunkEvent = Field(..., description="AG-UI ReasoningMessageChunk event payload")


class AgUiReasoningEndOutput(AgUiDataTrigger):
    """AG-UI ReasoningEnd event - signals end of a reasoning phase."""

    protocol: Literal["agui_reasoning_end"] = "agui_reasoning_end"
    event: ReasoningEndEvent = Field(..., description="AG-UI ReasoningEnd event payload")


class AgUiReasoningEncryptedValueOutput(AgUiDataTrigger):
    """AG-UI ReasoningEncryptedValue event - carries an encrypted reasoning value."""

    protocol: Literal["agui_reasoning_encrypted_value"] = "agui_reasoning_encrypted_value"
    event: ReasoningEncryptedValueEvent = Field(..., description="AG-UI ReasoningEncryptedValue event payload")


class AgUiThinkingStartOutput(AgUiDataTrigger):
    """AG-UI ThinkingStart event - signals start of a high-level thinking step."""

    protocol: Literal["agui_thinking_start"] = "agui_thinking_start"
    event: ThinkingStartEvent = Field(..., description="AG-UI ThinkingStart event payload")


class AgUiThinkingEndOutput(AgUiDataTrigger):
    """AG-UI ThinkingEnd event - signals end of a high-level thinking step."""

    protocol: Literal["agui_thinking_end"] = "agui_thinking_end"
    event: ThinkingEndEvent = Field(..., description="AG-UI ThinkingEnd event payload")


class AgUiRawEventOutput(AgUiDataTrigger):
    """AG-UI RawEvent event - passes through a raw/untyped event payload."""

    protocol: Literal["agui_raw"] = "agui_raw"
    event: RawEvent = Field(..., description="AG-UI RawEvent event payload")


class AgUiCustomEventOutput(AgUiDataTrigger):
    """AG-UI CustomEvent event - carries an application-defined custom event."""

    protocol: Literal["agui_custom"] = "agui_custom"
    event: CustomEvent = Field(..., description="AG-UI CustomEvent event payload")


AgUiEventOutput: TypeAlias = Annotated[
    (
        AgUiTextMessageStartOutput
        | AgUiTextMessageContentOutput
        | AgUiTextMessageEndOutput
        | AgUiTextMessageChunkOutput
        | AgUiThinkingTextMessageStartOutput
        | AgUiThinkingTextMessageContentOutput
        | AgUiThinkingTextMessageEndOutput
        | AgUiToolCallStartOutput
        | AgUiToolCallArgsOutput
        | AgUiToolCallEndOutput
        | AgUiToolCallChunkOutput
        | AgUiToolCallResultOutput
        | AgUiStateSnapshotOutput
        | AgUiStateDeltaOutput
        | AgUiMessagesSnapshotOutput
        | AgUiActivitySnapshotOutput
        | AgUiActivityDeltaOutput
        | AgUiRunStartedOutput
        | AgUiRunFinishedOutput
        | AgUiRunErrorOutput
        | AgUiSubagentStartedOutput
        | AgUiSubagentFinishedOutput
        | AgUiSubagentErrorOutput
        | AgUiReasoningStartOutput
        | AgUiReasoningMessageStartOutput
        | AgUiReasoningMessageContentOutput
        | AgUiReasoningMessageEndOutput
        | AgUiReasoningMessageChunkOutput
        | AgUiReasoningEndOutput
        | AgUiReasoningEncryptedValueOutput
        | AgUiThinkingStartOutput
        | AgUiThinkingEndOutput
        | AgUiRawEventOutput
        | AgUiCustomEventOutput
    ),
    Field(discriminator="protocol"),
]


class AgUiOutput(DataModel):
    """Output model for the Template module with discriminated union."""

    def model_dump(self, **kwargs: object) -> dict[str, object]:
        """Serialize with camelCase aliases and exclude None fields by default.

        Returns:
            Serialized model dictionary with camelCase keys and no null values.
        """
        kwargs.setdefault("by_alias", True)
        kwargs.setdefault("exclude_none", True)
        return super().model_dump(**kwargs)  # type: ignore[arg-type]

    root: AgUiEventOutput
