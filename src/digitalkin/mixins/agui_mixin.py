"""AG-UI event streaming mixin for DigitalKin modules.

This mixin provides utilities to convert framework-agnostic agent events
into AG-UI protocol events and send them through the module context callbacks.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

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
)
from digitalkin.models.module.ag_ui import (
    AgUiReasoningEndOutput,
    AgUiReasoningMessageContentOutput,
    AgUiReasoningMessageEndOutput,
    AgUiReasoningMessageStartOutput,
    AgUiReasoningStartOutput,
    AgUiRunErrorOutput,
    AgUiRunFinishedOutput,
    AgUiRunStartedOutput,
    AgUiTextMessageContentOutput,
    AgUiTextMessageEndOutput,
    AgUiTextMessageStartOutput,
    AgUiToolCallArgsOutput,
    AgUiToolCallEndOutput,
    AgUiToolCallResultOutput,
    AgUiToolCallStartOutput,
)

if TYPE_CHECKING:
    from digitalkin.models.module.module_context import ModuleContext


class AgUiMixin:
    """Mixin for converting agent events to AG-UI protocol and sending them.

    This mixin provides a `send_message` method that takes a generic BaseAgentRunEvent,
    converts it to the appropriate AG-UI protocol event, and sends it via the module
    context callbacks.

    The conversion logic is iterative and can be extended as needed for different
    event types.

    Usage::

        class MyTrigger(BaseTrigger, AgUiMixin):
            async def execute(self, context, input_data):
                # Receive events from agent
                async for event in agent.run(input_data.message, stream=True):
                    await self.send_message(context, event)
    """

    def __init__(self) -> None:
        """Initialize AG-UI state tracking."""
        # AG-UI protocol identifiers
        self._thread_id: str = str(uuid.uuid4())
        self._run_id: str = ""
        self._message_id: str = str(uuid.uuid4())
        self._reasoning_id: str = str(uuid.uuid4())

        # Open-sequence tracking
        self._text_started: bool = False
        self._reasoning_started: bool = False

    async def send_message(  # noqa: C901
        self,
        context: ModuleContext,
        event: BaseAgentRunEvent,
    ) -> None:
        """Convert agent event to AG-UI protocol and send via context callbacks.

        This method handles the conversion from framework-agnostic agent events
        to AG-UI protocol events. The conversion logic is implemented iteratively
        for each event type.

        Args:
            context: Module context containing the callbacks strategy.
            event: Agent run event to process and convert.

        Note:
            This method is designed to be extended over time. Start with the most
            critical events (run lifecycle, text content, tools) and add support
            for additional event types as needed.
        """
        event_type = event.event

        context.callbacks.logger.error(
            "Processing event: %s, content: %s",
            event_type,
            event.model_dump_json(indent=2),
            extra=context.session.current_ids(),
        )

        # ── Run Lifecycle Events ──
        if event_type == AgentRunEvent.RUN_STARTED:
            await self._handle_run_started(context, event)  # type: ignore[arg-type]

        elif event_type == AgentRunEvent.RUN_CONTENT:
            await self._handle_run_content(context, event)  # type: ignore[arg-type]

        elif event_type == AgentRunEvent.RUN_COMPLETED:
            await self._handle_run_completed(context, event)  # type: ignore[arg-type]

        elif event_type == AgentRunEvent.RUN_ERROR:
            await self._handle_run_error(context, event)  # type: ignore[arg-type]

        # ── Tool Call Events ──
        elif event_type == AgentRunEvent.TOOL_CALL_STARTED:
            await self._handle_tool_call_started(context, event)  # type: ignore[arg-type]

        elif event_type == AgentRunEvent.TOOL_CALL_COMPLETED:
            await self._handle_tool_call_completed(context, event)  # type: ignore[arg-type]

        elif event_type == AgentRunEvent.TOOL_CALL_ERROR:
            await self._handle_tool_call_error(context, event)  # type: ignore[arg-type]

        # ── Reasoning Events ──
        elif event_type == AgentRunEvent.REASONING_STARTED:
            await self._handle_reasoning_started(context, event)  # type: ignore[arg-type]

        elif event_type == AgentRunEvent.REASONING_CONTENT_DELTA:
            await self._handle_reasoning_delta(context, event)  # type: ignore[arg-type]

        elif event_type == AgentRunEvent.REASONING_STEP:
            await self._handle_reasoning_step(context, event)  # type: ignore[arg-type]

        elif event_type == AgentRunEvent.REASONING_COMPLETED:
            await self._handle_reasoning_completed(context, event)  # type: ignore[arg-type]

    # ── Private Event Handlers ───────────────────────────────────────────────

    async def _handle_run_started(
        self,
        context: ModuleContext,
        event: RunStartedEvent,
    ) -> None:
        """Handle run started event - emit AG-UI RunStarted."""
        from ag_ui.core.events import RunStartedEvent as AgUiRunStartedEvent  # pylint: disable=C0415

        self._run_id = event.run_id or str(uuid.uuid4())
        if event.thread_id:
            self._thread_id = event.thread_id

        output = AgUiRunStartedOutput(
            event=AgUiRunStartedEvent(
                thread_id=self._thread_id,
                run_id=self._run_id,
            )
        )
        await context.callbacks.send_message(output)

    async def _handle_run_content(
        self,
        context: ModuleContext,
        event: RunContentEvent,
    ) -> None:
        """Handle run content event - emit AG-UI TextMessageContent."""
        from ag_ui.core.events import (  # pylint: disable=C0415
            TextMessageContentEvent as AgUiTextMessageContentEvent,
        )
        from ag_ui.core.events import (  # pylint: disable=C0415
            TextMessageStartEvent as AgUiTextMessageStartEvent,
        )

        content = event.content
        if not content:
            return

        # Auto-open text message sequence on first content chunk
        if not self._text_started:
            start_output = AgUiTextMessageStartOutput(
                event=AgUiTextMessageStartEvent(
                    message_id=self._message_id,
                    role="assistant",
                )
            )
            await context.callbacks.send_message(start_output)
            self._text_started = True

        # Emit content delta
        content_output = AgUiTextMessageContentOutput(
            event=AgUiTextMessageContentEvent(
                message_id=self._message_id,
                delta=content,
            )
        )
        await context.callbacks.send_message(content_output)

    async def _handle_run_completed(
        self,
        context: ModuleContext,
        event: RunCompletedEvent,
    ) -> None:
        """Handle run completed event - close text message and emit AG-UI RunFinished."""
        from ag_ui.core.events import RunFinishedEvent as AgUiRunFinishedEvent  # pylint: disable=C0415
        from ag_ui.core.events import TextMessageEndEvent as AgUiTextMessageEndEvent  # pylint: disable=C0415

        # Close any open text message
        if self._text_started:
            end_output = AgUiTextMessageEndOutput(
                event=AgUiTextMessageEndEvent(message_id=self._message_id),
            )
            await context.callbacks.send_message(end_output)
            self._text_started = False

        # Close any open reasoning sequence
        await self._close_reasoning(context)

        # Emit run finished
        run_id = event.run_id or self._run_id
        finished_output = AgUiRunFinishedOutput(
            event=AgUiRunFinishedEvent(
                thread_id=self._thread_id,
                run_id=run_id,
            )
        )
        await context.callbacks.send_message(finished_output)

    async def _handle_run_error(  # noqa: PLR6301
        self,
        context: ModuleContext,
        event: RunErrorEvent,
    ) -> None:
        """Handle run error event - emit AG-UI RunError."""
        from ag_ui.core.events import RunErrorEvent as AgUiRunErrorEvent  # pylint: disable=C0415

        error_msg = event.content or "Agent run failed"
        output = AgUiRunErrorOutput(
            event=AgUiRunErrorEvent(
                message=error_msg,
                code=event.error_type,
            )
        )
        await context.callbacks.send_message(output)

    async def _handle_tool_call_started(
        self,
        context: ModuleContext,
        event: ToolCallStartedEvent,
    ) -> None:
        """Handle tool call started event - emit AG-UI ToolCallStart."""
        import json  # pylint: disable=C0415

        from ag_ui.core.events import ToolCallArgsEvent as AgUiToolCallArgsEvent  # pylint: disable=C0415
        from ag_ui.core.events import ToolCallStartEvent as AgUiToolCallStartEvent  # pylint: disable=C0415

        tool = event.tool
        if not tool or not tool.tool_name:
            return

        # Close any open text message (tool calls interrupt text streaming)
        if self._text_started:
            from ag_ui.core.events import (  # pylint: disable=C0415
                TextMessageEndEvent as AgUiTextMessageEndEvent,
            )

            end_output = AgUiTextMessageEndOutput(
                event=AgUiTextMessageEndEvent(message_id=self._message_id),
            )
            await context.callbacks.send_message(end_output)
            self._text_started = False

        tool_call_id = tool.tool_call_id or str(uuid.uuid4())

        # Emit tool call start
        start_output = AgUiToolCallStartOutput(
            event=AgUiToolCallStartEvent(
                tool_call_id=tool_call_id,
                tool_call_name=tool.tool_name,
                parent_message_id=self._message_id,
            )
        )
        await context.callbacks.send_message(start_output)

        # Emit tool args if available
        if tool.tool_args:
            args_str = json.dumps(tool.tool_args) if isinstance(tool.tool_args, dict) else str(tool.tool_args)  # pylint: disable=line-too-long
            args_output = AgUiToolCallArgsOutput(
                event=AgUiToolCallArgsEvent(
                    tool_call_id=tool_call_id,
                    delta=args_str,
                )
            )
            await context.callbacks.send_message(args_output)

    async def _handle_tool_call_completed(  # noqa: PLR6301
        self,
        context: ModuleContext,
        event: ToolCallCompletedEvent,
    ) -> None:
        """Handle tool call completed event - emit AG-UI ToolCallEnd and ToolCallResult."""
        from ag_ui.core.events import ToolCallEndEvent as AgUiToolCallEndEvent  # pylint: disable=C0415
        from ag_ui.core.events import ToolCallResultEvent as AgUiToolCallResultEvent  # pylint: disable=C0415

        tool = event.tool
        if not tool:
            return

        tool_call_id = tool.tool_call_id or str(uuid.uuid4())

        # Emit tool call end
        end_output = AgUiToolCallEndOutput(event=AgUiToolCallEndEvent(tool_call_id=tool_call_id))
        await context.callbacks.send_message(end_output)

        # Emit result if available
        result_content = tool.result or str(event.content or "")
        if result_content:
            result_msg_id = str(uuid.uuid4())
            result_output = AgUiToolCallResultOutput(
                event=AgUiToolCallResultEvent(
                    message_id=result_msg_id,
                    tool_call_id=tool_call_id,
                    content=result_content,
                    role="tool",
                )
            )
            await context.callbacks.send_message(result_output)

    async def _handle_tool_call_error(  # noqa: PLR6301
        self,
        context: ModuleContext,
        event: ToolCallErrorEvent,
    ) -> None:
        """Handle tool call error event - emit AG-UI ToolCallEnd."""
        from ag_ui.core.events import ToolCallEndEvent as AgUiToolCallEndEvent  # pylint: disable=C0415

        tool = event.tool
        if not tool:
            return

        tool_call_id = tool.tool_call_id or str(uuid.uuid4())
        output = AgUiToolCallEndOutput(event=AgUiToolCallEndEvent(tool_call_id=tool_call_id))
        await context.callbacks.send_message(output)

    async def _handle_reasoning_started(
        self,
        context: ModuleContext,
        event: ReasoningStartedEvent,  # noqa: ARG002 # pylint: disable=unused-argument
    ) -> None:
        """Handle reasoning started event - emit AG-UI ReasoningStart."""
        from ag_ui.core.events import (  # pylint: disable=import-outside-toplevel
            ReasoningMessageStartEvent as AgUiReasoningMessageStartEvent,
        )
        from ag_ui.core.events import (
            ReasoningStartEvent as AgUiReasoningStartEvent,  # pylint: disable=import-outside-toplevel
        )

        if self._reasoning_started:
            return

        self._reasoning_started = True

        # Emit ReasoningStart
        start_output = AgUiReasoningStartOutput(
            event=AgUiReasoningStartEvent(message_id=self._reasoning_id),
        )
        await context.callbacks.send_message(start_output)

        # Emit ReasoningMessageStart
        message_start_output = AgUiReasoningMessageStartOutput(
            event=AgUiReasoningMessageStartEvent(message_id=self._reasoning_id, role="reasoning")
        )
        await context.callbacks.send_message(message_start_output)

    async def _handle_reasoning_delta(
        self,
        context: ModuleContext,
        event: ReasoningContentDeltaEvent,
    ) -> None:
        """Handle reasoning content delta event - emit AG-UI ReasoningMessageContent."""
        from ag_ui.core.events import (  # pylint: disable=import-outside-toplevel
            ReasoningMessageContentEvent as AgUiReasoningMessageContentEvent,
        )

        delta = event.delta
        if not delta:
            return

        # Auto-open reasoning if we get a delta without a prior started event
        if not self._reasoning_started:
            dummy_event = ReasoningStartedEvent(
                event=AgentRunEvent.REASONING_STARTED,
                timestamp=datetime.now(timezone.utc).timestamp(),
                metadata={},
            )
            await self._handle_reasoning_started(context, dummy_event)

        # Emit reasoning content delta
        content_output = AgUiReasoningMessageContentOutput(
            event=AgUiReasoningMessageContentEvent(message_id=self._reasoning_id, delta=delta)
        )
        await context.callbacks.send_message(content_output)

    async def _handle_reasoning_step(
        self,
        context: ModuleContext,
        event: ReasoningStepEvent,
    ) -> None:
        """Handle reasoning step event - emit AG-UI ReasoningMessageContent."""
        from ag_ui.core.events import (  # pylint: disable=import-outside-toplevel
            ReasoningMessageContentEvent as AgUiReasoningMessageContentEvent,
        )

        delta = event.delta
        if not delta:
            return

        # Auto-open reasoning if needed
        if not self._reasoning_started:
            dummy_event = ReasoningStartedEvent(
                event=AgentRunEvent.REASONING_STARTED,
                timestamp=datetime.now(timezone.utc).timestamp(),
                metadata={},
            )
            await self._handle_reasoning_started(context, dummy_event)

        # Emit reasoning step as content
        content_output = AgUiReasoningMessageContentOutput(
            event=AgUiReasoningMessageContentEvent(message_id=self._reasoning_id, delta=delta)
        )
        await context.callbacks.send_message(content_output)

    async def _handle_reasoning_completed(
        self,
        context: ModuleContext,
        event: ReasoningCompletedEvent,  # noqa: ARG002 # pylint: disable=unused-argument
    ) -> None:
        """Handle reasoning completed event - close reasoning sequence."""
        await self._close_reasoning(context)

    async def _close_reasoning(self, context: ModuleContext) -> None:
        """Close an open Reasoning sequence if one is active."""
        from ag_ui.core.events import (  # pylint: disable=import-outside-toplevel
            ReasoningEndEvent as AgUiReasoningEndEvent,
        )
        from ag_ui.core.events import (  # pylint: disable=import-outside-toplevel
            ReasoningMessageEndEvent as AgUiReasoningMessageEndEvent,
        )

        if not self._reasoning_started:
            return

        # Emit ReasoningMessageEnd
        message_end_output = AgUiReasoningMessageEndOutput(
            event=AgUiReasoningMessageEndEvent(message_id=self._reasoning_id)
        )
        await context.callbacks.send_message(message_end_output)

        # Emit ReasoningEnd
        end_output = AgUiReasoningEndOutput(event=AgUiReasoningEndEvent(message_id=self._reasoning_id))
        await context.callbacks.send_message(end_output)

        self._reasoning_started = False
        self._reasoning_id = str(uuid.uuid4())
