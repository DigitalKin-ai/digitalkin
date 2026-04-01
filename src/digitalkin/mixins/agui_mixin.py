"""AG-UI event streaming mixin for DigitalKin modules.

This mixin provides utilities to convert framework-agnostic agent events
into AG-UI protocol events and send them through the module context callbacks.

The mixin is a stateless emitter: it receives events with all necessary info
(including IDs) and emits the corresponding AG-UI protocol events.
All state management (ID generation, lifecycle tracking) belongs in the adapter layer.
"""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, ClassVar

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
)

if TYPE_CHECKING:
    from digitalkin.models.module.ag_ui import AgUiEventOutput
    from digitalkin.models.module.module_context import ModuleContext


class AgUiMixin:
    """Mixin for converting agent events to AG-UI protocol and sending them.

    This mixin is a stateless emitter: each handler reads IDs from the event
    and emits the corresponding AG-UI event(s). The adapter is responsible for
    generating IDs and managing event lifecycle (start/complete sequences).

    Usage::

        class MyTrigger(BaseTrigger, AgUiMixin):
            async def execute(self, context, input_data):
                async for event in agent.run(input_data.message, stream=True):
                    await self.agui_send_message(context, event)
    """

    def __init__(self) -> None:
        """Initialize AG-UI mixin."""
        super().__init__()
        self._thread_id: str = str(uuid.uuid4())
        self._run_id: str = ""

    async def _send_agui(  # noqa: PLR6301
        self,
        context: ModuleContext,
        output: AgUiEventOutput,
    ) -> None:
        from digitalkin.models.module.ag_ui import AgUiOutput  # pylint: disable=C0415

        await context.callbacks.send_message(AgUiOutput(root=output))

    async def send_message(
        self,
        context: ModuleContext,
        event: BaseAgentRunEvent,
    ) -> None:
        """Convert agent event to AG-UI protocol and send via context callbacks.

        Args:
            context: Module context containing the callbacks strategy.
            event: Agent run event to process and convert.
        """
        context.callbacks.logger.debug(
            "AG-UI event: %s",
            event.event,
            extra=context.session.current_ids(),
        )

        handler_name = self._AGUI_HANDLER_MAP.get(event.event)
        if handler_name:
            await getattr(self, handler_name)(context, event)

    _AGUI_HANDLER_MAP: ClassVar[dict[str, str]] = {
        AgentRunEvent.RUN_STARTED: "_handle_run_started",
        AgentRunEvent.TEXT_MESSAGE_STARTED: "_handle_text_message_started",
        AgentRunEvent.RUN_CONTENT: "_handle_run_content",
        AgentRunEvent.TEXT_MESSAGE_COMPLETED: "_handle_text_message_completed",
        AgentRunEvent.RUN_COMPLETED: "_handle_run_completed",
        AgentRunEvent.RUN_ERROR: "_handle_run_error",
        AgentRunEvent.TOOL_CALL_STARTED: "_handle_tool_call_started",
        AgentRunEvent.TOOL_CALL_COMPLETED: "_handle_tool_call_completed",
        AgentRunEvent.TOOL_CALL_ERROR: "_handle_tool_call_error",
        AgentRunEvent.REASONING_STARTED: "_handle_reasoning_started",
        AgentRunEvent.REASONING_CONTENT_DELTA: "_handle_reasoning_delta",
        AgentRunEvent.REASONING_STEP: "_handle_reasoning_step",
        AgentRunEvent.REASONING_COMPLETED: "_handle_reasoning_completed",
    }

    # ── Private Event Handlers ───────────────────────────────────────────────

    async def _handle_run_started(
        self,
        context: ModuleContext,
        event: RunStartedEvent,
    ) -> None:
        """Handle run started event - emit AG-UI RunStarted."""
        from ag_ui.core.events import RunStartedEvent as AgUiRunStartedEvent  # pylint: disable=C0415

        from digitalkin.models.module.ag_ui import AgUiRunStartedOutput  # pylint: disable=C0415

        self._run_id = event.run_id or str(uuid.uuid4())
        if event.thread_id:
            self._thread_id = event.thread_id

        output = AgUiRunStartedOutput(
            event=AgUiRunStartedEvent(
                thread_id=self._thread_id,
                run_id=self._run_id,
            )
        )
        await self._send_agui(context, output)

    async def _handle_text_message_started(
        self,
        context: ModuleContext,
        event: TextMessageStartedEvent,
    ) -> None:
        """Handle text message started event - emit AG-UI TextMessageStart."""
        from ag_ui.core.events import (  # pylint: disable=C0415
            TextMessageStartEvent as AgUiTextMessageStartEvent,
        )

        from digitalkin.models.module.ag_ui import AgUiTextMessageStartOutput  # pylint: disable=C0415

        output = AgUiTextMessageStartOutput(
            event=AgUiTextMessageStartEvent(
                message_id=event.message_id,
                role="assistant",
            )
        )
        await self._send_agui(context, output)

    async def _handle_run_content(
        self,
        context: ModuleContext,
        event: RunContentEvent,
    ) -> None:
        """Handle run content event - emit AG-UI TextMessageContent."""
        from ag_ui.core.events import (  # pylint: disable=C0415
            TextMessageContentEvent as AgUiTextMessageContentEvent,
        )

        from digitalkin.models.module.ag_ui import AgUiTextMessageContentOutput  # pylint: disable=C0415

        content = event.content
        if not content:
            return

        message_id = event.message_id or ""

        output = AgUiTextMessageContentOutput(
            event=AgUiTextMessageContentEvent(
                message_id=message_id,
                delta=content,
            )
        )
        await self._send_agui(context, output)

    async def _handle_text_message_completed(
        self,
        context: ModuleContext,
        event: TextMessageCompletedEvent,
    ) -> None:
        """Handle text message completed event - emit AG-UI TextMessageEnd."""
        from ag_ui.core.events import TextMessageEndEvent as AgUiTextMessageEndEvent  # pylint: disable=C0415

        from digitalkin.models.module.ag_ui import AgUiTextMessageEndOutput  # pylint: disable=C0415

        output = AgUiTextMessageEndOutput(
            event=AgUiTextMessageEndEvent(message_id=event.message_id),
        )
        await self._send_agui(context, output)

    async def _handle_run_completed(
        self,
        context: ModuleContext,
        event: RunCompletedEvent,
    ) -> None:
        """Handle run completed event - emit AG-UI RunFinished."""
        from ag_ui.core.events import RunFinishedEvent as AgUiRunFinishedEvent  # pylint: disable=C0415

        from digitalkin.models.module.ag_ui import AgUiRunFinishedOutput  # pylint: disable=C0415

        run_id = event.run_id or self._run_id
        output = AgUiRunFinishedOutput(
            event=AgUiRunFinishedEvent(
                thread_id=self._thread_id,
                run_id=run_id,
            )
        )
        await self._send_agui(context, output)

    async def _handle_run_error(
        self,
        context: ModuleContext,
        event: RunErrorEvent,
    ) -> None:
        """Handle run error event - emit AG-UI RunError."""
        from ag_ui.core.events import RunErrorEvent as AgUiRunErrorEvent  # pylint: disable=C0415

        from digitalkin.models.module.ag_ui import AgUiRunErrorOutput  # pylint: disable=C0415

        error_msg = event.content or "Agent run failed"
        output = AgUiRunErrorOutput(
            event=AgUiRunErrorEvent(
                message=error_msg,
                code=event.error_type,
            )
        )
        await self._send_agui(context, output)

    async def _handle_tool_call_started(
        self,
        context: ModuleContext,
        event: ToolCallStartedEvent,
    ) -> None:
        """Handle tool call started event - emit AG-UI ToolCallStart."""
        import json  # pylint: disable=C0415

        from ag_ui.core.events import ToolCallArgsEvent as AgUiToolCallArgsEvent  # pylint: disable=C0415
        from ag_ui.core.events import ToolCallStartEvent as AgUiToolCallStartEvent  # pylint: disable=C0415

        from digitalkin.models.module.ag_ui import (  # pylint: disable=C0415
            AgUiToolCallArgsOutput,
            AgUiToolCallStartOutput,
        )

        tool = event.tool
        if not tool or not tool.tool_name:
            return

        tool_call_id = tool.tool_call_id or str(uuid.uuid4())

        start_output = AgUiToolCallStartOutput(
            event=AgUiToolCallStartEvent(
                tool_call_id=tool_call_id,
                tool_call_name=tool.tool_name,
            )
        )
        await self._send_agui(context, start_output)

        if tool.tool_args:
            args_str = json.dumps(tool.tool_args) if isinstance(tool.tool_args, dict) else str(tool.tool_args)
            args_output = AgUiToolCallArgsOutput(
                event=AgUiToolCallArgsEvent(
                    tool_call_id=tool_call_id,
                    delta=args_str,
                )
            )
            await self._send_agui(context, args_output)

    async def _handle_tool_call_completed(
        self,
        context: ModuleContext,
        event: ToolCallCompletedEvent,
    ) -> None:
        """Handle tool call completed event - emit AG-UI ToolCallEnd and ToolCallResult."""
        from ag_ui.core.events import ToolCallEndEvent as AgUiToolCallEndEvent  # pylint: disable=C0415
        from ag_ui.core.events import ToolCallResultEvent as AgUiToolCallResultEvent  # pylint: disable=C0415

        from digitalkin.models.module.ag_ui import (  # pylint: disable=C0415
            AgUiToolCallEndOutput,
            AgUiToolCallResultOutput,
        )

        tool = event.tool
        if not tool:
            return

        tool_call_id = tool.tool_call_id or str(uuid.uuid4())

        end_output = AgUiToolCallEndOutput(event=AgUiToolCallEndEvent(tool_call_id=tool_call_id))
        await self._send_agui(context, end_output)

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
            await self._send_agui(context, result_output)

    async def _handle_tool_call_error(
        self,
        context: ModuleContext,
        event: ToolCallErrorEvent,
    ) -> None:
        """Handle tool call error event - emit AG-UI ToolCallEnd."""
        from ag_ui.core.events import ToolCallEndEvent as AgUiToolCallEndEvent  # pylint: disable=C0415

        from digitalkin.models.module.ag_ui import AgUiToolCallEndOutput  # pylint: disable=C0415

        tool = event.tool
        if not tool:
            return

        tool_call_id = tool.tool_call_id or str(uuid.uuid4())
        output = AgUiToolCallEndOutput(event=AgUiToolCallEndEvent(tool_call_id=tool_call_id))
        await self._send_agui(context, output)

    async def _handle_reasoning_started(
        self,
        context: ModuleContext,
        event: ReasoningStartedEvent,
    ) -> None:
        """Handle reasoning started event - emit AG-UI ReasoningStart + ReasoningMessageStart."""
        from ag_ui.core.events import (  # pylint: disable=import-outside-toplevel
            ReasoningMessageStartEvent as AgUiReasoningMessageStartEvent,
        )
        from ag_ui.core.events import (  # pylint: disable=import-outside-toplevel
            ReasoningStartEvent as AgUiReasoningStartEvent,
        )

        from digitalkin.models.module.ag_ui import (  # pylint: disable=C0415
            AgUiReasoningMessageStartOutput,
            AgUiReasoningStartOutput,
        )

        reasoning_id = event.reasoning_id or str(uuid.uuid4())

        start_output = AgUiReasoningStartOutput(
            event=AgUiReasoningStartEvent(message_id=reasoning_id),
        )
        await self._send_agui(context, start_output)

        message_start_output = AgUiReasoningMessageStartOutput(
            event=AgUiReasoningMessageStartEvent(message_id=reasoning_id, role="reasoning")
        )
        await self._send_agui(context, message_start_output)

    async def _handle_reasoning_delta(
        self,
        context: ModuleContext,
        event: ReasoningContentDeltaEvent,
    ) -> None:
        """Handle reasoning content delta event - emit AG-UI ReasoningMessageContent."""
        from ag_ui.core.events import (  # pylint: disable=import-outside-toplevel
            ReasoningMessageContentEvent as AgUiReasoningMessageContentEvent,
        )

        from digitalkin.models.module.ag_ui import AgUiReasoningMessageContentOutput  # pylint: disable=C0415

        delta = event.delta
        if not delta:
            return

        reasoning_id = event.reasoning_id or ""

        output = AgUiReasoningMessageContentOutput(
            event=AgUiReasoningMessageContentEvent(message_id=reasoning_id, delta=delta)
        )
        await self._send_agui(context, output)

    async def _handle_reasoning_step(
        self,
        context: ModuleContext,
        event: ReasoningStepEvent,
    ) -> None:
        """Handle reasoning step event - emit AG-UI ReasoningMessageContent."""
        from ag_ui.core.events import (  # pylint: disable=import-outside-toplevel
            ReasoningMessageContentEvent as AgUiReasoningMessageContentEvent,
        )

        from digitalkin.models.module.ag_ui import AgUiReasoningMessageContentOutput  # pylint: disable=C0415

        delta = event.delta
        if not delta:
            return

        reasoning_id = event.reasoning_id or ""

        output = AgUiReasoningMessageContentOutput(
            event=AgUiReasoningMessageContentEvent(message_id=reasoning_id, delta=delta)
        )
        await self._send_agui(context, output)

    async def _handle_reasoning_completed(
        self,
        context: ModuleContext,
        event: ReasoningCompletedEvent,
    ) -> None:
        """Handle reasoning completed event - emit AG-UI ReasoningMessageEnd + ReasoningEnd."""
        from ag_ui.core.events import (  # pylint: disable=import-outside-toplevel
            ReasoningEndEvent as AgUiReasoningEndEvent,
        )
        from ag_ui.core.events import (  # pylint: disable=import-outside-toplevel
            ReasoningMessageEndEvent as AgUiReasoningMessageEndEvent,
        )

        from digitalkin.models.module.ag_ui import (  # pylint: disable=C0415
            AgUiReasoningEndOutput,
            AgUiReasoningMessageEndOutput,
        )

        reasoning_id = event.reasoning_id or ""

        message_end_output = AgUiReasoningMessageEndOutput(event=AgUiReasoningMessageEndEvent(message_id=reasoning_id))
        await self._send_agui(context, message_end_output)

        end_output = AgUiReasoningEndOutput(
            event=AgUiReasoningEndEvent(message_id=reasoning_id),
        )
        await self._send_agui(context, end_output)
