"""AG-UI event streaming mixin for DigitalKin modules.

This mixin provides utilities to convert framework-agnostic agent events
into AG-UI protocol events and send them through the module context callbacks.

The mixin is a stateless emitter: it receives events with all necessary info
(including IDs) and emits the corresponding AG-UI protocol events.
All state management (ID generation, lifecycle tracking) belongs in the adapter layer.
"""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING, Any, ClassVar

from ag_ui.core.events import ReasoningEndEvent as AgUiReasoningEndEvent
from ag_ui.core.events import ReasoningMessageContentEvent as AgUiReasoningMessageContentEvent
from ag_ui.core.events import ReasoningMessageEndEvent as AgUiReasoningMessageEndEvent
from ag_ui.core.events import ReasoningMessageStartEvent as AgUiReasoningMessageStartEvent
from ag_ui.core.events import ReasoningStartEvent as AgUiReasoningStartEvent
from ag_ui.core.events import RunErrorEvent as AgUiRunErrorEvent
from ag_ui.core.events import RunFinishedEvent as AgUiRunFinishedEvent
from ag_ui.core.events import RunStartedEvent as AgUiRunStartedEvent
from ag_ui.core.events import SubagentErrorEvent as AgUiSubagentErrorEvent
from ag_ui.core.events import SubagentFinishedEvent as AgUiSubagentFinishedEvent
from ag_ui.core.events import SubagentStartedEvent as AgUiSubagentStartedEvent
from ag_ui.core.events import TextMessageContentEvent as AgUiTextMessageContentEvent
from ag_ui.core.events import TextMessageEndEvent as AgUiTextMessageEndEvent
from ag_ui.core.events import TextMessageStartEvent as AgUiTextMessageStartEvent
from ag_ui.core.events import ToolCallArgsEvent as AgUiToolCallArgsEvent
from ag_ui.core.events import ToolCallEndEvent as AgUiToolCallEndEvent
from ag_ui.core.events import ToolCallResultEvent as AgUiToolCallResultEvent
from ag_ui.core.events import ToolCallStartEvent as AgUiToolCallStartEvent

from digitalkin.models.events import (
    AgentRunEvent,
    BaseAgentRunEvent,
    CustomEvent,
    ReasoningCompletedEvent,
    ReasoningContentDeltaEvent,
    ReasoningStartedEvent,
    ReasoningStepEvent,
    RunCompletedEvent,
    RunContentEvent,
    RunErrorEvent,
    RunStartedEvent,
    SubagentErrorEvent,
    SubagentFinishedEvent,
    SubagentStartedEvent,
    TextMessageCompletedEvent,
    TextMessageStartedEvent,
    ToolCallCompletedEvent,
    ToolCallErrorEvent,
    ToolCallStartedEvent,
)
from digitalkin.models.module.ag_ui import (
    AgUiOutput,
    AgUiReasoningEndOutput,
    AgUiReasoningMessageContentOutput,
    AgUiReasoningMessageEndOutput,
    AgUiReasoningMessageStartOutput,
    AgUiReasoningStartOutput,
    AgUiRunErrorOutput,
    AgUiRunFinishedOutput,
    AgUiRunStartedOutput,
    AgUiSubagentErrorOutput,
    AgUiSubagentFinishedOutput,
    AgUiSubagentStartedOutput,
    AgUiTextMessageContentOutput,
    AgUiTextMessageEndOutput,
    AgUiTextMessageStartOutput,
    AgUiToolCallArgsOutput,
    AgUiToolCallEndOutput,
    AgUiToolCallResultOutput,
    AgUiToolCallStartOutput,
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
        self._thread_id: str = ""
        self._run_id: str = ""

    async def _send_agui(  # noqa: PLR6301
        self,
        context: ModuleContext,
        output: AgUiEventOutput,
    ) -> None:

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
            "AG-UI event: %s thread_id=%s run_id=%s",
            event.event,
            self._thread_id,
            self._run_id,
            extra=context.session.current_ids(),
        )

        handler = self._agui_dispatch.get(event.event)
        if handler is not None:
            await handler(self, context, event)

    _agui_dispatch: ClassVar[dict[str, Any]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Build dispatch table from unbound method references."""
        super().__init_subclass__(**kwargs)
        cls._agui_dispatch = {
            AgentRunEvent.RUN_STARTED: cls._handle_run_started,
            AgentRunEvent.TEXT_MESSAGE_STARTED: cls._handle_text_message_started,
            AgentRunEvent.RUN_CONTENT: cls._handle_run_content,
            AgentRunEvent.TEXT_MESSAGE_COMPLETED: cls._handle_text_message_completed,
            AgentRunEvent.RUN_COMPLETED: cls._handle_run_completed,
            AgentRunEvent.RUN_ERROR: cls._handle_run_error,
            AgentRunEvent.SUBAGENT_STARTED: cls._handle_subagent_started,
            AgentRunEvent.SUBAGENT_FINISHED: cls._handle_subagent_finished,
            AgentRunEvent.SUBAGENT_ERROR: cls._handle_subagent_error,
            AgentRunEvent.TOOL_CALL_STARTED: cls._handle_tool_call_started,
            AgentRunEvent.TOOL_CALL_COMPLETED: cls._handle_tool_call_completed,
            AgentRunEvent.TOOL_CALL_ERROR: cls._handle_tool_call_error,
            AgentRunEvent.REASONING_STARTED: cls._handle_reasoning_started,
            AgentRunEvent.REASONING_CONTENT_DELTA: cls._handle_reasoning_delta,
            AgentRunEvent.REASONING_STEP: cls._handle_reasoning_step,
            AgentRunEvent.REASONING_COMPLETED: cls._handle_reasoning_completed,
            AgentRunEvent.CUSTOM: cls._handle_custom,
        }

    @staticmethod
    def _authored(event: BaseAgentRunEvent) -> dict[str, Any]:
        """Author fields shared by every AG-UI event this mixin emits.

        ``subagent_run_id`` is the attribution a client groups on. ``metadata`` is namespaced
        under ``digitalkin`` because AG-UI reserves the ``ag-ui`` key for itself and leaves the
        rest of the object to the application; a client merges it onto the message (or, for a
        tool call, onto the tool call) with last-write-wins per key.

        Run-level events carry no ``subagent_run_id`` — the adapter never sets one on them, as
        AG-UI treats RUN_STARTED / RUN_FINISHED / RUN_ERROR as unattributable.

        Args:
            event: The DigitalKin event being converted.

        Returns:
            Keyword arguments to splat into the AG-UI event constructor.
        """
        fields: dict[str, Any] = {}
        if event.metadata:
            fields["metadata"] = {"digitalkin": event.metadata}
        if event.subagent_run_id:
            fields["subagent_run_id"] = event.subagent_run_id
        return fields

    # ── Private Event Handlers ───────────────────────────────────────────────

    async def _handle_run_started(
        self,
        context: ModuleContext,
        event: RunStartedEvent,
    ) -> None:
        """Handle run started event - emit AG-UI RunStarted."""
        if not self._run_id:
            self._run_id = event.run_id or str(uuid.uuid4())
        if not self._thread_id:
            self._thread_id = event.thread_id or str(uuid.uuid4())

        context.callbacks.logger.info(
            "[agui-mixin] RUN_STARTED thread_id=%s run_id=%s event_run_id=%s event_thread_id=%s metadata=%s",
            self._thread_id,
            self._run_id,
            event.run_id,
            event.thread_id,
            event.metadata,
            extra=context.session.current_ids(),
        )

        output = AgUiRunStartedOutput(
            event=AgUiRunStartedEvent(
                thread_id=self._thread_id,
                run_id=self._run_id,
                **self._authored(event),
            )
        )
        await self._send_agui(context, output)

    async def _handle_text_message_started(
        self,
        context: ModuleContext,
        event: TextMessageStartedEvent,
    ) -> None:
        """Handle text message started event - emit AG-UI TextMessageStart."""
        # ``name`` labels the bubble with the step that owns it, so a client can attribute a
        # member's message when several stream at once. Typed on TextMessageStartEvent since
        # ag-ui-protocol 0.1.18, which is this package's floor.
        output = AgUiTextMessageStartOutput(
            event=AgUiTextMessageStartEvent(
                message_id=event.message_id,
                role="assistant",
                name=event.name,
                **self._authored(event),
            )
        )
        await self._send_agui(context, output)

    async def _handle_run_content(
        self,
        context: ModuleContext,
        event: RunContentEvent,
    ) -> None:
        """Handle run content event - emit AG-UI TextMessageContent."""
        content = event.content
        if not content:
            return

        message_id = event.message_id or ""

        output = AgUiTextMessageContentOutput(
            event=AgUiTextMessageContentEvent(
                message_id=message_id,
                delta=content,
                **self._authored(event),
            )
        )
        await self._send_agui(context, output)

    async def _handle_text_message_completed(
        self,
        context: ModuleContext,
        event: TextMessageCompletedEvent,
    ) -> None:
        """Handle text message completed event - emit AG-UI TextMessageEnd."""
        output = AgUiTextMessageEndOutput(
            event=AgUiTextMessageEndEvent(message_id=event.message_id, **self._authored(event)),
        )
        await self._send_agui(context, output)

    async def _handle_run_completed(
        self,
        context: ModuleContext,
        event: RunCompletedEvent,
    ) -> None:
        """Handle run completed event - emit AG-UI RunFinished."""
        run_id = self._run_id or event.run_id or str(uuid.uuid4())
        context.callbacks.logger.info(
            "[agui-mixin] RUN_FINISHED thread_id=%s event_run_id=%s self._run_id=%s resolved=%s metadata=%s",
            self._thread_id,
            event.run_id,
            self._run_id,
            run_id,
            event.metadata,
            extra=context.session.current_ids(),
        )
        output = AgUiRunFinishedOutput(
            event=AgUiRunFinishedEvent(
                thread_id=self._thread_id,
                run_id=run_id,
                **self._authored(event),
            )
        )
        await self._send_agui(context, output)

    async def _handle_run_error(
        self,
        context: ModuleContext,
        event: RunErrorEvent,
    ) -> None:
        """Handle run error event - emit AG-UI RunError."""
        error_msg = event.content or "Agent run failed"
        output = AgUiRunErrorOutput(
            event=AgUiRunErrorEvent(
                message=error_msg,
                code=event.error_type,
                **self._authored(event),
            )
        )
        await self._send_agui(context, output)

    async def _handle_subagent_started(
        self,
        context: ModuleContext,
        event: SubagentStartedEvent,
    ) -> None:
        """Handle subagent started event - emit AG-UI SubagentStarted."""
        output = AgUiSubagentStartedOutput(
            event=AgUiSubagentStartedEvent(
                subagent_run_id=event.subagent_run_id or "",
                name=event.name,
                parent_subagent_run_id=event.parent_subagent_run_id,
                parent_tool_call_id=event.parent_tool_call_id,
                metadata={"digitalkin": event.metadata} if event.metadata else None,
            )
        )
        await self._send_agui(context, output)

    async def _handle_subagent_finished(
        self,
        context: ModuleContext,
        event: SubagentFinishedEvent,
    ) -> None:
        """Handle subagent finished event - emit AG-UI SubagentFinished."""
        output = AgUiSubagentFinishedOutput(
            event=AgUiSubagentFinishedEvent(
                subagent_run_id=event.subagent_run_id or "",
                result=event.result,
                metadata={"digitalkin": event.metadata} if event.metadata else None,
            )
        )
        await self._send_agui(context, output)

    async def _handle_subagent_error(
        self,
        context: ModuleContext,
        event: SubagentErrorEvent,
    ) -> None:
        """Handle subagent error event - emit AG-UI SubagentError.

        Deliberately not a RUN_ERROR: AG-UI treats that as terminal for the whole stream, and
        one delegated agent failing does not end the parent's run.
        """
        output = AgUiSubagentErrorOutput(
            event=AgUiSubagentErrorEvent(
                subagent_run_id=event.subagent_run_id or "",
                message=event.message,
                code=event.code,
                metadata={"digitalkin": event.metadata} if event.metadata else None,
            )
        )
        await self._send_agui(context, output)

    async def _handle_tool_call_started(
        self,
        context: ModuleContext,
        event: ToolCallStartedEvent,
    ) -> None:
        """Handle tool call started event - emit AG-UI ToolCallStart."""
        tool = event.tool
        if not tool or not tool.tool_name:
            return

        tool_call_id = tool.tool_call_id or str(uuid.uuid4())

        start_output = AgUiToolCallStartOutput(
            event=AgUiToolCallStartEvent(
                tool_call_id=tool_call_id,
                tool_call_name=tool.tool_name,
                **self._authored(event),
            )
        )
        await self._send_agui(context, start_output)

        if tool.tool_args:
            args_str = json.dumps(tool.tool_args) if isinstance(tool.tool_args, dict) else str(tool.tool_args)
            args_output = AgUiToolCallArgsOutput(
                event=AgUiToolCallArgsEvent(
                    tool_call_id=tool_call_id,
                    delta=args_str,
                    **self._authored(event),
                )
            )
            await self._send_agui(context, args_output)

    async def _handle_tool_call_completed(
        self,
        context: ModuleContext,
        event: ToolCallCompletedEvent,
    ) -> None:
        """Handle tool call completed event - emit AG-UI ToolCallEnd and ToolCallResult."""
        tool = event.tool
        if not tool:
            return

        tool_call_id = tool.tool_call_id or str(uuid.uuid4())

        end_output = AgUiToolCallEndOutput(
            event=AgUiToolCallEndEvent(tool_call_id=tool_call_id, **self._authored(event))
        )
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
                    **self._authored(event),
                )
            )
            await self._send_agui(context, result_output)

    async def _handle_tool_call_error(
        self,
        context: ModuleContext,
        event: ToolCallErrorEvent,
    ) -> None:
        """Handle tool call error event - emit AG-UI ToolCallEnd."""
        tool = event.tool
        if not tool:
            return

        tool_call_id = tool.tool_call_id or str(uuid.uuid4())
        output = AgUiToolCallEndOutput(event=AgUiToolCallEndEvent(tool_call_id=tool_call_id, **self._authored(event)))
        await self._send_agui(context, output)

    async def _handle_reasoning_started(
        self,
        context: ModuleContext,
        event: ReasoningStartedEvent,
    ) -> None:
        """Handle reasoning started event - emit AG-UI ReasoningStart + ReasoningMessageStart."""
        reasoning_id = event.reasoning_id or str(uuid.uuid4())

        start_output = AgUiReasoningStartOutput(
            event=AgUiReasoningStartEvent(message_id=reasoning_id, **self._authored(event)),
        )
        await self._send_agui(context, start_output)

        message_start_output = AgUiReasoningMessageStartOutput(
            event=AgUiReasoningMessageStartEvent(message_id=reasoning_id, role="reasoning", **self._authored(event))
        )
        await self._send_agui(context, message_start_output)

    async def _handle_reasoning_delta(
        self,
        context: ModuleContext,
        event: ReasoningContentDeltaEvent,
    ) -> None:
        """Handle reasoning content delta event - emit AG-UI ReasoningMessageContent."""
        delta = event.delta
        if not delta:
            return

        reasoning_id = event.reasoning_id or ""

        output = AgUiReasoningMessageContentOutput(
            event=AgUiReasoningMessageContentEvent(message_id=reasoning_id, delta=delta, **self._authored(event))
        )
        await self._send_agui(context, output)

    async def _handle_reasoning_step(
        self,
        context: ModuleContext,
        event: ReasoningStepEvent,
    ) -> None:
        """Handle reasoning step event - emit AG-UI ReasoningMessageContent."""
        delta = event.delta
        if not delta:
            return

        reasoning_id = event.reasoning_id or ""

        output = AgUiReasoningMessageContentOutput(
            event=AgUiReasoningMessageContentEvent(message_id=reasoning_id, delta=delta, **self._authored(event))
        )
        await self._send_agui(context, output)

    async def _handle_reasoning_completed(
        self,
        context: ModuleContext,
        event: ReasoningCompletedEvent,
    ) -> None:
        """Handle reasoning completed event - emit AG-UI ReasoningMessageEnd + ReasoningEnd."""
        reasoning_id = event.reasoning_id or ""

        message_end_output = AgUiReasoningMessageEndOutput(
            event=AgUiReasoningMessageEndEvent(message_id=reasoning_id, **self._authored(event))
        )
        await self._send_agui(context, message_end_output)

        end_output = AgUiReasoningEndOutput(
            event=AgUiReasoningEndEvent(message_id=reasoning_id, **self._authored(event)),
        )
        await self._send_agui(context, end_output)

    async def _handle_custom(
        self,
        context: ModuleContext,
        event: CustomEvent,
    ) -> None:
        """Handle custom event - emit AG-UI CustomEvent."""
        from ag_ui.core.events import CustomEvent as AgUiCustomEvent  # pylint: disable=C0415

        from digitalkin.models.module.ag_ui import AgUiCustomEventOutput  # pylint: disable=C0415

        output = AgUiCustomEventOutput(
            event=AgUiCustomEvent(
                name=event.name,
                value=event.value,
            )
        )
        await self._send_agui(context, output)
