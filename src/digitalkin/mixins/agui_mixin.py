"""AG-UI event streaming mixin for DigitalKin modules.

This mixin provides utilities to convert framework-agnostic agent events
into AG-UI protocol events and send them through the module context callbacks.

The mixin receives events with all necessary info (including IDs) and emits the
corresponding AG-UI protocol events. Event-sequence bookkeeping belongs in the adapter;
the only state kept here is the SDK-owned pipeline view (``AgUiRunState``) mirrored to
the front as ``STATE_SNAPSHOT`` + ``STATE_DELTA``. See ``docs/agui_events.md``.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import TYPE_CHECKING, Any, ClassVar

from ag_ui.core.events import CustomEvent as AgUiCustomEvent
from ag_ui.core.events import ReasoningEndEvent as AgUiReasoningEndEvent
from ag_ui.core.events import ReasoningMessageContentEvent as AgUiReasoningMessageContentEvent
from ag_ui.core.events import ReasoningMessageEndEvent as AgUiReasoningMessageEndEvent
from ag_ui.core.events import ReasoningMessageStartEvent as AgUiReasoningMessageStartEvent
from ag_ui.core.events import ReasoningStartEvent as AgUiReasoningStartEvent
from ag_ui.core.events import RunErrorEvent as AgUiRunErrorEvent
from ag_ui.core.events import RunFinishedEvent as AgUiRunFinishedEvent
from ag_ui.core.events import RunStartedEvent as AgUiRunStartedEvent
from ag_ui.core.events import StateDeltaEvent, StateSnapshotEvent
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
from pydantic import BaseModel

from digitalkin.logger import logger
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
    AgUiCustomEventOutput,
    AgUiOutput,
    AgUiReasoningEndOutput,
    AgUiReasoningMessageContentOutput,
    AgUiReasoningMessageEndOutput,
    AgUiReasoningMessageStartOutput,
    AgUiReasoningStartOutput,
    AgUiRunErrorOutput,
    AgUiRunFinishedOutput,
    AgUiRunStartedOutput,
    AgUiStateDeltaOutput,
    AgUiStateSnapshotOutput,
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


class AgUiRunState:
    """SDK-owned pipeline state of one task's AG-UI run.

    The front only renders it: ``snapshot`` replaces its state, ``update`` sends the
    matching JSON Patch. Shape: ``run`` (status, error, heartbeatAt), ``tools`` and
    ``subagents`` keyed by id (name, status, startedAt, error), ``interrupts``.
    """

    def __init__(self) -> None:
        """Start a running run with an empty pipeline."""
        self.open = True
        self.state: dict[str, Any] = {
            "run": {"status": "running", "error": None, "heartbeatAt": None},
            "tools": {},
            "subagents": {},
            "interrupts": [],
        }

    async def snapshot(self, context: ModuleContext) -> None:
        """Emit the full state as ``STATE_SNAPSHOT``.

        Args:
            context: Module context whose callbacks carry the event.
        """
        await context.callbacks.send_message(
            AgUiOutput(root=AgUiStateSnapshotOutput(event=StateSnapshotEvent(snapshot=self.state)))
        )

    async def update(self, context: ModuleContext, *changes: tuple[tuple[str, ...], Any]) -> None:
        """Set each ``(key path, value)`` in the state and emit them as one ``STATE_DELTA``.

        Args:
            context: Module context whose callbacks carry the event.
            *changes: Key path into the state and the value to set there.
        """
        ops = []
        for keys, value in changes:
            node = self.state
            for key in keys[:-1]:
                node = node[key]
            ops.append({
                "op": "replace" if keys[-1] in node else "add",
                "path": "/" + "/".join(key.replace("~", "~0").replace("/", "~1") for key in keys),
                "value": value,
            })
            node[keys[-1]] = value.copy() if isinstance(value, dict | list) else value
        await context.callbacks.send_message(AgUiOutput(root=AgUiStateDeltaOutput(event=StateDeltaEvent(delta=ops))))

    async def fail(
        self,
        context: ModuleContext,
        status: str,
        message: str,
        code: str | None,
        **fields: Any,
    ) -> None:
        """Close the run once: record ``status``/``message`` in state, then emit ``RUN_ERROR``.

        Args:
            context: Module context whose callbacks carry the events.
            status: Terminal run status, ``failed`` or ``cancelled``.
            message: Human-readable error.
            code: ``RUN_ERROR`` code.
            **fields: Extra AG-UI event fields (metadata).
        """
        if not self.open:
            logger.info(
                "AG-UI run already closed (%s); dropping %s RUN_ERROR code=%s message=%s",
                self.state["run"]["status"],
                status,
                code,
                message,
                extra=context.session.current_ids(),
            )
            return
        self.open = False
        await self.update(context, (("run", "status"), status), (("run", "error"), message))
        await context.callbacks.send_message(
            AgUiOutput(root=AgUiRunErrorOutput(event=AgUiRunErrorEvent(message=message, code=code, **fields)))
        )

    @staticmethod
    def now_ms() -> int:
        """Wall-clock epoch milliseconds, the unit of every ``*At`` field.

        Returns:
            Current time in epoch milliseconds.
        """
        return time.time_ns() // 1_000_000


class AgUiMixin:
    """Mixin for converting agent events to AG-UI protocol and sending them.

    Each handler reads IDs from the event and emits the corresponding AG-UI event(s),
    plus the ``STATE_DELTA`` that keeps ``context.agui_run`` in sync. The adapter is
    responsible for generating IDs and managing event lifecycle (start/complete sequences).

    Usage::

        class MyTrigger(BaseTrigger, AgUiMixin):
            async def execute(self, context, input_data):
                async for event in agent.run(input_data.message, stream=True):
                    await self.send_message(context, event)
    """

    def __init__(self) -> None:
        """Initialize AG-UI mixin."""
        super().__init__()
        self._thread_id: str = ""
        self._run_id: str = ""

    async def _send_agui(  # ruff: ignore[no-self-use]
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
        context.agui_run = AgUiRunState()
        await context.agui_run.snapshot(context)

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
        run = context.agui_run
        if run is not None and run.open:
            run.open = False
            await run.update(context, (("run", "status"), "completed"))
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
        """Handle run error event - mark the run failed and emit AG-UI RunError."""
        error_msg = event.content or "Agent run failed"
        if context.agui_run is not None:
            await context.agui_run.fail(context, "failed", error_msg, event.error_type, **self._authored(event))
            return
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
        if context.agui_run is not None and event.subagent_run_id:
            await context.agui_run.update(
                context,
                (
                    ("subagents", event.subagent_run_id),
                    {"name": event.name, "status": "running", "startedAt": AgUiRunState.now_ms(), "error": None},
                ),
            )

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
        run = context.agui_run
        if run is not None and event.subagent_run_id is not None and event.subagent_run_id in run.state["subagents"]:
            await run.update(context, (("subagents", event.subagent_run_id, "status"), "completed"))

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
        run = context.agui_run
        if run is not None and event.subagent_run_id is not None and event.subagent_run_id in run.state["subagents"]:
            await run.update(
                context,
                (("subagents", event.subagent_run_id, "status"), "failed"),
                (("subagents", event.subagent_run_id, "error"), event.message),
            )

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

        if context.agui_run is not None:
            await context.agui_run.update(
                context,
                (
                    ("tools", tool_call_id),
                    {
                        "name": tool.tool_name,
                        "status": "running",
                        "subagentRunId": event.subagent_run_id,
                        "startedAt": AgUiRunState.now_ms(),
                        "error": None,
                    },
                ),
            )

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

        run = context.agui_run
        if run is not None and tool_call_id in run.state["tools"]:
            await run.update(context, (("tools", tool_call_id, "status"), "completed"))

    async def _handle_tool_call_error(
        self,
        context: ModuleContext,
        event: ToolCallErrorEvent,
    ) -> None:
        """Handle tool call error event - emit AG-UI ToolCallEnd + an error ToolCallResult, mark it failed.

        ``TOOL_CALL_RESULT`` has no error flag, so the failure is carried by its
        ``{"error": ...}`` content and by ``tools/<id>/status = failed`` in state.
        """
        tool = event.tool
        if not tool:
            return

        tool_call_id = tool.tool_call_id or str(uuid.uuid4())
        error_msg = event.error_message or "Tool call failed"
        output = AgUiToolCallEndOutput(event=AgUiToolCallEndEvent(tool_call_id=tool_call_id, **self._authored(event)))
        await self._send_agui(context, output)
        result_output = AgUiToolCallResultOutput(
            event=AgUiToolCallResultEvent(
                message_id=str(uuid.uuid4()),
                tool_call_id=tool_call_id,
                content=json.dumps({"error": error_msg}),
                role="tool",
                **self._authored(event),
            )
        )
        await self._send_agui(context, result_output)

        run = context.agui_run
        if run is not None and tool_call_id in run.state["tools"]:
            await run.update(
                context,
                (("tools", tool_call_id, "status"), "failed"),
                (("tools", tool_call_id, "error"), error_msg),
            )

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
        value = event.value
        output = AgUiCustomEventOutput(
            event=AgUiCustomEvent(
                name=event.name,
                value=value.model_dump(mode="json", exclude_none=True) if isinstance(value, BaseModel) else value,
                **self._authored(event),
            )
        )
        await self._send_agui(context, output)
