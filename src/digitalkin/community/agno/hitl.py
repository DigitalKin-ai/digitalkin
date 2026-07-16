"""HITL runner for Agno agents with AG-UI frontend tools.

Pauses on external tool calls, persists the run via storage, and
resumes via :meth:`Agent.acontinue_run` once the front replies.
See ``docs/community/agno.md`` for the full flow.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field

from digitalkin.community.agno.models import PauseInfo

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

    from ag_ui.core.types import Message as AgUiMessage
    from ag_ui.core.types import Tool as AgUiTool
    from agno.agent import Agent
    from agno.run.agent import RunOutput

    from digitalkin.community.agno.toolkits.tool_loader import ToolLoaderTools
    from digitalkin.models.events import BaseAgentRunEvent
    from digitalkin.models.module import ModuleContext
    from digitalkin.services.storage import StorageStrategy

logger = logging.getLogger(__name__)

_PAUSED_RUNS_COLLECTION = "paused_runs"
_AWAITING_STATUS = "awaiting_tool_result"


class PausedRunRecord(BaseModel):
    """Snapshot of an Agno run paused on external tool execution."""

    model_config = ConfigDict(extra="allow")

    thread_id: str
    run_id: str
    pending_tool_call_ids: list[str] = Field(default_factory=list)
    payload: dict[str, Any] = Field(default_factory=dict, description="RunOutput.to_dict()")


HITL_STORAGE_CONFIG: dict[str, type[BaseModel]] = {_PAUSED_RUNS_COLLECTION: PausedRunRecord}
"""Storage config fragment for the ``paused_runs`` collection."""


class PausedRunStore:
    """Storage wrapper for the ``paused_runs`` collection."""

    COLLECTION: ClassVar[str] = _PAUSED_RUNS_COLLECTION

    def __init__(self, storage: StorageStrategy) -> None:
        """Initialize the store.

        Args:
            storage: The module's storage strategy. The ``paused_runs``
                collection must be registered with :class:`PausedRunRecord`
                (see :data:`HITL_STORAGE_CONFIG`).
        """
        self._storage = storage

    async def save(self, run_output: RunOutput, thread_id: str) -> PauseInfo:
        """Serialize and store a paused ``RunOutput``.

        Args:
            run_output: The paused Agno run (``is_paused=True``).
            thread_id: AG-UI thread identifier (the record key).

        Returns:
            A :class:`PauseInfo` describing what was persisted.
        """
        # Use run_output.tools (not requirements): Agno only emits a
        # RunRequirement for the last tool in each paused batch.
        seen: set[str] = set()
        pending: list[str] = []
        for tool in run_output.tools or []:
            tid = tool.tool_call_id
            # Skip tools already resolved in-process (e.g. a use_setup call handled by the
            # runner): only genuinely unresolved external tools go to the front.
            if tid and tid not in seen and tool.external_execution_required and tool.result is None:
                seen.add(tid)
                pending.append(tid)
        record = PausedRunRecord(
            thread_id=thread_id,
            run_id=run_output.run_id or "",
            pending_tool_call_ids=pending,
            payload=run_output.to_dict(),
        )
        await self._storage.upsert(
            collection=self.COLLECTION,
            record_id=thread_id,
            data=record.model_dump(),
        )
        logger.info(
            "PausedRunStore: saved thread_id=%s run_id=%s pending=%s",
            thread_id,
            record.run_id,
            pending,
        )
        return PauseInfo(
            thread_id=thread_id,
            run_id=record.run_id,
            pending_tool_call_ids=pending,
        )

    async def load(self, thread_id: str) -> PausedRunRecord | None:
        """Fetch the paused run record for a thread, or ``None``.

        Args:
            thread_id: AG-UI thread identifier.

        Returns:
            The :class:`PausedRunRecord` if one exists, else ``None``.
        """
        record = await self._storage.read(collection=self.COLLECTION, record_id=thread_id)
        if record is None:
            return None
        return PausedRunRecord.model_validate(record.data)

    async def delete(self, thread_id: str) -> None:
        """Remove the paused run record for a thread."""
        await self._storage.remove(collection=self.COLLECTION, record_id=thread_id)


class HitlEvents:
    """AG-UI message conversion and event emission for the HITL flow."""

    @staticmethod
    def agno_messages_to_agui(agno_messages: list[Any]) -> list[AgUiMessage]:
        """Convert Agno messages into AG-UI messages.

        Drops system/developer/reasoning; reshapes assistant ``tool_calls``
        into AG-UI :class:`~ag_ui.core.types.ToolCall` objects.

        Args:
            agno_messages: Value of ``RunOutput.messages`` at pause time.

        Returns:
            AG-UI :class:`~ag_ui.core.types.Message` instances.
        """
        from ag_ui.core.types import (
            AssistantMessage as AgUiAssistantMessage,
        )
        from ag_ui.core.types import (
            FunctionCall as AgUiFunctionCall,
        )
        from ag_ui.core.types import (
            ToolCall as AgUiToolCall,
        )
        from ag_ui.core.types import (
            ToolMessage as AgUiToolMessage,
        )
        from ag_ui.core.types import (
            UserMessage as AgUiUserMessage,
        )

        result: list[AgUiMessage] = []
        for msg in agno_messages or []:
            role = getattr(msg, "role", None)
            msg_id = getattr(msg, "id", None) or ""
            content = getattr(msg, "content", None)
            if isinstance(content, list):
                content = " ".join(str(part) for part in content if part is not None)

            if role == "user":
                result.append(AgUiUserMessage(id=msg_id, role="user", content=content or ""))
            elif role == "assistant":
                raw_calls = getattr(msg, "tool_calls", None) or []
                agui_tool_calls: list[AgUiToolCall] = []
                for tc in raw_calls:
                    tc_id = tc.get("id") if isinstance(tc, dict) else getattr(tc, "id", None)
                    func = tc.get("function") if isinstance(tc, dict) else getattr(tc, "function", None)
                    if not tc_id or func is None:
                        continue
                    func_name = func.get("name") if isinstance(func, dict) else getattr(func, "name", None)
                    func_args = func.get("arguments") if isinstance(func, dict) else getattr(func, "arguments", None)
                    if not isinstance(func_args, str):
                        func_args = json.dumps(func_args) if func_args is not None else "{}"
                    agui_tool_calls.append(
                        AgUiToolCall(
                            id=tc_id,
                            type="function",
                            function=AgUiFunctionCall(name=func_name or "", arguments=func_args),
                        )
                    )
                result.append(
                    AgUiAssistantMessage(
                        id=msg_id,
                        role="assistant",
                        content=content if isinstance(content, str) else None,
                        tool_calls=agui_tool_calls or None,
                    )
                )
            elif role == "tool":
                tool_call_id = getattr(msg, "tool_call_id", None)
                if not tool_call_id:
                    continue
                result.append(
                    AgUiToolMessage(
                        id=msg_id,
                        role="tool",
                        tool_call_id=tool_call_id,
                        content=content if isinstance(content, str) else "",
                    )
                )
        return result

    @staticmethod
    async def emit_messages_snapshot(
        context: ModuleContext,
        messages: list[AgUiMessage],
    ) -> None:
        """Emit an AG-UI ``MessagesSnapshot`` event.

        Typically called just before :func:`emit_awaiting_tool_result` on a
        paused run so the front has an authoritative view of the conversation
        (including the assistant message carrying the frontend ``tool_calls``,
        which cannot be reconstructed from the streamed tool-call events alone).

        Args:
            context: Current module context.
            messages: List of AG-UI messages, typically produced by
                :func:`agno_messages_to_agui` from ``RunOutput.messages``.
        """
        if not messages:
            return

        from ag_ui.core.events import MessagesSnapshotEvent as AgUiMessagesSnapshotEvent

        from digitalkin.models.module.ag_ui import (
            AgUiMessagesSnapshotOutput,
            AgUiOutput,
        )

        output = AgUiOutput(
            root=AgUiMessagesSnapshotOutput(
                event=AgUiMessagesSnapshotEvent(messages=messages),
            )
        )
        await context.callbacks.send_message(output)
        logger.info("emit_messages_snapshot: sent %d message(s)", len(messages))

    @staticmethod
    async def emit_awaiting_tool_result(
        context: ModuleContext,
        *,
        thread_id: str,
        run_id: str,
        pending_tool_call_ids: list[str],
    ) -> None:
        """Emit an AG-UI ``RunFinished`` with ``status="awaiting_tool_result"``.

        This is the protocol signal telling the front "the run paused on a
        client-side tool; execute it and reply with a ``ToolMessage``". It
        goes out via ``context.callbacks.send_message`` (bypassing the
        standard :class:`~digitalkin.mixins.agui_mixin.AgUiMixin` event
        mapping, which has no notion of an "awaiting" status).

        Args:
            context: Current module context.
            thread_id: AG-UI thread identifier.
            run_id: Run identifier to echo back in the finished event.
            pending_tool_call_ids: The ``tool_call_id`` values the front must
                execute and resolve — echoed in ``result.pending_tool_call_ids``
                so the front can match them.
        """
        from ag_ui.core.events import RunFinishedEvent as AgUiRunFinishedEvent

        from digitalkin.models.module.ag_ui import (
            AgUiOutput,
            AgUiRunFinishedOutput,
        )

        output = AgUiOutput(
            root=AgUiRunFinishedOutput(
                event=AgUiRunFinishedEvent(
                    thread_id=thread_id,
                    run_id=run_id,
                    result={
                        "status": _AWAITING_STATUS,
                        "pending_tool_call_ids": pending_tool_call_ids,
                    },
                )
            )
        )
        await context.callbacks.send_message(output)
        logger.info(
            "emit_awaiting_tool_result: thread_id=%s pending=%s",
            thread_id,
            pending_tool_call_ids,
        )


class AgnoHitlRunner:
    """Runs an Agno agent and persists/resumes paused runs on external tools."""

    def __init__(
        self,
        *,
        agent: Agent,
        storage: StorageStrategy | None = None,
        store: PausedRunStore | None = None,
        dependency_key: str = "agui_tools",
        tool_loader: ToolLoaderTools | None = None,
    ) -> None:
        """Initialize the runner.

        Args:
            agent: Agno agent built with ``tools=make_tools_factory(...)``
                and ``cache_callables=False``.
            storage: If provided without ``store``, a :class:`PausedRunStore`
                is built automatically.
            store: Pre-built paused-run store; wins over ``storage``.
            dependency_key: Agno dependencies key carrying the AG-UI tool
                list (must match :func:`make_tools_factory`).
            tool_loader: The :class:`ToolLoaderTools` bound to the agent's tool list
                (``ToolLoaderTools.find(tools)``). When present, a ``use_setup`` pause is
                resolved and the run auto-continues instead of surfacing to the front.
                When omitted, the runner locates it in ``agent.tools`` itself — otherwise
                a ``use_setup`` pause would surface to the front as a frontend tool no
                client implements, wedging the thread.

        Raises:
            ValueError: If neither ``storage`` nor ``store`` is provided.
        """
        if store is None:
            if storage is None:
                msg = "AgnoHitlRunner requires either `storage` or `store`."
                raise ValueError(msg)
            store = PausedRunStore(storage)
        if tool_loader is None:
            # Lazy import: ToolLoaderTools requires the optional agno dependency at
            # import time, while this module must stay importable without it (same
            # convention as the rest of community.agno). vars(): test fakes may not
            # carry a tools attribute at all.
            from digitalkin.community.agno.toolkits.tool_loader import ToolLoaderTools

            tool_loader = ToolLoaderTools.find(vars(agent).get("tools"))
        self._agent = agent
        self._store = store
        self._dependency_key = dependency_key
        self._tool_loader = tool_loader

    async def run(
        self,
        message: str,
        *,
        send: Callable[[BaseAgentRunEvent], Coroutine[Any, Any, None]],
        thread_id: str,
        agui_tools: list[AgUiTool] | None = None,
        images: list[Any] | None = None,
    ) -> PauseInfo | None:
        """Stream a fresh Agno run.

        Args:
            message: User prompt.
            send: Async callback for each digitalkin event.
            thread_id: AG-UI thread identifier (storage key on pause).
            agui_tools: Frontend tools declared by the AG-UI client.
            images: Optional multimodal inputs.

        Returns:
            ``None`` on completion; a :class:`PauseInfo` if paused.
        """
        from agno.run.agent import RunOutput

        logger.info("AgnoHitlRunner.run: starting (thread_id=%s, msg_len=%d)", thread_id, len(message))

        stream = self._agent.arun(
            message,
            images=images,
            stream=True,
            stream_events=True,
            yield_run_output=True,
            dependencies={self._dependency_key: agui_tools or []},
        )
        return await self._drive(
            stream=stream, send=send, thread_id=thread_id, run_output_cls=RunOutput, agui_tools=agui_tools
        )

    async def continue_paused_run(
        self,
        thread_id: str,
        tool_results: dict[str, str],
        *,
        send: Callable[[BaseAgentRunEvent], Coroutine[Any, Any, None]],
        run_id: str | None = None,
        agui_tools: list[AgUiTool] | None = None,
    ) -> PauseInfo | None:
        """Resume a previously paused run with tool results.

        Args:
            thread_id: AG-UI thread identifier (storage key).
            tool_results: ``tool_call_id`` → serialized result. Every
                pending tool must be resolved.
            send: Digitalkin-event callback.
            run_id: AG-UI run id for this resume turn.
            agui_tools: Frontend tool definitions (re-send the original list).

        Returns:
            ``None`` on completion or missing record; a new
            :class:`PauseInfo` on re-pause.
        """
        from agno.run.agent import RunOutput

        record = await self._store.load(thread_id)
        if record is None:
            logger.warning("continue_paused_run: no paused record for thread_id=%s", thread_id)
            return None

        run_output = RunOutput.from_dict(record.payload)
        logger.info(
            "continue_paused_run: resuming thread_id=%s run_id=%s with %d result(s)",
            thread_id,
            record.run_id,
            len(tool_results),
        )

        # AG-UI requires RUN_STARTED first; Agno emits RunContinued on resume.
        from digitalkin.models.events import AgentRunEvent, RunStartedEvent

        await send(
            RunStartedEvent(
                event=AgentRunEvent.RUN_STARTED,
                run_id=run_id,
                thread_id=thread_id,
                timestamp=None,
                metadata=None,
            )
        )

        # Write results onto run_output.tools: after to_dict/from_dict the
        # requirements' ToolExecution instances differ from run_output.tools[].
        for tool in run_output.tools or []:
            tid = getattr(tool, "tool_call_id", None)
            if tid and tid in tool_results:
                tool.result = tool_results[tid]

        # Keep requirements in sync (unused by acontinue_run, useful for debug).
        for req in run_output.requirements or []:
            tool_exec = req.tool_execution
            if (
                tool_exec is not None
                and getattr(req, "needs_external_execution", False)
                and tool_exec.tool_call_id in tool_results
            ):
                req.set_external_execution_result(tool_results[tool_exec.tool_call_id])

        stream = self._agent.acontinue_run(
            run_response=run_output,
            stream=True,
            stream_events=True,
            yield_run_output=True,
            dependencies={self._dependency_key: agui_tools or []},
        )
        pause_info = await self._drive(
            stream=stream, send=send, thread_id=thread_id, run_output_cls=RunOutput, agui_tools=agui_tools
        )

        if pause_info is None:
            await self._store.delete(thread_id)
            logger.info("continue_paused_run: thread_id=%s completed, record cleared", thread_id)
        return pause_info

    async def try_resume(
        self,
        input_data: Any,
        *,
        send: Callable[[BaseAgentRunEvent], Coroutine[Any, Any, None]],
    ) -> tuple[bool, PauseInfo | None]:
        """Try to resume a paused run from an AG-UI input.

        Returns:
            ``(False, None)`` if no resume should happen, ``(True, None)``
            on completion, or ``(True, PauseInfo)`` on re-pause.
        """
        from ag_ui.core.types import ToolMessage, UserMessage

        thread_id = getattr(input_data, "thread_id", None)
        if not thread_id:
            return False, None

        record = await self._store.load(thread_id)
        if record is None:
            return False, None

        messages = getattr(input_data, "messages", None) or []
        pending = set(record.pending_tool_call_ids)

        tool_results: dict[str, str] = {}
        for msg in messages:
            if isinstance(msg, ToolMessage) and msg.tool_call_id in pending:
                tool_results[msg.tool_call_id] = msg.content

        if not tool_results:
            last = messages[-1] if messages else None
            if isinstance(last, UserMessage):
                logger.info(
                    "try_resume: dropping stale paused record for thread_id=%s (new user message)",
                    thread_id,
                )
                await self._store.delete(thread_id)
            return False, None

        # All tool calls must resolve in one shot; otherwise emit RUN_ERROR
        # and keep the record so the client can retry.
        missing = pending - set(tool_results.keys())
        if missing:
            from digitalkin.models.events import AgentRunEvent, RunErrorEvent, RunStartedEvent

            input_run_id = getattr(input_data, "run_id", None)
            logger.warning(
                "try_resume: partial tool results for thread_id=%s — resolved %d/%d, missing=%s",
                thread_id,
                len(tool_results),
                len(pending),
                sorted(missing),
            )
            await send(
                RunStartedEvent(
                    event=AgentRunEvent.RUN_STARTED,
                    run_id=input_run_id,
                    thread_id=thread_id,
                    timestamp=None,
                    metadata=None,
                )
            )
            await send(
                RunErrorEvent(
                    event=AgentRunEvent.RUN_ERROR,
                    error_type="partial_tool_results",
                    content=(
                        f"Partial tool results: {len(tool_results)}/{len(pending)} "
                        f"resolved, missing {sorted(missing)}. The agent is paused on "
                        f"{len(pending)} frontend tool call(s); all pending ToolMessages "
                        "must be provided in a single RunAgentInput for the run to "
                        "resume. The paused state has been preserved — retry once all "
                        "tool results are available."
                    ),
                    error_details=None,
                    timestamp=None,
                    metadata=None,
                )
            )
            return True, None

        logger.info(
            "try_resume: resuming thread_id=%s with %d tool result(s)",
            thread_id,
            len(tool_results),
        )
        pause_info = await self.continue_paused_run(
            thread_id=thread_id,
            tool_results=tool_results,
            send=send,
            run_id=getattr(input_data, "run_id", None),
            agui_tools=getattr(input_data, "tools", None),
        )
        return True, pause_info

    async def handle_agui_input(
        self,
        input_data: Any,
        *,
        send: Callable[[BaseAgentRunEvent], Coroutine[Any, Any, None]],
        context: ModuleContext | None = None,
        message: str | None = None,
        images: list[Any] | None = None,
    ) -> PauseInfo | None:
        """Dispatch an AG-UI ``RunAgentInput`` (resume / abandon / fresh).

        When a run pauses and ``context`` is provided, the awaiting
        ``RunFinished`` event is emitted automatically.

        Args:
            input_data: Object exposing ``thread_id``, ``messages``, ``tools``.
            send: Digitalkin-event callback.
            context: If provided, emit the awaiting ``RunFinished`` on pause.
            message: Override the user prompt (default: last ``UserMessage``).
            images: Optional multimodal inputs.

        Returns:
            ``None`` on completion or no actionable input; a :class:`PauseInfo`
            on pause.
        """
        from ag_ui.core.types import UserMessage

        resumed, pause_info = await self.try_resume(input_data=input_data, send=send)
        if resumed:
            if pause_info is not None and context is not None:
                await HitlEvents.emit_awaiting_tool_result(
                    context,
                    thread_id=pause_info.thread_id,
                    run_id=pause_info.run_id,
                    pending_tool_call_ids=pause_info.pending_tool_call_ids,
                )
            return pause_info

        if message is None:
            messages = getattr(input_data, "messages", None) or []
            user_messages = [m for m in messages if isinstance(m, UserMessage)]
            if not user_messages:
                logger.warning("handle_agui_input: no user message in input, nothing to do")
                return None
            content = user_messages[-1].content
            if isinstance(content, list):
                content = " ".join(getattr(p, "text", "") for p in content if hasattr(p, "text"))
            message = content

        pause_info = await self.run(
            message=message,
            send=send,
            thread_id=getattr(input_data, "thread_id", ""),
            agui_tools=getattr(input_data, "tools", None),
            images=images,
        )
        if pause_info is not None and context is not None:
            await HitlEvents.emit_awaiting_tool_result(
                context,
                thread_id=pause_info.thread_id,
                run_id=pause_info.run_id,
                pending_tool_call_ids=pause_info.pending_tool_call_ids,
            )
        return pause_info

    async def _drive(
        self,
        *,
        stream: Any,
        send: Callable[[BaseAgentRunEvent], Coroutine[Any, Any, None]],
        thread_id: str,
        run_output_cls: type[RunOutput],
        agui_tools: list[AgUiTool] | None = None,
    ) -> PauseInfo | None:
        """Drain an Agno stream, forward events, and persist or auto-continue on pause.

        A ``use_setup`` pause (dynamic tool load) is resolved in-process and the run
        auto-continues with the enlarged tool list; a frontend-tool pause is persisted and
        surfaced. The loop is bounded so a model that keeps calling ``use_setup`` cannot spin
        forever.

        Args:
            stream: The Agno event stream to drain.
            send: Digitalkin-event callback for each forwarded event.
            thread_id: AG-UI thread identifier (storage key on a frontend pause).
            run_output_cls: The ``RunOutput`` class used to spot the terminal run object.
            agui_tools: Frontend tools to re-pass to Agno on an auto-continue.

        Returns:
            :class:`PauseInfo` on a frontend pause, ``None`` on completion.
        """
        from digitalkin.community.agno.agno_adapter import AgnoStreamAdapter

        for _ in range(20):
            adapter = AgnoStreamAdapter()
            final_run_output: RunOutput | None = None

            async for raw_event in stream:
                if isinstance(raw_event, run_output_cls):
                    final_run_output = raw_event
                    continue
                for event in adapter.to_digitalkin_events(raw_event):
                    await send(event)

            for event in adapter.flush():
                await send(event)

            if not (adapter.is_paused and final_run_output is not None and final_run_output.is_paused):
                return None

            # Resolve any use_setup calls in-process; if the pause has nothing left for the
            # front, auto-continue so discover -> load -> use reads as a single turn.
            if await self._load_paused_tools(final_run_output) and not self._pending_external(final_run_output):
                stream = self._agent.acontinue_run(
                    run_response=final_run_output,
                    stream=True,
                    stream_events=True,
                    yield_run_output=True,
                    dependencies={self._dependency_key: agui_tools or []},
                )
                continue

            pause_info = await self._store.save(run_output=final_run_output, thread_id=thread_id)
            # Attach AG-UI-shaped messages so the front can materialise the tool_call.
            pause_info.new_messages = HitlEvents.agno_messages_to_agui(final_run_output.messages or [])
            return pause_info

        logger.warning("AgnoHitlRunner: auto-continue limit reached for thread_id=%s", thread_id)
        from digitalkin.models.events import AgentRunEvent, RunErrorEvent

        await send(
            RunErrorEvent(
                event=AgentRunEvent.RUN_ERROR,
                error_type="auto_continue_limit",
                content=(
                    "The run was stopped after too many consecutive in-process tool "
                    "loads (use_setup). Send a new message to continue."
                ),
                error_details=None,
                timestamp=None,
                metadata=None,
            )
        )
        return None

    async def _load_paused_tools(self, run_output: RunOutput) -> bool:
        """Resolve ``use_setup`` calls in a paused run, writing each tool result in place.

        Args:
            run_output: The paused Agno run.

        Returns:
            ``True`` if at least one ``use_setup`` call was handled, else ``False`` (no
            loader wired, or the pause carries only frontend tools).
        """
        if self._tool_loader is None:
            return False
        handled = False
        loader_tool = self._tool_loader.tool_name
        for tool in run_output.tools or []:
            if tool.external_execution_required and tool.result is None and tool.tool_name == loader_tool:
                setup_id = (tool.tool_args or {}).get("setup_id", "")
                tool.result = await self._tool_loader.load(setup_id)
                handled = True
        return handled

    @staticmethod
    def _pending_external(run_output: RunOutput) -> bool:
        """Report whether any external tool in the paused run still needs a result.

        Args:
            run_output: The paused Agno run (after :meth:`_load_paused_tools`).

        Returns:
            ``True`` if a frontend tool call remains unresolved (must go to the front).
        """
        return any(tool.external_execution_required and tool.result is None for tool in run_output.tools or [])
