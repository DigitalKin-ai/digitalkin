"""Dynamic tool-loading tests that require the real agno dependency.

Covered here (not in the fake-agno toolkit tests): use_setup is a real external-execution
Function, ``ToolLoaderTools.load`` builds/append a ModuleToolkit, and ``AgnoHitlRunner``
resolves a use_setup pause in-process and auto-continues.
"""

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("agno", reason="optional agno dependency not installed")

from agno.models.response import ToolExecution

from digitalkin.community.agno.hitl import AgnoHitlRunner
from digitalkin.community.agno.models import PauseInfo
from digitalkin.community.agno.toolkits import ToolLoaderTools


class _FakeModuleToolkit:
    """Stand-in for ModuleToolkit — records the info it wraps, no agno introspection."""

    def __init__(self, context: Any, info: Any) -> None:
        self._context = context
        self.tool_module_info = info


def _tool(name: str, args: dict[str, Any], *, result: str | None = None, tid: str = "tc1") -> ToolExecution:
    return ToolExecution(
        tool_call_id=tid,
        tool_name=name,
        tool_args=args,
        external_execution_required=True,
        result=result,
    )


async def _agen(*items: Any) -> Any:
    for item in items:
        yield item


def test_use_setup_is_registered_as_external_execution() -> None:
    loader = ToolLoaderTools()
    fn = loader.async_functions["use_setup"]
    assert fn.external_execution is True
    fn.process_entrypoint()
    assert "setup_id" in (fn.parameters or {}).get("properties", {})


@pytest.mark.asyncio
async def test_load_appends_module_toolkit_and_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    import digitalkin.community.agno.module_toolkit as mt

    monkeypatch.setattr(mt, "ModuleToolkit", _FakeModuleToolkit)
    info = SimpleNamespace(
        setup_id="s1", tool_name="Duda", module_name="tool-duda", slug="duda", tools=[SimpleNamespace(name="run")]
    )
    send_message = AsyncMock()
    context = SimpleNamespace(resolve_tool=AsyncMock(return_value=info), callbacks=SimpleNamespace(send_message=send_message))
    base_tools: list[Any] = []
    loader = ToolLoaderTools(context=context)  # type: ignore[arg-type]
    loader.bind_tools(base_tools)

    msg = await loader.load("s1")

    assert "loaded" in msg and "Duda" in msg
    assert len(base_tools) == 1
    assert isinstance(base_tools[0], _FakeModuleToolkit)
    send_message.assert_awaited()  # a "tool_loaded" AG-UI event was emitted

    # Loading the same setup again does not duplicate the toolkit.
    await loader.load("s1")
    assert len(base_tools) == 1


@pytest.mark.asyncio
async def test_load_rejects_setup_with_no_tools() -> None:
    """A resolvable setup whose schema yields zero tools is a failure, not a phantom load."""
    info = SimpleNamespace(setup_id="s1", tool_name="Duda", module_name="tool-duda", slug="duda", tools=[])
    context = SimpleNamespace(resolve_tool=AsyncMock(return_value=info), callbacks=SimpleNamespace())
    base_tools: list[Any] = []
    loader = ToolLoaderTools(context=context)  # type: ignore[arg-type]
    loader.bind_tools(base_tools)

    msg = await loader.load("s1")

    assert "no callable tools" in msg
    assert base_tools == []


def _runner(tool_loader: Any = None, store: Any = None) -> AgnoHitlRunner:
    return AgnoHitlRunner(agent=SimpleNamespace(), store=store or SimpleNamespace(), tool_loader=tool_loader)


class TestPausedToolHandling:
    """Unit coverage for the runner's pause-classification helpers."""

    @pytest.mark.asyncio
    async def test_load_paused_tools_resolves_use_setup(self) -> None:
        loader = SimpleNamespace(tool_name="use_setup", load=AsyncMock(return_value="loaded"))
        runner = _runner(tool_loader=loader)
        run_output = SimpleNamespace(tools=[_tool("use_setup", {"setup_id": "s1"})])

        assert await runner._load_paused_tools(run_output) is True
        assert run_output.tools[0].result == "loaded"
        loader.load.assert_awaited_once_with("s1")

    @pytest.mark.asyncio
    async def test_load_paused_tools_ignores_frontend_tools(self) -> None:
        loader = SimpleNamespace(tool_name="use_setup", load=AsyncMock())
        runner = _runner(tool_loader=loader)
        run_output = SimpleNamespace(tools=[_tool("frontend_tool", {})])

        assert await runner._load_paused_tools(run_output) is False
        loader.load.assert_not_called()

    @pytest.mark.asyncio
    async def test_load_paused_tools_without_loader(self) -> None:
        runner = _runner(tool_loader=None)
        run_output = SimpleNamespace(tools=[_tool("use_setup", {"setup_id": "s1"})])
        assert await runner._load_paused_tools(run_output) is False

    def test_pending_external_reflects_unresolved_tools(self) -> None:
        runner = _runner()
        assert runner._pending_external(SimpleNamespace(tools=[_tool("f", {}, result=None)])) is True
        assert runner._pending_external(SimpleNamespace(tools=[_tool("f", {}, result="done")])) is False


class _FakeAdapter:
    """Reports a pause and passes no events through (drives _drive deterministically)."""

    is_paused = True

    def to_digitalkin_events(self, _event: Any) -> list[Any]:
        return []

    def flush(self) -> list[Any]:
        return []


class _FakeRunOutput:
    """Minimal RunOutput: pause state, tools, messages, to_dict."""

    def __init__(self, tools: list[Any], is_paused: bool) -> None:
        self.tools = tools
        self.is_paused = is_paused
        self.messages: list[Any] = []

    def to_dict(self) -> dict[str, Any]:
        return {}


class TestDriveAutoContinue:
    """The _drive loop: use_setup pauses auto-continue, frontend pauses persist."""

    @pytest.mark.asyncio
    async def test_use_setup_pause_auto_continues(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import digitalkin.community.agno.agno_adapter as aa

        monkeypatch.setattr(aa, "AgnoStreamAdapter", _FakeAdapter)
        loader = SimpleNamespace(tool_name="use_setup", load=AsyncMock(return_value="loaded"))
        completed = _FakeRunOutput(tools=[], is_paused=False)
        agent = SimpleNamespace(acontinue_run=lambda **_: _agen(completed))
        store = SimpleNamespace(save=AsyncMock())
        runner = AgnoHitlRunner(agent=agent, store=store, tool_loader=loader)
        paused = _FakeRunOutput(tools=[_tool("use_setup", {"setup_id": "s1"})], is_paused=True)

        result = await runner._drive(
            stream=_agen(paused), send=AsyncMock(), thread_id="t1", run_output_cls=_FakeRunOutput, agui_tools=[]
        )

        assert result is None  # ran to completion, no frontend round-trip
        assert paused.tools[0].result == "loaded"
        loader.load.assert_awaited_once_with("s1")
        store.save.assert_not_called()

    @pytest.mark.asyncio
    async def test_frontend_pause_is_persisted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import digitalkin.community.agno.agno_adapter as aa

        monkeypatch.setattr(aa, "AgnoStreamAdapter", _FakeAdapter)
        loader = SimpleNamespace(tool_name="use_setup", load=AsyncMock())
        store = SimpleNamespace(
            save=AsyncMock(return_value=PauseInfo(thread_id="t1", run_id="r1", pending_tool_call_ids=["tc1"]))
        )
        runner = AgnoHitlRunner(agent=SimpleNamespace(), store=store, tool_loader=loader)
        paused = _FakeRunOutput(tools=[_tool("frontend_tool", {}, tid="tc1")], is_paused=True)

        result = await runner._drive(
            stream=_agen(paused), send=AsyncMock(), thread_id="t1", run_output_cls=_FakeRunOutput, agui_tools=[]
        )

        assert result is not None
        assert result.thread_id == "t1"
        loader.load.assert_not_called()
        store.save.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_auto_continue_limit_emits_run_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A model spinning use_setup forever ends with RUN_ERROR, not a silent stream death."""
        import digitalkin.community.agno.agno_adapter as aa

        from digitalkin.models.events import AgentRunEvent

        monkeypatch.setattr(aa, "AgnoStreamAdapter", _FakeAdapter)
        loader = SimpleNamespace(tool_name="use_setup", load=AsyncMock(return_value="loaded"))
        counter = iter(range(1000))

        def _next_paused(**_: Any) -> Any:
            return _agen(_FakeRunOutput(tools=[_tool("use_setup", {"setup_id": "s"}, tid=f"tc{next(counter)}")], is_paused=True))

        agent = SimpleNamespace(acontinue_run=_next_paused)
        store = SimpleNamespace(save=AsyncMock())
        runner = AgnoHitlRunner(agent=agent, store=store, tool_loader=loader)
        send = AsyncMock()

        result = await runner._drive(
            stream=_next_paused(), send=send, thread_id="t1", run_output_cls=_FakeRunOutput, agui_tools=[]
        )

        assert result is None
        store.save.assert_not_called()
        errors = [c.args[0] for c in send.await_args_list if c.args[0].event == AgentRunEvent.RUN_ERROR]
        assert len(errors) == 1
        assert errors[0].error_type == "auto_continue_limit"


class TestRunnerLoaderAutoFind:
    """The runner locates ToolLoaderTools in agent.tools when not passed explicitly."""

    def test_finds_loader_in_tools_list(self) -> None:
        loader = ToolLoaderTools()
        agent = SimpleNamespace(tools=[SimpleNamespace(), loader])
        runner = AgnoHitlRunner(agent=agent, store=SimpleNamespace())
        assert runner._tool_loader is loader

    def test_finds_loader_via_tools_factory(self) -> None:
        loader = ToolLoaderTools()
        agent = SimpleNamespace(tools=lambda _run_context=None: [loader])
        runner = AgnoHitlRunner(agent=agent, store=SimpleNamespace())
        assert runner._tool_loader is loader

    def test_no_tools_attribute_stays_none(self) -> None:
        runner = AgnoHitlRunner(agent=SimpleNamespace(), store=SimpleNamespace())
        assert runner._tool_loader is None

    def test_explicit_loader_wins(self) -> None:
        explicit = ToolLoaderTools()
        agent = SimpleNamespace(tools=[ToolLoaderTools()])
        runner = AgnoHitlRunner(agent=agent, store=SimpleNamespace(), tool_loader=explicit)
        assert runner._tool_loader is explicit
