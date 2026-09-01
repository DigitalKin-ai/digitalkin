"""Dynamic tool-loading tests that require the real agno dependency.

Covered here (not in the fake-agno toolkit tests): load_tool is a real external-execution
Function, ``LoadToolAction.execute`` builds/append a ModuleToolkit, and ``AgnoHitlRunner``
resolves a load_tool pause in-process and auto-continues.
"""

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest

pytest.importorskip("agno", reason="optional agno dependency not installed")

from agno.models.base import Model
from agno.models.response import ModelResponse, ToolExecution
from agno.run.requirement import RunRequirement
from agno.tools.function import Function

from digitalkin.community.agno.hitl import AgnoHitlRunner
from digitalkin.community.agno.models import PauseInfo
from digitalkin.community.agno.toolkits import LoadManager
from digitalkin.models.module.tool_cache import ToolCache
from digitalkin.models.services.registry import RegistryModuleType


def _tool_info(
    setup_id: str = "s1", module_id: str = "modules:duda", tools: list[Any] | None = None
) -> SimpleNamespace:
    """A resolved ToolModuleInfo stand-in for a TOOL_MODULE setup."""
    return SimpleNamespace(
        setup_id=setup_id,
        module_id=module_id,
        module_type=RegistryModuleType.TOOL_MODULE,
        tool_name="Duda",
        module_name="tool-duda",
        slug="duda",
        tools=[SimpleNamespace(name="run")] if tools is None else tools,
    )


class _FakeModuleToolkit:
    """Stand-in for ModuleToolkit — records the info it wraps, no agno introspection."""

    def __init__(self, context: Any, info: Any) -> None:
        self._context = context
        self.tool_module_info = info
        self.functions: dict[str, Any] = {}
        self.async_functions: dict[str, Any] = {f"{info.slug}__{tool.name}": None for tool in info.tools}


def _load_context(
    info: Any,
    *,
    module_type: RegistryModuleType = RegistryModuleType.TOOL_MODULE,
    module_id: str = "modules:duda",
    send_message: Any = None,
) -> SimpleNamespace:
    """A ModuleContext stub whose registry resolves the setup family, then resolve_tool → ``info``.

    The load path reads the family first via ``registry.get_setup``/``discover_by_id``,
    then resolves the tool. Pass ``module_type=`` a non-tool family to exercise the kind gate.
    """
    setup = SimpleNamespace(module_id=module_id)
    registry = SimpleNamespace(
        get_setup=AsyncMock(return_value=setup),
        discover_by_id=AsyncMock(return_value=SimpleNamespace(module_type=module_type)),
    )
    callbacks = SimpleNamespace() if send_message is None else SimpleNamespace(send_message=send_message)
    return SimpleNamespace(
        registry=registry,
        resolve_tool=AsyncMock(return_value=info),
        # A successful load persists its setup_id so it survives into the mission's next turn.
        persist_loaded_tool=AsyncMock(return_value=True),
        # The already-loaded path consults ``declared`` to skip persisting a setup-declared tool.
        tool_cache=ToolCache(),
        callbacks=callbacks,
    )


def _tool(name: str, args: dict[str, Any], *, result: str | None = None, tid: str = "tc1") -> ToolExecution:
    return ToolExecution(
        tool_call_id=tid,
        tool_name=name,
        tool_args=args,
        external_execution_required=True,
        result=result,
    )


def _requirement(tool: ToolExecution) -> RunRequirement:
    """The RunRequirement real Agno emits alongside a paused external tool."""
    return RunRequirement(tool_execution=tool)


async def _agen(*items: Any) -> Any:
    for item in items:
        yield item


def test_load_manager_is_registered_as_external_execution() -> None:
    loader = LoadManager()
    fn = loader.async_functions["load_manager"]
    assert fn.external_execution is True
    fn.process_entrypoint()
    assert "action" in (fn.parameters or {}).get("properties", {})


@pytest.mark.asyncio
async def test_load_appends_module_toolkit_and_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    import digitalkin.community.agno.module_toolkit as mt

    monkeypatch.setattr(mt, "ModuleToolkit", _FakeModuleToolkit)
    info = _tool_info()
    send_message = AsyncMock()
    context = _load_context(info, send_message=send_message)
    base_tools: list[Any] = []
    loader = LoadManager(context=context)  # type: ignore[arg-type]
    loader.bind_tools(base_tools)

    env = json.loads(await loader.run_paused({"action": {"action": "tool", "setup_id": "s1"}}))

    assert env["metadata"]["success"] is True  # canonical envelope, not a bare string
    assert env["output"]["status"] == "loaded"
    assert env["output"]["tool_name"] == "Duda"
    assert "duda__run" in env["output"]["loaded_functions"]  # names the now-callable function
    assert len(base_tools) == 1
    assert isinstance(base_tools[0], _FakeModuleToolkit)
    send_message.assert_awaited()  # a "tool_loaded" AG-UI event was emitted

    # Loading the same setup again does not duplicate the toolkit.
    again = json.loads(await loader.run_paused({"action": {"action": "tool", "setup_id": "s1"}}))
    assert again["metadata"]["success"] is True
    assert again["output"]["status"] == "already_loaded"
    assert len(base_tools) == 1


@pytest.mark.asyncio
async def test_load_refuses_a_different_setup_of_an_already_loaded_module(monkeypatch: pytest.MonkeyPatch) -> None:
    """A second setup of the same module can't rebind — refuse instead of confirming falsely."""
    import digitalkin.community.agno.module_toolkit as mt

    monkeypatch.setattr(mt, "ModuleToolkit", _FakeModuleToolkit)
    context = _load_context(None, module_id="modules:shared", send_message=AsyncMock())
    base_tools: list[Any] = []
    loader = LoadManager(context=context)  # type: ignore[arg-type]
    loader.bind_tools(base_tools)

    context.resolve_tool.return_value = _tool_info(setup_id="setups:a", module_id="modules:shared")
    await loader.run_paused({"action": {"action": "tool", "setup_id": "setups:a"}})
    # Different setup, same backing module.
    context.resolve_tool.return_value = _tool_info(setup_id="setups:b", module_id="modules:shared")
    env = json.loads(await loader.run_paused({"action": {"action": "tool", "setup_id": "setups:b"}}))

    assert env["metadata"]["success"] is False
    assert "already loaded via setup setups:a" in env["error"]
    assert len(base_tools) == 1  # the second setup was NOT appended (no duplicate tool names)


@pytest.mark.asyncio
async def test_load_rejects_setup_with_no_tools() -> None:
    """A resolvable setup whose schema yields zero tools is a failure, not a phantom load."""
    info = _tool_info(tools=[])
    context = _load_context(info)
    base_tools: list[Any] = []
    loader = LoadManager(context=context)  # type: ignore[arg-type]
    loader.bind_tools(base_tools)

    env = json.loads(await loader.run_paused({"action": {"action": "tool", "setup_id": "s1"}}))

    assert env["metadata"]["success"] is False
    assert "no callable tools" in env["error"]
    assert base_tools == []


@pytest.mark.asyncio
async def test_load_refuses_a_non_tool_family() -> None:
    """A Kin/Service setup is a different family, refused (with a distinct message)."""
    context = _load_context(None, module_type=RegistryModuleType.ARCHETYPE)
    base_tools: list[Any] = []
    loader = LoadManager(context=context)  # type: ignore[arg-type]
    loader.bind_tools(base_tools)

    env = json.loads(await loader.run_paused({"action": {"action": "tool", "setup_id": "k1"}}))

    assert env["metadata"]["success"] is False
    assert "not a tool" in env["error"]
    assert "archetype" in env["error"]
    assert base_tools == []
    context.resolve_tool.assert_not_awaited()  # the kind gate refuses before resolve_tool


def _runner(tool_loader: Any = None, store: Any = None) -> AgnoHitlRunner:
    return AgnoHitlRunner(agent=SimpleNamespace(), store=store or SimpleNamespace(), tool_loader=tool_loader)


class TestPausedToolHandling:
    """Unit coverage for the runner's pause-classification helpers."""

    @pytest.mark.asyncio
    async def test_load_paused_tools_resolves_load_tool(self) -> None:
        loader = SimpleNamespace(tool_name="load_manager", run_paused=AsyncMock(return_value="loaded"))
        runner = _runner(tool_loader=loader)
        tool = _tool("load_manager", {"action": {"action": "tool", "setup_id": "s1"}})
        run_output = SimpleNamespace(tools=[tool], requirements=[_requirement(tool)])

        assert await runner._load_paused_tools(run_output) is True
        assert run_output.tools[0].result == "loaded"
        loader.run_paused.assert_awaited_once_with({"action": {"action": "tool", "setup_id": "s1"}})

    @pytest.mark.asyncio
    async def test_load_paused_tools_ignores_frontend_tools(self) -> None:
        loader = SimpleNamespace(tool_name="load_manager", run_paused=AsyncMock())
        runner = _runner(tool_loader=loader)
        tool = _tool("frontend_tool", {})
        run_output = SimpleNamespace(tools=[tool], requirements=[_requirement(tool)])

        assert await runner._load_paused_tools(run_output) is False
        loader.run_paused.assert_not_called()

    @pytest.mark.asyncio
    async def test_load_paused_tools_without_loader(self) -> None:
        runner = _runner(tool_loader=None)
        tool = _tool("load_manager", {"action": {"action": "tool", "setup_id": "s1"}})
        run_output = SimpleNamespace(tools=[tool], requirements=[_requirement(tool)])
        assert await runner._load_paused_tools(run_output) is False

    @pytest.mark.asyncio
    async def test_load_paused_tools_resolves_the_runs_requirement(self) -> None:
        """Writing ``tool.result`` alone leaves the pause unresolved for Agno.

        ``acontinue_run`` gates on ``RunRequirement.is_resolved()``, and
        ``needs_external_execution`` stays True until ``external_execution_result`` is set —
        setting ``tool_execution.result`` does not clear it. A requirement left unresolved makes
        the continue re-pause immediately (no model call), so the loaded tool is never used.
        """
        loader = SimpleNamespace(tool_name="load_manager", run_paused=AsyncMock(return_value="loaded"))
        runner = _runner(tool_loader=loader)
        tool = _tool("load_manager", {"action": {"action": "tool", "setup_id": "s1"}})
        requirement = _requirement(tool)
        run_output = SimpleNamespace(tools=[tool], requirements=[requirement])

        assert await runner._load_paused_tools(run_output) is True

        assert requirement.is_resolved() is True
        assert requirement.external_execution_result == "loaded"

    @pytest.mark.asyncio
    async def test_load_paused_tools_leaves_frontend_requirements_unresolved(self) -> None:
        """Only the loader's own requirement is resolved; a frontend tool still needs the front."""
        loader = SimpleNamespace(tool_name="load_manager", run_paused=AsyncMock(return_value="loaded"))
        runner = _runner(tool_loader=loader)
        load_tool = _tool("load_manager", {"action": {"action": "tool", "setup_id": "s1"}}, tid="tc1")
        frontend_tool = _tool("frontend_tool", {}, tid="tc2")
        requirements = [_requirement(load_tool), _requirement(frontend_tool)]
        run_output = SimpleNamespace(tools=[load_tool, frontend_tool], requirements=requirements)

        assert await runner._load_paused_tools(run_output) is True

        assert requirements[0].is_resolved() is True
        assert requirements[1].is_resolved() is False

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
    """Minimal RunOutput: pause state, tools, requirements, messages, to_dict.

    ``requirements`` is not decoration: Agno gates ``acontinue_run`` on
    ``RunRequirement.is_resolved()``, not on ``tools[].result``, so a fake without them
    cannot show whether a resolved pause actually continues.
    """

    def __init__(self, tools: list[Any], is_paused: bool, requirements: list[Any] | None = None) -> None:
        self.tools = tools
        self.requirements = [_requirement(tool) for tool in tools] if requirements is None else requirements
        self.is_paused = is_paused
        self.messages: list[Any] = []

    def to_dict(self) -> dict[str, Any]:
        return {}


class TestDriveAutoContinue:
    """The _drive loop: load_tool pauses auto-continue, frontend pauses persist."""

    @pytest.mark.asyncio
    async def test_load_tool_pause_auto_continues(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import digitalkin.community.agno.agno_adapter as aa

        monkeypatch.setattr(aa, "AgnoStreamAdapter", _FakeAdapter)
        loader = SimpleNamespace(tool_name="load_manager", run_paused=AsyncMock(return_value="loaded"))
        completed = _FakeRunOutput(tools=[], is_paused=False)
        agent = SimpleNamespace(acontinue_run=lambda **_: _agen(completed))
        store = SimpleNamespace(save=AsyncMock())
        runner = AgnoHitlRunner(agent=agent, store=store, tool_loader=loader)
        paused = _FakeRunOutput(
            tools=[_tool("load_manager", {"action": {"action": "tool", "setup_id": "s1"}})], is_paused=True
        )

        result = await runner._drive(
            stream=_agen(paused), send=AsyncMock(), thread_id="t1", run_output_cls=_FakeRunOutput, agui_tools=[]
        )

        assert result is None  # ran to completion, no frontend round-trip
        assert paused.tools[0].result == "loaded"
        loader.run_paused.assert_awaited_once_with({"action": {"action": "tool", "setup_id": "s1"}})
        store.save.assert_not_called()

    @pytest.mark.asyncio
    async def test_load_tool_pause_invalidates_tools_cache_before_continue(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The freshly-loaded tool must be callable on continue.

        Agno caches the tools-factory output (``cache_callables`` defaults to True), so without an
        invalidation ``acontinue_run`` re-resolves the stale pre-load list and the appended toolkit
        is absent from the model's function map. The runner clears the tools cache before continuing.
        """
        import digitalkin.community.agno.agno_adapter as aa

        monkeypatch.setattr(aa, "AgnoStreamAdapter", _FakeAdapter)
        loader = SimpleNamespace(tool_name="load_manager", run_paused=AsyncMock(return_value="loaded"))
        completed = _FakeRunOutput(tools=[], is_paused=False)
        # A populated cache stands in for the stale pre-load tools resolution.
        agent = SimpleNamespace(acontinue_run=lambda **_: _agen(completed), _callable_tools_cache={"k": ["stale"]})
        runner = AgnoHitlRunner(agent=agent, store=SimpleNamespace(save=AsyncMock()), tool_loader=loader)
        paused = _FakeRunOutput(
            tools=[_tool("load_manager", {"action": {"action": "tool", "setup_id": "s1"}})], is_paused=True
        )

        await runner._drive(
            stream=_agen(paused), send=AsyncMock(), thread_id="t1", run_output_cls=_FakeRunOutput, agui_tools=[]
        )

        assert agent._callable_tools_cache == {}  # invalidated so the continue re-resolves the enlarged tool list

    @pytest.mark.asyncio
    async def test_frontend_pause_is_persisted(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import digitalkin.community.agno.agno_adapter as aa

        monkeypatch.setattr(aa, "AgnoStreamAdapter", _FakeAdapter)
        loader = SimpleNamespace(tool_name="load_manager", run_paused=AsyncMock())
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
        loader.run_paused.assert_not_called()
        store.save.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_auto_continue_limit_emits_run_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A model spinning load_tool forever ends with RUN_ERROR, not a silent stream death."""
        import digitalkin.community.agno.agno_adapter as aa
        from digitalkin.models.events import AgentRunEvent

        monkeypatch.setattr(aa, "AgnoStreamAdapter", _FakeAdapter)
        loader = SimpleNamespace(tool_name="load_manager", run_paused=AsyncMock(return_value="loaded"))
        counter = iter(range(1000))

        def _next_paused(**_: Any) -> Any:
            return _agen(
                _FakeRunOutput(
                    tools=[
                        _tool("load_manager", {"action": {"action": "tool", "setup_id": "s"}}, tid=f"tc{next(counter)}")
                    ],
                    is_paused=True,
                )
            )

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
    """The runner locates LoadManager in agent.tools when not passed explicitly."""

    def test_finds_loader_in_tools_list(self) -> None:
        loader = LoadManager()
        agent = SimpleNamespace(tools=[SimpleNamespace(), loader])
        runner = AgnoHitlRunner(agent=agent, store=SimpleNamespace())
        assert runner._tool_loader is loader

    def test_finds_loader_via_tools_factory(self) -> None:
        loader = LoadManager()
        agent = SimpleNamespace(tools=lambda _run_context=None: [loader])
        runner = AgnoHitlRunner(agent=agent, store=SimpleNamespace())
        assert runner._tool_loader is loader

    def test_no_tools_attribute_stays_none(self) -> None:
        runner = AgnoHitlRunner(agent=SimpleNamespace(), store=SimpleNamespace())
        assert runner._tool_loader is None

    def test_explicit_loader_wins(self) -> None:
        explicit = LoadManager()
        agent = SimpleNamespace(tools=[LoadManager()])
        runner = AgnoHitlRunner(agent=agent, store=SimpleNamespace(), tool_loader=explicit)
        assert runner._tool_loader is explicit


class _ScriptedModel(Model):
    """Emits a preset tool-call sequence and records the function list it was handed.

    The fakes above cannot catch a tools-resolution regression: only a real Agno run
    builds the model's function map, and that map is what decides whether a tool call
    resolves or comes back as "the requested tool does not exist".
    """

    def __init__(self, script: list[dict[str, Any]], seen: list[list[str]], **kwargs: Any) -> None:
        """Record the call script and the list each model call appends its tool names to."""
        kwargs.setdefault("id", "scripted")
        super().__init__(**kwargs)
        self._script = script
        self._seen = seen
        self._turn = 0

    def invoke(self, *args: Any, **kwargs: Any) -> Any:
        """Unused: only the async streaming path is exercised."""
        raise NotImplementedError

    async def ainvoke(self, *args: Any, **kwargs: Any) -> Any:
        """Unused: only the async streaming path is exercised."""
        raise NotImplementedError

    def invoke_stream(self, *args: Any, **kwargs: Any) -> Any:
        """Unused: only the async streaming path is exercised."""
        raise NotImplementedError

    def _parse_provider_response(self, response: Any, **kwargs: Any) -> Any:
        """Unused: only the async streaming path is exercised."""
        raise NotImplementedError

    async def ainvoke_stream(self, *args: Any, **kwargs: Any) -> Any:
        """Yield the next scripted step as a parsed delta.

        Yields:
            The scripted tool call, or a closing text response once the script is exhausted.
        """
        turn = self._turn
        self._turn = turn + 1
        yield self._parse_provider_response_delta({
            "step": self._script[turn] if turn < len(self._script) else None,
            "turn": turn,
        })

    def _parse_provider_response_delta(self, response: Any) -> ModelResponse:
        """Turn a scripted step into a tool-call delta, or finish the run."""
        step, turn = response["step"], response["turn"]
        if step is None:
            return ModelResponse(content="done")
        return ModelResponse(
            tool_calls=[
                {
                    "id": f"tc{turn}",
                    "type": "function",
                    "function": {"name": step["name"], "arguments": json.dumps(step["args"])},
                }
            ]
        )

    async def aresponse_stream(self, messages: Any, tools: Any = None, **kwargs: Any) -> Any:
        """Record the function map for this call, then delegate to the real implementation.

        Yields:
            Whatever the real ``aresponse_stream`` yields.
        """
        self._seen.append(sorted(tool.name for tool in (tools or []) if isinstance(tool, Function)))
        async for response in super().aresponse_stream(messages, tools=tools, **kwargs):
            yield response


class TestContinueKeepsResolvedTools:
    """A load auto-continue must not strip the tools the model already had.

    A Team's async ``acontinue_run`` never resolves a callable ``tools`` factory, so without the
    runner's own resolution *every* call fails, not just the freshly loaded one.
    """

    @staticmethod
    def _toolkits(info: Any) -> tuple[type, type]:
        """Build the always-present toolkit and the one a load appends."""
        from agno.tools import Toolkit

        class RagToolkit(Toolkit):
            def __init__(self, context: Any = None, module_info: Any = None) -> None:
                super().__init__(name="rag", tools=[self.rag__search_documents])
                self.tool_module_info = info if module_info is None else module_info

            async def rag__search_documents(self, query: str) -> str:
                """Search documents.

                Args:
                    query: The search query.

                Returns:
                    The results.
                """
                return "results"

        class ToolsManager(Toolkit):
            def __init__(self) -> None:
                super().__init__(name="tools_manager", tools=[self.tools_manager])

            async def tools_manager(self, action: str) -> str:
                """Administer tool setups.

                Args:
                    action: The action to run.

                Returns:
                    The result.
                """
                return "found"

        return RagToolkit, ToolsManager

    @pytest.mark.asyncio
    async def test_team_keeps_its_tools_across_a_load_auto_continue(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from agno.agent import Agent
        from agno.run.team import TeamRunOutput
        from agno.team import Team

        from digitalkin.community.agno import module_toolkit
        from digitalkin.community.agno.agui_tools import AguiTools

        info = _tool_info(setup_id="setups:rag", module_id="modules:rag")
        info.slug = "rag"
        rag_toolkit, tools_manager = self._toolkits(info)
        monkeypatch.setattr(module_toolkit, "ModuleToolkit", rag_toolkit)

        loader = LoadManager(context=_load_context(info, module_id="modules:rag"))
        base_tools: list[Any] = [tools_manager(), loader]
        loader.bind_tools(base_tools)

        seen: list[list[str]] = []
        model = _ScriptedModel(
            script=[
                {"name": "tools_manager", "args": {"action": "search"}},
                {"name": "load_manager", "args": {"action": "tool", "setup_id": "setups:rag"}},
                {"name": "rag__search_documents", "args": {"query": "hi"}},
            ],
            seen=seen,
        )
        factory = AguiTools.make_tools_factory(base_tools)
        team = Team(
            name="team",
            model=model,
            members=[Agent(name="member", model=_ScriptedModel(script=[], seen=[]), telemetry=False)],
            tools=factory,
            cache_callables=False,
            telemetry=False,
        )
        runner = AgnoHitlRunner(
            agent=team,
            store=SimpleNamespace(save=AsyncMock(), load=AsyncMock(return_value=None), delete=AsyncMock()),
            tool_loader=loader,
        )

        await runner._drive(
            stream=team.arun(
                "go", stream=True, stream_events=True, yield_run_output=True, dependencies={"agui_tools": []}
            ),
            send=AsyncMock(),
            thread_id="t1",
            run_output_cls=TeamRunOutput,
            agui_tools=[],
        )

        assert len(seen) == 2, f"expected a pre-load and a post-load model call, got {seen}"
        assert "tools_manager" in seen[0]
        # The continued leader must keep what it already had *and* see the freshly loaded tool.
        assert "tools_manager" in seen[1], f"pre-existing tools were dropped on continue: {seen[1]}"
        assert "rag__search_documents" in seen[1], f"loaded tool is not callable: {seen[1]}"
        # The factory must survive so the next turn still merges that turn's frontend tools.
        assert team.tools is factory
