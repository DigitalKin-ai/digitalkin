"""AgnoHitlRunner with an Agno ``Team`` host, and the sub-agent ``LoadManager`` surface.

A Team yields ``TeamRunOutput``: the runner must detect its pause and round-trip it through
storage. A member's ``load_manager`` call pauses its run; Agno lifts that pause onto the team run
as a requirement tagged with the member id, and the runner resolves it with the member's own loader.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, Mock

import pytest
from ag_ui.core.types import Tool as AgUiTool
from ag_ui.core.types import ToolMessage
from agno.agent import Agent
from agno.db.in_memory import InMemoryDb
from agno.models.base import Model
from agno.models.response import ModelResponse, ToolExecution
from agno.run.base import RunStatus
from agno.run.requirement import RunRequirement
from agno.run.team import RunCompletedEvent, RunPausedEvent, TeamRunOutput
from agno.team import Team
from agno.tools import Toolkit

from digitalkin.community.agno import AgnoHitlRunner, AguiTools
from digitalkin.community.agno.toolkits import DefaultToolkits, LoadManager

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from agno.models.message import Message

PAUSED = "paused_runs"


class FakeStorage:
    """In-memory StorageStrategy surface used by PausedRunStore."""

    def __init__(self) -> None:
        self.db: dict[tuple[str, str], dict[str, Any]] = {}

    async def upsert(self, *, collection: str, record_id: str, data: dict[str, Any]) -> Any:
        self.db[collection, record_id] = data
        return SimpleNamespace(data=data)

    async def read(self, *, collection: str, record_id: str) -> Any:
        data = self.db.get((collection, record_id))
        return SimpleNamespace(data=data) if data is not None else None

    async def remove(self, *, collection: str, record_id: str) -> None:
        self.db.pop((collection, record_id), None)


def _external_tool(tool_call_id: str = "tc1") -> ToolExecution:
    return ToolExecution(
        tool_call_id=tool_call_id, tool_name="frontend_tool", tool_args={}, external_execution_required=True
    )


class FakeTeam(Team):
    """A Team whose leader pauses on a frontend tool, then completes on continue.

    ``Team.__init__`` is skipped: only the attributes the runner reads are set. ``tools`` is a
    make_tools_factory like a real leader's; ``tools_at_continue`` records what the continue saw.
    """

    def __init__(self, base_tools: list[Any] | None = None) -> None:  # pylint: disable=super-init-not-called
        self.tools = AguiTools.make_tools_factory(base_tools or [], dependency_key="agui_tools")
        self.members = []
        self.tools_at_continue: Any = None

    def arun(self, *_args: Any, **_kwargs: Any) -> Any:  # type: ignore[override]
        async def gen() -> AsyncIterator[Any]:
            yield RunPausedEvent(run_id="run1", session_id="sess1", tools=[_external_tool()])
            output = TeamRunOutput(run_id="run1", session_id="sess1", content="", tools=[_external_tool()])
            output.status = RunStatus.paused
            yield output

        return gen()

    def acontinue_run(self, *_args: Any, **_kwargs: Any) -> Any:  # type: ignore[override]
        self.tools_at_continue = self.tools

        async def gen() -> AsyncIterator[Any]:
            yield RunCompletedEvent(run_id="run1", session_id="sess1", content="done")
            output = TeamRunOutput(run_id="run1", session_id="sess1", content="done")
            output.status = RunStatus.completed
            yield output

        return gen()


def _resume_input() -> SimpleNamespace:
    return SimpleNamespace(
        thread_id="thread-1",
        run_id="run2",
        messages=[ToolMessage(id="m1", role="tool", tool_call_id="tc1", content="42")],
        tools=[],
    )


class TestTeamRoundTrip:
    async def test_team_leader_pause_persists_then_resume_clears(self) -> None:
        storage = FakeStorage()
        runner = AgnoHitlRunner(agent=FakeTeam(), storage=storage)  # type: ignore[arg-type]

        pause = await runner.run("hello", send=AsyncMock(), thread_id="thread-1", agui_tools=[])

        assert pause is not None
        assert pause.pending_tool_call_ids == ["tc1"]
        assert (PAUSED, "thread-1") in storage.db

        resumed, repause = await runner.try_resume(_resume_input(), send=AsyncMock())  # type: ignore[arg-type]

        assert (resumed, repause) == (True, None)
        assert (PAUSED, "thread-1") not in storage.db

    async def test_resume_hands_the_team_a_resolved_tool_list_then_restores_the_factory(self) -> None:
        storage = FakeStorage()
        storage.db[PAUSED, "thread-1"] = {
            "thread_id": "thread-1",
            "run_id": "run1",
            "pending_tool_call_ids": ["tc1"],
            "payload": TeamRunOutput(run_id="run1", session_id="sess1", content="", tools=[_external_tool()]).to_dict(),
        }
        base_tool = object()
        team = FakeTeam([base_tool])
        factory = team.tools
        runner = AgnoHitlRunner(agent=team, storage=storage)  # type: ignore[arg-type]

        await runner.continue_paused_run(
            thread_id="thread-1",
            tool_results={"tc1": "42"},
            send=AsyncMock(),
            run_id="run2",
            agui_tools=[AgUiTool(name="ask_form", description="Ask the user", parameters={})],
        )

        # A Team's acontinue_run never resolves a tools factory: the runner hands it a list.
        assert isinstance(team.tools_at_continue, list)
        assert base_tool in team.tools_at_continue
        assert [tool.name for tool in team.tools_at_continue if tool is not base_tool] == ["ask_form"]
        assert team.tools is factory


class ScriptedModel(Model):
    """A model whose every reply is computed from the messages and tool names it is given."""

    def __init__(self, script: Callable[[list[Message], list[str]], ModelResponse]) -> None:
        super().__init__(id="scripted", name="scripted", provider="test")
        self._script = script

    def _reply(self, messages: list[Message], tools: list[dict[str, Any]] | None) -> ModelResponse:
        return self._script(messages, [tool["function"]["name"] for tool in tools or [] if "function" in tool])

    def invoke(self, *_args: Any, **kwargs: Any) -> ModelResponse:
        return self._reply(kwargs["messages"], kwargs.get("tools"))

    async def ainvoke(self, *_args: Any, **kwargs: Any) -> ModelResponse:
        return self._reply(kwargs["messages"], kwargs.get("tools"))

    def invoke_stream(self, *_args: Any, **kwargs: Any) -> Any:
        yield self._reply(kwargs["messages"], kwargs.get("tools"))

    async def ainvoke_stream(self, *_args: Any, **kwargs: Any) -> AsyncIterator[ModelResponse]:  # type: ignore[override]
        yield self._reply(kwargs["messages"], kwargs.get("tools"))

    def _parse_provider_response(self, response: Any, **_kwargs: Any) -> ModelResponse:
        return response

    def _parse_provider_response_delta(self, response: Any) -> ModelResponse:
        return response


def _call(name: str, arguments: dict[str, Any], call_id: str) -> ModelResponse:
    return ModelResponse(
        role="assistant",
        tool_calls=[
            {"id": call_id, "type": "function", "function": {"name": name, "arguments": json.dumps(arguments)}}
        ],
    )


def _answered(messages: list[Message], tool_name: str) -> bool:
    return any(message.role == "tool" and message.tool_name == tool_name for message in messages)


def _member_script(messages: list[Message], tools: list[str]) -> ModelResponse:
    """Load the weather tool, call it once it is in the tool list, then answer."""
    if not _answered(messages, "load_manager"):
        return _call("load_manager", {"action": {"action": "tool", "setup_id": "setup-weather"}}, "call-load")
    if "get_weather" in tools and not _answered(messages, "get_weather"):
        return _call("get_weather", {}, "call-weather")
    return ModelResponse(role="assistant", content="sunny" if _answered(messages, "get_weather") else "no tool")


class WeatherTools(Toolkit):
    """Stands in for the ModuleToolkit a real load resolves."""

    def __init__(self, calls: list[str]) -> None:
        super().__init__(name="weather", tools=[self.get_weather])
        self._calls = calls

    def get_weather(self) -> str:
        """Return the weather.

        Returns:
            The forecast.
        """
        self._calls.append("get_weather")
        return "sunny"


@pytest.fixture
def loads(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Fake the registry side of a load: append WeatherTools to the loader's bound list."""
    record: dict[str, Any] = {"loaders": [], "weather_calls": []}

    async def fake_run_paused(self: LoadManager, _tool_args: dict[str, Any]) -> str:
        record["loaders"].append(self)
        assert self._base_tools is not None
        self._base_tools.append(WeatherTools(record["weather_calls"]))
        return json.dumps({"output": {"status": "loaded", "loaded_functions": ["get_weather"]}})

    monkeypatch.setattr(LoadManager, "run_paused", fake_run_paused)
    return record


def _member(name: str) -> Agent:
    """A sub-agent wired the way a module builds one: defaults without chat history, then bound."""
    context = SimpleNamespace(user_profile=None, setup=None)
    tools = DefaultToolkits.build(context, chat_history=False)  # type: ignore[arg-type]
    agent = Agent(name=name, model=ScriptedModel(_member_script), tools=tools)
    DefaultToolkits.bind_host(tools, agent)
    return agent


class TestTeamMemberLoad:
    async def test_member_loads_and_calls_a_tool_in_one_turn(self, loads: dict[str, Any]) -> None:
        member = _member("analyst")

        def leader_script(messages: list[Message], _tools: list[str]) -> ModelResponse:
            if not _answered(messages, "delegate_task_to_member"):
                return _call("delegate_task_to_member", {"member_id": "analyst", "task": "weather?"}, "call-delegate")
            return ModelResponse(role="assistant", content="leader done")

        team = Team(name="team", model=ScriptedModel(leader_script), members=[member], db=InMemoryDb())
        store = AsyncMock()
        runner = AgnoHitlRunner(agent=team, store=store)

        pause = await runner.run("weather?", send=AsyncMock(), thread_id="thread-1")

        assert pause is None
        store.save.assert_not_called()
        # The member's own loader ran the load, into the member's own tool list.
        assert loads["loaders"] == [LoadManager.find(member.tools)]
        assert any(isinstance(tool, WeatherTools) for tool in member.tools)  # type: ignore[union-attr]
        assert loads["weather_calls"] == ["get_weather"]


def _requirement(tool_name: str, *, member: str | None = "analyst") -> RunRequirement:
    requirement = RunRequirement(
        tool_execution=ToolExecution(
            tool_call_id=f"call-{tool_name}", tool_name=tool_name, tool_args={}, external_execution_required=True
        )
    )
    requirement.member_agent_id = member
    return requirement


class TestPendingAfterLoads:
    """A load is auto-continued only when nothing else in the pause needs the front."""

    def test_unresolved_member_requirement_keeps_the_pause(self) -> None:
        assert AgnoHitlRunner._pending_external(Mock(tools=[], requirements=[_requirement("show_chart")]))

    def test_resolved_member_requirement_lets_the_run_continue(self) -> None:
        requirement = _requirement("load_manager")
        requirement.set_external_execution_result("loaded")
        assert not AgnoHitlRunner._pending_external(Mock(tools=[], requirements=[requirement]))

    def test_host_requirements_are_judged_by_their_tools(self) -> None:
        # A host (non-member) requirement is covered by run_output.tools, already resolved here.
        assert not AgnoHitlRunner._pending_external(Mock(tools=[], requirements=[_requirement("x", member=None)]))


class TestLoadManagerSubAgentSurface:
    async def test_only_load_manager_requirements_are_resolved(self, loads: dict[str, Any]) -> None:
        loader = LoadManager(context=None)
        loader.bind_tools([])
        other = _requirement("show_chart")

        assert not await loader.resolve_requirement(other)
        assert other.needs_external_execution
        assert loads["loaders"] == []

    async def test_a_pending_load_requirement_is_run_and_resolved(self, loads: dict[str, Any]) -> None:
        loader = LoadManager(context=None)
        loader.bind_tools([])
        requirement = _requirement("load_manager")

        assert await loader.resolve_requirement(requirement)
        assert requirement.is_resolved()
        assert loads["loaders"] == [loader]
        # Already resolved: a second pass leaves it alone.
        assert not await loader.resolve_requirement(requirement)

    def test_find_through_a_list_and_a_tools_factory(self) -> None:
        loader = LoadManager(context=None)
        tools = [object(), loader]
        assert LoadManager.find(tools) is loader
        assert LoadManager.find(AguiTools.make_tools_factory(tools)) is loader
        assert LoadManager.find([object()]) is None
        assert LoadManager.find(None) is None

    def test_bind_to_targets_the_agents_own_list(self) -> None:
        loader = LoadManager(context=None)
        tools: list[Any] = [loader]
        loader.bind_tools(tools)
        agent = Agent(tools=tools)

        loader.bind_to(agent)

        assert loader._base_tools is agent.tools

    def test_bind_to_keeps_a_factory_binding(self) -> None:
        loader = LoadManager(context=None)
        tools: list[Any] = [loader]
        loader.bind_tools(tools)

        loader.bind_to(Agent(tools=AguiTools.make_tools_factory(tools), cache_callables=False))

        assert loader._base_tools is tools
