"""Functions a tool setup declares in its content (``setup_tools``)."""

from typing import Any

import pytest
from agentic_mesh_protocol.registry.v1 import registry_messages_pb2
from google.protobuf.struct_pb2 import Struct
from tests.modules.test_tool_reference import FakeRegistry, create_mock_communication

from digitalkin.models.module import SETUP_TOOLS_KEY, SetupTool, ToolDefinition, ToolModuleInfo
from digitalkin.models.module.setup_tools import parameters_schema, read_setup_tools
from digitalkin.models.module.tool_reference import ToolReference, ToolSelection
from digitalkin.models.services.registry import ModuleInfo, RegistryModuleType, SetupInfo
from digitalkin.services.registry.grpc_registry import GrpcRegistry


def _tool(name: str, **extra: Any) -> dict[str, Any]:
    return {
        "name": name,
        "trigger": "call_tool",
        "input_schema": {"type": "object", "properties": {"q": {"type": "string"}}},
        **extra,
    }


class TestReadSetupTools:
    """Which stored entries become functions."""

    def test_reads_entries_in_order(self) -> None:
        """Valid entries come back as SetupTool, in their stored order."""
        config = {SETUP_TOOLS_KEY: [_tool("b", title="B"), _tool("a")]}

        tools = read_setup_tools(config, prefix="mcp", reserved={"call_tool"})

        assert [t.name for t in tools] == ["b", "a"]
        assert tools[0] == SetupTool(name="b", title="B", trigger="call_tool", input_schema=_tool("b")["input_schema"])

    @pytest.mark.parametrize("config", [None, {}, {SETUP_TOOLS_KEY: []}, {SETUP_TOOLS_KEY: "x"}])
    def test_nothing_declared(self, config: dict[str, Any] | None) -> None:
        """No content, no key, an empty or a non-list value: no setup tools."""
        assert read_setup_tools(config, prefix="mcp", reserved={"call_tool"}) == []

    def test_bad_entries_are_skipped_not_fatal(self) -> None:
        """One bad entry must not cost the setup its other tools."""
        config = {
            SETUP_TOOLS_KEY: [
                {"title": "no name", "trigger": "call_tool"},
                {"name": "no_trigger"},
                _tool("unknown_trigger", trigger="ghost"),
                "not an object",
                _tool("call_tool"),  # module trigger
                _tool("dup"),
                _tool("dup"),  # duplicate
                _tool("a.b"),  # dot: not a function name
                _tool("x" * 60),  # mcp__ + 60 > 64
                _tool("resolve-library-id"),  # dash: fine
            ]
        }

        tools = read_setup_tools(config, prefix="mcp", reserved={"call_tool"})

        assert [t.name for t in tools] == ["dup", "resolve-library-id"]


class TestParametersSchema:
    """The parameters the LLM sees."""

    def test_keeps_only_the_object_shape(self) -> None:
        """Draft markers and titles go; required names are kept only if declared."""
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Args",
            "type": "object",
            "properties": {"q": {"type": "string"}},
            "required": ["q", "ghost"],
        }

        assert parameters_schema(schema) == {
            "type": "object",
            "properties": {"q": {"type": "string"}},
            "required": ["q"],
        }

    def test_inlines_refs(self) -> None:
        """Pydantic-style schemas with $defs are inlined."""
        schema = {
            "$defs": {"Opts": {"type": "object", "properties": {"n": {"type": "integer"}}}},
            "type": "object",
            "properties": {"opts": {"$ref": "#/$defs/Opts"}},
        }

        assert parameters_schema(schema)["properties"]["opts"]["properties"] == {"n": {"type": "integer"}}

    def test_no_properties(self) -> None:
        """A tool without arguments gets an empty object schema."""
        assert parameters_schema({}) == {"type": "object", "properties": {}, "required": []}


class TestEnabledBy:
    """Trigger selection applied to module triggers and setup functions."""

    def test_module_trigger(self) -> None:
        """A module trigger serves itself."""
        tool = ToolDefinition(name="search", description="")
        assert tool.enabled_by({"search"})
        assert not tool.enabled_by({"other"})

    def test_setup_function_follows_its_trigger(self) -> None:
        """Not its own name: the trigger that serves it."""
        tool = ToolDefinition(name="ask", description="", trigger="call_tool")
        assert tool.enabled_by({"call_tool"})
        assert not tool.enabled_by({"list_tools"})


class TestSetupToolDefinitions:
    """ToolModuleInfo builds one function per setup tool."""

    def test_definition_fields(self) -> None:
        """Name, label, description, serving trigger and parameters are carried over."""
        config = {SETUP_TOOLS_KEY: [_tool("ask", title="Ask", description="Ask it")]}

        (definition,) = ToolModuleInfo.setup_tool_definitions(config, "My Wiki", {"call_tool"})

        assert definition.name == "ask"
        assert definition.display_name == "Ask"
        assert definition.description == "Ask it"
        assert definition.trigger == "call_tool"
        assert definition.parameters_schema["properties"] == {"q": {"type": "string"}}

    def test_prefix_is_the_setup_slug(self) -> None:
        """The length check uses the function name the toolkit will build: ``<slug>__<name>``."""
        name = "t" * 50
        config = {SETUP_TOOLS_KEY: [_tool(name)]}

        assert ToolModuleInfo.setup_tool_definitions(config, "abc", {"call_tool"})  # abc__ + 50 = 55
        assert not ToolModuleInfo.setup_tool_definitions(config, "a much longer setup name", {"call_tool"})


@pytest.fixture
def registry() -> FakeRegistry:
    """A registry with one search module and two setups of it, one declaring setup tools."""
    reg = FakeRegistry()
    reg.add_module(
        ModuleInfo(
            module_id="tool-search-001",
            module_type=RegistryModuleType.TOOL_MODULE,
            address="localhost",
            port=50051,
            version="1.0.0",
            module_name="SearchTool",
        )
    )
    reg.add_setup("plain", "tool-search-001", "Plain")
    reg._setups["wiki"] = SetupInfo(
        setup_id="wiki",
        name="Wiki",
        module_id="tool-search-001",
        config={
            "server_url": "https://wiki/mcp",
            SETUP_TOOLS_KEY: [_tool("ask", trigger="search"), _tool("lookup", trigger="search")],
        },
    )
    return reg


class TestResolve:
    """Setup tools reach the agent through ToolReference.resolve, per setup."""

    async def test_setup_tools_follow_module_triggers(self, registry: FakeRegistry) -> None:
        """Module triggers first, then the setup's own functions, kept since their trigger is enabled."""
        ref = ToolReference(selected_tools=[ToolSelection(setup_id="wiki", triggers={"search": True})])

        (info,) = await ref.resolve(registry, create_mock_communication())

        assert [t.name for t in info.tools] == ["search", "ask", "lookup"]

    async def test_other_setups_of_the_module_do_not_get_them(self, registry: FakeRegistry) -> None:
        """Two setups of one module: each sees only its own tools."""
        ref = ToolReference(
            selected_tools=[
                ToolSelection(setup_id="plain", triggers={"search": True}),
                ToolSelection(setup_id="wiki", triggers={"search": True}),
            ]
        )

        plain, wiki = await ref.resolve(registry, create_mock_communication())

        assert [t.name for t in plain.tools] == ["search"]
        assert [t.name for t in wiki.tools] == ["search", "ask", "lookup"]

    async def test_disabled_trigger_drops_its_setup_tools(self, registry: FakeRegistry) -> None:
        """Turning off the serving trigger turns off the setup's functions with it."""
        ref = ToolReference(
            selected_tools=[ToolSelection(setup_id="wiki", triggers={"search": False, "healthcheck_ping": True})]
        )

        assert await ref.resolve(registry, create_mock_communication()) == []


class TestRegistryConfig:
    """The registry hands setup content over as plain Python values."""

    def test_nested_values_are_plain(self) -> None:
        """Lists and objects inside the content are not left as protobuf containers."""
        config = Struct()
        config.update({"server_url": "https://wiki/mcp", SETUP_TOOLS_KEY: [_tool("ask")]})
        descriptor = registry_messages_pb2.SetupDescriptor(id="wiki", name="Wiki", config=config)

        info = GrpcRegistry._proto_to_setup_info(descriptor)

        assert info.config == {"server_url": "https://wiki/mcp", SETUP_TOOLS_KEY: [_tool("ask")]}
        assert read_setup_tools(info.config, prefix="wiki", reserved={"call_tool"})[0].name == "ask"
