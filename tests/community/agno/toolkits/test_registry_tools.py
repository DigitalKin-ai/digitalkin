"""Tests for RegistryTools — setup search safety filter, kind mapping, trimming."""

import json

from digitalkin.community.agno.toolkits import RegistryTools
from digitalkin.models.services.registry import (
    RegistryModuleType,
    RegistrySetupStatus,
    SetupInfo,
)
from digitalkin.services.registry import DefaultRegistry
from digitalkin.services.registry.exceptions import RegistryServiceError


def _registry() -> DefaultRegistry:
    registry = DefaultRegistry("", "", "")
    registry.add_setup(
        SetupInfo(
            setup_id="setups:duda",
            name="Duda Builder",
            documentation="Builds websites on the Duda platform. " + "x" * 400,
            status=RegistrySetupStatus.READY,
            module_id="modules:duda",
            module_name="tool-duda",
            module_type=RegistryModuleType.TOOL_MODULE,
            setup_version="1.0.0",
            config={"secret": "MUST-NOT-LEAK"},
        )
    )
    registry.add_setup(
        SetupInfo(
            setup_id="setups:isaac",
            name="Isaac",
            documentation="Multi-agent orchestration kin",
            status=RegistrySetupStatus.CONFIGURATION_SUCCEEDED,
            module_id="modules:isaac",
            module_name="archetype-isaac",
            module_type=RegistryModuleType.ARCHETYPE,
            setup_version="2.0.0",
        )
    )
    registry.add_setup(
        SetupInfo(
            setup_id="setups:draft",
            name="Draft Tool",
            status=RegistrySetupStatus.DRAFT,
            module_type=RegistryModuleType.TOOL_MODULE,
        )
    )
    return registry


async def test_search_setups_returns_only_invocable() -> None:
    tools = RegistryTools(_registry())
    result = json.loads(await tools.search_setups())
    assert {s["setup_id"] for s in result["output"]["setups"]} == {"setups:duda", "setups:isaac"}


async def test_search_setups_kind_filter() -> None:
    tools = RegistryTools(_registry())
    tool_result = json.loads(await tools.search_setups(kind="tool"))
    assert [s["setup_id"] for s in tool_result["output"]["setups"]] == ["setups:duda"]
    kin_result = json.loads(await tools.search_setups(kind="kin"))
    assert [s["setup_id"] for s in kin_result["output"]["setups"]] == ["setups:isaac"]


async def test_search_setups_invalid_kind_errors() -> None:
    tools = RegistryTools(_registry())
    assert "error" in json.loads(await tools.search_setups(kind="bogus"))


async def test_search_setups_never_emits_config() -> None:
    tools = RegistryTools(_registry())
    raw = await tools.search_setups()
    assert "config" not in raw
    assert "MUST-NOT-LEAK" not in raw


async def test_search_setups_truncates_description() -> None:
    tools = RegistryTools(_registry())
    result = json.loads(await tools.search_setups(query="duda"))
    assert len(result["output"]["setups"][0]["description"]) == 300


async def test_search_modules_kind_and_limit() -> None:
    registry = _registry()
    await registry.register("modules:duda", "localhost", 50051, "1.0.0", RegistryModuleType.TOOL_MODULE)
    await registry.register("modules:isaac", "localhost", 50052, "2.0.0", RegistryModuleType.ARCHETYPE)
    tools = RegistryTools(registry)

    result = json.loads(await tools.search_modules(kind="tool"))
    assert [m["module_id"] for m in result["output"]["modules"]] == ["modules:duda"]
    assert result["output"]["modules"][0]["kind"] == "tool_module"
    # network location must never be surfaced to the LLM
    assert "localhost" not in json.dumps(result)

    limited = json.loads(await tools.search_modules(limit=1))
    assert limited["output"]["total_returned"] == 1


class _RaisingRegistry(DefaultRegistry):
    """Registry whose searches always fail — exercises toolkit graceful degradation."""

    async def search_setups(self, *args: object, **kwargs: object) -> list:
        msg = "boom"
        raise RegistryServiceError(msg)

    async def search(self, *args: object, **kwargs: object) -> list:
        msg = "boom"
        raise RegistryServiceError(msg)


async def test_search_setups_degrades_without_raising() -> None:
    result = json.loads(await RegistryTools(_RaisingRegistry("", "", "")).search_setups())
    assert result["error"]


async def test_search_modules_degrades_without_raising() -> None:
    result = json.loads(await RegistryTools(_RaisingRegistry("", "", "")).search_modules())
    assert result["error"]


async def test_search_setups_truncated_flag() -> None:
    tools = RegistryTools(_registry())
    capped = json.loads(await tools.search_setups(limit=1))
    assert capped["output"]["truncated"] is True
    assert capped["output"]["total_returned"] == 1
    full = json.loads(await tools.search_setups(limit=10))
    assert full["output"]["truncated"] is False
