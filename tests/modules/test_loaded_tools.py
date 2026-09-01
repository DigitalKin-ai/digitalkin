"""Mission-scoped lifetime of runtime-loaded tools.

The invariant under test, in one line: a tool the agent loads mid-conversation must
survive every later turn of *that* mission and appear in no other mission of the same
setup. Both halves used to be wrong in opposite directions — the archetype either
rebuilt its toolkits from the setup (load lost next turn) or from the whole shared
tool cache (load leaked into unrelated missions).
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.models.module.loaded_tools import (
    LOADED_TOOLS_STORAGE_CONFIG,
    LoadedToolRecord,
    LoadedToolStore,
)
from digitalkin.models.module.module_context import ModuleContext, Session
from digitalkin.models.module.tool_cache import ToolCache, ToolModuleInfo
from digitalkin.models.services.registry import ModuleInfo, RegistryModuleType, SetupInfo


def _info(setup_id: str, module_id: str = "modules:tool", name: str = "Tool") -> ToolModuleInfo:
    """A minimal resolved tool entry."""
    return ToolModuleInfo(module_id=module_id, setup_id=setup_id, tool_name=name)


def _registry_resolving(setup_id: str, module_id: str, name: str) -> AsyncMock:
    """Mock registry that resolves ``setup_id`` to a tool module."""
    registry = AsyncMock()
    registry.get_setup.return_value = SetupInfo(setup_id=setup_id, name=name, module_id=module_id)
    registry.discover_by_id.return_value = ModuleInfo(
        module_id=module_id,
        module_type=RegistryModuleType.TOOL_MODULE,
        address="localhost",
        port=50051,
        version="1.0.0",
        module_name=name,
    )
    return registry


def _communication() -> AsyncMock:
    """Mock communication whose module exposes a single ``search`` protocol."""
    comm = AsyncMock()
    comm.get_module_schemas.return_value = {
        "input": {"json_schema": {"$defs": {"SearchInput": {"properties": {"protocol": {"const": "search"}}}}}}
    }
    return comm


def _storage(*, registered: bool = True, records: list[str] | None = None) -> AsyncMock:
    """Mock storage strategy for the ``loaded_tools`` collection."""
    storage = AsyncMock()
    storage.config = dict(LOADED_TOOLS_STORAGE_CONFIG) if registered else {}
    # The strategy hands back the model instance it validated on write, not a raw dict.
    storage.list.return_value = [SimpleNamespace(data=LoadedToolRecord(setup_id=sid)) for sid in (records or [])]
    return storage


def _context(
    tool_cache: ToolCache, *, storage: AsyncMock | None = None, registry: AsyncMock | None = None
) -> ModuleContext:
    """Build a bare ModuleContext exposing only what the load path touches."""
    ctx = ModuleContext.__new__(ModuleContext)
    ctx.tool_cache = tool_cache
    ctx.registry = registry or _registry_resolving("setups:new", "modules:tool", "Tool")
    ctx.communication = _communication()
    ctx.storage = storage or _storage()
    ctx.session = Session(job_id="job-1", mission_id="missions:m1", setup_id="setups:s1", setup_version_id="sv-1")
    return ctx


class TestToolCacheLayers:
    """The declared/dynamic split — different lifetimes, never the same dict."""

    def test_add_goes_to_declared_and_add_dynamic_to_dynamic(self) -> None:
        """Setup-declared and runtime-loaded tools land in separate layers."""
        cache = ToolCache()
        cache.add(_info("setups:declared"))
        cache.add_dynamic(_info("setups:loaded"))

        assert set(cache.declared) == {"setups:declared"}
        assert set(cache.dynamic) == {"setups:loaded"}

    def test_entries_merges_both_layers(self) -> None:
        """Consumers reading ``entries`` see declared + dynamic."""
        cache = ToolCache()
        cache.add(_info("setups:declared"))
        cache.add_dynamic(_info("setups:loaded"))

        assert set(cache.entries) == {"setups:declared", "setups:loaded"}
        assert set(cache.list_tools()) == {"setups:declared", "setups:loaded"}

    def test_entries_is_a_copy_so_it_cannot_corrupt_the_shared_layer(self) -> None:
        """``entries`` must not hand out the shared declared mapping."""
        cache = ToolCache()
        cache.add(_info("setups:declared"))

        cache.entries["setups:injected"] = _info("setups:injected")

        assert "setups:injected" not in cache.declared

    def test_get_prefers_the_mission_layer(self) -> None:
        """A runtime load of an already-declared setup wins for this mission."""
        cache = ToolCache()
        cache.add(_info("setups:x", name="Declared"))
        cache.add_dynamic(_info("setups:x", name="Loaded"))

        found = cache.get("setups:x")

        assert found is not None
        assert found.tool_name == "Loaded"

    def test_mission_view_isolates_dynamic_between_missions(self) -> None:
        """Two missions off one shared cache never see each other's loads."""
        shared = ToolCache()
        shared.add(_info("setups:declared"))

        mission_a = shared.mission_view()
        mission_b = shared.mission_view()
        mission_a.add_dynamic(_info("setups:loaded-by-a"))

        assert set(mission_a.entries) == {"setups:declared", "setups:loaded-by-a"}
        assert set(mission_b.entries) == {"setups:declared"}
        assert set(shared.declared) == {"setups:declared"}
        assert shared.dynamic == {}


class TestResolveToolWritesToMissionLayer:
    """``resolve_tool`` is the runtime loader — its writes must not escape the mission."""

    @pytest.mark.asyncio
    async def test_resolution_lands_in_dynamic_not_declared(self) -> None:
        """The regression: a runtime load written to ``declared`` leaked across missions."""
        shared = ToolCache()
        shared.add(_info("setups:declared"))
        ctx = _context(shared.mission_view())

        info = await ctx.resolve_tool("setups:new")

        assert info is not None
        assert "setups:new" in ctx.tool_cache.dynamic
        assert "setups:new" not in ctx.tool_cache.declared
        # The object every other mission of this setup shares stays untouched.
        assert set(shared.declared) == {"setups:declared"}
        assert shared.dynamic == {}

    @pytest.mark.asyncio
    async def test_cache_hit_on_dynamic_still_checks_permission(self) -> None:
        """A second load of the same id skips discovery but never the authz gate."""
        registry = _registry_resolving("setups:new", "modules:tool", "Tool")
        ctx = _context(ToolCache(), registry=registry)

        first = await ctx.resolve_tool("setups:new")
        second = await ctx.resolve_tool("setups:new")

        assert first is second
        assert registry.get_setup.await_count == 2
        assert registry.discover_by_id.await_count == 1


class TestLoadedToolStore:
    """Persistence of loaded ids — mission-scoped and fail-soft."""

    @pytest.mark.asyncio
    async def test_save_then_list_round_trips_the_setup_id(self) -> None:
        """A saved id comes back for the next turn of the same mission."""
        storage = _storage(records=["setups:loaded"])

        assert await LoadedToolStore(storage).save("setups:loaded") is True
        assert await LoadedToolStore(storage).list_setup_ids() == ["setups:loaded"]
        storage.upsert.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unregistered_collection_degrades_without_touching_storage(self) -> None:
        """A module that never opted in must not crash, and must not pay an RPC."""
        storage = _storage(registered=False, records=["setups:loaded"])
        store = LoadedToolStore(storage)

        assert await store.save("setups:loaded") is False
        assert await store.list_setup_ids() == []
        storage.upsert.assert_not_awaited()
        storage.list.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_storage_failure_is_swallowed(self) -> None:
        """A storage outage must not propagate into the run through the HITL runner."""
        storage = _storage()
        storage.upsert.side_effect = RuntimeError("storage down")
        storage.list.side_effect = RuntimeError("storage down")
        store = LoadedToolStore(storage)

        assert await store.save("setups:loaded") is False
        assert await store.list_setup_ids() == []


class TestRehydrateLoadedTools:
    """What ``prepare()`` runs on every turn to make a load outlive its turn."""

    @pytest.mark.asyncio
    async def test_restores_persisted_tools_into_the_dynamic_layer(self) -> None:
        """The next turn gets the tool back without the agent re-loading it."""
        ctx = _context(ToolCache(), storage=_storage(records=["setups:new"]))

        restored = await ctx.rehydrate_loaded_tools()

        assert restored == 1
        assert "setups:new" in ctx.tool_cache.dynamic

    @pytest.mark.asyncio
    async def test_nothing_persisted_is_a_no_op(self) -> None:
        """A mission that never loaded a tool pays no resolution."""
        registry = _registry_resolving("setups:new", "modules:tool", "Tool")
        ctx = _context(ToolCache(), storage=_storage(records=[]), registry=registry)

        assert await ctx.rehydrate_loaded_tools() == 0
        registry.get_setup.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_revoked_tool_is_dropped_from_the_mission(self) -> None:
        """Access lost between turns: forget the id instead of retrying it forever."""
        registry = _registry_resolving("setups:gone", "modules:tool", "Tool")
        registry.get_setup.side_effect = PermissionDeniedError("nope")
        storage = _storage(records=["setups:gone"])
        ctx = _context(ToolCache(), storage=storage, registry=registry)

        assert await ctx.rehydrate_loaded_tools() == 0
        assert ctx.tool_cache.dynamic == {}
        storage.remove.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_deleted_tool_is_dropped_from_the_mission(self) -> None:
        """A setup that no longer exists resolves to None and is forgotten."""
        registry = _registry_resolving("setups:gone", "modules:tool", "Tool")
        registry.get_setup.return_value = None
        storage = _storage(records=["setups:gone"])
        ctx = _context(ToolCache(), storage=storage, registry=registry)

        assert await ctx.rehydrate_loaded_tools() == 0
        storage.remove.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_transient_failure_keeps_the_id_for_a_later_retry(self) -> None:
        """A registry hiccup must not silently un-load the user's tool."""
        registry = _registry_resolving("setups:new", "modules:tool", "Tool")
        registry.get_setup.side_effect = RuntimeError("registry flapping")
        storage = _storage(records=["setups:new"])
        ctx = _context(ToolCache(), storage=storage, registry=registry)

        assert await ctx.rehydrate_loaded_tools() == 0
        storage.remove.assert_not_awaited()
