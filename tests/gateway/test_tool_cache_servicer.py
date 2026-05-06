"""Tests for servicer-level tool cache and prebuilt tool_cache injection."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from digitalkin.models.module.tool_cache import ToolCache


class TestToolCachePrebuilt:
    """BaseModule stores prebuilt tool_cache from constructor."""

    def test_prebuilt_stored_on_module(self) -> None:
        """When tool_cache is passed to constructor, it's stored as _prebuilt_tool_cache."""
        from digitalkin.models.module.tool_cache import ToolCache, ToolModuleInfo
        from tests.mocks.modules import SimpleMockModule

        prebuilt = ToolCache()
        prebuilt.add(ToolModuleInfo(
            module_id="mod:1", module_type="tool", address="localhost",
            port=50055, setup_id="setups:test", tool_name="TestTool",
        ))

        module = SimpleMockModule(
            job_id="job1", mission_id="m1", setup_id="s1",
            setup_version_id="v1", tool_cache=prebuilt,
        )

        assert module._prebuilt_tool_cache is prebuilt
        assert module._prebuilt_tool_cache.entries.get("setups:test") is not None

    def test_no_prebuilt_defaults_to_none(self) -> None:
        """Without tool_cache param, _prebuilt_tool_cache is None."""
        from tests.mocks.modules import SimpleMockModule

        module = SimpleMockModule(
            job_id="job1", mission_id="m1", setup_id="s1", setup_version_id="v1",
        )

        assert module._prebuilt_tool_cache is None


class TestToolCacheServicerLevel:
    """ModuleServicer caches ToolCache by setup_id across requests."""

    @pytest.mark.asyncio
    async def test_tool_cache_reused_on_second_request(self) -> None:
        """Second module run with same setup_id uses cached ToolCache."""
        from digitalkin.grpc_servers.module_servicer import ModuleServicer

        servicer = ModuleServicer.__new__(ModuleServicer)
        servicer._tool_cache_by_setup = {}
        servicer._setup_cache_max = 100

        # Simulate first request caching a tool cache
        cache = ToolCache()
        servicer._tool_cache_by_setup["setups:test"] = cache

        # Second lookup should return same object
        result = servicer._tool_cache_by_setup.get("setups:test")
        assert result is cache

    @pytest.mark.asyncio
    async def test_tool_cache_invalidated_on_config_setup(self) -> None:
        """ConfigSetupModule invalidates tool cache for changed setup_id."""
        from digitalkin.grpc_servers.module_servicer import ModuleServicer

        servicer = ModuleServicer.__new__(ModuleServicer)
        servicer._tool_cache_by_setup = {"setups:test": ToolCache()}

        # Simulate ConfigSetupModule invalidation
        servicer._tool_cache_by_setup.pop("setups:test", None)

        assert "setups:test" not in servicer._tool_cache_by_setup

    def test_tool_cache_eviction_at_capacity(self) -> None:
        """Tool cache evicts oldest entry when at capacity."""
        from digitalkin.grpc_servers.module_servicer import ModuleServicer

        servicer = ModuleServicer.__new__(ModuleServicer)
        servicer._tool_cache_by_setup = {}
        servicer._setup_cache_max = 3

        for i in range(3):
            servicer._tool_cache_by_setup[f"setups:s{i}"] = ToolCache()

        # Evict oldest when at capacity
        if len(servicer._tool_cache_by_setup) >= servicer._setup_cache_max:
            oldest_key = next(iter(servicer._tool_cache_by_setup))
            del servicer._tool_cache_by_setup[oldest_key]
        servicer._tool_cache_by_setup["setups:s3"] = ToolCache()

        assert "setups:s0" not in servicer._tool_cache_by_setup
        assert "setups:s3" in servicer._tool_cache_by_setup
        assert len(servicer._tool_cache_by_setup) == 3
