"""Tests for L4 — TTL'd tool cache + INVALIDATE_TOOLS bulk flush."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest


def _make_servicer():
    """Build a ModuleServicer skeleton with just the cache state."""
    from digitalkin.grpc_servers.module_servicer import ModuleServicer

    inst = ModuleServicer.__new__(ModuleServicer)
    inst._tool_cache_by_setup = {}  # noqa: SLF001
    inst._setup_cache_max = 100  # noqa: SLF001
    return inst


class TestToolCacheTTL:
    def test_set_then_get_returns_value(self) -> None:
        s = _make_servicer()
        s.set_tool_cache("setups:s1", "value-1")
        assert s.get_tool_cache("setups:s1") == "value-1"

    def test_get_returns_none_after_ttl_expiry(self) -> None:
        s = _make_servicer()
        # Patch TOOLKIT_CACHE_TTL_S read in module_servicer to a tiny value
        with patch("digitalkin.grpc_servers.module_servicer.TOOLKIT_CACHE_TTL_S", 0.01):
            s.set_tool_cache("setups:s1", "value-1")
            assert s.get_tool_cache("setups:s1") == "value-1"
            time.sleep(0.05)
            assert s.get_tool_cache("setups:s1") is None
            # Expired entry was popped, so a second lookup is also None.
            assert "setups:s1" not in s._tool_cache_by_setup  # noqa: SLF001

    def test_get_unknown_key_returns_none(self) -> None:
        s = _make_servicer()
        assert s.get_tool_cache("setups:never-set") is None

    def test_invalidate_tool_cache_clears_all_regardless_of_ttl(self) -> None:
        s = _make_servicer()
        s.set_tool_cache("setups:s1", "v1")
        s.set_tool_cache("setups:s2", "v2")
        assert s.get_tool_cache("setups:s1") == "v1"
        assert s.get_tool_cache("setups:s2") == "v2"
        s.invalidate_tool_cache()
        assert s.get_tool_cache("setups:s1") is None
        assert s.get_tool_cache("setups:s2") is None

    def test_set_evicts_oldest_when_at_capacity(self) -> None:
        s = _make_servicer()
        s._setup_cache_max = 2  # noqa: SLF001
        s.set_tool_cache("setups:s1", "v1")
        s.set_tool_cache("setups:s2", "v2")
        s.set_tool_cache("setups:s3", "v3")
        assert s.get_tool_cache("setups:s1") is None  # evicted
        assert s.get_tool_cache("setups:s2") == "v2"
        assert s.get_tool_cache("setups:s3") == "v3"


class TestInvalidateToolsSignal:
    """Confirms the existing INVALIDATE_TOOLS SendSignal flow flushes the cache."""

    @pytest.mark.asyncio
    async def test_invalidate_tools_action_flushes_cache(self) -> None:
        """The real `_invalidate_tools` (in ModuleServer) wraps the sync
        `invalidate_tool_cache` as an async method. Verify the round-trip:
        SendSignal handler → ModuleServer._invalidate_tools → cache empty.
        """
        from digitalkin.grpc_servers.module_server import ModuleServer

        ms = ModuleServer.__new__(ModuleServer)
        ms.module_servicer = _make_servicer()  # type: ignore[attr-defined]
        ms.module_servicer.set_tool_cache("setups:s1", "v1")
        ms.module_servicer.set_tool_cache("setups:s2", "v2")

        # Bound bypass via the real invalidator method on ModuleServer:
        await ModuleServer._invalidate_tools(ms)  # type: ignore[arg-type]
        assert ms.module_servicer.get_tool_cache("setups:s1") is None
        assert ms.module_servicer.get_tool_cache("setups:s2") is None
