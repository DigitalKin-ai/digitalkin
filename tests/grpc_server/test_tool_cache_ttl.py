"""Tests for the servicer-level caches: L1 setup-content cache + L2 TTL'd tool cache.

Covers `_tool_cache_by_setup` (TTL, capacity eviction, bulk + scoped invalidation)
and `_setup_cache` (scoped invalidation), plus the scoped-only invalidation policy
on `ModuleServer._invalidate_setup` / `_invalidate_tools` / `_invalidate_all`.
"""

from __future__ import annotations

import time

import pytest


def _make_servicer():
    """Build a ModuleServicer skeleton with just the cache state used here."""
    from digitalkin.grpc_servers.module_servicer import ModuleServicer

    inst = ModuleServicer.__new__(ModuleServicer)
    inst._tool_cache_by_setup = {}  # noqa: SLF001
    inst._setup_cache = {}  # noqa: SLF001
    inst._setup_inflight = {}  # noqa: SLF001
    return inst


class TestToolCacheTTL:
    """L2 — `_tool_cache_by_setup` TTL behaviour."""

    def test_set_then_get_returns_value(self) -> None:
        s = _make_servicer()
        s.set_tool_cache("setups:s1", "value-1")
        assert s.get_tool_cache("setups:s1") == "value-1"

    def test_get_returns_none_after_ttl_expiry(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from digitalkin.models.settings.gateway import get_gateway_settings

        monkeypatch.setenv("DIGITALKIN_GATEWAY_QUEUE_TOOLKIT_CACHE_TTL_S", "0.01")
        get_gateway_settings.cache_clear()

        s = _make_servicer()
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

    def test_set_evicts_oldest_when_at_capacity(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from digitalkin.models.settings.server.servicer import get_module_servicer_settings

        monkeypatch.setenv("DIGITALKIN_MODULE_SERVICER_SETUP_CACHE_MAX", "2")
        get_module_servicer_settings.cache_clear()
        s = _make_servicer()
        s.set_tool_cache("setups:s1", "v1")
        s.set_tool_cache("setups:s2", "v2")
        s.set_tool_cache("setups:s3", "v3")
        assert s.get_tool_cache("setups:s1") is None  # evicted
        assert s.get_tool_cache("setups:s2") == "v2"
        assert s.get_tool_cache("setups:s3") == "v3"


class TestScopedInvalidation:
    """Scoped-only policy: per-setup_id pops, never a silent full wipe."""

    @pytest.mark.asyncio
    async def test_invalidate_tools_pops_only_target_setup(self) -> None:
        from digitalkin.grpc_servers.module_server import ModuleServer

        ms = ModuleServer.__new__(ModuleServer)
        ms.module_servicer = _make_servicer()  # type: ignore[attr-defined]
        ms.module_servicer.set_tool_cache("setups:s1", "v1")
        ms.module_servicer.set_tool_cache("setups:s2", "v2")

        await ModuleServer._invalidate_tools(ms, "setups:s1")  # type: ignore[arg-type]

        assert ms.module_servicer.get_tool_cache("setups:s1") is None  # popped
        assert ms.module_servicer.get_tool_cache("setups:s2") == "v2"  # sibling untouched

    @pytest.mark.asyncio
    async def test_invalidate_tools_without_setup_id_is_noop(self) -> None:
        from digitalkin.grpc_servers.module_server import ModuleServer

        ms = ModuleServer.__new__(ModuleServer)
        ms.module_servicer = _make_servicer()  # type: ignore[attr-defined]
        ms.module_servicer.set_tool_cache("setups:s1", "v1")

        # Scoped-only policy: missing setup_id logs + skips, never wipes.
        await ModuleServer._invalidate_tools(ms, "")  # type: ignore[arg-type]
        assert ms.module_servicer.get_tool_cache("setups:s1") == "v1"

    @pytest.mark.asyncio
    async def test_invalidate_setup_pops_only_target_setup(self) -> None:
        from digitalkin.grpc_servers.module_server import ModuleServer

        ms = ModuleServer.__new__(ModuleServer)
        ms.module_servicer = _make_servicer()  # type: ignore[attr-defined]
        ms.module_servicer._setup_cache["setups:s1"] = object()  # noqa: SLF001
        ms.module_servicer._setup_cache["setups:s2"] = object()  # noqa: SLF001

        await ModuleServer._invalidate_setup(ms, "setups:s1")  # type: ignore[arg-type]

        assert "setups:s1" not in ms.module_servicer._setup_cache  # noqa: SLF001
        assert "setups:s2" in ms.module_servicer._setup_cache  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_invalidate_setup_without_setup_id_is_noop(self) -> None:
        from digitalkin.grpc_servers.module_server import ModuleServer

        ms = ModuleServer.__new__(ModuleServer)
        ms.module_servicer = _make_servicer()  # type: ignore[attr-defined]
        ms.module_servicer._setup_cache["setups:s1"] = object()  # noqa: SLF001

        await ModuleServer._invalidate_setup(ms, "")  # type: ignore[arg-type]
        assert "setups:s1" in ms.module_servicer._setup_cache  # noqa: SLF001


class TestInvalidateAll:
    """`_invalidate_all` is the only path that bulk-clears both servicer caches."""

    @pytest.mark.asyncio
    async def test_invalidate_all_clears_setup_and_tool_caches(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from digitalkin.grpc_servers.module_server import ModuleServer

        ms = ModuleServer.__new__(ModuleServer)
        ms.module_servicer = _make_servicer()  # type: ignore[attr-defined]
        ms.module_servicer.set_tool_cache("setups:s1", "v1")
        ms.module_servicer.set_tool_cache("setups:s2", "v2")
        ms.module_servicer._setup_cache["setups:s1"] = object()  # noqa: SLF001

        # Stub the non-cache side effects of _invalidate_all so the test stays unit-scoped.
        async def _noop() -> None:
            return

        monkeypatch.setattr(ms, "_invalidate_shared", _noop)
        monkeypatch.setattr(ms, "_invalidate_models", _noop)
        monkeypatch.setattr(ms, "_invalidate_channels", _noop)

        await ModuleServer._invalidate_all(ms)  # type: ignore[arg-type]

        assert ms.module_servicer.get_tool_cache("setups:s1") is None
        assert ms.module_servicer.get_tool_cache("setups:s2") is None
        assert ms.module_servicer._setup_cache == {}  # noqa: SLF001
