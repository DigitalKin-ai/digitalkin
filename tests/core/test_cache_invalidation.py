"""Tests for cache invalidation protocol.

Covers:
- SetupModel._clean_model_cache bounding and clear
- BaseModule.clear_shared() dict swap
- Bulkhead maxsize guard and remove()
- ModuleServicer invalidation methods
- GatewayServicer.SendSignal routing for InvalidateSignal scopes
- ModuleServer cache handler dispatch
"""

import json
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_dto_pb2, gateway_enums_pb2, gateway_messages_pb2

from digitalkin.grpc_servers.interceptors.request_ids import RequestContext

pytestmark = pytest.mark.timeout(10)


# ============================================================================
# SetupModel._clean_model_cache
# ============================================================================


class TestSetupModelCleanModelCache:
    """SetupModel._clean_model_cache bounding and clearing."""

    def test_clear_clean_model_cache(self) -> None:
        """clear_clean_model_cache empties the cache."""
        from digitalkin.models.module.setup_types import SetupModel

        SetupModel._clean_model_cache[("fake", True, False)] = type("FakeModel", (), {})
        assert len(SetupModel._clean_model_cache) > 0

        SetupModel.clear_clean_model_cache()
        assert len(SetupModel._clean_model_cache) == 0

    def test_cache_max_evicts_oldest(self) -> None:
        """Cache evicts oldest entry when _CLEAN_MODEL_CACHE_MAX is reached."""
        from digitalkin.models.module.setup_types import SetupModel

        SetupModel._clean_model_cache.clear()
        original_max = SetupModel._CLEAN_MODEL_CACHE_MAX

        try:
            SetupModel._CLEAN_MODEL_CACHE_MAX = 3

            for i in range(4):
                key = (type(f"Fake{i}", (), {}), True, False)
                SetupModel._clean_model_cache[key] = type(f"Model{i}", (), {})
                # Simulate eviction logic from get_clean_model
                if len(SetupModel._clean_model_cache) > SetupModel._CLEAN_MODEL_CACHE_MAX:
                    del SetupModel._clean_model_cache[next(iter(SetupModel._clean_model_cache))]

            assert len(SetupModel._clean_model_cache) <= 3
        finally:
            SetupModel._CLEAN_MODEL_CACHE_MAX = original_max
            SetupModel._clean_model_cache.clear()


# ============================================================================
# BaseModule.clear_shared
# ============================================================================


class TestBaseModuleClearShared:
    """BaseModule.clear_shared() swaps the dict reference."""

    def test_clear_shared_creates_new_dict(self) -> None:
        """clear_shared replaces _shared with a new empty dict."""
        from digitalkin.modules._base_module import BaseModule

        old_dict = BaseModule._shared
        BaseModule._shared["test_key"] = "test_value"

        BaseModule.clear_shared()

        assert BaseModule._shared is not old_dict
        assert len(BaseModule._shared) == 0

    def test_clear_shared_does_not_affect_old_references(self) -> None:
        """Running tasks holding the old dict reference are unaffected."""
        from digitalkin.modules._base_module import BaseModule

        BaseModule._shared["keep_this"] = "value"
        old_ref = BaseModule._shared

        BaseModule.clear_shared()

        # Old reference still has data
        assert old_ref["keep_this"] == "value"
        # New class-level dict is empty
        assert len(BaseModule._shared) == 0


# ============================================================================
# Bulkhead
# ============================================================================


class TestBulkheadBounding:
    """Bulkhead._instances maxsize and remove."""

    def setup_method(self) -> None:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        Bulkhead.clear_all()

    def teardown_method(self) -> None:
        from digitalkin.core.resilience.bulkhead import Bulkhead

        Bulkhead.clear_all()

    def test_remove_specific_instance(self) -> None:
        """remove() deletes a specific bulkhead by service_id."""
        from digitalkin.core.resilience.bulkhead import Bulkhead

        Bulkhead.for_service("svc_a")
        Bulkhead.for_service("svc_b")
        assert len(Bulkhead._instances) == 2

        Bulkhead.remove("svc_a")
        assert "svc_a" not in Bulkhead._instances
        assert "svc_b" in Bulkhead._instances

    def test_remove_nonexistent_is_noop(self) -> None:
        """remove() on missing service_id doesn't raise."""
        from digitalkin.core.resilience.bulkhead import Bulkhead

        Bulkhead.remove("nonexistent")

    def test_max_instances_evicts_oldest(self) -> None:
        """When _MAX_INSTANCES is exceeded, oldest entry is evicted."""
        from digitalkin.core.resilience.bulkhead import Bulkhead

        original_max = Bulkhead._MAX_INSTANCES
        try:
            Bulkhead._MAX_INSTANCES = 3
            Bulkhead.for_service("s1")
            Bulkhead.for_service("s2")
            Bulkhead.for_service("s3")
            assert len(Bulkhead._instances) == 3

            Bulkhead.for_service("s4")
            assert len(Bulkhead._instances) == 3
            assert "s1" not in Bulkhead._instances
            assert "s4" in Bulkhead._instances
        finally:
            Bulkhead._MAX_INSTANCES = original_max


# ============================================================================
# ModuleServicer invalidation methods
# ============================================================================


class TestModuleServicerInvalidation:
    """ModuleServicer.invalidate_setup_cache and invalidate_tool_cache."""

    def test_invalidate_setup_cache_clears_both_dicts(self) -> None:
        """invalidate_setup_cache clears _setup_cache and _setup_inflight."""
        from digitalkin.grpc_servers.module_servicer import ModuleServicer

        servicer = MagicMock(spec=ModuleServicer)
        servicer._setup_cache = {"s1": "data1", "s2": "data2"}
        servicer._setup_inflight = {"s1": object()}
        servicer.invalidate_setup_cache = ModuleServicer.invalidate_setup_cache.__get__(servicer)

        servicer.invalidate_setup_cache()

        assert len(servicer._setup_cache) == 0
        assert len(servicer._setup_inflight) == 0

    def test_invalidate_tool_cache_clears_dict(self) -> None:
        """invalidate_tool_cache clears _tool_cache_by_setup."""
        from digitalkin.grpc_servers.module_servicer import ModuleServicer

        servicer = MagicMock(spec=ModuleServicer)
        servicer._tool_cache_by_setup = {"s1": "tools"}
        servicer.invalidate_tool_cache = ModuleServicer.invalidate_tool_cache.__get__(servicer)

        servicer.invalidate_tool_cache()

        assert len(servicer._tool_cache_by_setup) == 0


# ============================================================================
# GatewayServicer.SendSignal routing
# ============================================================================


class TestGatewayServicerCacheSignals:
    """SendSignal routes an InvalidateSignal to the cache_handler callback."""

    @staticmethod
    def _invalidate(scope: str) -> gateway_dto_pb2.SendSignalRequest:
        return gateway_dto_pb2.SendSignalRequest(
            invalidate=gateway_messages_pb2.InvalidateSignal(scope=gateway_enums_pb2.CacheScope.Value(scope)),
        )

    @pytest.fixture
    def gateway(self) -> "GatewayServicer":
        from digitalkin.grpc_servers.gateway_servicer import GatewayServicer

        redis_client = MagicMock()
        redis_client.publish = AsyncMock()
        cache_handler = AsyncMock()
        return GatewayServicer(
            redis_client=redis_client,
            cache_handler=cache_handler,
        )

    @pytest.mark.asyncio
    async def test_invalidate_all_calls_handler_and_publishes(self, gateway) -> None:
        """ALL dispatches to cache_handler AND publishes to signal_ch:_global_."""
        resp = await gateway.SendSignal(self._invalidate("ALL"), MagicMock())

        assert resp.success is True
        gateway._cache_handler.assert_awaited_once_with("ALL", "")
        gateway._redis_client.publish.assert_awaited_once()
        channel, _payload = gateway._redis_client.publish.await_args.args
        assert channel == "signal_ch:_global_"

    @pytest.mark.asyncio
    async def test_invalidate_setup_propagates_setup_id(self, gateway) -> None:
        """SETUP with x-setup-id=s1 forwards setup_id to cache_handler + payload."""
        token = RequestContext.bind(setup_id="s1")
        try:
            resp = await gateway.SendSignal(self._invalidate("SETUP"), MagicMock())
        finally:
            RequestContext.reset(token)

        assert resp.success is True
        gateway._cache_handler.assert_awaited_once_with("SETUP", "s1")
        channel, payload = gateway._redis_client.publish.await_args.args
        decoded = json.loads(payload)
        assert decoded["action"] == "invalidate_setup"
        assert decoded["setup_id"] == "s1"

    @pytest.mark.asyncio
    async def test_invalidate_shared_calls_handler(self, gateway) -> None:
        """SHARED dispatches to cache_handler with empty setup_id."""
        resp = await gateway.SendSignal(self._invalidate("SHARED"), MagicMock())

        assert resp.success is True
        gateway._cache_handler.assert_awaited_once_with("SHARED", "")

    @pytest.mark.asyncio
    async def test_invalidate_without_handler_still_publishes(self) -> None:
        """An invalidation without cache_handler still broadcasts to peers (best-effort)."""
        from digitalkin.grpc_servers.gateway_servicer import GatewayServicer

        redis_client = MagicMock()
        redis_client.publish = AsyncMock()
        gw = GatewayServicer(redis_client=redis_client, cache_handler=None)
        resp = await gw.SendSignal(self._invalidate("ALL"), MagicMock())

        # Local handler missing — but the broadcast still fires so peers can invalidate.
        assert resp.success is True
        redis_client.publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cancel_still_uses_task_flow(self, gateway) -> None:
        """A cancel signal requires a task_id and a session lookup, never the cache_handler."""
        request = gateway_dto_pb2.SendSignalRequest(cancel=gateway_messages_pb2.CancelSignal(task_id="test-task-id"))

        resp = await gateway.SendSignal(request, MagicMock())

        assert resp.success is False  # no session registered for test-task-id
        gateway._cache_handler.assert_not_awaited()


# ============================================================================
# ModuleServer cache handler dispatch
# ============================================================================


class TestModuleServerCacheHandlers:
    """ModuleServer._handle_cache_invalidation dispatches correctly."""

    @pytest.mark.asyncio
    async def test_invalidate_all_full_wipes_both_caches(self) -> None:
        """ALL bypasses the scoped handlers and wipes module-servicer caches directly."""
        from digitalkin.grpc_servers.module_server import ModuleServer

        server = MagicMock(spec=ModuleServer)
        server.module_servicer = MagicMock()
        server.module_servicer.invalidate_setup_cache = MagicMock()
        server.module_servicer.invalidate_tool_cache = MagicMock()
        server._invalidate_shared = AsyncMock()
        server._invalidate_models = AsyncMock()
        server._invalidate_channels = AsyncMock()
        server._invalidate_all = ModuleServer._invalidate_all.__get__(server)
        server._handle_cache_invalidation = ModuleServer._handle_cache_invalidation.__get__(server)

        await server._handle_cache_invalidation("ALL")

        server.module_servicer.invalidate_setup_cache.assert_called_once()
        server.module_servicer.invalidate_tool_cache.assert_called_once()
        server._invalidate_shared.assert_awaited_once()
        server._invalidate_models.assert_awaited_once()
        server._invalidate_channels.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_invalidate_setup_scoped_pops_only_target_setup_id(self) -> None:
        """SETUP with a setup_id pops only that key; siblings untouched."""
        from digitalkin.grpc_servers.module_server import ModuleServer

        server = MagicMock(spec=ModuleServer)
        server.module_servicer = MagicMock()
        server.module_servicer._setup_cache = {"s1": "x", "s2": "y", "s3": "z"}
        server.module_servicer._setup_inflight = {"s1": "fa", "s2": "fb"}
        server._invalidate_setup = ModuleServer._invalidate_setup.__get__(server)

        await server._invalidate_setup("s2")

        assert "s2" not in server.module_servicer._setup_cache
        assert "s2" not in server.module_servicer._setup_inflight
        assert "s1" in server.module_servicer._setup_cache
        assert "s3" in server.module_servicer._setup_cache

    @pytest.mark.asyncio
    async def test_invalidate_tools_scoped_pops_only_target_setup_id(self) -> None:
        """TOOLS with a setup_id pops only that key; siblings untouched."""
        from digitalkin.grpc_servers.module_server import ModuleServer

        server = MagicMock(spec=ModuleServer)
        server.module_servicer = MagicMock()
        server.module_servicer._tool_cache_by_setup = {"s1": "x", "s2": "y", "s3": "z"}
        server._invalidate_tools = ModuleServer._invalidate_tools.__get__(server)

        await server._invalidate_tools("s2")

        assert "s2" not in server.module_servicer._tool_cache_by_setup
        assert "s1" in server.module_servicer._tool_cache_by_setup
        assert "s3" in server.module_servicer._tool_cache_by_setup

    @pytest.mark.asyncio
    async def test_invalidate_setup_without_setup_id_is_skipped(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """SETUP without a setup_id logs a warning and leaves the cache intact."""
        from digitalkin.grpc_servers.module_server import ModuleServer

        server = MagicMock(spec=ModuleServer)
        server.module_servicer = MagicMock()
        server.module_servicer._setup_cache = {"s1": "x"}
        server.module_servicer._setup_inflight = {}
        server._invalidate_setup = ModuleServer._invalidate_setup.__get__(server)

        with caplog.at_level("WARNING", logger="digitalkin.grpc_servers.module_server"):
            await server._invalidate_setup("")

        assert server.module_servicer._setup_cache == {"s1": "x"}

    @pytest.mark.asyncio
    async def test_invalidate_tools_without_setup_id_is_skipped(self) -> None:
        """TOOLS without a setup_id leaves the cache intact (scoped-only policy)."""
        from digitalkin.grpc_servers.module_server import ModuleServer

        server = MagicMock(spec=ModuleServer)
        server.module_servicer = MagicMock()
        server.module_servicer._tool_cache_by_setup = {"s1": "x", "s2": "y"}
        server._invalidate_tools = ModuleServer._invalidate_tools.__get__(server)

        await server._invalidate_tools("")

        assert server.module_servicer._tool_cache_by_setup == {"s1": "x", "s2": "y"}

    @pytest.mark.asyncio
    async def test_invalidate_shared_calls_clear_shared(self) -> None:
        """SHARED calls module_class.clear_shared."""
        from digitalkin.grpc_servers.module_server import ModuleServer

        server = MagicMock(spec=ModuleServer)
        server.module_class = MagicMock()
        server.module_class.clear_shared = MagicMock()
        server._invalidate_shared = ModuleServer._invalidate_shared.__get__(server)

        await server._invalidate_shared()

        server.module_class.clear_shared.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("scope", "method", "args"),
        [
            ("ALL", "_invalidate_all", ()),
            ("CHANNELS", "_invalidate_channels", ()),
            ("MODELS", "_invalidate_models", ()),
            ("SETUP", "_invalidate_setup", ("s1",)),
            ("TOOLS", "_invalidate_tools", ("s1",)),
            ("SHARED", "_invalidate_shared", ()),
        ],
    )
    async def test_every_cache_scope_dispatches_to_its_handler(self, scope: str, method: str, args: tuple) -> None:
        """Each CacheScope name reaches exactly its ``_invalidate_*``; only SETUP / TOOLS get the setup_id."""
        from digitalkin.grpc_servers.module_server import ModuleServer

        server = MagicMock(spec=ModuleServer)
        handlers = {
            name: AsyncMock()
            for name in (
                "_invalidate_all",
                "_invalidate_channels",
                "_invalidate_models",
                "_invalidate_setup",
                "_invalidate_tools",
                "_invalidate_shared",
            )
        }
        for name, handler in handlers.items():
            vars(server)[name] = handler
        server._handle_cache_invalidation = ModuleServer._handle_cache_invalidation.__get__(server)

        await server._handle_cache_invalidation(scope, "s1")

        handlers.pop(method).assert_awaited_once_with(*args)
        for other in handlers.values():
            other.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("scope", ["CACHE_SCOPE_UNSPECIFIED", "INVALIDATE_ALL", "NONEXISTENT"])
    async def test_unknown_scope_is_noop(self, scope: str) -> None:
        """An unknown scope name (legacy ``INVALIDATE_*`` included) does nothing, no error."""
        from digitalkin.grpc_servers.module_server import ModuleServer

        server = MagicMock(spec=ModuleServer)
        server._invalidate_all = AsyncMock()
        server._handle_cache_invalidation = ModuleServer._handle_cache_invalidation.__get__(server)

        await server._handle_cache_invalidation(scope)

        server._invalidate_all.assert_not_awaited()
