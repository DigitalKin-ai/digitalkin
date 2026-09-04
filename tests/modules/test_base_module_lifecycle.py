"""Tests for BaseModule lifecycle, model creation, and discovery.

Covers __init__, status, get_module_id, create_*_model, discover, register,
run, _run_lifecycle, start, stop, _resolve_tools, start_config_setup.
"""

import asyncio
from typing import Any, ClassVar, Literal
from unittest.mock import AsyncMock, Mock, patch

import pytest
from pydantic import BaseModel, Field

from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.models.module.module import ModuleCodeModel, ModuleStatus
from digitalkin.models.module.module_types import DataModel, DataTrigger, SetupModel
from digitalkin.models.module.tool_cache import ToolCache
from digitalkin.models.module.utility import HealthcheckPingInput
from digitalkin.modules._base_module import BaseModule
from digitalkin.utils.package_discover import ModuleDiscoverer


# ---------------------------------------------------------------------------
# Test Models
# ---------------------------------------------------------------------------


class _LcInputTrigger(DataTrigger):
    protocol: Literal["lc_test"] = "lc_test"
    message: str = ""


class _LcInputModel(DataModel[_LcInputTrigger]):
    pass


class _LcOutputTrigger(DataTrigger):
    protocol: Literal["lc_test"] = "lc_test"
    result: str = ""


class _LcOutputModel(DataModel[_LcOutputTrigger]):
    pass


class _LcSetupModel(SetupModel):
    name: str = Field(default="test")
    timeout: int = Field(default=30, json_schema_extra={"config": True})
    internal: str = Field(default="", json_schema_extra={"ui:widget": "hidden"})


class _LcSecretModel(BaseModel):
    api_key: str = Field(default="secret")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SERVICE_NAMES = {
    "communication",
    "cost",
    "filesystem",
    "identity",
    "registry",
    "secret",
    "storage",
    "user_profile",
}


def _make_module_cls() -> type[BaseModule]:
    """Create a fresh concrete module class."""

    class _LifecycleModule(BaseModule[_LcInputModel, _LcOutputModel, _LcSetupModel, _LcSecretModel]):
        name = "lifecycle_test"
        description = "Test module"
        setup_format = _LcSetupModel
        input_format = _LcInputModel
        output_format = _LcOutputModel
        secret_format = _LcSecretModel
        metadata: ClassVar[dict[str, Any]] = {"module_id": "lifecycle_test_id"}
        triggers_discoverer = ModuleDiscoverer(["test_pkg"])
        services_config_strategies: ClassVar[dict] = {}
        services_config_params: ClassVar[dict] = {}
        _builds_tool_cache: ClassVar[bool] = True

        async def initialize(self, context, setup_data) -> None:  # noqa: ARG002
            pass

        async def cleanup(self) -> None:
            pass

    return _LifecycleModule


def _instantiate(cls: type[BaseModule]) -> BaseModule:
    """Instantiate a module class with mocked services."""
    mock_config = Mock()
    mock_config.valid_strategy_names.return_value = _SERVICE_NAMES
    mock_config.init_strategy.side_effect = lambda *a, **kw: Mock()
    mock_config._stateless_strategies = frozenset()
    cls.services_config = mock_config
    return cls(
        job_id="job-1",
        mission_id="mission-1",
        setup_id="setup-1",
        setup_version_id="sv-1",
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestGetModuleId:
    """Tests for BaseModule.get_module_id."""

    def test_from_env_var(self) -> None:
        """Returns DIGITALKIN_MODULE_ID env var when set."""
        cls = _make_module_cls()
        with patch.dict("os.environ", {"DIGITALKIN_MODULE_ID": "env-id"}):
            assert cls.get_module_id() == "env-id"

    def test_from_metadata(self) -> None:
        """Falls back to metadata module_id."""
        cls = _make_module_cls()
        with patch.dict("os.environ", {}, clear=True):
            assert cls.get_module_id() == "lifecycle_test_id"

    def test_unknown_fallback(self) -> None:
        """Returns 'unknown' when neither env var nor metadata exist."""
        cls = _make_module_cls()
        cls.metadata = {}
        with patch.dict("os.environ", {}, clear=True):
            assert cls.get_module_id() == "unknown"


class TestInit:
    """Tests for BaseModule.__init__ and _init_strategies."""

    def test_creates_context_with_services(self) -> None:
        """__init__ creates a ModuleContext with all services."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        assert module.context is not None
        assert module.context.session.job_id == "job-1"
        assert module.context.session.mission_id == "mission-1"
        assert module.context.session.setup_id == "setup-1"
        assert module.context.session.setup_version_id == "sv-1"

    def test_initial_status_is_created(self) -> None:
        """Module starts in CREATED status."""
        cls = _make_module_cls()
        module = _instantiate(cls)
        assert module.status == ModuleStatus.CREATED

    def test_init_strategies_called_for_all_services(self) -> None:
        """_init_strategies calls init_strategy for each valid service."""
        cls = _make_module_cls()
        mock_config = Mock()
        mock_config.valid_strategy_names.return_value = _SERVICE_NAMES
        mock_config.init_strategy.side_effect = lambda *a, **kw: Mock()
        mock_config._stateless_strategies = frozenset()
        cls.services_config = mock_config

        cls(job_id="j", mission_id="m", setup_id="s", setup_version_id="sv")

        assert mock_config.init_strategy.call_count == len(_SERVICE_NAMES)


class TestCreateModels:
    """Tests for create_*_model methods."""

    def test_create_config_setup_model(self) -> None:
        """Creates setup model from dict."""
        cls = _make_module_cls()
        model = cls.create_config_setup_model({"name": "custom", "timeout": 60})
        assert model.name == "custom"
        assert model.timeout == 60

    def test_create_input_model(self) -> None:
        """Creates input model from dict."""
        cls = _make_module_cls()
        model = cls.create_input_model({"root": {"protocol": "lc_test", "message": "hi"}})
        assert model.root.message == "hi"

    def test_create_input_model_accepts_utility_protocol(self) -> None:
        """create_input_model accepts utility protocols after discover()."""
        cls = _make_module_cls()
        cls.discover()
        model = cls.create_input_model({"root": {"protocol": "healthcheck_ping"}})
        assert isinstance(model.root, HealthcheckPingInput)

    async def test_create_setup_model(self) -> None:
        """Creates filtered setup model via get_clean_model."""
        cls = _make_module_cls()
        model = await cls.create_setup_model({"name": "rt", "internal": "state"})
        assert model.name == "rt"

    async def test_create_setup_model_config_fields(self) -> None:
        """Creates config-filtered setup model."""
        cls = _make_module_cls()
        model = await cls.create_setup_model({"timeout": 99}, config_fields=True)
        assert model.timeout == 99

    def test_create_secret_model(self) -> None:
        """Creates secret model from dict."""
        cls = _make_module_cls()
        model = cls.create_secret_model({"api_key": "key123"})
        assert model.api_key == "key123"

    def test_create_output_model(self) -> None:
        """Creates output model from dict."""
        cls = _make_module_cls()
        model = cls.create_output_model({"root": {"protocol": "lc_test", "result": "ok"}})
        assert model.root.result == "ok"


class TestDiscoverAndRegister:
    """Tests for discover and register."""

    def test_discover_calls_discoverer(self) -> None:
        """discover() delegates to triggers_discoverer.discover_modules."""
        cls = _make_module_cls()
        cls.triggers_discoverer = Mock()
        cls.triggers_discoverer.discover_modules = Mock()

        with patch("digitalkin.models.module.utility.UtilityRegistry") as mock_registry:
            mock_registry.get_builtin_triggers.return_value = ()
            cls.discover()

        cls.triggers_discoverer.discover_modules.assert_called_once()

    def test_register_delegates_to_discoverer(self) -> None:
        """register() delegates to triggers_discoverer.register_trigger."""
        cls = _make_module_cls()
        cls.triggers_discoverer = Mock()
        mock_handler = Mock()
        cls.triggers_discoverer.register_trigger.return_value = mock_handler

        result = cls.register(mock_handler)

        cls.triggers_discoverer.register_trigger.assert_called_once_with(mock_handler)
        assert result is mock_handler


class TestRun:
    """Tests for BaseModule.run."""

    async def test_dispatches_to_trigger_handler(self) -> None:
        """run() validates input and dispatches to trigger handler."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        mock_handler = AsyncMock()
        module.triggers_discoverer = Mock()
        module.triggers_discoverer.get_trigger.return_value = mock_handler

        input_data = _LcInputModel(root=_LcInputTrigger(message="hello"))
        setup_data = _LcSetupModel()

        await module.run(input_data, setup_data)

        mock_handler.handle.assert_awaited_once()
        # Verify correct args: root trigger, setup, context
        call_args = mock_handler.handle.call_args
        assert call_args[0][1] is setup_data
        assert call_args[0][2] is module.context

    async def test_run_accepts_extended_input_model(self) -> None:
        """run() accepts input created by create_input_model (extended model instance)."""
        cls = _make_module_cls()
        cls.discover()
        module = _instantiate(cls)

        mock_handler = AsyncMock()
        module.triggers_discoverer = Mock()
        module.triggers_discoverer.get_trigger.return_value = mock_handler

        # Simulate the real flow: servicer calls create_input_model, passes result to run()
        input_data = cls.create_input_model({"root": {"protocol": "lc_test", "message": "hi"}})
        setup_data = _LcSetupModel()

        await module.run(input_data, setup_data)

        mock_handler.handle.assert_awaited_once()

    async def test_run_accepts_utility_protocol_from_create_input_model(self) -> None:
        """run() dispatches utility protocols created via create_input_model."""
        cls = _make_module_cls()
        cls.discover()
        module = _instantiate(cls)

        mock_handler = AsyncMock()
        module.triggers_discoverer = Mock()
        module.triggers_discoverer.get_trigger.return_value = mock_handler

        input_data = cls.create_input_model({"root": {"protocol": "healthcheck_ping"}})
        setup_data = _LcSetupModel()

        await module.run(input_data, setup_data)

        mock_handler.handle.assert_awaited_once()
        call_args = mock_handler.handle.call_args
        assert isinstance(call_args[0][0], HealthcheckPingInput)

    async def test_raises_on_unknown_protocol(self) -> None:
        """run() raises ValueError for unregistered protocol."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        module.triggers_discoverer = Mock()
        module.triggers_discoverer.get_trigger.side_effect = ValueError("No handler")

        input_data = _LcInputModel(root=_LcInputTrigger())
        setup_data = _LcSetupModel()

        with pytest.raises(ValueError, match="No handler"):
            await module.run(input_data, setup_data)


class TestRunLifecycle:
    """Tests for BaseModule._run_lifecycle."""

    async def test_success_sets_stopping(self) -> None:
        """Successful run sets status to STOPPING."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        with patch.object(module, "run", new_callable=AsyncMock):
            await module._run_lifecycle(_LcInputModel(root=_LcInputTrigger()), _LcSetupModel())

        assert module.status == ModuleStatus.STOPPING

    async def test_exception_sets_failed(self) -> None:
        """Exception in run sets status to FAILED."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        with patch.object(module, "run", new_callable=AsyncMock, side_effect=RuntimeError("boom")):
            await module._run_lifecycle(_LcInputModel(root=_LcInputTrigger()), _LcSetupModel())

        assert module.status == ModuleStatus.FAILED

    async def test_cancel_sets_cancelled(self) -> None:
        """CancelledError in run sets status to CANCELLED and re-raises (proper asyncio)."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        with (
            patch.object(module, "run", new_callable=AsyncMock, side_effect=asyncio.CancelledError),
            pytest.raises(asyncio.CancelledError),
        ):
            await module._run_lifecycle(_LcInputModel(root=_LcInputTrigger()), _LcSetupModel())

        assert module.status == ModuleStatus.CANCELLED
        assert module.context.session.cancelled is True

    @pytest.mark.unit
    @pytest.mark.regression
    async def test_permission_denied_notifies_and_stops(self) -> None:
        """An uncaught PermissionDeniedError from run() sends a PermissionDenied code and stops (FAILED)."""
        cls = _make_module_cls()
        module = _instantiate(cls)
        module.context.callbacks.send_message = AsyncMock()

        with patch.object(module, "run", new_callable=AsyncMock, side_effect=PermissionDeniedError("denied")):
            await module._run_lifecycle(_LcInputModel(root=_LcInputTrigger()), _LcSetupModel())

        assert module.status == ModuleStatus.FAILED
        module.context.callbacks.send_message.assert_awaited_once()
        sent = module.context.callbacks.send_message.call_args[0][0]
        assert isinstance(sent, ModuleCodeModel)
        assert sent.code == "PermissionDenied"
        assert sent.message == "denied"

    @pytest.mark.unit
    @pytest.mark.edge_case
    async def test_permission_denied_caught_by_handler_continues(self) -> None:
        """If run() catches PermissionDeniedError itself, the module completes cleanly (STOPPING), not FAILED."""
        cls = _make_module_cls()
        module = _instantiate(cls)
        module.context.callbacks.send_message = AsyncMock()

        async def _run_catches(input_data: object, setup_data: object) -> None:  # noqa: RUF029
            denied = PermissionDeniedError("denied")
            try:
                raise denied
            except PermissionDeniedError:
                pass  # author tolerates an optional-service denial and keeps going

        with patch.object(module, "run", new=_run_catches):
            await module._run_lifecycle(_LcInputModel(root=_LcInputTrigger()), _LcSetupModel())

        assert module.status == ModuleStatus.STOPPING
        module.context.callbacks.send_message.assert_not_awaited()


class TestStart:
    """Tests for BaseModule.start."""

    async def test_success_path(self) -> None:
        """start() runs full lifecycle: callback, init, handlers, run, stop."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        callback = AsyncMock()
        setup_data = _LcSetupModel()
        input_data = _LcInputModel(root=_LcInputTrigger())

        with (
            patch.object(module, "initialize", new_callable=AsyncMock) as mock_init,
            patch.object(module, "_run_lifecycle", new_callable=AsyncMock),
            patch.object(module, "stop", new_callable=AsyncMock) as mock_stop,
        ):
            module.triggers_discoverer = Mock()
            module.triggers_discoverer.init_handlers = Mock(return_value={})

            await module.start(input_data, setup_data, callback)

        # Module start info is now sent by the gateway, not by start()
        mock_init.assert_awaited_once()
        mock_stop.assert_awaited_once()

    async def test_init_error_sends_error_code(self) -> None:
        """start() sends ModuleCodeModel on initialize error."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        callback = AsyncMock()
        setup_data = _LcSetupModel()
        input_data = _LcInputModel(root=_LcInputTrigger())

        with (
            patch.object(module, "initialize", new_callable=AsyncMock, side_effect=RuntimeError("init fail")),
            patch.object(module, "stop", new_callable=AsyncMock),
        ):
            await module.start(input_data, setup_data, callback)

        assert module.status == ModuleStatus.FAILED
        # Error callback sends ModuleCodeModel
        error_call = callback.call_args_list[0]
        assert isinstance(error_call[0][0], ModuleCodeModel)
        assert error_call[0][0].code == "Error"

    @pytest.mark.unit
    async def test_init_permission_denied_sends_code(self) -> None:
        """start() sends a PermissionDenied code (not generic Error) when init hits PERMISSION_DENIED."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        callback = AsyncMock()
        done_callback = AsyncMock()
        setup_data = _LcSetupModel()
        input_data = _LcInputModel(root=_LcInputTrigger())

        with (
            patch.object(module, "initialize", new_callable=AsyncMock, side_effect=PermissionDeniedError("nope")),
            patch.object(module, "stop", new_callable=AsyncMock),
        ):
            await module.start(input_data, setup_data, callback, done_callback=done_callback)

        assert module.status == ModuleStatus.FAILED
        sent = callback.call_args_list[0][0][0]
        assert isinstance(sent, ModuleCodeModel)
        assert sent.code == "PermissionDenied"
        done_callback.assert_awaited_once_with(None)

    async def test_init_error_with_done_callback(self) -> None:
        """start() calls done_callback when init fails."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        callback = AsyncMock()
        done_callback = AsyncMock()
        setup_data = _LcSetupModel()
        input_data = _LcInputModel(root=_LcInputTrigger())

        with (
            patch.object(module, "initialize", new_callable=AsyncMock, side_effect=RuntimeError("fail")),
            patch.object(module, "stop", new_callable=AsyncMock),
        ):
            await module.start(input_data, setup_data, callback, done_callback=done_callback)

        done_callback.assert_awaited_once_with(None)

    async def test_lifecycle_error_sets_failed(self) -> None:
        """start() sets FAILED on lifecycle exception."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        callback = AsyncMock()
        setup_data = _LcSetupModel()
        input_data = _LcInputModel(root=_LcInputTrigger())

        with (
            patch.object(module, "initialize", new_callable=AsyncMock),
            patch.object(module, "_run_lifecycle", new_callable=AsyncMock, side_effect=RuntimeError("lifecycle")),
            patch.object(module, "stop", new_callable=AsyncMock),
        ):
            module.triggers_discoverer = Mock()
            module.triggers_discoverer.init_handlers = Mock(return_value={})

            await module.start(input_data, setup_data, callback)

        assert module.status == ModuleStatus.FAILED


class TestStop:
    """Tests for BaseModule.stop."""

    async def test_success_sets_stopped(self) -> None:
        """stop() cleans up and sends EndOfStream."""
        cls = _make_module_cls()
        module = _instantiate(cls)
        module.context.callbacks.send_message = AsyncMock()

        with patch.object(module, "cleanup", new_callable=AsyncMock):
            await module.stop()

        assert module.status == ModuleStatus.STOPPED
        module.context.callbacks.send_message.assert_awaited_once()
        # Verify EndOfStream was sent
        sent = module.context.callbacks.send_message.call_args[0][0]
        assert sent.root.protocol == "stream.end"

    async def test_slow_cleanup_is_warned_about(self, caplog: pytest.LogCaptureFixture) -> None:
        """A cleanup hook that blocks the loop must name itself in the logs.

        The damage lands on unrelated in-flight work (bogus ``REDIS_UNAVAILABLE`` on gateway
        streams), so without this warning the module that actually stalled is invisible.
        """
        cls = _make_module_cls()
        module = _instantiate(cls)
        module.context.callbacks.send_message = AsyncMock()

        async def _slow_cleanup() -> None:
            await asyncio.sleep(1.05)

        with patch.object(module, "cleanup", new=_slow_cleanup), caplog.at_level("WARNING", logger="digitalkin"):
            await module.stop()

        warnings = [r for r in caplog.records if "cleanup() took" in r.getMessage()]
        assert len(warnings) == 1
        assert cls.__name__ in warnings[0].getMessage()
        assert "asyncio.to_thread" in warnings[0].getMessage()

    async def test_fast_cleanup_is_not_warned_about(self, caplog: pytest.LogCaptureFixture) -> None:
        """The normal path stays quiet."""
        cls = _make_module_cls()
        module = _instantiate(cls)
        module.context.callbacks.send_message = AsyncMock()

        with (
            patch.object(module, "cleanup", new_callable=AsyncMock),
            caplog.at_level("WARNING", logger="digitalkin"),
        ):
            await module.stop()

        assert not [r for r in caplog.records if "cleanup() took" in r.getMessage()]

    async def test_cleanup_error_sets_failed(self) -> None:
        """stop() sets FAILED when cleanup raises."""
        cls = _make_module_cls()
        module = _instantiate(cls)
        module.context.callbacks.send_message = AsyncMock()

        with patch.object(module, "cleanup", new_callable=AsyncMock, side_effect=RuntimeError("cleanup fail")):
            await module.stop()

        assert module.status == ModuleStatus.FAILED

    async def test_flushes_instance_trigger_handlers(self) -> None:
        """stop() calls flush on handlers from self.trigger_handlers."""
        cls = _make_module_cls()
        module = _instantiate(cls)
        module.context.callbacks.send_message = AsyncMock()

        h1 = AsyncMock()
        h2 = AsyncMock()
        module.trigger_handlers = {"proto_a": (h1,), "proto_b": (h2,)}

        with patch.object(module, "cleanup", new_callable=AsyncMock):
            await module.stop()

        h1.flush_file_history.assert_awaited_once_with(module.context)
        h2.flush_file_history.assert_awaited_once_with(module.context)

    async def test_does_not_call_clear_mission_cache(self) -> None:
        """stop() must not call clear_ch_mission_cache or clear_fh_mission_cache (regression)."""
        cls = _make_module_cls()
        module = _instantiate(cls)
        module.context.callbacks.send_message = AsyncMock()

        handler = AsyncMock()
        module.trigger_handlers = {"proto": (handler,)}

        with patch.object(module, "cleanup", new_callable=AsyncMock):
            await module.stop()

        handler.clear_ch_mission_cache.assert_not_called()
        handler.clear_fh_mission_cache.assert_not_called()

    async def test_empty_trigger_handlers_no_error(self) -> None:
        """stop() succeeds when trigger_handlers is empty (no handlers initialized)."""
        cls = _make_module_cls()
        module = _instantiate(cls)
        module.context.callbacks.send_message = AsyncMock()
        assert module.trigger_handlers == {}

        with patch.object(module, "cleanup", new_callable=AsyncMock):
            await module.stop()

        assert module.status == ModuleStatus.STOPPED


class TestResolveTools:
    """Tests for BaseModule._resolve_tools."""

    async def test_with_registry_and_communication(self) -> None:
        """_resolve_tools calls build_tool_cache with services."""
        cls = _make_module_cls()
        module = _instantiate(cls)
        module.context.registry = Mock()
        module.context.communication = Mock()

        setup_data = _LcSetupModel()

        with patch.object(type(setup_data), "build_tool_cache", new_callable=AsyncMock, return_value=ToolCache()) as mock_build:
            await module._resolve_tools(setup_data)

        mock_build.assert_awaited_once_with(
            module.context.registry,
            module.context.communication,
        )
        assert isinstance(module.context.tool_cache, ToolCache)

    async def test_without_registry(self) -> None:
        """_resolve_tools passes None registry to build_tool_cache."""
        cls = _make_module_cls()
        module = _instantiate(cls)
        module.context.registry = None
        module.context.communication = Mock()

        setup_data = _LcSetupModel()

        with patch.object(type(setup_data), "build_tool_cache", new_callable=AsyncMock, return_value=ToolCache()) as mock_build:
            await module._resolve_tools(setup_data)

        mock_build.assert_awaited_once_with(None, module.context.communication)

    async def test_without_communication(self) -> None:
        """_resolve_tools passes None communication to build_tool_cache."""
        cls = _make_module_cls()
        module = _instantiate(cls)
        module.context.registry = Mock()
        module.context.communication = None

        setup_data = _LcSetupModel()

        with patch.object(type(setup_data), "build_tool_cache", new_callable=AsyncMock, return_value=ToolCache()) as mock_build:
            await module._resolve_tools(setup_data)

        mock_build.assert_awaited_once_with(module.context.registry, None)


class TestStartConfigSetup:
    """Tests for BaseModule.start_config_setup."""

    async def test_success_path(self) -> None:
        """start_config_setup resolves tools, runs config, sends result."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        callback = AsyncMock()
        setup_data = _LcSetupModel(name="initial")

        with (
            patch.object(module, "_resolve_tools", new_callable=AsyncMock),
            patch.object(module, "run_config_setup", new_callable=AsyncMock, return_value=setup_data),
            patch.object(cls, "create_setup_model", new_callable=AsyncMock, return_value=setup_data),
        ):
            await module.start_config_setup(setup_data, callback)

        assert module.status == ModuleStatus.STOPPING
        callback.assert_awaited_once()

    async def test_error_sets_failed(self) -> None:
        """start_config_setup sets FAILED on exception."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        callback = AsyncMock()
        setup_data = _LcSetupModel()

        with patch.object(module, "_resolve_tools", new_callable=AsyncMock, side_effect=RuntimeError("resolve fail")):
            await module.start_config_setup(setup_data, callback)

        assert module.status == ModuleStatus.FAILED


class TestTriggerHandlerIsolation:
    """Tests for per-instance trigger_handlers (not ClassVar)."""

    def test_init_creates_empty_dict(self) -> None:
        """__init__ sets trigger_handlers to an empty dict."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        assert module.trigger_handlers == {}
        assert isinstance(module.trigger_handlers, dict)

    def test_two_instances_independent(self) -> None:
        """Mutating one instance's trigger_handlers does not affect another."""
        cls = _make_module_cls()
        m1 = _instantiate(cls)
        m2 = _instantiate(cls)

        m1.trigger_handlers["proto"] = (Mock(),)

        assert "proto" in m1.trigger_handlers
        assert "proto" not in m2.trigger_handlers

    def test_not_a_class_attribute(self) -> None:
        """trigger_handlers lives on the instance, not the class."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        assert "trigger_handlers" in module.__dict__
        assert "trigger_handlers" not in cls.__dict__

    async def test_start_stores_handlers_on_instance(self) -> None:
        """start() stores init_handlers result on self.trigger_handlers."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        fake_handlers = {"proto": (Mock(),)}
        module.triggers_discoverer = Mock()
        module.triggers_discoverer.init_handlers = Mock(return_value=fake_handlers)

        callback = AsyncMock()
        with (
            patch.object(module, "initialize", new_callable=AsyncMock),
            patch.object(module, "_run_lifecycle", new_callable=AsyncMock),
            patch.object(module, "stop", new_callable=AsyncMock),
        ):
            await module.start(
                _LcInputModel(root=_LcInputTrigger()),
                _LcSetupModel(),
                callback,
            )

        assert module.trigger_handlers is fake_handlers

    async def test_run_passes_instance_handlers_to_get_trigger(self) -> None:
        """run() passes self.trigger_handlers as first arg to get_trigger."""
        cls = _make_module_cls()
        module = _instantiate(cls)

        mock_handler = AsyncMock()
        fake_handlers = {"lc_test": (mock_handler,)}
        module.trigger_handlers = fake_handlers

        module.triggers_discoverer = Mock()
        module.triggers_discoverer.get_trigger.return_value = mock_handler

        input_data = _LcInputModel(root=_LcInputTrigger(message="hi"))
        await module.run(input_data, _LcSetupModel())

        module.triggers_discoverer.get_trigger.assert_called_once_with(
            fake_handlers,
            "lc_test",
            input_data.root,
        )

    async def test_two_concurrent_modules_isolated_handlers(self) -> None:
        """Two module instances from the same class get independent handler dicts."""
        cls = _make_module_cls()
        m1 = _instantiate(cls)
        m2 = _instantiate(cls)

        h1 = {"proto": (Mock(),)}
        h2 = {"proto": (Mock(),)}
        m1.triggers_discoverer = Mock()
        m1.triggers_discoverer.init_handlers = Mock(return_value=h1)
        m2.triggers_discoverer = Mock()
        m2.triggers_discoverer.init_handlers = Mock(return_value=h2)

        callback = AsyncMock()
        with (
            patch.object(m1, "initialize", new_callable=AsyncMock),
            patch.object(m1, "_run_lifecycle", new_callable=AsyncMock),
            patch.object(m1, "stop", new_callable=AsyncMock),
            patch.object(m2, "initialize", new_callable=AsyncMock),
            patch.object(m2, "_run_lifecycle", new_callable=AsyncMock),
            patch.object(m2, "stop", new_callable=AsyncMock),
        ):
            await asyncio.gather(
                m1.start(_LcInputModel(root=_LcInputTrigger()), _LcSetupModel(), callback),
                m2.start(_LcInputModel(root=_LcInputTrigger()), _LcSetupModel(), callback),
            )

        assert m1.trigger_handlers is h1
        assert m2.trigger_handlers is h2
        assert m1.trigger_handlers is not m2.trigger_handlers

    async def test_stop_only_flushes_own_handlers(self) -> None:
        """Stopping one module does not flush another module's handlers."""
        cls = _make_module_cls()
        m1 = _instantiate(cls)
        m2 = _instantiate(cls)
        m1.context.callbacks.send_message = AsyncMock()
        m2.context.callbacks.send_message = AsyncMock()

        h1 = AsyncMock()
        h2 = AsyncMock()
        m1.trigger_handlers = {"proto": (h1,)}
        m2.trigger_handlers = {"proto": (h2,)}

        with patch.object(m1, "cleanup", new_callable=AsyncMock):
            await m1.stop()

        h1.flush_file_history.assert_awaited_once()
        h2.flush_file_history.assert_not_called()
