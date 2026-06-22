"""BaseModule is the abstract base for all modules in the DigitalKin SDK."""

import asyncio
import json
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from typing import Any, ClassVar, Generic

from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.grpc_servers.utils.utility_schema_extender import UtilitySchemaExtender
from digitalkin.logger import logger
from digitalkin.models.module.module import ModuleCodeModel, ModuleStatus
from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.module_types import (
    DataModel,
    InputModelT,
    OutputModelT,
    SecretModelT,
    SetupModelT,
)
from digitalkin.models.module.select_schema import SelectSchema
from digitalkin.models.module.tool_cache import ToolCache
from digitalkin.models.module.utility import EndOfStreamOutput, UtilityProtocol
from digitalkin.models.services.registry import RegistryModuleType
from digitalkin.models.services.storage import BaseRole
from digitalkin.models.settings.module import get_module_settings
from digitalkin.modules.trigger_handler import TriggerHandler
from digitalkin.services.services_config import ServicesConfig, ServicesStrategy
from digitalkin.utils.package_discover import ModuleDiscoverer
from digitalkin.utils.schema_splitter import SchemaSplitter

# Pre-built generic; avoids regenerating one per start()/stop().
_EndOfStreamDataModel: type[DataModel] = DataModel[EndOfStreamOutput]


class BaseModule(  # Module SDK base class requires many public methods # noqa: PLR0904
    ABC,
    Generic[
        InputModelT,
        OutputModelT,
        SetupModelT,
        SecretModelT,
    ],
):
    """BaseModule is the abstract base for all modules in the DigitalKin SDK."""

    name: str
    description: str = ""

    setup_format: type[SetupModelT]
    input_format: type[InputModelT]
    select_format: type[SelectSchema] = SelectSchema
    output_format: type[OutputModelT]
    secret_format: type[SecretModelT]
    metadata: ClassVar[dict[str, Any]]

    context: ModuleContext
    triggers_discoverer: ClassVar[ModuleDiscoverer]
    _extended_input_format: ClassVar[type[DataModel] | None] = None
    _shared: ClassVar[dict[str, Any]] = {}
    _builds_tool_cache: ClassVar[bool] = False
    registry_type: ClassVar[RegistryModuleType] = RegistryModuleType.UNSPECIFIED
    """Only ArchetypeModule (tool-composing) resolves a tool cache."""

    @classmethod
    def clear_shared(cls) -> None:
        """Swap shared cache with a fresh dict.

        Running tasks keep their existing ``context.shared`` reference
        (old dict). New module instances get the fresh empty dict.
        """
        cls._shared = {}

    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]]
    services_config_params: ClassVar[dict[str, dict[str, Any | None] | None]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Ensure each subclass has its own copy of mutable class variables."""
        super().__init_subclass__(**kwargs)
        if "services_config_strategies" not in cls.__dict__:
            cls.services_config_strategies = (
                dict(cls.services_config_strategies) if "services_config_strategies" in dir(cls) else {}
            )
        if "services_config_params" not in cls.__dict__:
            cls.services_config_params = (
                dict(cls.services_config_params) if "services_config_params" in dir(cls) else {}
            )

    services_config: ServicesConfig

    @classmethod
    def get_module_id(cls) -> str:
        """Get the module ID from settings or metadata.

        Returns:
            The module_id from ModuleSettings.id (env DIGITALKIN_MODULE_ID), or
            metadata module_id, or "unknown" if neither exists.
        """
        return get_module_settings().id or cls.metadata.get("module_id", "unknown")

    def _init_strategies(self, mission_id: str, setup_id: str, setup_version_id: str) -> dict[str, Any]:
        """Initialize the services configuration.

        Returns:
            dict of services with name: Strategy
                cost: CostStrategy
                filesystem: FilesystemStrategy
                identity: IdentityStrategy
                registry: RegistryStrategy
                storage: StorageStrategy
                user_profile: UserProfileStrategy
        """
        logger.debug("Service initialisation: %s", self.services_config_strategies.keys())
        return {
            service_name: self.services_config.init_strategy(
                service_name,
                mission_id,
                setup_id,
                setup_version_id,
            )
            for service_name in self.services_config.valid_strategy_names()
        }

    def __init__(
        self,
        job_id: str,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        request_metadata: dict[str, str] | None = None,
        tool_cache: ToolCache | None = None,
    ) -> None:
        """Initialize the module.

        Args:
            job_id: Unique job identifier.
            mission_id: Mission identifier.
            setup_id: Setup identifier.
            setup_version_id: Setup version identifier.
            request_metadata: gRPC request metadata (headers) from the incoming request.
            tool_cache: Pre-resolved ToolCache (skips per-request gRPC resolution).
        """
        self._status = ModuleStatus.CREATED
        self._prebuilt_tool_cache = tool_cache
        self.trigger_handlers: dict[str, tuple] = {}
        # Set by idempotent prepare() so start() can short-circuit.
        self._prepared: bool = False

        self.context = ModuleContext(
            **self._init_strategies(mission_id, setup_id, setup_version_id),
            session={
                "setup_id": setup_id,
                "mission_id": mission_id,
                "setup_version_id": setup_version_id,
                "job_id": job_id,
            },
            borrowed=self.services_config._stateless_strategies,  # noqa: SLF001
            callbacks={"logger": logger},
            request_metadata=request_metadata,
            shared=self._shared,
        )

    @property
    def status(self) -> ModuleStatus:
        """The module status.

        Returns:
            The module status
        """
        return self._status

    @classmethod
    async def get_secret_format(cls, *, llm_format: bool) -> str:
        """Get the JSON schema of the secret format model.

        Args:
            llm_format: If True, return LLM-optimized schema format with inlined
                references and simplified structure.

        Returns:
            The JSON schema of the secret format as a JSON string.

        Raises:
            NotImplementedError: If the `secret_format` class attribute is not defined.
        """
        if cls.secret_format is not None:
            if llm_format:
                result_json, result_ui = SchemaSplitter.split(cls.secret_format.model_json_schema())
                return json.dumps({"json_schema": result_json, "ui_schema": result_ui}, indent=2)
            return json.dumps(cls.secret_format.model_json_schema(), indent=2)
        msg = f"{cls.__name__}' class does not define a 'secret_format'."
        raise NotImplementedError(msg)

    @classmethod
    async def get_input_format(cls, *, llm_format: bool) -> str:
        """Get the JSON schema of the input format model.

        Args:
            llm_format: If True, return LLM-optimized schema format with inlined
                references and simplified structure.

        Returns:
            The JSON schema of the input format as a JSON string.

        Raises:
            NotImplementedError: If the `input_format` class attribute is not defined.
        """
        if cls.input_format is None:
            msg = f"{cls.__name__}' class does not define an 'input_format'."
            raise NotImplementedError(msg)

        extended_model = UtilitySchemaExtender.create_extended_input_model(cls.input_format)

        if llm_format:
            result_json, _ = SchemaSplitter.split(extended_model.model_json_schema())
            return json.dumps({"json_schema": result_json}, indent=2)
        return json.dumps(extended_model.model_json_schema(), indent=2)

    @classmethod
    async def get_select_input_format(cls) -> str:
        """Get the JSON schema for trigger selection UI.

        Returns:
            The JSON schema with json_schema and ui_schema keys as a JSON string,
            or empty object if no select_format is defined.
        """
        if cls.select_format is None:
            return json.dumps({}, indent=2)

        protocols_info = cls.triggers_discoverer.get_registered_protocols_with_info(exclude_utility=True)
        select_schema = cls.select_format.build(protocols_info)

        if select_schema is None:
            return json.dumps({}, indent=2)

        return json.dumps(select_schema, indent=2)

    @classmethod
    def build_registry_documentation(cls) -> str:
        """Assemble the registry documentation: author description + LLM-readable trigger table.

        Enforces an author-written description of the archetype/tool specificity
        (``cls.description``, falling back to ``metadata['description']``), then appends a
        markdown table of the module's non-utility triggers for registry index search.

        Returns:
            Markdown documentation string sent as the registration ``documentation``.

        Raises:
            ValueError: If the module declares no description.
        """
        description = (cls.description or cls.metadata.get("description", "")).strip()
        if not description:
            msg = f"{cls.__name__} must define a non-empty 'description' for registry indexing"
            raise ValueError(msg)
        protocols = cls.triggers_discoverer.get_registered_protocols_with_info(exclude_utility=True)
        rows = "\n".join(f"| {protocol} | {desc} |" for protocol, desc in sorted(protocols.items()))
        table = f"| Trigger | Description |\n| --- | --- |\n{rows}" if rows else "_No triggers._"
        return f"{description}\n\n## Triggers\n\n{table}"

    @classmethod
    async def get_output_format(cls, *, llm_format: bool) -> str:
        """Get the JSON schema of the output format model.

        Args:
            llm_format: If True, return LLM-optimized schema format with inlined
                references and simplified structure.

        Returns:
            The JSON schema of the output format as a JSON string.

        Raises:
            NotImplementedError: If the `output_format` class attribute is not defined.
        """
        if cls.output_format is None:
            msg = f"'{cls.__name__}' class does not define an 'output_format'."
            raise NotImplementedError(msg)

        extended_model = UtilitySchemaExtender.create_extended_output_model(cls.output_format)

        if llm_format:
            result_json, _ = SchemaSplitter.split(extended_model.model_json_schema())
            return json.dumps({"json_schema": result_json}, indent=2)
        return json.dumps(extended_model.model_json_schema(), indent=2)

    @classmethod
    async def get_config_setup_format(cls, *, llm_format: bool) -> str:
        """Gets the JSON schema of the config setup format model.

        The config setup format is used only to initialize the module with configuration
        data. It includes fields marked with `json_schema_extra={"config": True}` and
        excludes hidden runtime fields.

        Dynamic schema fields are always resolved when generating the schema, as this
        method is typically called during module discovery or schema generation where
        fresh values are needed.

        Args:
            llm_format: If True, return LLM-optimized schema format with inlined
                references and simplified structure.

        Returns:
            The JSON schema of the config setup format as a JSON string.

        Raises:
            NotImplementedError: If the `setup_format` class attribute is not defined.
        """
        if cls.setup_format is not None:
            setup_format = await cls.setup_format.get_clean_model(config_fields=True, hidden_fields=False, force=True)
            if llm_format:
                result_json, result_ui = SchemaSplitter.split(setup_format.model_json_schema())
                return json.dumps({"json_schema": result_json, "ui_schema": result_ui}, indent=2)
            return json.dumps(setup_format.model_json_schema(), indent=2)
        msg = "'%s' class does not define an 'config_setup_format'."
        raise NotImplementedError(msg)

    @classmethod
    async def get_setup_format(cls, *, llm_format: bool) -> str:
        """Gets the JSON schema of the setup format model.

        The setup format is used at runtime and includes hidden fields but excludes
        config-only fields. This is the schema used when running the module.

        Dynamic schema fields are always resolved when generating the schema, as this
        method is typically called during module discovery or schema generation where
        fresh values are needed.

        Args:
            llm_format: If True, return LLM-optimized schema format with inlined
                references and simplified structure.

        Returns:
            The JSON schema of the setup format as a JSON string.

        Raises:
            NotImplementedError: If the `setup_format` class attribute is not defined.
        """
        if cls.setup_format is not None:
            setup_format = await cls.setup_format.get_clean_model(config_fields=False, hidden_fields=True, force=True)
            if llm_format:
                result_json, _ = SchemaSplitter.split(setup_format.model_json_schema())
                return json.dumps({"json_schema": result_json}, indent=2)
            return json.dumps(setup_format.model_json_schema(), indent=2)
        msg = "'%s' class does not define an 'setup_format'."
        raise NotImplementedError(msg)

    @classmethod
    async def get_cost_format(cls, *, llm_format: bool) -> str:
        """Get the JSON schema of the cost configuration.

        Extracts CostConfig from `services_config_params["cost"]["config"]`
        and returns as JSON schema.

        Args:
            llm_format: If True, return LLM-optimized schema format with inlined
                references and simplified structure.

        Returns:
            The JSON schema of the cost configuration as a JSON string.
        """
        cost_params = cls.services_config_params.get("cost", {})
        config = cost_params.get("config", {}) if cost_params else {}

        if not config:
            return json.dumps({}, indent=2)

        cost_schema = {
            name: {
                "name": cost_config.cost_name,
                "type": cost_config.cost_type,
                "description": cost_config.description,
                "unit": cost_config.unit,
                "rate": cost_config.rate,
            }
            for name, cost_config in config.items()
        }

        if llm_format:
            result_json, result_ui = SchemaSplitter.split({"costs": cost_schema})
            return json.dumps({"json_schema": result_json, "ui_schema": result_ui}, indent=2)
        return json.dumps(cost_schema, indent=2)

    @classmethod
    def create_config_setup_model(cls, config_setup_data: dict[str, Any]) -> SetupModelT:
        """Create the setup model from the setup data.

        Args:
            config_setup_data: The setup data to create the model from.

        Returns:
            The setup model.
        """
        return cls.setup_format(**config_setup_data)

    @classmethod
    def create_input_model(cls, input_data: dict[str, Any]) -> DataModel:
        """Create the input model from the input data.

        Args:
            input_data: The input data to create the model from.

        Returns:
            The input model, validated against the extended format that
            includes SDK utility protocols (healthcheck, etc.).
        """
        model_cls = cls._extended_input_format or cls.input_format
        return model_cls(**input_data)

    @classmethod
    async def create_setup_model(cls, setup_data: dict[str, Any], *, config_fields: bool = False) -> SetupModelT:
        """Create the setup model from the setup data.

        Creates a filtered setup model instance based on the provided data.
        Uses `get_clean_model()` internally to get the appropriate model class
        with field filtering applied.

        Args:
            setup_data: The setup data to create the model from.
            config_fields: If True, include only fields with json_schema_extra["config"] == True.

        Returns:
            An instance of the setup model with the provided data.
        """
        model_cls = await cls.setup_format.get_clean_model(config_fields=config_fields, hidden_fields=True)
        return model_cls(**setup_data)

    @classmethod
    def create_secret_model(cls, secret_data: dict[str, Any]) -> SecretModelT:
        """Create the secret model from the secret data.

        Args:
            secret_data: The secret data to create the model from.

        Returns:
            The secret model.
        """
        return cls.secret_format(**secret_data)

    @classmethod
    def create_output_model(cls, output_data: dict[str, Any]) -> OutputModelT:
        """Create the output model from the output data.

        Args:
            output_data: The output data to create the model from.

        Returns:
            The output model.
        """
        return cls.output_format(**output_data)

    @classmethod
    def discover(cls) -> None:
        """Discover and register all TriggerHandler subclasses in the specified package or current directory.

        Dynamically import all Python modules in the specified package or current directory,
        triggering class registrations for subclasses of TriggerHandler whose names end with 'Trigger'.

        If a package is provided, all .py files within its path are imported; otherwise, the current
        working directory is searched. For each imported module, any class matching the criteria is
        registered via cls.register(). Errors during import are logged at debug level.

        Built-in healthcheck handlers (ping, services, status) are automatically registered
        to provide standard healthcheck functionality for all modules.
        """
        from digitalkin.models.module.utility import UtilityRegistry

        cls.triggers_discoverer.discover_modules()

        for trigger_cls in UtilityRegistry.get_builtin_triggers():
            cls.triggers_discoverer.register_trigger(trigger_cls)

        if cls.input_format is not None:
            cls._extended_input_format = UtilitySchemaExtender.create_extended_input_model(cls.input_format)

        logger.debug("discovered: %s", cls.triggers_discoverer)

    @classmethod
    def register(cls, handler_cls: type[TriggerHandler]) -> type[TriggerHandler]:
        """Dynamically register the trigger class.

        Args:
            handler_cls: type of the trigger handler to register.

        Returns:
            type of the trigger handler.
        """
        return cls.triggers_discoverer.register_trigger(handler_cls)

    @abstractmethod
    async def initialize(self, context: ModuleContext, setup_data: SetupModelT) -> None:
        """Initialize the module."""
        ...

    async def run(
        self,
        input_data: InputModelT,
        setup_data: SetupModelT,
    ) -> None:
        """Run the module by dispatching to the appropriate trigger handler.

        Args:
            input_data: Input data to process.
            setup_data: Configuration data for the module.

        Raises:
            ValueError: If no handler for the protocol is found.
        """
        model_cls = self._extended_input_format or self.input_format
        input_instance = model_cls.model_validate(input_data)

        if (
            cost_limits := input_instance.model_dump().get("cost_limits")
        ) is not None and self.context.cost is not None:
            await self.context.cost.set_limits(cost_limits)

        handler_instance = self.triggers_discoverer.get_trigger(
            self.trigger_handlers,
            input_instance.root.protocol,
            input_instance.root,
        )

        logger.debug(
            "debug:run dispatching protocol=%s handler=%s",
            input_instance.root.protocol,
            type(handler_instance).__name__,
        )
        await handler_instance.handle(
            input_instance.root,
            setup_data,
            self.context,
        )
        await handler_instance.flush_file_history(self.context)

    @abstractmethod
    async def cleanup(self) -> None:
        """Run the module."""
        ...

    async def run_config_setup(  # Default implementation; subclasses may use self # noqa: PLR6301
        self,
        context: ModuleContext,  # Available for subclass overrides # noqa: ARG002
        config_setup_data: SetupModelT,
    ) -> SetupModelT:
        """Run config setup the module.

        The config setup is used to initialize the setup with configuration data.
        This method is typically used to set up the module with necessary configuration before running it,
        especially for processing data like files.
        The function needs to save the setup in the storage.
        The module will be initialize with the setup and not the config setup.
        This method is optional, the config setup and setup can be the same.

        Returns:
            The updated setup model after running the config setup.
        """
        return config_setup_data

    async def _run_lifecycle(
        self,
        input_data: InputModelT,
        setup_data: SetupModelT,
    ) -> None:
        """Run the module lifecycle.

        Raises:
            asyncio.CancelledError: If the module is cancelled
        """
        try:
            logger.info("Starting module %s", self.name, extra=self.context.session.current_ids())
            await self.run(input_data, setup_data)
            logger.info("Module %s finished", self.name, extra=self.context.session.current_ids())
        except asyncio.CancelledError:
            self._status = ModuleStatus.CANCELLED
            logger.info("Module %s cancelled", self.name, extra=self.context.session.current_ids())
            raise
        except PermissionDeniedError as e:
            self._status = ModuleStatus.FAILED
            logger.warning("Permission denied in module %s: %s", self.name, e, extra=self.context.session.current_ids())
            await self._notify_permission_denied(self.context.callbacks.send_message, e)
        except Exception as e:
            self._status = ModuleStatus.FAILED
            logger.exception("Error inside module %s", self.name, extra=self.context.session.current_ids())
            try:
                await self.context.callbacks.send_message(
                    ModuleCodeModel(
                        code="Error",
                        short_description="Module execution failed",
                        message=str(e),
                    )
                )
            except Exception:
                logger.exception("Failed to send error callback", extra=self.context.session.current_ids())
        else:
            self._status = ModuleStatus.STOPPING

    async def _notify_permission_denied(
        self,
        callback: Callable[..., Coroutine[Any, Any, None]],
        e: PermissionDeniedError,
    ) -> None:
        """Send a PermissionDenied notification to the user via the callback.

        Args:
            callback: The output callback installed on the module context.
            e: The permission-denied error raised by a service call.
        """
        try:
            await callback(
                ModuleCodeModel(
                    code="PermissionDenied",
                    short_description="Permission denied",
                    message=str(e),
                )
            )
        except Exception:
            logger.exception("Failed to send permission-denied callback", extra=self.context.session.current_ids())

    async def prepare(
        self,
        setup_data: SetupModelT,
        callback: Callable[[OutputModelT | ModuleCodeModel | DataModel[UtilityProtocol]], Coroutine[Any, Any, None]],
    ) -> None:
        """Wire callbacks, build tool cache, run ``initialize()``, discover triggers.

        Idempotent — second call is a no-op. Lets the dial-back
        orchestrator pay the ``initialize()`` cost off the critical path.

        Args:
            setup_data: The setup configuration for the module.
            callback: Output callback installed on the module context.

        Raises:
            Exception: anything raised by ``build_tool_cache``,
                ``initialize``, or ``init_handlers`` propagates so the
                caller can convert to ``stream.error``.
        """
        if self._prepared:
            return
        from digitalkin.core.profiling.step_timer import StepTimer

        timer = StepTimer()
        self.context.callbacks.send_message = callback
        timer.mark("set_callback")

        if self._builds_tool_cache:
            tool_cache = self._prebuilt_tool_cache or await setup_data.build_tool_cache(
                self.context.registry,
                self.context.communication,
            )
            if tool_cache.entries:
                self.context.tool_cache = tool_cache
            timer.mark("build_tool_cache")

        await self.initialize(self.context, setup_data)
        timer.mark("initialize")

        self.trigger_handlers = self.triggers_discoverer.init_handlers(self.context)
        timer.mark("init_handlers")

        self._prepared = True
        timer.log("module.prepare", task_id=self.context.session.current_ids().get("job_id", ""))

    async def start(
        self,
        input_data: InputModelT,
        setup_data: SetupModelT,
        callback: Callable[[OutputModelT | ModuleCodeModel | DataModel[UtilityProtocol]], Coroutine[Any, Any, None]],
        done_callback: Callable | None = None,
    ) -> None:
        """Start the module."""
        from digitalkin.core.profiling.step_timer import StepTimer

        timer = StepTimer()
        try:
            await self.prepare(setup_data, callback)
            timer.mark("prepare")
        except PermissionDeniedError as e:
            self._status = ModuleStatus.FAILED
            logger.warning("Permission denied initializing module: %s", e, extra=self.context.session.current_ids())
            await self._notify_permission_denied(callback, e)
            if done_callback is not None:
                await done_callback(None)
            await self.stop()
            return
        except Exception as e:
            self._status = ModuleStatus.FAILED
            short_description = "Error initializing module"
            error_detail = f"{type(e).__name__}: {e}" if str(e) else type(e).__name__
            logger.exception("%s: %s", short_description, error_detail, extra=self.context.session.current_ids())
            await callback(
                ModuleCodeModel(
                    code="Error",
                    short_description=short_description,
                    message=error_detail,
                )
            )
            if done_callback is not None:
                await done_callback(None)
            await self.stop()
            return

        try:
            await self._run_lifecycle(input_data, setup_data)
            timer.mark("run_lifecycle")
        except Exception:
            self._status = ModuleStatus.FAILED
            logger.exception("Error during module lifecycle", extra=self.context.session.current_ids())
        finally:
            timer.log("module.start", task_id=self.context.session.current_ids().get("job_id", ""))
            await self.stop()

    async def stop(self) -> None:
        """Stop the module. Idempotent — second call is a no-op."""
        t0 = time.perf_counter_ns()
        if self._status in {ModuleStatus.STOPPED, ModuleStatus.FAILED}:
            return
        try:  # noqa: PLW0717
            self._status = ModuleStatus.STOPPING
            await self.cleanup()
            t1 = time.perf_counter_ns()
            try:
                for handlers in self.trigger_handlers.values():
                    for handler in handlers:
                        await handler.flush_file_history(self.context)
            except Exception:
                logger.warning("Failed to flush handler history during stop", exc_info=True)
            t2 = time.perf_counter_ns()
            if "send_message" in vars(self.context.callbacks):
                await self.context.callbacks.send_message(
                    _EndOfStreamDataModel(
                        root=EndOfStreamOutput(),
                        annotations={"role": BaseRole.SYSTEM},
                    )
                )
            else:
                logger.debug("send_message not registered; skipping end-of-stream (config-setup path)")
            t3 = time.perf_counter_ns()
            self._status = ModuleStatus.STOPPED
            ids = self.context.session.current_ids()
            logger.info(
                "[close-debug] module.stop: cleanup=%.2fms flush=%.2fms eos=%.2fms "
                "total=%.2fms t_done_ns=%d task_id=%s mission_id=%s",
                (t1 - t0) / 1e6,
                (t2 - t1) / 1e6,
                (t3 - t2) / 1e6,
                (t3 - t0) / 1e6,
                t3,
                ids.get("job_id", ""),
                ids.get("mission_id", ""),
            )
        except Exception:
            self._status = ModuleStatus.FAILED
            logger.exception("Error stopping module", extra=self.context.session.current_ids())

    async def _resolve_tools(self, config_setup_data: SetupModelT) -> None:
        """Resolve tool references and build cache.

        Args:
            config_setup_data: Setup data containing tool references.
        """
        if not self._builds_tool_cache:
            return
        logger.debug("Starting tool resolution", extra=self.context.session.current_ids())
        # New setup version: discard any inherited resolved_tools so the live
        # tool-module schemas are re-fetched. Mission runs reuse the persisted
        # resolved_tools (via build_tool_cache in start()) and never reach here.
        config_setup_data.resolved_tools = {}
        tool_cache = await config_setup_data.build_tool_cache(self.context.registry, self.context.communication)
        self.context.tool_cache = tool_cache
        logger.debug(
            "Tool cache built with %d entries: %s",
            len(tool_cache.entries),
            list(tool_cache.entries.keys()),
            extra=self.context.session.current_ids(),
        )

    async def start_config_setup(
        self,
        config_setup_data: SetupModelT,
        callback: Callable[[SetupModelT | ModuleCodeModel], Coroutine[Any, Any, None]],
    ) -> None:
        """Run config setup lifecycle with tool resolution in parallel.

        Args:
            config_setup_data: Initial setup data to configure.
            callback: Callback to send the configured setup model.
        """
        try:  # noqa: PLW0717
            logger.debug("Run Config Setup lifecycle", extra=self.context.session.current_ids())
            self._status = ModuleStatus.RUNNING
            self.context.callbacks.set_config_setup = callback

            # Resolve tools first so config setup sees populated companion fields.
            await self._resolve_tools(config_setup_data)
            updated_config = await self.run_config_setup(self.context, config_setup_data)

            setup_model = await self.create_setup_model(updated_config.model_dump())
            await callback(setup_model)
            self._status = ModuleStatus.STOPPING
        except Exception:
            self._status = ModuleStatus.FAILED
            logger.exception("Error during config setup lifecycle", extra=self.context.session.current_ids())
