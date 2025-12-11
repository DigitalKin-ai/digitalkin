"""BaseModule is the abstract base for all modules in the DigitalKin SDK."""

import asyncio
import json
import os
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from typing import Any, ClassVar, Generic

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
from digitalkin.models.module.utility import EndOfStreamOutput
from digitalkin.models.services.storage import BaseRole
from digitalkin.modules.trigger_handler import TriggerHandler
from digitalkin.services.services_config import ServicesConfig, ServicesStrategy
from digitalkin.utils.llm_ready_schema import llm_ready_schema
from digitalkin.utils.package_discover import ModuleDiscoverer


class BaseModule(  # noqa: PLR0904
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
    description: str

    setup_format: type[SetupModelT]
    input_format: type[InputModelT]
    output_format: type[OutputModelT]
    secret_format: type[SecretModelT]
    metadata: ClassVar[dict[str, Any]]

    context: ModuleContext
    triggers_discoverer: ClassVar[ModuleDiscoverer]

    # service config params
    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
    services_config_params: ClassVar[dict[str, dict[str, Any | None] | None]] = {}
    services_config: ServicesConfig

    @classmethod
    def get_module_id(cls) -> str:
        """Get the module ID from environment variable or metadata.

        Returns:
            The module_id from DIGITALKIN_MODULE_ID env var, or metadata module_id,
            or "unknown" if neither exists.
        """
        return os.environ.get("DIGITALKIN_MODULE_ID") or cls.metadata.get("module_id", "unknown")

    def _init_strategies(self, mission_id: str, setup_id: str, setup_version_id: str) -> dict[str, Any]:
        """Initialize the services configuration.

        Returns:
            dict of services with name: Strategy
                agent: AgentStrategy
                cost: CostStrategy
                filesystem: FilesystemStrategy
                identity: IdentityStrategy
                registry: RegistryStrategy
                snapshot: SnapshotStrategy
                storage: StorageStrategy
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
    ) -> None:
        """Initialize the module."""
        self._status = ModuleStatus.CREATED

        # Initialize minimum context
        self.context = ModuleContext(
            # Initialize services configuration
            **self._init_strategies(mission_id, setup_id, setup_version_id),
            session={
                "setup_id": setup_id,
                "mission_id": mission_id,
                "setup_version_id": setup_version_id,
                "job_id": job_id,
            },
            callbacks={"logger": logger},
        )

    @property
    def status(self) -> ModuleStatus:
        """Get the module status.

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
                return json.dumps(llm_ready_schema(cls.secret_format), indent=2)
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
            return json.dumps(llm_ready_schema(extended_model), indent=2)
        return json.dumps(extended_model.model_json_schema(), indent=2)

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
            return json.dumps(llm_ready_schema(extended_model), indent=2)
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
                return json.dumps(llm_ready_schema(setup_format), indent=2)
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
                return json.dumps(llm_ready_schema(setup_format), indent=2)
            return json.dumps(setup_format.model_json_schema(), indent=2)
        msg = "'%s' class does not define an 'setup_format'."
        raise NotImplementedError(msg)

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
    def create_input_model(cls, input_data: dict[str, Any]) -> InputModelT:
        """Create the input model from the input data.

        Args:
            input_data: The input data to create the model from.

        Returns:
            The input model.
        """
        return cls.input_format(**input_data)

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
        from digitalkin.models.module.utility import UtilityRegistry  # noqa: PLC0415

        cls.triggers_discoverer.discover_modules()

        # Auto-register built-in SDK triggers (healthcheck, etc.)
        for trigger_cls in UtilityRegistry.get_builtin_triggers():
            cls.triggers_discoverer.register_trigger(trigger_cls)

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

    async def run_config_setup(  # noqa: PLR6301
        self,
        context: ModuleContext,  # noqa: ARG002
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

    @abstractmethod
    async def initialize(self, context: ModuleContext, setup_data: SetupModelT) -> None:
        """Initialize the module."""
        raise NotImplementedError

    async def run(
        self,
        input_data: InputModelT,
        setup_data: SetupModelT,
    ) -> None:
        """Run the module with the given input and setup data.

        This method validates the input data, determines the protocol from the input,
        and dispatches the request to the corresponding trigger handler. The trigger handler
        is responsible for processing the input and invoking the callback with the result.

        Triggers:
            - The method is triggered when a module run is requested with specific input and setup data.
            - The protocol specified in the input determines which trigger handler is invoked.

        Args:
            input_data (InputModelT): The input data to be processed by the module.
            setup_data (SetupModelT): The setup or configuration data required for the module.

        Raises:
            ValueError: If no handler for the protocol is found.
        """
        input_instance = self.input_format.model_validate(input_data)
        handler_instance = self.triggers_discoverer.get_trigger(
            input_instance.root.protocol,
            input_instance.root,
        )

        await handler_instance.handle(
            input_instance.root,
            setup_data,
            self.context,
        )

    @abstractmethod
    async def cleanup(self) -> None:
        """Run the module."""
        raise NotImplementedError

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
            logger.error("Module %s cancelled", self.name, extra=self.context.session.current_ids())
        except Exception:
            self._status = ModuleStatus.FAILED
            logger.exception("Error inside module %s", self.name, extra=self.context.session.current_ids())
        else:
            self._status = ModuleStatus.STOPPING

    async def start(
        self,
        input_data: InputModelT,
        setup_data: SetupModelT,
        callback: Callable[[OutputModelT | ModuleCodeModel], Coroutine[Any, Any, None]],
        done_callback: Callable | None = None,
    ) -> None:
        """Start the module."""
        try:
            self.context.callbacks.send_message = callback
            logger.info(f"Inititalize module {self.context.session.job_id}")
            await self.initialize(self.context, setup_data)
        except Exception as e:
            self._status = ModuleStatus.FAILED
            short_description = "Error initializing module"
            logger.exception("%s: %s", short_description, e)
            await callback(
                ModuleCodeModel(
                    code="Error",
                    short_description=short_description,
                    message=str(e),
                )
            )
            if done_callback is not None:
                await done_callback(None)
            await self.stop()
            return

        try:
            logger.debug("Init the discovered input handlers.")
            self.triggers_discoverer.init_handlers(self.context)
            logger.debug(f"Run lifecycle {self.context.session.job_id}")
            await self._run_lifecycle(input_data, setup_data)
        except Exception:
            self._status = ModuleStatus.FAILED
            logger.exception("Error during module lifecyle")
        finally:
            await self.stop()

    async def stop(self) -> None:
        """Stop the module."""
        logger.info("Stopping module %s | job_id=%s", self.name, self.context.session.job_id)
        try:
            self._status = ModuleStatus.STOPPING
            logger.debug("Module %s stopped", self.name)
            await self.cleanup()
            await self.context.callbacks.send_message(
                DataModel(
                    root=EndOfStreamOutput(),
                    annotations={"role": BaseRole.SYSTEM},
                )
            )
            self._status = ModuleStatus.STOPPED
            logger.debug("Module %s cleaned", self.name)
        except Exception:
            self._status = ModuleStatus.FAILED
            logger.exception("Error stopping module")

    async def start_config_setup(
        self,
        config_setup_data: SetupModelT,
        callback: Callable[[SetupModelT | ModuleCodeModel], Coroutine[Any, Any, None]],
    ) -> None:
        """Start the module."""
        try:
            logger.info("Run Config Setup lifecycle", extra=self.context.session.current_ids())
            self._status = ModuleStatus.RUNNING
            self.context.callbacks.set_config_setup = callback
            content = await self.run_config_setup(self.context, config_setup_data)

            wrapper = config_setup_data.model_dump()
            wrapper["content"] = content.model_dump()
            setup_model = await self.create_setup_model(wrapper)
            await callback(setup_model)
            self._status = ModuleStatus.STOPPING
        except Exception:
            logger.error("Error during module lifecyle")
            self._status = ModuleStatus.FAILED
            logger.exception("Error during module lifecyle", extra=self.context.session.current_ids())
