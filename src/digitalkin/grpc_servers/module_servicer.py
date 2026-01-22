"""Module servicer implementation for DigitalKin."""

from argparse import ArgumentParser, Namespace
from collections.abc import AsyncGenerator
from typing import Any

import grpc
from agentic_mesh_protocol.module.v1 import module_dto_pb2, module_messages_pb2, module_service_pb2_grpc
from google.protobuf import json_format, struct_pb2

from digitalkin.core.job_manager.base_job_manager import BaseJobManager
from digitalkin.grpc_servers.utils.exceptions import ServicerError
from digitalkin.logger import logger
from digitalkin.models.core.job_manager_models import JobManagerMode
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.registry import GrpcRegistry, RegistryStrategy
from digitalkin.services.services_models import ServicesMode
from digitalkin.services.setup.setup_default import DefaultSetup
from digitalkin.services.setup.setup_grpc import GrpcSetup
from digitalkin.services.setup.setup_strategy import SetupStrategy
from digitalkin.utils.arg_parser import ArgParser
from digitalkin.utils.development_mode_action import DevelopmentModeMappingAction


class ModuleServicer(module_service_pb2_grpc.ModuleServiceServicer, ArgParser):
    """Implementation of the ModuleService.

    This servicer handles interactions with a DigitalKin module.

    Attributes:
        module: The module instance being served.
        active_jobs: Dictionary tracking active module jobs.
    """

    args: Namespace
    setup: SetupStrategy
    job_manager: BaseJobManager
    _registry_cache: RegistryStrategy | None = None

    def _add_parser_args(self, parser: ArgumentParser) -> None:
        super()._add_parser_args(parser)
        parser.add_argument(
            "-d",
            "--dev-mode",
            env_var="SERVICE_MODE",
            choices=ServicesMode.__members__,
            default="local",
            action=DevelopmentModeMappingAction,
            dest="services_mode",
            help="Define Module Service configurations for endpoints",
        )
        parser.add_argument(
            "-jm",
            "--job-manager",
            type=JobManagerMode,
            choices=list(JobManagerMode),
            default=JobManagerMode.SINGLE,
            dest="job_manager_mode",
            help="Define Module job manager configurations for load balancing",
        )

    def __init__(self, module_class: type[BaseModule]) -> None:
        """Initialize the module servicer.

        Args:
            module_class: The module type to serve.
        """
        super().__init__()
        module_class.discover()
        self.module_class = module_class
        job_manager_class = self.args.job_manager_mode.get_manager_class()
        self.job_manager = job_manager_class(module_class, self.args.services_mode)

        logger.debug(
            "ModuleServicer initialized with job manager: %s",
            self.args.job_manager_mode,
            extra={"job_manager": self.job_manager},
        )
        self.setup = GrpcSetup() if self.args.services_mode == ServicesMode.REMOTE else DefaultSetup()

    def _get_registry(self) -> RegistryStrategy | None:
        """Get a cached registry instance if configured.

        Returns:
            Cached GrpcRegistry instance if registry config exists, None otherwise.
        """
        if self._registry_cache is not None:
            return self._registry_cache

        registry_config = self.module_class.services_config_params.get("registry")
        if not registry_config:
            return None

        client_config = registry_config.get("client_config")
        if not client_config:
            return None

        self._registry_cache = GrpcRegistry("", "", "", client_config)
        return self._registry_cache

    async def ConfigSetupModule(  # noqa: N802
        self,
        request: module_dto_pb2.ConfigSetupModuleRequest,
        context: grpc.aio.ServicerContext,
    ) -> module_dto_pb2.ConfigSetupModuleResponse:
        """Configure the module setup.

        Args:
            request: The configuration request.
            context: The gRPC context.

        Returns:
            A response indicating success or failure.

        Raises:
            ServicerError: if the setup data is not returned or job creation fails.
        """
        logger.info(
            "ConfigSetupVersion called for module: '%s'",
            self.module_class.__name__,
            extra={
                "module_class": self.module_class,
                "setup_version": request.setup_version,
                "mission_id": request.mission_id,
            },
        )
        setup_version = request.setup_version
        config_setup_data = self.module_class.create_config_setup_model(json_format.MessageToDict(request.content))
        setup_version_data = await self.module_class.create_setup_model(
            json_format.MessageToDict(request.setup_version.content),
            config_fields=True,
        )

        if not setup_version_data:
            msg = "No setup data returned."
            raise ServicerError(msg)

        if not config_setup_data:
            msg = "No config setup data returned."
            raise ServicerError(msg)

        # create a task to run the module in background
        job_id = await self.job_manager.create_config_setup_instance_job(
            config_setup_data,
            request.mission_id,
            setup_version.setup_id,
            setup_version.id,
        )

        if job_id is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Failed to create module instance")
            result = module_messages_pb2.ModuleResult(success=False)
            return module_dto_pb2.ConfigSetupModuleResponse(result=result)

        updated_setup_data = await self.job_manager.generate_config_setup_module_response(job_id)
        logger.info("Setup updated", extra={"job_id": job_id})
        logger.debug("Updated setup data", extra={"job_id": job_id, "setup_data": updated_setup_data})
        setup_version.content = json_format.ParseDict(
            updated_setup_data,
            struct_pb2.Struct(),
            ignore_unknown_fields=True,
        )
        result = module_messages_pb2.ModuleResult(setup_version=setup_version, success=True)
        return module_dto_pb2.ConfigSetupModuleResponse(result=result)

    async def StartModule(  # noqa: N802
        self,
        request: module_dto_pb2.StartModuleRequest,
        context: grpc.aio.ServicerContext,
    ) -> AsyncGenerator[module_dto_pb2.StartModuleResponse, Any]:
        """Start a module execution.

        Args:
            request: Iterator of start module requests.
            context: The gRPC context.

        Yields:
            Responses during module execution.

        Raises:
            ServicerError: the necessary query didn't work.
        """
        logger.info(
            "StartModule called for module: '%s'",
            self.module_class.__name__,
            extra={"module_class": self.module_class, "setup_id": request.setup_id, "mission_id": request.mission_id},
        )
        # Process the module input
        # TODO: Check failure of input data format
        input_data = self.module_class.create_input_model(json_format.MessageToDict(request.input))

        setup_data_class = self.setup.get(
            setup_dict={
                "setup_id": request.setup_id,
                "mission_id": request.mission_id,
            }
        )

        if not setup_data_class:
            msg = "No setup data returned."
            raise ServicerError(msg)

        setup_data = await self.module_class.create_setup_model(setup_data_class.current_setup_version.content)

        # create a task to run the module in background
        job_id = await self.job_manager.create_module_instance_job(
            input_data,
            setup_data,
            mission_id=request.mission_id,
            setup_id=setup_data_class.current_setup_version.setup_id,
            setup_version_id=setup_data_class.current_setup_version.id,
        )

        if job_id is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Failed to create module instance")
            result = module_messages_pb2.ModuleResult(success=False)
            yield module_dto_pb2.StartModuleResponse(result=result)
            return

        try:
            async with self.job_manager.generate_stream_consumer(job_id) as stream:  # type: ignore
                async for message in stream:
                    if message.get("error", None) is not None:
                        logger.error("Error in output_data", extra={"message": message})
                        context.set_code(message["error"]["code"])
                        context.set_details(message["error"]["error_message"])
                        result = module_messages_pb2.ModuleResult(success=False)
                        yield module_dto_pb2.StartModuleResponse(result=result, job_id=job_id)
                        break

                    if message.get("exception", None) is not None:
                        logger.error("Exception in output_data", extra={"message": message})
                        context.set_code(message["short_description"])
                        context.set_details(message["exception"])
                        result = module_messages_pb2.ModuleResult(success=False)
                        yield module_dto_pb2.StartModuleResponse(result=result, job_id=job_id)
                        break

                    logger.info("Yielding message from job %s: %s", job_id, message)
                    proto = json_format.ParseDict(message, struct_pb2.Struct(), ignore_unknown_fields=True)
                    result = module_messages_pb2.ModuleResult(success=True, output=proto)
                    yield module_dto_pb2.StartModuleResponse(result=result, job_id=job_id)

                    if message.get("root", {}).get("protocol") == "end_of_stream":
                        logger.info(
                            "End of stream signal received",
                            extra={"job_id": job_id, "mission_id": request.mission_id},
                        )
                        break
        finally:
            await self.job_manager.wait_for_completion(job_id)
            await self.job_manager.clean_session(job_id, mission_id=request.mission_id)

        logger.info("Job %s finished", job_id)

    async def StopModule(  # noqa: N802
        self,
        request: module_dto_pb2.StopModuleRequest,
        context: grpc.ServicerContext,
    ) -> module_dto_pb2.StopModuleResponse:
        """Stop a running module execution.

        Args:
            request: The stop module request.
            context: The gRPC context.

        Returns:
            A response indicating success or failure.
        """
        logger.debug(
            "StopModule called",
            extra={"module_class": self.module_class.__name__, "job_id": request.job_id},
        )

        response: bool = await self.job_manager.stop_module(request.job_id)
        if not response:
            logger.warning("Job not found for stop request", extra={"job_id": request.job_id})
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Job {request.job_id} not found")
            result = module_messages_pb2.ModuleResult(success=False)
            return module_dto_pb2.StopModuleResponse(result=result)

        logger.debug("Job stopped successfully", extra={"job_id": request.job_id})
        result = module_messages_pb2.ModuleResult(success=True)
        return module_dto_pb2.StopModuleResponse(result=result)

    async def GetModuleInput(  # noqa: N802
        self,
        request: module_dto_pb2.GetModuleInputRequest,
        context: grpc.ServicerContext,
    ) -> module_dto_pb2.GetModuleInputResponse:
        """Get information about the module's expected input.

        Args:
            request: The get module input request.
            context: The gRPC context.

        Returns:
            A response with the module's input schema.
        """
        logger.debug("GetModuleInput called for module: '%s'", self.module_class.__name__)

        # Get input schema if available
        try:
            # Convert schema to proto format
            input_schema_proto = await self.module_class.get_input_format(
                llm_format=request.llm_format,
            )
            input_format_struct = json_format.Parse(
                text=input_schema_proto,
                message=struct_pb2.Struct(),  # pylint: disable=no-member
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(str(e))
            return module_dto_pb2.GetModuleInputResponse()

        result = module_messages_pb2.ModuleResult(input_schema=input_format_struct, success=True)
        return module_dto_pb2.GetModuleInputResponse(result=result)

    async def GetModuleOutput(  # noqa: N802
        self,
        request: module_dto_pb2.GetModuleOutputRequest,
        context: grpc.ServicerContext,
    ) -> module_dto_pb2.GetModuleOutputResponse:
        """Get information about the module's expected output.

        Args:
            request: The get module output request.
            context: The gRPC context.

        Returns:
            A response with the module's output schema.
        """
        logger.debug("GetModuleOutput called for module: '%s'", self.module_class.__name__)

        # Get output schema if available
        try:
            # Convert schema to proto format
            output_schema_proto = await self.module_class.get_output_format(
                llm_format=request.llm_format,
            )
            output_format_struct = json_format.Parse(
                text=output_schema_proto,
                message=struct_pb2.Struct(),  # pylint: disable=no-member
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(str(e))
            return module_dto_pb2.GetModuleOutputResponse()

        result = module_messages_pb2.ModuleResult(output_schema=output_format_struct, success=True)
        return module_dto_pb2.GetModuleOutputResponse(result=result)

    async def GetModuleSetup(  # noqa: N802
        self,
        request: module_dto_pb2.GetModuleSetupRequest,
        context: grpc.ServicerContext,
    ) -> module_dto_pb2.GetModuleSetupResponse:
        """Get information about the module's setup and configuration.

        Args:
            request: The get module setup request.
            context: The gRPC context.

        Returns:
            A response with the module's setup information.
        """
        logger.debug("GetModuleSetup called for module: '%s'", self.module_class.__name__)

        # Get setup schema if available
        try:
            # Convert schema to proto format
            setup_schema_proto = await self.module_class.get_setup_format(llm_format=request.llm_format)
            setup_format_struct = json_format.Parse(
                text=setup_schema_proto,
                message=struct_pb2.Struct(),  # pylint: disable=no-member
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(str(e))
            return module_dto_pb2.GetModuleSetupResponse()

        result = module_messages_pb2.ModuleResult(secret_schema=setup_format_struct, success=True)
        return module_dto_pb2.GetModuleSetupResponse(result=result)

    async def GetModuleSecret(  # noqa: N802
        self,
        request: module_dto_pb2.GetModuleSecretRequest,
        context: grpc.ServicerContext,
    ) -> module_dto_pb2.GetModuleSecretResponse:
        """Get information about the module's secrets.

        Args:
            request: The get module secret request.
            context: The gRPC context.

        Returns:
            A response with the module's secret schema.
        """
        logger.info("GetModuleSecret called for module: '%s'", self.module_class.__name__)

        # Get secret schema if available
        try:
            # Convert schema to proto format
            secret_schema_proto = await self.module_class.get_secret_format(llm_format=request.llm_format)
            secret_format_struct = json_format.Parse(
                text=secret_schema_proto,
                message=struct_pb2.Struct(),  # pylint: disable=no-member
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(str(e))
            return module_dto_pb2.GetModuleSecretResponse()

        result = module_messages_pb2.ModuleResult(secret_schema=secret_format_struct, success=True)
        return module_dto_pb2.GetModuleSecretResponse(result=result)

    async def GetConfigSetupModule(  # noqa: N802
        self,
        request: module_dto_pb2.GetConfigSetupModuleRequest,
        context: grpc.ServicerContext,
    ) -> module_dto_pb2.GetConfigSetupModuleResponse:
        """Get information about the module's setup and configuration.

        Args:
            request: The get module setup request.
            context: The gRPC context.

        Returns:
            A response with the module's setup information.
        """
        logger.debug("GetConfigSetupModule called for module: '%s'", self.module_class.__name__)

        # Get setup schema if available
        try:
            # Convert schema to proto format
            config_setup_schema_proto = await self.module_class.get_config_setup_format(llm_format=request.llm_format)
            config_setup_format_struct = json_format.Parse(
                text=config_setup_schema_proto,
                message=struct_pb2.Struct(),  # pylint: disable=no-member
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(str(e))
            return module_dto_pb2.GetConfigSetupModuleResponse()

        result = module_messages_pb2.ModuleResult(config_setup_schema=config_setup_format_struct, success=True)
        return module_dto_pb2.GetConfigSetupModuleResponse(result=result)
