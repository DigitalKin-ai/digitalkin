"""Module servicer implementation for DigitalKin."""

from argparse import ArgumentParser, Namespace
from collections.abc import AsyncGenerator
from typing import Any

import grpc
from digitalkin_proto.digitalkin.module.v2 import (
    information_pb2,
    lifecycle_pb2,
    module_service_pb2_grpc,
    monitoring_pb2,
)
from google.protobuf import json_format, struct_pb2

from digitalkin.grpc_servers.utils.exceptions import ServicerError
from digitalkin.logger import logger
from digitalkin.models.module.module import ModuleStatus
from digitalkin.modules._base_module import BaseModule
from digitalkin.modules.job_manager.base_job_manager import BaseJobManager
from digitalkin.modules.job_manager.job_manager_models import JobManagerMode
from digitalkin.services.services_models import ServicesMode
from digitalkin.services.setup.default_setup import DefaultSetup
from digitalkin.services.setup.grpc_setup import GrpcSetup
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
            "ModuleServicer initialized with job manager: %s | %s",
            self.args.job_manager_mode,
            self.job_manager,
        )
        self.setup = GrpcSetup() if self.args.services_mode == ServicesMode.REMOTE else DefaultSetup()

    async def ConfigSetupModule(  # noqa: N802
        self,
        request: lifecycle_pb2.ConfigSetupModuleRequest,
        context: grpc.aio.ServicerContext,
    ) -> lifecycle_pb2.ConfigSetupModuleResponse:
        """Configure the module setup.

        Args:
            request: The configuration request.
            context: The gRPC context.

        Returns:
            A response indicating success or failure.

        Raises:
            ServicerError: if the setup data is not returned or job creation fails.
        """
        logger.info("ConfigSetupVersion called for module: '%s'", self.module_class.__name__)
        # Process the module input
        # TODO: Secret should be used here as well
        setup_version = request.setup_version
        config_setup_data = self.module_class.create_config_setup_model(json_format.MessageToDict(request.content))
        setup_version_data = self.module_class.create_setup_model(
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
            return lifecycle_pb2.ConfigSetupModuleResponse(success=False)

        updated_setup_data = await self.job_manager.generate_config_setup_module_response(job_id)
        logger.warning(f"Updated setup data: {updated_setup_data=}")
        setup_version.content = json_format.ParseDict(
            updated_setup_data,
            struct_pb2.Struct(),
            ignore_unknown_fields=True,
        )
        return lifecycle_pb2.ConfigSetupModuleResponse(success=True, setup_version=setup_version)

    async def StartModule(  # noqa: N802
        self,
        request: lifecycle_pb2.StartModuleRequest,
        context: grpc.aio.ServicerContext,
    ) -> AsyncGenerator[lifecycle_pb2.StartModuleResponse, Any]:
        """Start a module execution.

        Args:
            request: Iterator of start module requests.
            context: The gRPC context.

        Yields:
            Responses during module execution.

        Raises:
            ServicerError: the necessary query didn't work.
        """
        logger.info("StartModule called for module: '%s'", self.module_class.__name__)
        # Process the module input
        # TODO: Check failure of input data format
        input_data = self.module_class.create_input_model(dict(request.input.items()))
        setup_data_class = self.setup.get_setup(
            setup_dict={
                "setup_id": request.setup_id,
                "mission_id": request.mission_id,
            }
        )

        if not setup_data_class:
            msg = "No setup data returned."
            raise ServicerError(msg)

        setup_data = self.module_class.create_setup_model(setup_data_class.current_setup_version.content)

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
            yield lifecycle_pb2.StartModuleResponse(success=False)
            return

        async with self.job_manager.generate_stream_consumer(job_id) as stream:  # type: ignore
            async for message in stream:
                if message.get("error", None) is not None:
                    context.set_code(message["error"]["code"])
                    context.set_details(message["error"]["error_message"])
                    yield lifecycle_pb2.StartModuleResponse(success=False, job_id=job_id)
                    break

                if message.get("exception", None) is not None:
                    logger.error("Error in output_data")
                    context.set_code(message["short_description"])
                    context.set_details(message["exception"])
                    yield lifecycle_pb2.StartModuleResponse(success=False, job_id=job_id)
                    break

                if message.get("code", None) is not None and message.get("code") == "__END_OF_STREAM__":
                    yield lifecycle_pb2.StartModuleResponse(success=True, job_id=job_id)
                    break

                proto = json_format.ParseDict(message, struct_pb2.Struct(), ignore_unknown_fields=True)
                yield lifecycle_pb2.StartModuleResponse(success=True, output=proto, job_id=job_id)
        logger.info("Job %s finished", job_id)

    async def StopModule(  # noqa: N802
        self,
        request: lifecycle_pb2.StopModuleRequest,
        context: grpc.ServicerContext,
    ) -> lifecycle_pb2.StopModuleResponse:
        """Stop a running module execution.

        Args:
            request: The stop module request.
            context: The gRPC context.

        Returns:
            A response indicating success or failure.
        """
        logger.debug("StopModule called for module: '%s'", self.module_class.__name__)

        response: bool = await self.job_manager.stop_module(request.job_id)
        if not response:
            message = f"Job {request.job_id} not found"
            logger.warning(message)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(message)
            return lifecycle_pb2.StopModuleResponse(success=False)

        logger.debug("Job %s stopped successfully", request.job_id)
        return lifecycle_pb2.StopModuleResponse(success=True)

    async def GetModuleStatus(  # noqa: N802
        self,
        request: monitoring_pb2.GetModuleStatusRequest,
        context: grpc.ServicerContext,
    ) -> monitoring_pb2.GetModuleStatusResponse:
        """Get the status of a module.

        Args:
            request: The get module status request.
            context: The gRPC context.

        Returns:
            A response with the module status.
        """
        logger.debug("GetModuleStatus called for module: '%s'", self.module_class.__name__)

        if not request.job_id:
            logger.debug("Job %s status: '%s'", request.job_id, ModuleStatus.NOT_FOUND)
            return monitoring_pb2.GetModuleStatusResponse(
                success=False,
                status=ModuleStatus.NOT_FOUND.name,
                job_id=request.job_id,
            )

        status = await self.job_manager.get_module_status(request.job_id)

        if status is None:
            message = f"Job {request.job_id} not found"
            logger.warning(message)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(message)
            return monitoring_pb2.GetModuleStatusResponse()

        logger.debug("Job %s status: '%s'", request.job_id, status)
        return monitoring_pb2.GetModuleStatusResponse(
            success=True,
            status=status.name,
            job_id=request.job_id,
        )

    async def GetModuleJobs(  # noqa: N802
        self,
        request: monitoring_pb2.GetModuleJobsRequest,  # noqa: ARG002
        context: grpc.ServicerContext,  # noqa: ARG002
    ) -> monitoring_pb2.GetModuleJobsResponse:
        """Get information about the module's jobs.

        Args:
            request: The get module jobs request.
            context: The gRPC context.

        Returns:
            A response with information about active jobs.
        """
        logger.debug("GetModuleJobs called for module: '%s'", self.module_class.__name__)

        modules = await self.job_manager.list_modules()

        # Create job info objects for each active job
        return monitoring_pb2.GetModuleJobsResponse(
            jobs=[
                monitoring_pb2.JobInfo(
                    job_id=job_id,
                    job_status=job_data["status"].name,
                )
                for job_id, job_data in modules.items()
            ],
        )

    async def GetModuleInput(  # noqa: N802
        self,
        request: information_pb2.GetModuleInputRequest,
        context: grpc.ServicerContext,
    ) -> information_pb2.GetModuleInputResponse:
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
            input_schema_proto = self.module_class.get_input_format(llm_format=request.llm_format)
            input_format_struct = json_format.Parse(
                text=input_schema_proto,
                message=struct_pb2.Struct(),  # pylint: disable=no-member
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(str(e))
            return information_pb2.GetModuleInputResponse()

        return information_pb2.GetModuleInputResponse(
            success=True,
            input_schema=input_format_struct,
        )

    async def GetModuleOutput(  # noqa: N802
        self,
        request: information_pb2.GetModuleOutputRequest,
        context: grpc.ServicerContext,
    ) -> information_pb2.GetModuleOutputResponse:
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
            output_schema_proto = self.module_class.get_output_format(llm_format=request.llm_format)
            output_format_struct = json_format.Parse(
                text=output_schema_proto,
                message=struct_pb2.Struct(),  # pylint: disable=no-member
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(str(e))
            return information_pb2.GetModuleOutputResponse()

        return information_pb2.GetModuleOutputResponse(
            success=True,
            output_schema=output_format_struct,
        )

    async def GetModuleSetup(  # noqa: N802
        self,
        request: information_pb2.GetModuleSetupRequest,
        context: grpc.ServicerContext,
    ) -> information_pb2.GetModuleSetupResponse:
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
            setup_schema_proto = self.module_class.get_setup_format(llm_format=request.llm_format)
            setup_format_struct = json_format.Parse(
                text=setup_schema_proto,
                message=struct_pb2.Struct(),  # pylint: disable=no-member
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(str(e))
            return information_pb2.GetModuleSetupResponse()

        return information_pb2.GetModuleSetupResponse(
            success=True,
            setup_schema=setup_format_struct,
        )

    def GetModuleSecret(  # noqa: N802
        self,
        request: information_pb2.GetModuleSecretRequest,
        context: grpc.ServicerContext,
    ) -> information_pb2.GetModuleSecretResponse:
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
            secret_schema_proto = self.module_class.get_secret_format(llm_format=request.llm_format)
            secret_format_struct = json_format.Parse(
                text=secret_schema_proto,
                message=struct_pb2.Struct(),  # pylint: disable=no-member
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(str(e))
            return information_pb2.GetModuleSecretResponse()

        return information_pb2.GetModuleSecretResponse(
            success=True,
            secret_schema=secret_format_struct,
        )

    async def GetConfigSetupModule(  # noqa: N802
        self,
        request: information_pb2.GetConfigSetupModuleRequest,
        context: grpc.ServicerContext,
    ) -> information_pb2.GetConfigSetupModuleResponse:
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
            config_setup_schema_proto = self.module_class.get_config_setup_format(llm_format=request.llm_format)
            config_setup_format_struct = json_format.Parse(
                text=config_setup_schema_proto,
                message=struct_pb2.Struct(),  # pylint: disable=no-member
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(str(e))
            return information_pb2.GetConfigSetupModuleResponse()

        return information_pb2.GetConfigSetupModuleResponse(
            success=True,
            config_setup_schema=config_setup_format_struct,
        )
