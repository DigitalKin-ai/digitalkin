"""Module servicer implementation for DigitalKin."""

import asyncio
import logging
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
from digitalkin.models.module import OutputModelT
from digitalkin.models.module.module import ModuleStatus
from digitalkin.modules._base_module import BaseModule
from digitalkin.modules.job_manager import JobManager
from digitalkin.services.services_models import ServicesMode
from digitalkin.services.setup.default_setup import DefaultSetup
from digitalkin.services.setup.grpc_setup import GrpcSetup
from digitalkin.services.setup.setup_strategy import SetupStrategy

logger = logging.getLogger(__name__)


class ModuleServicer(module_service_pb2_grpc.ModuleServiceServicer):
    """Implementation of the ModuleService.

    This servicer handles interactions with a DigitalKin module.

    Attributes:
        module: The module instance being served.
        active_jobs: Dictionary tracking active module jobs.
    """

    setup: SetupStrategy

    def __init__(self, module_class: type[BaseModule]) -> None:
        """Initialize the module servicer.

        Args:
            module_class: The module type to serve.
        """
        super().__init__()
        self.queue: asyncio.Queue = asyncio.Queue()
        self.module_class = module_class
        self.job_manager = JobManager(module_class)
        self.setup = GrpcSetup() if self.job_manager.args.services_mode == ServicesMode.REMOTE else DefaultSetup()

    async def add_to_queue(self, job_id: str, output_data: OutputModelT) -> None:  # type: ignore
        """Callback used to add the output data to the queue of messages."""
        logger.info("JOB: %s added an output_data: %s", job_id, output_data)
        await self.queue.put({job_id: output_data})

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
        # TODO: Check failure of setup data format
        setup_data = self.module_class.create_setup_model(setup_data_class.current_setup_version.content)

        # setup_id should be use to request a precise setup from the module
        # Create a job for this execution
        result: tuple[str, BaseModule] = await self.job_manager.create_job(
            input_data,
            setup_data,
            mission_id=request.mission_id,
            setup_version_id=setup_data_class.current_setup_version.id,
            callback=self.add_to_queue,
        )
        job_id, module = result

        while module.status == ModuleStatus.RUNNING or not self.queue.empty():
            output_data: dict = await self.queue.get()

            if job_id not in output_data or job_id not in self.job_manager.modules:
                message = f"Job {job_id} not found"
                logger.warning(message)
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(message)
                yield lifecycle_pb2.StartModuleResponse(success=False)
                return

            if output_data[job_id].get("error", None) is not None:
                context.set_code(output_data[job_id]["error"]["code"])
                context.set_details(output_data[job_id]["error"]["error_message"])
                yield lifecycle_pb2.StartModuleResponse(success=False)
                return

            output_proto = json_format.ParseDict(
                output_data[job_id],
                struct_pb2.Struct(),
                ignore_unknown_fields=True,
            )
            yield lifecycle_pb2.StartModuleResponse(
                success=True,
                output=output_proto,
                job_id=job_id,
            )

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
        logger.info("StopModule called for module: '%s'", self.module_class.__name__)

        job_id = request.job_id
        if job_id not in self.job_manager.modules:
            message = f"Job {job_id} not found"
            logger.warning(message)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(message)
            return lifecycle_pb2.StopModuleResponse(success=False)

        # Update the job status
        await self.job_manager.modules[job_id].stop()

        logger.info("Job %s stopped successfully", job_id)
        return lifecycle_pb2.StopModuleResponse(success=True)

    def GetModuleStatus(  # noqa: N802
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
        logger.info("GetModuleStatus called for module: '%s'", self.module_class.__name__)

        # If job_id is specified, get status for that job
        if request.job_id:
            if request.job_id not in self.job_manager.modules:
                message = f"Job {request.job_id} not found"
                logger.warning(message)
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(message)
                return monitoring_pb2.GetModuleStatusResponse()

            status = self.job_manager.modules[request.job_id].status
            logger.info("Job %s status: '%s'", request.job_id, status)
            return monitoring_pb2.GetModuleStatusResponse(
                success=True,
                status=status.name,
                job_id=request.job_id,
            )

        logger.info("Job %s status: '%s'", request.job_id, ModuleStatus.NOT_FOUND)
        return monitoring_pb2.GetModuleStatusResponse(
            success=False,
            status=ModuleStatus.NOT_FOUND.name,
            job_id=request.job_id,
        )

    def GetModuleJobs(  # noqa: N802
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
        logger.info("GetModuleJobs called for module: '%s'", self.module_class.__name__)

        # Create job info objects for each active job
        return monitoring_pb2.GetModuleJobsResponse(
            jobs=[
                monitoring_pb2.JobInfo(
                    job_id=job_id,
                    job_status=job_data.status.name,
                )
                for job_id, job_data in self.job_manager.modules.items()
            ],
        )

    def GetModuleInput(  # noqa: N802
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
        logger.info("GetModuleInput called for module: '%s'", self.module_class.__name__)

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
            context.set_details(e)
            return information_pb2.GetModuleInputResponse()

        return information_pb2.GetModuleInputResponse(
            success=True,
            input_schema=input_format_struct,
        )

    def GetModuleOutput(  # noqa: N802
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
        logger.info("GetModuleOutput called for module: '%s'", self.module_class.__name__)

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
            context.set_details(e)
            return information_pb2.GetModuleOutputResponse()

        return information_pb2.GetModuleOutputResponse(
            success=True,
            output_schema=output_format_struct,
        )

    def GetModuleSetup(  # noqa: N802
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
        logger.info("GetModuleSetup called for module: '%s'", self.module_class.__name__)

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
            context.set_details(e)
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
            context.set_details(e)
            return information_pb2.GetModuleSecretResponse()

        return information_pb2.GetModuleSecretResponse(
            success=True,
            secret_schema=secret_format_struct,
        )
