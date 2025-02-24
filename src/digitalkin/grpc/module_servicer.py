"""Module servicer implementation for DigitalKin."""

import logging
import uuid
from collections.abc import Iterator

import grpc
from digitalkin_proto.digitalkin.module.v1 import (
    information_pb2,
    lifecycle_pb2,
    module_service_pb2,
    module_service_pb2_grpc,
    monitoring_pb2,
)

from digitalkin.modules._base_module import BaseModule

logger = logging.getLogger(__name__)


class ModuleServicer(module_service_pb2_grpc.ModuleServiceServicer):
    """Implementation of the ModuleService.

    This servicer handles interactions with a DigitalKin module.

    Attributes:
        module: The module instance being served.
        active_jobs: Dictionary tracking active module jobs.
    """

    def __init__(self, module: BaseModule):
        """Initialize the module servicer.

        Args:
            module: The module instance to serve.
        """
        self.module = module
        self.active_jobs: dict[str, dict] = {}

    def StartModule(  # noqa: N802
        self,
        request_iterator: Iterator[lifecycle_pb2.StartModuleRequest],
        context: grpc.ServicerContext,
    ) -> Iterator[lifecycle_pb2.StartModuleResponse]:
        """Start a module execution.

        Args:
            request_iterator: Iterator of start module requests.
            context: The gRPC context.

        Yields:
            Responses during module execution.
        """
        logger.info(f"StartModule called for {self.module.metadata['name']}")

        # Process each request in the stream
        for request in request_iterator:
            try:
                # Create a job for this execution
                job_id = request.job_id or str(uuid.uuid4())
                self.active_jobs[job_id] = {"status": "RUNNING"}

                # Process the module input
                input_data = dict(request.input_data.items())

                # Execute the module
                output_data = self.module.execute(input_data)

                # Convert output to proto format
                output_proto = {key: str(value) for key, value in output_data.items()}

                # Update job status
                self.active_jobs[job_id]["status"] = "COMPLETED"

                # Return response
                yield lifecycle_pb2.StartModuleResponse(
                    job_id=job_id,
                    status="COMPLETED",
                    output_data=output_proto,
                    message="Module execution completed successfully",
                )

            except Exception as e:
                logger.error(f"Error in StartModule: {e!s}")
                if "job_id" in locals():
                    self.active_jobs[job_id]["status"] = "FAILED"

                yield lifecycle_pb2.StartModuleResponse(
                    job_id=job_id if "job_id" in locals() else "",
                    status="FAILED",
                    message=f"Module execution failed: {e!s}",
                )

                # Set the error in the gRPC context
                context.set_code(grpc.StatusCode.INTERNAL)
                context.set_details(f"Module execution failed: {e!s}")
                return

    def StopModule(  # noqa: N802
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
        job_id = request.job_id
        logger.info(f"StopModule called for job: {job_id}")

        if job_id not in self.active_jobs:
            message = f"Job {job_id} not found"
            logger.warning(message)
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(message)
            return lifecycle_pb2.StopModuleResponse(
                success=False,
                message=message,
            )

        # Update the job status
        self.active_jobs[job_id]["status"] = "STOPPED"

        return lifecycle_pb2.StopModuleResponse(
            success=True,
            message=f"Job {job_id} stopped successfully",
        )

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
        logger.info(f"GetModuleStatus called for {self.module.metadata['name']}")

        # If job_id is specified, get status for that job
        if request.job_id:
            job_id = request.job_id
            if job_id not in self.active_jobs:
                message = f"Job {job_id} not found"
                logger.warning(message)
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(message)
                return monitoring_pb2.GetModuleStatusResponse(
                    status="UNKNOWN",
                    message=message,
                )

            status = self.active_jobs[job_id]["status"]
            return monitoring_pb2.GetModuleStatusResponse(
                status=status,
                message=f"Job {job_id} status: {status}",
            )

        # Otherwise, return overall module status
        status = "READY"  # Default status

        # Check if module has a status method
        if hasattr(self.module, "get_status") and callable(self.module.get_status):
            status = self.module.get_status()

        return monitoring_pb2.GetModuleStatusResponse(
            status=status,
            message=f"Module status: {status}",
        )

    def GetModuleJobs(  # noqa: N802
        self,
        request: monitoring_pb2.GetModuleJobsRequest,
        context: grpc.ServicerContext,
    ) -> monitoring_pb2.GetModuleJobsResponse:
        """Get information about the module's jobs.

        Args:
            request: The get module jobs request.
            context: The gRPC context.

        Returns:
            A response with information about active jobs.
        """
        logger.info(f"GetModuleJobs called for {self.module.metadata['name']}")

        # Create job info objects for each active job
        job_infos = []
        for job_id, job_data in self.active_jobs.items():
            job_info = module_service_pb2.JobInfo(
                job_id=job_id,
                status=job_data.get("status", "UNKNOWN"),
            )
            job_infos.append(job_info)

        return monitoring_pb2.GetModuleJobsResponse(
            jobs=job_infos,
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
        logger.info(f"GetModuleInput called for {self.module.metadata['name']}")

        # Get input schema if available
        input_schema = {}
        if hasattr(self.module, "get_input_schema") and callable(self.module.get_input_schema):
            input_schema = self.module.get_input_schema()

        # Convert schema to proto format
        input_schema_proto = {key: str(value) for key, value in input_schema.items()}

        return information_pb2.GetModuleInputResponse(
            input_schema=input_schema_proto,
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
        logger.info(f"GetModuleOutput called for {self.module.metadata['name']}")

        # Get output schema if available
        output_schema = {}
        if hasattr(self.module, "get_output_schema") and callable(self.module.get_output_schema):
            output_schema = self.module.get_output_schema()

        # Convert schema to proto format
        output_schema_proto = {key: str(value) for key, value in output_schema.items()}

        return information_pb2.GetModuleOutputResponse(
            output_schema=output_schema_proto,
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
        logger.info(f"GetModuleSetup called for {self.module.metadata['name']}")

        # Get module metadata
        metadata = self.module.metadata

        # Create module info
        module_info = module_service_pb2.ModuleInfo(
            module_id=metadata.module_id,
            name=metadata.name,
            description=metadata.description,
            version=metadata.version,
            tags=metadata.tags,
        )

        # Get configuration if available
        config = {}
        if hasattr(self.module, "get_config") and callable(self.module.get_config):
            config = self.module.get_config()

        # Convert config to proto format
        config_proto = {key: str(value) for key, value in config.items()}

        return information_pb2.GetModuleSetupResponse(
            module_info=module_info,
            config=config_proto,
        )
