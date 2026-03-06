"""Module servicer implementation for DigitalKin."""

import asyncio
import os
from argparse import ArgumentParser, Namespace
from collections.abc import AsyncGenerator
from typing import Any, cast

import grpc
from agentic_mesh_protocol.module.v1 import (
    information_pb2,
    lifecycle_pb2,
    module_service_pb2_grpc,
    monitoring_pb2,
)
from google.protobuf import json_format, struct_pb2
from pydantic import ValidationError

from digitalkin.core.job_manager.base_job_manager import BaseJobManager
from digitalkin.grpc_servers.utils.exceptions import ServerError, ServicerError
from digitalkin.logger import logger
from digitalkin.models.core.job_manager_models import JobManagerMode
from digitalkin.models.module.module import ModuleCodeModel, ModuleStatus
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.registry import GrpcRegistry, RegistryStrategy
from digitalkin.services.services_models import ServicesMode
from digitalkin.services.setup.default_setup import DefaultSetup
from digitalkin.services.setup.grpc_setup import GrpcSetup
from digitalkin.services.setup.setup_strategy import SetupServiceError, SetupStrategy, SetupVersionData
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
        self._setup_cache: dict[str, SetupVersionData] = {}
        self._setup_cache_max = int(os.environ.get("DIGITALKIN_SETUP_CACHE_MAX", "100"))
        self._completion_timeout = float(os.environ.get("DIGITALKIN_COMPLETION_TIMEOUT", "300.0"))

    async def shutdown(self) -> None:
        """Release servicer-level resources (GrpcSetup channel, registry cache)."""
        if isinstance(self.setup, GrpcSetup):
            try:
                await self.setup.close_channel()
            except Exception:
                logger.exception("Error closing GrpcSetup channel")

        if isinstance(self._registry_cache, GrpcRegistry):
            try:
                await self._registry_cache.close_channel()
            except Exception:
                logger.exception("Error closing registry cache channel")
            self._registry_cache = None

        self._setup_cache.clear()

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

    async def _resolve_setup(self, setup_id: str, mission_id: str) -> SetupVersionData:
        """Return setup version data from cache or remote service.

        Args:
            setup_id: The setup identifier.
            mission_id: The mission identifier (used only on cache miss).

        Returns:
            SetupVersionData with at least id, setup_id, and content populated.

        Raises:
            LookupError: No setup data found for setup_id.
            SetupServiceError: Remote setup service returned an error.
            ServerError: gRPC communication failed.
            ValidationError: Setup data failed validation.
        """
        if (cached := self._setup_cache.get(setup_id)) is not None:
            logger.debug("debug:_resolve_setup cache hit setup_id=%s", setup_id)
            return cached
        logger.debug("debug:_resolve_setup cache miss setup_id=%s mission_id=%s", setup_id, mission_id)
        if (setup_data := await self.setup.get_setup({"setup_id": setup_id, "mission_id": mission_id})) is not None:
            return setup_data.current_setup_version
        raise LookupError(setup_id)

    async def ConfigSetupModule(
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

        # Extract gRPC request metadata (headers) for propagation
        request_metadata: dict[str, str] = {
            str(k): str(v) for k, v in cast("list[tuple[str, str]]", context.invocation_metadata() or ())
        }

        # create a task to run the module in background
        job_id = await self.job_manager.create_config_setup_instance_job(
            config_setup_data,
            request.mission_id,
            setup_version.setup_id,
            setup_version.id,
            request_metadata=request_metadata,
        )

        if job_id is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Failed to create module instance")
            return lifecycle_pb2.ConfigSetupModuleResponse(success=False)

        updated_setup_data = await self.job_manager.generate_config_setup_module_response(job_id)
        logger.info("Setup response received", extra={"job_id": job_id})

        # Check if response is an error
        if isinstance(updated_setup_data, ModuleCodeModel):
            logger.error(
                "Config setup failed",
                extra={"job_id": job_id, "code": updated_setup_data.code, "error_message": updated_setup_data.message},
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(updated_setup_data.message or "Config setup failed")
            return lifecycle_pb2.ConfigSetupModuleResponse(success=False)

        if isinstance(updated_setup_data, dict) and "code" in updated_setup_data:
            # ModuleCodeModel was serialized to dict
            logger.error(
                "Config setup failed",
                extra={
                    "job_id": job_id,
                    "code": updated_setup_data["code"],
                    "error_message": updated_setup_data.get("message"),
                },
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(updated_setup_data.get("message") or "Config setup failed")
            return lifecycle_pb2.ConfigSetupModuleResponse(success=False)

        logger.debug("Updated setup data", extra={"job_id": job_id, "setup_data": updated_setup_data})

        # Update cache (cap size to prevent unbounded growth)
        if len(self._setup_cache) >= self._setup_cache_max:
            # Evict oldest entry (FIFO)
            oldest_key = next(iter(self._setup_cache))
            del self._setup_cache[oldest_key]
        self._setup_cache[setup_version.setup_id] = SetupVersionData.model_construct(
            id=setup_version.id,
            setup_id=setup_version.setup_id,
            content=updated_setup_data,
        )
        setup_version.content = json_format.ParseDict(  # type: ignore[misc]  # proto __slots__ not fully typed
            updated_setup_data,
            struct_pb2.Struct(),
            ignore_unknown_fields=True,
        )
        return lifecycle_pb2.ConfigSetupModuleResponse(success=True, setup_version=setup_version)

    async def StartModule(  # noqa: C901, PLR0911, PLR0912, PLR0915
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
        logger.info(
            "StartModule called for module: '%s'",
            self.module_class.__name__,
            extra={"module_class": self.module_class, "setup_id": request.setup_id, "mission_id": request.mission_id},
        )
        # Process the module input
        # TODO: Check failure of input data format
        input_data = self.module_class.create_input_model(json_format.MessageToDict(request.input))

        try:
            setup_version = await self._resolve_setup(request.setup_id, request.mission_id)
        except LookupError:
            logger.error(
                "No setup data returned (setup_id=%s, mission_id=%s)",
                request.setup_id,
                request.mission_id,
                extra={
                    "setup_id": request.setup_id,
                    "mission_id": request.mission_id,
                    "module_class": self.module_class.__name__,
                },
            )
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(
                f"[gRPC-server:ModuleService.StartModule] (setup_id={request.setup_id}, "
                f"mission_id={request.mission_id}) No setup data found for setup_id"
            )
            yield lifecycle_pb2.StartModuleResponse(success=False)
            return
        except SetupServiceError as e:
            logger.error(
                "SetupServiceError: %s (setup_id=%s, mission_id=%s, mode=%s)",
                e,
                request.setup_id,
                request.mission_id,
                self.args.services_mode.name,
                extra={
                    "setup_id": request.setup_id,
                    "mission_id": request.mission_id,
                    "module_class": self.module_class.__name__,
                    "error_type": "SetupServiceError",
                },
                exc_info=True,
            )
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(
                f"[gRPC-server:ModuleService.StartModule] (setup_id={request.setup_id}, "
                f"mission_id={request.mission_id}) Setup service unavailable: {e}"
            )
            yield lifecycle_pb2.StartModuleResponse(success=False)
            return
        except ServerError as e:
            logger.error(
                "ServerError fetching setup: %s (setup_id=%s, mission_id=%s)",
                e,
                request.setup_id,
                request.mission_id,
                extra={
                    "setup_id": request.setup_id,
                    "mission_id": request.mission_id,
                    "module_class": self.module_class.__name__,
                    "error_type": "ServerError",
                },
                exc_info=True,
            )
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(
                f"[gRPC-server:ModuleService.StartModule] (setup_id={request.setup_id}, "
                f"mission_id={request.mission_id}) gRPC communication error with Setup service: {e}"
            )
            yield lifecycle_pb2.StartModuleResponse(success=False)
            return
        except ValidationError as e:
            logger.error(
                "ValidationError on setup data: %s (setup_id=%s, mission_id=%s)",
                e,
                request.setup_id,
                request.mission_id,
                extra={
                    "setup_id": request.setup_id,
                    "mission_id": request.mission_id,
                    "module_class": self.module_class.__name__,
                    "error_type": "ValidationError",
                },
                exc_info=True,
            )
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                f"[gRPC-server:ModuleService.StartModule] (setup_id={request.setup_id}, "
                f"mission_id={request.mission_id}) Setup data validation failed: {e}"
            )
            yield lifecycle_pb2.StartModuleResponse(success=False)
            return
        except Exception as e:
            error_type = type(e).__name__
            logger.error(
                "Unexpected %s fetching setup: %s (setup_id=%s, mission_id=%s)",
                error_type,
                e,
                request.setup_id,
                request.mission_id,
                extra={
                    "setup_id": request.setup_id,
                    "mission_id": request.mission_id,
                    "module_class": self.module_class.__name__,
                    "error_type": error_type,
                },
                exc_info=True,
            )
            context.set_code(grpc.StatusCode.UNKNOWN)
            context.set_details(
                f"[gRPC-server:ModuleService.StartModule] (setup_id={request.setup_id}, "
                f"mission_id={request.mission_id}) Unexpected {error_type} during setup fetch: {e}"
            )
            yield lifecycle_pb2.StartModuleResponse(success=False)
            return

        setup_data = await self.module_class.create_setup_model(setup_version.content)

        # Extract gRPC request metadata (headers) for propagation
        request_metadata: dict[str, str] = {
            str(k): str(v) for k, v in cast("list[tuple[str, str]]", context.invocation_metadata() or ())
        }

        # create a task to run the module in background
        logger.debug(
            "debug:StartModule creating job mission_id=%s setup_id=%s setup_version_id=%s",
            request.mission_id,
            setup_version.setup_id,
            setup_version.id,
        )
        try:
            job_id = await self.job_manager.create_module_instance_job(
                input_data,
                setup_data,
                mission_id=request.mission_id,
                setup_id=setup_version.setup_id,
                setup_version_id=setup_version.id,
                request_metadata=request_metadata,
            )
        except ConnectionError as e:
            logger.error(
                "Failed to create job, database connection error (setup_id=%s, mission_id=%s): %s",
                request.setup_id,
                request.mission_id,
                e,
                extra={
                    "setup_id": request.setup_id,
                    "mission_id": request.mission_id,
                    "module_class": self.module_class.__name__,
                },
            )
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(
                f"[gRPC-server:ModuleService.StartModule] (setup_id={request.setup_id}, "
                f"mission_id={request.mission_id}) Database connection failed: {e}"
            )
            yield lifecycle_pb2.StartModuleResponse(success=False)
            return
        except RuntimeError as e:
            logger.error(
                "Failed to create job, resource exhausted (setup_id=%s, mission_id=%s): %s",
                request.setup_id,
                request.mission_id,
                e,
                extra={
                    "setup_id": request.setup_id,
                    "mission_id": request.mission_id,
                    "module_class": self.module_class.__name__,
                },
            )
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details(
                f"[gRPC-server:ModuleService.StartModule] (setup_id={request.setup_id}, "
                f"mission_id={request.mission_id}) {e}"
            )
            yield lifecycle_pb2.StartModuleResponse(success=False)
            return
        except Exception as e:
            error_type = type(e).__name__
            logger.error(
                "Failed to create job, unexpected %s (setup_id=%s, mission_id=%s): %s",
                error_type,
                request.setup_id,
                request.mission_id,
                e,
                extra={
                    "setup_id": request.setup_id,
                    "mission_id": request.mission_id,
                    "module_class": self.module_class.__name__,
                    "error_type": error_type,
                },
                exc_info=True,
            )
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(
                f"[gRPC-server:ModuleService.StartModule] (setup_id={request.setup_id}, "
                f"mission_id={request.mission_id}) Failed to create job: {error_type}: {e}"
            )
            yield lifecycle_pb2.StartModuleResponse(success=False)
            return

        if job_id is None:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Failed to create module instance")
            yield lifecycle_pb2.StartModuleResponse(success=False)
            return

        try:
            async with self.job_manager.generate_stream_consumer(job_id) as stream:
                async for message in stream:
                    # Early detection of client disconnection
                    if context.cancelled():
                        logger.info("Client disconnected", extra={"job_id": job_id})
                        break

                    if message.get("error", None) is not None:
                        logger.error("Error in output_data", extra={"message": message})
                        context.set_code(message["error"]["code"])
                        context.set_details(message["error"]["error_message"])
                        yield lifecycle_pb2.StartModuleResponse(success=False, job_id=job_id)
                        break

                    if message.get("exception", None) is not None:
                        logger.error("Exception in output_data", extra={"message": message})
                        context.set_code(message["short_description"])
                        context.set_details(message["exception"])
                        yield lifecycle_pb2.StartModuleResponse(success=False, job_id=job_id)
                        break

                    logger.debug("Yielding message from job %s", job_id)
                    proto = json_format.ParseDict(message, struct_pb2.Struct(), ignore_unknown_fields=True)
                    yield lifecycle_pb2.StartModuleResponse(success=True, output=proto, job_id=job_id)

                    if message.get("root", {}).get("protocol") == "end_of_stream":
                        logger.debug(
                            "End of stream signal received",
                            extra={"job_id": job_id, "mission_id": request.mission_id},
                        )
                        break
        finally:
            try:
                completion_timeout = self._completion_timeout
                await asyncio.wait_for(
                    self.job_manager.wait_for_completion(job_id),
                    timeout=completion_timeout,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Timeout waiting for job completion, forcing cleanup",
                    extra={"job_id": job_id, "mission_id": request.mission_id},
                )
                # Set cancellation reason on the session if it exists
                if (session := self.job_manager.tasks_sessions.get(job_id)) is not None:
                    from digitalkin.models.core.task_monitor import CancellationReason

                    session.cancellation_reason = CancellationReason.TIMEOUT
            except Exception:
                logger.exception(
                    "Error waiting for job completion",
                    extra={"job_id": job_id, "mission_id": request.mission_id},
                )
            try:
                await self.job_manager.clean_session(job_id, mission_id=request.mission_id)
            except Exception:
                logger.exception(
                    "Error cleaning session",
                    extra={"job_id": job_id, "mission_id": request.mission_id},
                )

        logger.info("Job %s finished", job_id)

    async def StopModule(
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
        logger.debug(
            "StopModule called",
            extra={"module_class": self.module_class.__name__, "job_id": request.job_id},
        )

        response: bool = await self.job_manager.stop_module(request.job_id)
        if not response:
            logger.warning("Job not found for stop request", extra={"job_id": request.job_id})
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Job {request.job_id} not found")
            return lifecycle_pb2.StopModuleResponse(success=False)

        logger.debug("Job stopped successfully", extra={"job_id": request.job_id})
        return lifecycle_pb2.StopModuleResponse(success=True)

    async def GetModuleInput(
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
            return information_pb2.GetModuleInputResponse()
        except Exception as e:
            logger.exception("Failed to get input format for module '%s'", self.module_class.__name__)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Failed to get input format: {e}")
            return information_pb2.GetModuleInputResponse()

        return information_pb2.GetModuleInputResponse(
            success=True,
            input_schema=input_format_struct,
        )

    async def GetModuleSelectInput(
        self,
        request: information_pb2.GetModuleSelectInputRequest,  # gRPC servicer signature # noqa: ARG002
        context: grpc.ServicerContext,  # gRPC servicer signature
    ) -> information_pb2.GetModuleSelectInputResponse:
        """Get the trigger selection schema for the module.

        Args:
            request: The get module select input request.
            context: The gRPC context.

        Returns:
            A response with the module's select input schema.
        """
        logger.debug("GetModuleSelectInput called for module: '%s'", self.module_class.__name__)

        try:
            select_input_schema_proto = await self.module_class.get_select_input_format()
            select_input_format_struct = json_format.Parse(
                text=select_input_schema_proto,
                message=struct_pb2.Struct(),
                ignore_unknown_fields=True,
            )
        except Exception as e:
            logger.exception("Failed to get select input format for module '%s'", self.module_class.__name__)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Failed to get select input format: {e}")
            return information_pb2.GetModuleSelectInputResponse()

        return information_pb2.GetModuleSelectInputResponse(
            success=True,
            select_input_schema=select_input_format_struct,
        )

    async def GetModuleOutput(
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
            return information_pb2.GetModuleOutputResponse()
        except Exception as e:
            logger.exception("Failed to get output format for module '%s'", self.module_class.__name__)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Failed to get output format: {e}")
            return information_pb2.GetModuleOutputResponse()

        return information_pb2.GetModuleOutputResponse(
            success=True,
            output_schema=output_format_struct,
        )

    async def GetModuleSetup(
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
            return information_pb2.GetModuleSetupResponse()
        except Exception as e:
            logger.exception("Failed to get setup format for module '%s'", self.module_class.__name__)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Failed to get setup format: {e}")
            return information_pb2.GetModuleSetupResponse()

        return information_pb2.GetModuleSetupResponse(
            success=True,
            setup_schema=setup_format_struct,
        )

    async def GetModuleSecret(
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
            return information_pb2.GetModuleSecretResponse()
        except Exception as e:
            logger.exception("Failed to get secret format for module '%s'", self.module_class.__name__)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Failed to get secret format: {e}")
            return information_pb2.GetModuleSecretResponse()

        return information_pb2.GetModuleSecretResponse(
            success=True,
            secret_schema=secret_format_struct,
        )

    async def GetConfigSetupModule(
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
            return information_pb2.GetConfigSetupModuleResponse()
        except Exception as e:
            logger.exception("Failed to get config setup format for module '%s'", self.module_class.__name__)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Failed to get config setup format: {e}")
            return information_pb2.GetConfigSetupModuleResponse()

        return information_pb2.GetConfigSetupModuleResponse(
            success=True,
            config_setup_schema=config_setup_format_struct,
        )

    async def GetModuleCost(
        self,
        request: information_pb2.GetModuleCostRequest,
        context: grpc.ServicerContext,
    ) -> information_pb2.GetModuleCostResponse:
        """Get information about the module's cost configuration.

        Args:
            request: The get module cost request.
            context: The gRPC context.

        Returns:
            A response with the module's cost schema.
        """
        logger.debug("GetModuleCost called for module: '%s'", self.module_class.__name__)

        try:
            cost_schema_proto = await self.module_class.get_cost_format(llm_format=request.llm_format)
            cost_format_struct = json_format.Parse(
                text=cost_schema_proto,
                message=struct_pb2.Struct(),
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details(str(e))
            return information_pb2.GetModuleCostResponse()
        except Exception as e:
            logger.exception("Failed to get cost format for module '%s'", self.module_class.__name__)
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Failed to get cost format: {e}")
            return information_pb2.GetModuleCostResponse()

        return information_pb2.GetModuleCostResponse(
            success=True,
            cost_schema=cost_format_struct,
        )
