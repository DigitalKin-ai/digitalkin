"""Module servicer implementation for DigitalKin."""

import asyncio
import os
import time
from argparse import ArgumentParser, Namespace
from typing import Any, cast

import grpc
from agentic_mesh_protocol.module.v1 import (
    information_pb2,
    lifecycle_pb2,
    module_service_pb2_grpc,
    monitoring_pb2,
)
from google.protobuf import json_format, struct_pb2

from digitalkin.core.job_manager.base_job_manager import BaseJobManager
from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.grpc_servers.gateway_constants import TOOLKIT_CACHE_TTL_S
from digitalkin.grpc_servers.utils.exceptions import ServicerError
from digitalkin.logger import logger
from digitalkin.models.module.module import ModuleCodeModel, ModuleStatus
from digitalkin.models.module.setup_types import SetupModel
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.registry import GrpcRegistry, RegistryStrategy
from digitalkin.services.services_models import ServicesMode
from digitalkin.services.setup.default_setup import DefaultSetup
from digitalkin.services.setup.grpc_setup import GrpcSetup
from digitalkin.services.setup.setup_strategy import SetupStrategy, SetupVersionData
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
    _registry_cache: RegistryStrategy | None
    # Maps setup_id -> (tool_cache, expires_at_perf_counter_ns).
    # TTL'd so a slow-changing tool definition still gets refreshed
    # without a SendSignal/INVALIDATE_TOOLS in the loop. The signal
    # path (`invalidate_tool_cache`) bypasses TTL with a full clear.
    _tool_cache_by_setup: dict[str, tuple[Any, float]]
    _communication_cache: Any

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

    def __init__(self, module_class: type[BaseModule]) -> None:
        """Initialize the module servicer.

        Args:
            module_class: The module type to serve.

        Raises:
            RuntimeError: If DIGITALKIN_REDIS_URL is not set.
        """
        super().__init__()
        module_class.discover()
        self.module_class = module_class

        redis_url = os.environ.get("DIGITALKIN_REDIS_URL")
        if not redis_url:
            msg = "DIGITALKIN_REDIS_URL is required"
            raise RuntimeError(msg)
        from digitalkin.core.task_manager.redis import RedisClient

        self._redis_client = RedisClient(redis_url)
        self.job_manager = SingleJobManager(module_class, self.args.services_mode, redis_client=self._redis_client)

        logger.debug("ModuleServicer initialized with SingleJobManager")
        self.setup = GrpcSetup() if self.args.services_mode == ServicesMode.REMOTE else DefaultSetup()
        self._setup_cache: dict[str, SetupVersionData] = {}
        self._setup_cache_max = int(os.environ.get("DIGITALKIN_SETUP_CACHE_MAX", "100"))
        self._setup_inflight: dict[str, asyncio.Future[SetupVersionData]] = {}
        self._completion_timeout = float(os.environ.get("DIGITALKIN_COMPLETION_TIMEOUT", "300.0"))

        self._registry_cache = None
        self._tool_cache_by_setup = {}
        self._communication_cache = None

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
        self._tool_cache_by_setup.clear()
        SetupModel.clear_clean_model_cache()

    def invalidate_setup_cache(self) -> None:
        """Clear setup cache. Next request re-fetches from services-provider."""
        self._setup_cache.clear()
        self._setup_inflight.clear()

    def invalidate_tool_cache(self) -> None:
        """Clear tool cache. Next request re-resolves tool definitions."""
        self._tool_cache_by_setup.clear()

    def get_tool_cache(self, setup_id: str) -> Any | None:
        """TTL'd lookup. Returns None if missing or expired (the caller
        is expected to recompute and call `set_tool_cache`).

        Args:
            setup_id: Setup identifier.

        Returns:
            Cached tool definition object, or None on miss/expiry.
        """
        entry = self._tool_cache_by_setup.get(setup_id)
        if entry is None:
            return None
        value, expires_at = entry
        if time.monotonic() >= expires_at:
            self._tool_cache_by_setup.pop(setup_id, None)
            return None
        return value

    def set_tool_cache(self, setup_id: str, value: Any) -> None:
        """Insert ``value`` with TTL ``TOOLKIT_CACHE_TTL_S``.

        Args:
            setup_id: Setup identifier.
            value: Tool definition object to cache.
        """
        if len(self._tool_cache_by_setup) >= self._setup_cache_max:
            oldest_key = next(iter(self._tool_cache_by_setup))
            del self._tool_cache_by_setup[oldest_key]
        self._tool_cache_by_setup[setup_id] = (value, time.monotonic() + TOOLKIT_CACHE_TTL_S)

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

    def _get_communication(self) -> Any:
        """Get a cached communication instance for tool resolution.

        Returns:
            CommunicationStrategy if configured, None otherwise.
        """
        if self._communication_cache is not None:
            return self._communication_cache

        comm_config = self.module_class.services_config_params.get("communication")
        if not comm_config:
            return None

        client_config = comm_config.get("client_config")
        if not client_config:
            return None

        from digitalkin.services.communication.grpc_communication import GrpcCommunication

        self._communication_cache = GrpcCommunication("", "", "", client_config)
        return self._communication_cache

    def _cache_setup(self, setup_id: str, version_data: SetupVersionData) -> None:
        """Cache setup version data, evicting oldest entry if at capacity."""
        if len(self._setup_cache) >= self._setup_cache_max:
            oldest_key = next(iter(self._setup_cache))
            del self._setup_cache[oldest_key]
        self._setup_cache[setup_id] = version_data

    async def _resolve_setup(self, setup_id: str, mission_id: str) -> SetupVersionData:
        """Return setup version data from cache or remote service.

        Args:
            setup_id: The setup identifier.
            mission_id: The mission identifier (used only on cache miss).

        Returns:
            SetupVersionData with at least id, setup_id, and content populated.

        Raises:
            LookupError: No setup data found for setup_id.
        """
        # Fast path: cache hit
        if (cached := self._setup_cache.get(setup_id)) is not None:
            logger.debug("debug:_resolve_setup cache hit setup_id=%s", setup_id)
            return cached

        # Coalesce concurrent misses: first caller fetches, others await the same future
        if setup_id in self._setup_inflight:
            logger.debug("debug:_resolve_setup coalesced setup_id=%s", setup_id)
            return await self._setup_inflight[setup_id]

        loop = asyncio.get_running_loop()
        fut: asyncio.Future[SetupVersionData] = loop.create_future()
        self._setup_inflight[setup_id] = fut
        try:
            result = await self._fetch_setup(setup_id, mission_id)
        except BaseException as exc:
            if not fut.done():
                fut.set_exception(exc)
            raise
        else:
            fut.set_result(result)
            return result
        finally:
            self._setup_inflight.pop(setup_id, None)

    async def _fetch_setup(self, setup_id: str, mission_id: str) -> SetupVersionData:
        """Fetch setup from remote service and cache it.

        Returns:
            Resolved SetupVersionData.

        Raises:
            LookupError: No setup data found for setup_id.
        """
        logger.debug("debug:_resolve_setup cache miss setup_id=%s mission_id=%s", setup_id, mission_id)
        setup_data = await self.setup.get_setup({"setup_id": setup_id, "mission_id": mission_id})
        if setup_data is None:
            raise LookupError(setup_id)
        result = setup_data.current_setup_version
        self._cache_setup(setup_id, result)
        return result

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

        # Update cache + invalidate tool cache (setup changed)
        self._cache_setup(
            setup_version.setup_id,
            SetupVersionData.model_construct(
                id=setup_version.id,
                setup_id=setup_version.setup_id,
                content=updated_setup_data,
            ),
        )
        self._tool_cache_by_setup.pop(setup_version.setup_id, None)
        setup_version.content = json_format.ParseDict(  # type: ignore[misc]  # proto __slots__ not fully typed
            updated_setup_data,
            struct_pb2.Struct(),
            ignore_unknown_fields=True,
        )
        return lifecycle_pb2.ConfigSetupModuleResponse(success=True, setup_version=setup_version)

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
        request: information_pb2.GetModuleSelectInputRequest,  # noqa: ARG002
        context: grpc.ServicerContext,
    ) -> information_pb2.GetModuleSelectInputResponse:
        """Get the trigger selection schema for the module.

        Args:
            request: The get module select input request.
            context: The gRPC context.

        Returns:
            A response with the module's select input schema.
        """
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
