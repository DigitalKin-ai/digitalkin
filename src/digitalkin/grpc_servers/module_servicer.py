"""Module servicer implementation for DigitalKin."""

import asyncio
import json
import time
from argparse import ArgumentParser, Namespace
from collections.abc import Awaitable, Callable
from typing import Any, cast

import grpc
from agentic_mesh_protocol.module.v1 import (
    module_dto_pb2,
    module_messages_pb2,
    module_service_pb2_grpc,
)
from google.protobuf import json_format, struct_pb2
from pydantic import ValidationError

from digitalkin.core.job_manager.base_job_manager import BaseJobManager
from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.core.task_manager.redis.redis_signal import SharedRedisListener
from digitalkin.grpc_servers.exceptions import PermissionDeniedError
from digitalkin.grpc_servers.interceptors.request_ids import RequestContext
from digitalkin.logger import logger
from digitalkin.models.module.module import ModuleCodeModel
from digitalkin.models.module.setup_types import SetupModel
from digitalkin.models.services.services import ServicesMode
from digitalkin.models.settings.gateway import get_gateway_settings
from digitalkin.models.settings.redis import get_redis_settings
from digitalkin.models.settings.server.servicer import get_module_servicer_settings
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.registry import GrpcRegistry, RegistryStrategy
from digitalkin.services.setup.default_setup import DefaultSetup
from digitalkin.services.setup.grpc_setup import GrpcSetup
from digitalkin.services.setup.setup_strategy import SetupStrategy, SetupVersionData
from digitalkin.services.user_profile import DefaultUserProfile, GrpcUserProfile, UserProfileStrategy
from digitalkin.utils.arg_parser import ArgParser
from digitalkin.utils.development_mode_action import DevelopmentModeMappingAction
from digitalkin.utils.env_manager import EnvManager


class ModuleServicer(module_service_pb2_grpc.ModuleServiceServicer, ArgParser):
    """gRPC ModuleService implementation.

    ``ModuleResult.identifier`` is the request ``module_id`` for the schema RPCs and the
    ``setup_version.id`` for ``ConfigSetupModule``; the request validation bounds both to
    256 characters. A failure aborts with its ``grpc.StatusCode`` instead of an ``OperationError``.
    """

    args: Namespace
    setup: SetupStrategy
    user_profile: UserProfileStrategy
    job_manager: BaseJobManager
    _registry_cache: RegistryStrategy | None
    _tool_cache_by_setup: dict[str, tuple[Any, float]]
    _communication_cache: Any

    def _add_parser_args(self, parser: ArgumentParser) -> None:
        super()._add_parser_args(parser)
        parser.add_argument(
            "-d",
            "--dev-mode",
            choices=ServicesMode.__members__,
            action=DevelopmentModeMappingAction,
            dest="services_mode",
            help="Define Module Service configurations for endpoints",
        )

    def __init__(self, module_class: type[BaseModule]) -> None:
        """Initialize the module servicer.

        Args:
            module_class: The module type to serve.

        Raises:
            RuntimeError: If DIGITALKIN_REDIS_URL resolves to an empty URL.
        """
        super().__init__()
        module_class.discover()
        self.module_class = module_class

        redis_url = get_redis_settings().pool.url.get_secret_value()
        if not redis_url:
            msg = "DIGITALKIN_REDIS_URL is required"
            raise RuntimeError(msg)
        from digitalkin.core.task_manager.redis import RedisClient

        self._redis_client = RedisClient(redis_url)
        self.job_manager = SingleJobManager(module_class, self.args.services_mode, redis_client=self._redis_client)

        logger.debug("ModuleServicer initialized with SingleJobManager")
        self.setup = GrpcSetup() if self.args.services_mode == ServicesMode.REMOTE else DefaultSetup()
        # Access-control client gating the setup cache. Always built, always called (fail-closed).
        if self.args.services_mode == ServicesMode.REMOTE:
            up_cfg = self.module_class.services_config_params.get("user_profile") or {}
            up_client_config = up_cfg.get("client_config") or EnvManager.client_config()
            self.user_profile = GrpcUserProfile("", "", "", up_client_config)
        else:
            self.user_profile = DefaultUserProfile("", "", "")
        self._setup_cache: dict[str, tuple[float, SetupVersionData]] = {}
        self._setup_inflight: dict[str, asyncio.Future[SetupVersionData]] = {}

        self._registry_cache = None
        self._tool_cache_by_setup: dict[str, tuple[Any, float]] = {}
        self._tool_cache_inflight: dict[str, asyncio.Future[Any]] = {}
        self._communication_cache = None

    async def shutdown(self) -> None:
        """Release servicer-level resources (GrpcSetup channel, registry cache, Redis pools)."""
        if isinstance(self.setup, GrpcSetup):
            try:
                await self.setup.close_channel()
            except Exception:
                logger.exception("Error closing GrpcSetup channel")
        if isinstance(self.user_profile, GrpcUserProfile):
            try:
                await self.user_profile.close_channel()
            except Exception:
                logger.exception("Error closing GrpcUserProfile channel")
        # M8: close the Redis connection pools (were leaked on every server stop).
        await self._redis_client.close()

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
        if self._tool_cache_by_setup:
            logger.info("tool cache invalidated, dropped setups: %s", list(self._tool_cache_by_setup))
        self._tool_cache_by_setup.clear()

    def get_tool_cache(self, setup_id: str) -> Any | None:
        """TTL'd lookup; ``None`` on miss or expiry.

        Args:
            setup_id: Setup identifier.

        Returns:
            Cached tool definition, or ``None``.
        """
        entry = self._tool_cache_by_setup.get(setup_id)
        if entry is None:
            return None
        value, expires_at = entry
        if time.monotonic() >= expires_at:
            self._tool_cache_by_setup.pop(setup_id, None)
            logger.debug("tool cache expired for setup '%s'", setup_id)
            return None
        return value

    def set_tool_cache(self, setup_id: str, value: Any) -> None:
        """Insert ``value`` with TTL ``GatewayQueueSettings.toolkit_cache_ttl_s``.

        Args:
            setup_id: Setup identifier.
            value: Tool definition object to cache.
        """
        if len(self._tool_cache_by_setup) >= get_module_servicer_settings().setup_cache_max:
            oldest_key = next(iter(self._tool_cache_by_setup))
            del self._tool_cache_by_setup[oldest_key]
            logger.warning(
                "tool cache full (%d), evicting setup '%s'",
                get_module_servicer_settings().setup_cache_max,
                oldest_key,
            )
        ttl_s = get_gateway_settings().queue.toolkit_cache_ttl_s
        self._tool_cache_by_setup[setup_id] = (value, time.monotonic() + ttl_s)
        logger.debug("tool cache set for setup '%s' (ttl %.0fs)", setup_id, ttl_s)

    async def get_or_build_tool_cache(
        self,
        setup_id: str,
        builder: Callable[[], Awaitable[Any]],
    ) -> Any:
        """Singleflight TTL'd lookup; ``builder()`` runs at most once per miss.

        Args:
            setup_id: Setup identifier.
            builder: Zero-arg coroutine factory; called only on miss.

        Returns:
            Cached or freshly-built tool cache value.
        """
        cached = self.get_tool_cache(setup_id)
        if cached is not None:
            logger.debug("tool cache hit for setup '%s'", setup_id)
            return cached
        inflight = self._tool_cache_inflight.get(setup_id)
        if inflight is not None:
            logger.debug("tool cache build in flight for setup '%s', awaiting", setup_id)
            return await inflight
        loop = asyncio.get_event_loop()
        fut: asyncio.Future[Any] = loop.create_future()
        self._tool_cache_inflight[setup_id] = fut
        try:
            value = await builder()
            # Persist regardless of entry count, matching `_setup_cache`'s
            # content-agnostic policy. An empty result (all tool refs
            # NOT_FOUND in a degraded registry) is still a real observation
            # the agent will act on. On recovery the existing
            # ``invalidate_tool_cache`` hook (called from setup-update at
            # ``module_servicer.py:367``) clears the entry.
            if value is not None:
                self.set_tool_cache(setup_id, value)
                logger.info("tool cache built for setup '%s'", setup_id)
            fut.set_result(value)
        except Exception as exc:
            fut.set_exception(exc)
            raise
        else:
            return value
        finally:
            self._tool_cache_inflight.pop(setup_id, None)

    def _get_registry(self) -> RegistryStrategy | None:
        """Return the cached registry instance, or ``None`` if not configured.

        Returns:
            ``GrpcRegistry`` or ``None``.
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
        """Return the cached communication instance, or ``None``.

        Returns:
            ``CommunicationStrategy`` or ``None``.
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

        gateway_backend_config = (self.module_class.services_config_params.get("user_profile") or {}).get(
            "client_config"
        )
        self._communication_cache = GrpcCommunication(
            "", "", "", client_config, gateway_backend_config=gateway_backend_config
        )
        return self._communication_cache

    def _cache_setup(self, setup_id: str, version_data: SetupVersionData) -> None:
        """Cache setup version data, evicting oldest entry if at capacity."""
        if len(self._setup_cache) >= get_module_servicer_settings().setup_cache_max:
            oldest_key = next(iter(self._setup_cache))
            del self._setup_cache[oldest_key]
        self._setup_cache[setup_id] = (time.monotonic(), version_data)

    async def _check_setup_access(self, setup_id: str) -> None:
        """Block if the caller may not access the setup (``CheckResourceAccess`` on ``setup_id``).

        Args:
            setup_id: The setup identifier being resolved.

        Raises:
            PermissionDeniedError: If access to the setup is denied.
        """
        allowed = await self.user_profile.check_resource_access("setup_id", setup_id)
        ids = RequestContext.current()
        if not allowed:
            logger.info(
                "[VALIDATE AC1] setup access DENIED: setup_id=%s", setup_id, extra=ids
            )  # TODO(validate): remove after prod validation
            msg = f"access denied to setup {setup_id}"
            raise PermissionDeniedError(msg)
        logger.info(
            "[VALIDATE AC1] setup access granted: setup_id=%s", setup_id, extra=ids
        )  # TODO(validate): remove after prod validation

    async def resolve_setup(self, setup_id: str, mission_id: str) -> SetupVersionData:
        """Return setup version data from cache or remote service.

        Args:
            setup_id: The setup identifier.
            mission_id: The mission identifier (used only on cache miss).

        Returns:
            SetupVersionData with at least id, setup_id, and content populated.

        Raises:
            LookupError: No setup data found for setup_id.
            PermissionDeniedError: If the caller may not access this setup.
        """
        await self._check_setup_access(setup_id)
        # Fast path: cache hit within TTL
        if (cached := self._setup_cache.get(setup_id)) is not None:
            if time.monotonic() - cached[0] < get_module_servicer_settings().setup_cache_ttl:
                logger.debug("debug:_resolve_setup cache hit setup_id=%s", setup_id)
                return cached[1]
            del self._setup_cache[setup_id]
            logger.debug("debug:_resolve_setup cache expired setup_id=%s", setup_id)

        if setup_id in self._setup_inflight:
            logger.debug("debug:resolve_setup coalesced setup_id=%s", setup_id)
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
        logger.debug("debug:resolve_setup cache miss setup_id=%s mission_id=%s", setup_id, mission_id)
        setup_data = await self.setup.get_setup({"setup_id": setup_id, "mission_id": mission_id})
        if setup_data is None:
            raise LookupError(setup_id)
        result = setup_data.current_setup_version
        self._cache_setup(setup_id, result)
        return result

    async def ConfigSetupModule(
        self,
        request: module_dto_pb2.ConfigSetupModuleRequest,
        context: grpc.aio.ServicerContext,
    ) -> module_dto_pb2.ConfigSetupModuleResponse:
        """Configure the module setup.

        Aborts with PERMISSION_DENIED (setup access refused), INVALID_ARGUMENT (setup content
        not matching the setup model), the status the failed job names (DEADLINE_EXCEEDED when
        the module does not answer in time, NOT_FOUND when its session is gone) or INTERNAL
        (module failed to start, or failed with a free-form code).

        Args:
            request: The configuration request.
            context: The gRPC context.

        Returns:
            The configured setup version, identified by its ``setup_version.id``.
        """
        logger.info(
            "ConfigSetupVersion called for module '%s' setup_version=%s",
            self.module_class.__name__,
            request.setup_version.id,
            extra={"mission_id": request.mission_id},
        )
        setup_version = request.setup_version
        if not await self.user_profile.check_resource_access("setup_id", setup_version.setup_id):
            logger.info(
                "[VALIDATE AC1] setup config access DENIED: setup_id=%s", setup_version.setup_id
            )  # TODO(validate): remove after prod validation
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, f"access denied to setup {setup_version.setup_id}")
        # Invalidate cached setup so concurrent/subsequent starts refetch the reconfigured version
        self._setup_cache.pop(setup_version.setup_id, None)
        try:
            config_setup_data = self.module_class.create_config_setup_model(json_format.MessageToDict(request.content))
            await self.module_class.create_setup_model(
                json_format.MessageToDict(request.setup_version.content),
                config_fields=True,
            )
        except ValidationError as error:
            # Without this the pydantic error escapes the servicer and grpc reports UNKNOWN
            # with a stack trace, so a malformed setup reads as an SDK crash. Name the fields
            # instead: the sender is the only one who can fix them.
            fields = ", ".join(".".join(str(part) for part in item["loc"]) for item in error.errors())
            msg = (
                f"setup version {setup_version.id} does not match the module's setup model (missing/invalid: {fields})"
            )
            logger.error(
                "ConfigSetupModule rejected setup_version=%s: %s",
                setup_version.id,
                error,
                extra={"mission_id": request.mission_id},
            )
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, msg)

        request_metadata: dict[str, str] = {
            str(k): str(v) for k, v in cast("list[tuple[str, str]]", context.invocation_metadata() or ())
        }

        try:
            job_id = await self.job_manager.create_config_setup_instance_job(
                config_setup_data,
                request.mission_id,
                setup_version.setup_id,
                setup_version.id,
                request_metadata=request_metadata,
            )
        except Exception as error:
            logger.exception(
                "[VALIDATE CFGSTART] config setup module failed to start: setup_version=%s",
                setup_version.id,
                extra={"mission_id": request.mission_id},
            )  # TODO(validate): remove after prod validation
            await context.abort(grpc.StatusCode.INTERNAL, f"module failed to start config setup: {error}")

        updated_setup_data = await self.job_manager.generate_config_setup_module_response(job_id)
        logger.info("Setup response received", extra={"job_id": job_id})

        if isinstance(updated_setup_data, ModuleCodeModel):
            updated_setup_data = updated_setup_data.model_dump()
        if isinstance(updated_setup_data, dict) and "code" in updated_setup_data:
            # The job manager and modules name the failure with a gRPC status (``DEADLINE_EXCEEDED``,
            # ``StatusCode.NOT_FOUND``) or a free code; only the latter is INTERNAL.
            status = grpc.StatusCode.__members__.get(
                str(updated_setup_data["code"]).removeprefix("StatusCode."), grpc.StatusCode.INTERNAL
            )
            if status is grpc.StatusCode.OK:
                status = grpc.StatusCode.INTERNAL
            logger.error(
                "[VALIDATE CFGCODE] config setup failed: code=%s status=%s message=%s",
                updated_setup_data["code"],
                status.name,
                updated_setup_data.get("message"),
                extra={"job_id": job_id},
            )  # TODO(validate): remove after prod validation
            await context.abort(status, updated_setup_data.get("message") or "Config setup failed")

        logger.debug("Updated setup data", extra={"job_id": job_id})

        self._cache_setup(
            setup_version.setup_id,
            SetupVersionData.model_construct(
                id=setup_version.id,
                setup_id=setup_version.setup_id,
                content=updated_setup_data,
            ),
        )
        self._tool_cache_by_setup.pop(setup_version.setup_id, None)

        publish_ns = time.time_ns()
        for action in ("invalidate_setup", "invalidate_tools"):
            payload = json.dumps({
                "action": action,
                "setup_id": setup_version.setup_id,
                "published_at_ns": publish_ns,
                "origin": SharedRedisListener.PROCESS_ID,
            })
            try:
                await self._redis_client.publish("signal_ch:_global_", payload)
            except Exception:
                logger.warning(
                    "[gateway] cache-invalidate fan-out publish failed for action=%s "
                    "setup_id=%s — peers may keep stale cache until TTL",
                    action,
                    setup_version.setup_id,
                    exc_info=True,
                )

        setup_version.content = json_format.ParseDict(  # type: ignore[misc]
            updated_setup_data,
            struct_pb2.Struct(),
            ignore_unknown_fields=True,
        )
        return module_dto_pb2.ConfigSetupModuleResponse(
            result=module_messages_pb2.ModuleResult(identifier=setup_version.id, setup_version=setup_version)
        )

    async def GetModuleInput(
        self,
        request: module_dto_pb2.GetModuleInputRequest,
        context: grpc.aio.ServicerContext,
    ) -> module_dto_pb2.GetModuleInputResponse:
        """Get information about the module's expected input.

        Aborts with NOT_FOUND when ``module_id`` is not this module's id (``DIGITALKIN_MODULE_ID``),
        UNIMPLEMENTED when the module has no input format, INTERNAL on any other failure.

        Args:
            request: The get module input request.
            context: The gRPC context.

        Returns:
            A response with the module's input schema.
        """
        logger.debug("GetModuleInput called for module: '%s'", self.module_class.__name__)
        module_id = self.module_class.get_module_id()
        if request.module_id != module_id:
            logger.warning(
                "[VALIDATE MODID] GetModuleInput refused: requested module_id=%s, this module is %s",
                request.module_id,
                module_id,
            )  # TODO(validate): remove after prod validation
            await context.abort(
                grpc.StatusCode.NOT_FOUND, f"module {request.module_id} is not served here (this module is {module_id})"
            )

        try:
            input_schema_proto = await self.module_class.get_input_format(
                llm_format=request.llm_format,
            )
            input_format_struct = json_format.Parse(
                text=input_schema_proto,
                message=struct_pb2.Struct(),
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, str(e))
        except Exception as e:
            logger.exception("Failed to get input format for module '%s'", self.module_class.__name__)
            await context.abort(grpc.StatusCode.INTERNAL, f"Failed to get input format: {e}")

        return module_dto_pb2.GetModuleInputResponse(
            result=module_messages_pb2.ModuleResult(identifier=request.module_id, input_schema=input_format_struct)
        )

    async def GetModuleSelectInput(
        self,
        request: module_dto_pb2.GetModuleSelectInputRequest,
        context: grpc.aio.ServicerContext,
    ) -> module_dto_pb2.GetModuleSelectInputResponse:
        """Get the trigger selection schema for the module.

        Aborts with INTERNAL on failure.

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
            await context.abort(grpc.StatusCode.INTERNAL, f"Failed to get select input format: {e}")

        return module_dto_pb2.GetModuleSelectInputResponse(
            result=module_messages_pb2.ModuleResult(
                identifier=request.module_id, select_input_schema=select_input_format_struct
            )
        )

    async def GetModuleOutput(
        self,
        request: module_dto_pb2.GetModuleOutputRequest,
        context: grpc.aio.ServicerContext,
    ) -> module_dto_pb2.GetModuleOutputResponse:
        """Get information about the module's expected output.

        Aborts with UNIMPLEMENTED when the module has no output format, INTERNAL on any other failure.

        Args:
            request: The get module output request.
            context: The gRPC context.

        Returns:
            A response with the module's output schema.
        """
        logger.debug("GetModuleOutput called for module: '%s'", self.module_class.__name__)

        try:
            output_schema_proto = await self.module_class.get_output_format(
                llm_format=request.llm_format,
            )
            output_format_struct = json_format.Parse(
                text=output_schema_proto,
                message=struct_pb2.Struct(),
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, str(e))
        except Exception as e:
            logger.exception("Failed to get output format for module '%s'", self.module_class.__name__)
            await context.abort(grpc.StatusCode.INTERNAL, f"Failed to get output format: {e}")

        return module_dto_pb2.GetModuleOutputResponse(
            result=module_messages_pb2.ModuleResult(identifier=request.module_id, output_schema=output_format_struct)
        )

    async def GetModuleSetup(
        self,
        request: module_dto_pb2.GetModuleSetupRequest,
        context: grpc.aio.ServicerContext,
    ) -> module_dto_pb2.GetModuleSetupResponse:
        """Get information about the module's setup and configuration.

        Aborts with UNIMPLEMENTED when the module has no setup format, INTERNAL on any other failure.

        Args:
            request: The get module setup request.
            context: The gRPC context.

        Returns:
            A response with the module's setup information.
        """
        logger.debug("GetModuleSetup called for module: '%s'", self.module_class.__name__)

        try:
            setup_schema_proto = await self.module_class.get_setup_format(llm_format=request.llm_format)
            setup_format_struct = json_format.Parse(
                text=setup_schema_proto,
                message=struct_pb2.Struct(),
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, str(e))
        except Exception as e:
            logger.exception("Failed to get setup format for module '%s'", self.module_class.__name__)
            await context.abort(grpc.StatusCode.INTERNAL, f"Failed to get setup format: {e}")

        return module_dto_pb2.GetModuleSetupResponse(
            result=module_messages_pb2.ModuleResult(identifier=request.module_id, setup_schema=setup_format_struct)
        )

    async def GetModuleSecret(
        self,
        request: module_dto_pb2.GetModuleSecretRequest,
        context: grpc.aio.ServicerContext,
    ) -> module_dto_pb2.GetModuleSecretResponse:
        """Get information about the module's secrets.

        Aborts with UNIMPLEMENTED when the module has no secret format, INTERNAL on any other failure.

        Args:
            request: The get module secret request.
            context: The gRPC context.

        Returns:
            A response with the module's secret schema.
        """
        logger.info("GetModuleSecret called for module: '%s'", self.module_class.__name__)

        try:
            secret_schema_proto = await self.module_class.get_secret_format(llm_format=request.llm_format)
            secret_format_struct = json_format.Parse(
                text=secret_schema_proto,
                message=struct_pb2.Struct(),
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, str(e))
        except Exception as e:
            logger.exception("Failed to get secret format for module '%s'", self.module_class.__name__)
            await context.abort(grpc.StatusCode.INTERNAL, f"Failed to get secret format: {e}")

        return module_dto_pb2.GetModuleSecretResponse(
            result=module_messages_pb2.ModuleResult(identifier=request.module_id, secret_schema=secret_format_struct)
        )

    async def GetConfigSetupModule(
        self,
        request: module_dto_pb2.GetConfigSetupModuleRequest,
        context: grpc.aio.ServicerContext,
    ) -> module_dto_pb2.GetConfigSetupModuleResponse:
        """Get information about the module's setup and configuration.

        Aborts with UNIMPLEMENTED when the module has no config setup format, INTERNAL on any other failure.

        Args:
            request: The get module setup request.
            context: The gRPC context.

        Returns:
            A response with the module's setup information.
        """
        logger.debug("GetConfigSetupModule called for module: '%s'", self.module_class.__name__)

        try:
            config_setup_schema_proto = await self.module_class.get_config_setup_format(llm_format=request.llm_format)
            config_setup_format_struct = json_format.Parse(
                text=config_setup_schema_proto,
                message=struct_pb2.Struct(),
                ignore_unknown_fields=True,
            )
        except NotImplementedError as e:
            logger.warning(e)
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, str(e))
        except Exception as e:
            logger.exception("Failed to get config setup format for module '%s'", self.module_class.__name__)
            await context.abort(grpc.StatusCode.INTERNAL, f"Failed to get config setup format: {e}")

        return module_dto_pb2.GetConfigSetupModuleResponse(
            result=module_messages_pb2.ModuleResult(
                identifier=request.module_id, config_setup_schema=config_setup_format_struct
            )
        )

    async def GetModuleCost(
        self,
        request: module_dto_pb2.GetModuleCostRequest,
        context: grpc.aio.ServicerContext,
    ) -> module_dto_pb2.GetModuleCostResponse:
        """Get information about the module's cost configuration.

        Aborts with UNIMPLEMENTED when the module has no cost format, INTERNAL on any other failure.

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
            await context.abort(grpc.StatusCode.UNIMPLEMENTED, str(e))
        except Exception as e:
            logger.exception("Failed to get cost format for module '%s'", self.module_class.__name__)
            await context.abort(grpc.StatusCode.INTERNAL, f"Failed to get cost format: {e}")

        return module_dto_pb2.GetModuleCostResponse(
            result=module_messages_pb2.ModuleResult(identifier=request.module_id, cost_schema=cost_format_struct)
        )
