"""Base gRPC server implementation for DigitalKin."""

import abc
import asyncio
import os
import sys

from digitalkin.models.settings.profiling import get_profiling_settings

if get_profiling_settings().uvloop:
    try:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass
from collections.abc import Callable, Sequence
from concurrent import futures
from pathlib import Path
from typing import Any, cast

import grpc
from grpc import aio as grpc_aio

from digitalkin.grpc_servers.exceptions import (
    ConfigurationError,
    ReflectionError,
    SecurityError,
    ServerStateError,
    ServicerError,
)
from digitalkin.grpc_servers.interceptors.request_ids import RequestIdServerInterceptor
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.types import GrpcServer, ServiceDescriptor, T
from digitalkin.models.settings.server.server import get_server_settings
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode


class BaseServer(abc.ABC):
    """Foundation for sync/async gRPC servers with secure/insecure modes.

    Attributes:
        server: The gRPC server instance.
        _servicers: Registered servicers.
        _service_names: Service names exposed via reflection.
        _health_servicer: Optional health-check servicer.
    """

    def __init__(
        self,
        interceptors: Sequence[Any] | None = None,
    ) -> None:
        """Initialize the base gRPC server.

        Args:
            interceptors: Optional sequence of gRPC server interceptors.
        """
        self.server: GrpcServer | None = None
        self._servicers: list[Any] = []
        self._service_names: list[str] = []
        self._health_servicer: Any = None
        self._interceptors: list[Any] = list(interceptors) if interceptors else []

    def register_servicer(
        self,
        servicer: T,
        add_to_server_fn: Callable[[T, GrpcServer], None],
        service_descriptor: ServiceDescriptor | None = None,
        service_names: list[str] | None = None,
    ) -> None:
        """Register a servicer and track its names for reflection.

        Args:
            servicer: The servicer instance.
            add_to_server_fn: Function adding the servicer to the server.
            service_descriptor: Optional pb2 DESCRIPTOR.
            service_names: Optional explicit list of service full names.

        Raises:
            ServicerError: If the server is not created.
        """
        if self.server is None:
            msg = "Server must be created before registering servicers"
            raise ServicerError(msg)

        try:
            add_to_server_fn(servicer, self.server)
            self._servicers.append(servicer)
        except Exception as e:
            msg = f"Failed to register servicer: {e}"
            raise ServicerError(msg) from e

        if service_names:
            for name in service_names:
                if name not in self._service_names:
                    self._service_names.append(name)
                    logger.debug("Registered explicit service name for reflection: %s", name)

        if service_descriptor is not None:
            for service in service_descriptor.services_by_name.values():
                if service.full_name not in self._service_names:
                    self._service_names.append(service.full_name)
                    logger.debug("Registered service name from descriptor: %s", service.full_name)

    @abc.abstractmethod
    def _register_servicers(self) -> None:
        """Register servicers (subclass hook).

        Raises:
            ServicerError: If the server is not created.
        """

    def _add_reflection(self) -> None:
        """Register both v1 and v1alpha reflection on the server.

        v1 and v1alpha wire formats are identical — only the service name
        differs. Registering both keeps Postman 10.x+ and other v1-first
        clients working.

        Raises:
            ReflectionError: If reflection initialization fails.
        """
        if not get_server_settings().reflection or self.server is None or not self._service_names:
            return

        try:  # noqa: PLW0717
            import grpc
            from grpc_reflection.v1alpha import reflection as reflection_v1alpha
            from grpc_reflection.v1alpha import reflection_pb2 as reflection_pb2_v1alpha

            service_names = self._service_names.copy()
            service_names.append(reflection_v1alpha.SERVICE_NAME)
            v1_service_name = "grpc.reflection.v1.ServerReflection"
            service_names.append(v1_service_name)

            reflection_v1alpha.enable_server_reflection(service_names, self.server)

            servicer = reflection_v1alpha.ReflectionServicer(service_names)
            method_handlers = {
                "ServerReflectionInfo": grpc.stream_stream_rpc_method_handler(
                    servicer.ServerReflectionInfo,
                    request_deserializer=reflection_pb2_v1alpha.ServerReflectionRequest.FromString,
                    response_serializer=reflection_pb2_v1alpha.ServerReflectionResponse.SerializeToString,
                ),
            }
            handler = grpc.method_handlers_generic_handler(v1_service_name, method_handlers)
            self.server.add_generic_rpc_handlers((handler,))

            logger.debug("Added gRPC reflection v1 + v1alpha: %s", service_names)
        except ImportError:
            logger.warning("Could not enable reflection: grpcio-reflection package not installed")
        except Exception as e:
            error_msg = f"Failed to enable reflection: {e}"
            logger.warning(error_msg)
            raise ReflectionError(error_msg) from e

    def _add_health_service(self) -> None:
        """Register the gRPC health-check service and mark all services SERVING."""
        if self.server is None:
            return

        try:  # noqa: PLW0717
            from grpc_health.v1 import health_pb2, health_pb2_grpc
            from grpc_health.v1.health import HealthServicer

            health_servicer = HealthServicer()
            health_pb2_grpc.add_HealthServicer_to_server(health_servicer, self.server)

            if health_pb2.DESCRIPTOR.services_by_name:
                service_name = health_pb2.DESCRIPTOR.services_by_name["Health"].full_name
                if service_name not in self._service_names:
                    self._service_names.append(service_name)

            logger.debug("Added gRPC health checking service")

            for service_name in self._service_names:
                health_servicer.set(service_name, health_pb2.HealthCheckResponse.SERVING)
            health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

            self._health_servicer = health_servicer

        except ImportError:
            logger.warning("Could not enable health service: grpcio-health-checking package not installed")
        except Exception as e:
            logger.warning("Failed to enable health service: %s", e)

    def _create_server(self) -> GrpcServer:
        """Create a gRPC server from current settings.

        Returns:
            A configured gRPC server instance.

        Raises:
            ConfigurationError: If the server settings are invalid.
        """
        try:  # noqa: PLW0717
            grpc_compression = get_server_settings().grpc.compression.to_grpc()

            # sched_getaffinity is Linux-only; the sys.platform guard lets mypy skip
            # it as unreachable on other platforms without a type: ignore.
            if sys.platform == "linux":
                try:
                    cpu_count = len(os.sched_getaffinity(0))
                    logger.info("vCPU count: %d", cpu_count)
                except OSError:
                    cpu_count = os.cpu_count() or 1
                    logger.info("CPU count: %d", cpu_count)
            else:
                cpu_count = os.cpu_count() or 1
                logger.info("CPU count: %d", cpu_count)

            logger.info(
                "gRPC server settings.server: cpus=%d, max_concurrent_rpcs=%d, thread_pool_workers=%d, mode=%s",
                cpu_count,
                get_server_settings().max_concurrent_rpcs,
                get_server_settings().thread_pool_workers,
                get_server_settings().channel.communication_mode.value,
            )

            if get_server_settings().channel.communication_mode == ControlFlow.ASYNC:
                server = grpc_aio.server(
                    options=get_server_settings().grpc.options,
                    compression=grpc_compression,
                    interceptors=[RequestIdServerInterceptor(), *self._interceptors],
                    maximum_concurrent_rpcs=get_server_settings().max_concurrent_rpcs,
                    migration_thread_pool=futures.ThreadPoolExecutor(
                        max_workers=get_server_settings().thread_pool_workers
                    ),
                )
            else:
                server = grpc.server(  # type: ignore[assignment]
                    futures.ThreadPoolExecutor(max_workers=get_server_settings().max_workers),
                    options=get_server_settings().grpc.options,
                    compression=grpc_compression,
                    interceptors=self._interceptors or None,
                    maximum_concurrent_rpcs=get_server_settings().max_concurrent_rpcs,
                )

            if get_server_settings().channel.security == SecurityMode.SECURE:
                self._add_secure_port(server)
            else:
                self._add_insecure_port(server)

        except Exception as e:
            msg = f"Failed to create server: {e}"
            raise ConfigurationError(msg) from e
        else:
            return server

    def _add_secure_port(self, server: GrpcServer) -> None:  # noqa: PLR6301
        """Add a secure port using credentials from settings.

        Args:
            server: The gRPC server to add the port to.

        Raises:
            SecurityError: If credentials are missing or unreadable.
        """
        creds = get_server_settings().channel.credentials
        if not creds:
            msg = "Credentials must be provided for secure server"
            raise SecurityError(msg)

        try:  # noqa: PLW0717
            if creds.key_path and creds.cert_path:
                private_key = Path(creds.key_path).read_bytes()
                certificate_chain = Path(creds.cert_path).read_bytes()
            else:
                msg = "Key path and certificate path must be provided for secure server"
                raise SecurityError(msg)

            root_certificates = None
            if creds.root_cert_path:
                root_certificates = Path(creds.root_cert_path).read_bytes()
        except OSError as e:
            msg = f"Failed to read credential files: {e}"
            raise SecurityError(msg) from e

        try:  # noqa: PLW0717
            server_credentials = grpc.ssl_server_credentials(
                [(private_key, certificate_chain)],
                root_certificates=root_certificates,
                require_client_auth=(root_certificates is not None),
            )

            if get_server_settings().channel.communication_mode == ControlFlow.ASYNC:
                async_server = cast("grpc_aio.Server", server)
                async_server.add_secure_port(get_server_settings().channel.address, server_credentials)
            else:
                sync_server = cast("grpc.Server", server)
                sync_server.add_secure_port(get_server_settings().channel.address, server_credentials)

            logger.debug("Added secure port %s", get_server_settings().channel.address)
        except Exception as e:
            msg = f"Failed to configure with actual settings secure port: {e}"
            raise SecurityError(msg) from e

    def _add_insecure_port(self, server: GrpcServer) -> None:  # noqa: PLR6301
        """Add an insecure port.

        Args:
            server: The gRPC server to add the port to.

        Raises:
            ConfigurationError: If adding the insecure port fails.
        """
        try:  # noqa: PLW0717
            if get_server_settings().channel.communication_mode == ControlFlow.ASYNC:
                async_server = cast("grpc_aio.Server", server)
                async_server.add_insecure_port(get_server_settings().channel.address)
            else:
                sync_server = cast("grpc.Server", server)
                sync_server.add_insecure_port(get_server_settings().channel.address)

            logger.debug("Added insecure port %s", get_server_settings().channel.address)
        except Exception as e:
            msg = f"Failed to add insecure port: {e}"
            raise ConfigurationError(msg) from e

    def start(self) -> None:
        """Start the gRPC server (sync or async per settings).

        Raises:
            ServerStateError: If the server fails to start.
        """
        self.server = self._create_server()
        self._register_servicers()
        self._add_health_service()
        self._add_reflection()

        logger.debug("Starting gRPC server on %s", get_server_settings().channel.address)
        try:  # noqa: PLW0717
            if get_server_settings().channel.communication_mode == ControlFlow.ASYNC:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                loop.run_until_complete(self._start_async())
            else:
                sync_server = cast("grpc.Server", self.server)
                sync_server.start()
            logger.debug("✅ gRPC server started on %s", get_server_settings().channel.address)
        except Exception as e:
            logger.exception("❎ Error starting server")
            msg = f"Failed to start server: {e}"
            raise ServerStateError(msg) from e

    async def _start_async(self) -> None:
        """Start the async gRPC server.

        Raises:
            ServerStateError: If the server is not created.
        """
        if self.server is None:
            msg = "Server is not created"
            raise ServerStateError(msg)

        async_server = cast("grpc_aio.Server", self.server)
        await async_server.start()

    async def start_async(self) -> None:
        """Start the gRPC server in an async context.

        Raises:
            ServerStateError: If the server fails to start.
        """
        self.server = self._create_server()
        self._register_servicers()
        self._add_health_service()
        self._add_reflection()

        logger.debug("Starting gRPC server on %s", get_server_settings().channel.address)
        try:  # noqa: PLW0717
            if get_server_settings().channel.communication_mode == ControlFlow.ASYNC:
                await self._start_async()
            else:
                sync_server = cast("grpc.Server", self.server)
                sync_server.start()
            logger.debug("✅ gRPC server started on %s", get_server_settings().channel.address)
        except Exception as e:
            logger.exception("❎ Error starting server")
            msg = f"Failed to start server: {e}"
            raise ServerStateError(msg) from e

    def stop(self, grace: float | None = None) -> None:
        """Stop the gRPC server and close cached client channels.

        Args:
            grace: Optional grace period in seconds.
        """
        if self.server is None:
            logger.warning("Attempted to stop server, but no server is running")
            return

        logger.debug("Stopping gRPC server...")
        if get_server_settings().channel.communication_mode == ControlFlow.ASYNC:
            try:  # noqa: PLW0717
                loop = asyncio.get_event_loop()

                if loop.is_running():
                    logger.warning(
                        "Called stop() on async server from a running event loop. "
                        "Use await stop_async() in async contexts instead."
                    )
                    self.server = None
                    logger.debug("✅ gRPC server marked as stopped")
                    return
                loop.run_until_complete(self._stop_async(grace))
                loop.run_until_complete(GrpcClientWrapper.close_all_cached_channels())
            except RuntimeError:
                logger.debug("Creating new event loop for shutdown")
                try:
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    new_loop.run_until_complete(self._stop_async(grace))
                    new_loop.run_until_complete(GrpcClientWrapper.close_all_cached_channels())
                finally:
                    new_loop.close()
        else:
            sync_server = cast("grpc.Server", self.server)
            sync_server.stop(grace=grace)

        logger.debug("✅ gRPC server stopped")
        self.server = None

    async def _stop_async(self, grace: float | None = None) -> None:
        """Stop the async gRPC server.

        Args:
            grace: Optional grace period in seconds.
        """
        if self.server is None:
            return

        async_server = cast("grpc_aio.Server", self.server)
        await async_server.stop(grace=grace)

    async def stop_async(self, grace: float | None = None) -> None:
        """Stop the gRPC server in an async context and close cached channels.

        Args:
            grace: Optional grace period in seconds.
        """
        if self.server is None:
            logger.warning("Attempted to stop server, but no server is running")
            return

        logger.debug("Stopping gRPC server asynchronously...")
        if get_server_settings().channel.communication_mode == ControlFlow.ASYNC:
            await self._stop_async(grace)
        else:
            sync_server = cast("grpc.Server", self.server)
            sync_server.stop(grace=grace)

        await GrpcClientWrapper.close_all_cached_channels()
        logger.debug("✅ gRPC server stopped")
        self.server = None

    def wait_for_termination(self) -> None:
        """Block until the sync server terminates; warn on async mode."""
        if self.server is None:
            logger.warning("Attempted to wait for termination, but no server is running")
            return

        if get_server_settings().channel.communication_mode == ControlFlow.SYNC:
            sync_server = cast("grpc.Server", self.server)
            sync_server.wait_for_termination()
        else:
            logger.warning(
                "Called wait_for_termination on async server. Use await_termination instead for async servers.",
            )

    async def await_termination(self) -> None:
        """Await termination of the async server; warn on sync mode."""
        if get_server_settings().channel.communication_mode == ControlFlow.SYNC:
            logger.warning(
                "Called await_termination on sync server. Use wait_for_termination instead for sync servers.",
            )
            return

        if self.server is None:
            logger.warning("Attempted to await termination, but no server is running")
            return

        async_server = cast("grpc_aio.Server", self.server)
        await async_server.wait_for_termination()
