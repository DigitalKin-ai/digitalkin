"""Base gRPC server implementation for DigitalKin."""

import abc
import asyncio
import logging
from collections.abc import Callable
from concurrent import futures
from pathlib import Path
from typing import Any, cast

import grpc
from grpc import aio as grpc_aio

from digitalkin.grpc_servers.utils.exceptions import (
    ConfigurationError,
    ReflectionError,
    SecurityError,
    ServerStateError,
    ServicerError,
)
from digitalkin.grpc_servers.utils.models import SecurityMode, ServerConfig, ServerMode
from digitalkin.grpc_servers.utils.types import GrpcServer, ServiceDescriptor, T

logger = logging.getLogger(__name__)


class BaseServer(abc.ABC):
    """Base class for gRPC servers in DigitalKin.

    This class provides the foundation for both synchronous and asynchronous gRPC
    servers used in the DigitalKin ecosystem. It supports both secure and insecure
    communication modes.

    Attributes:
        config: The server configuration.
        server: The gRPC server instance (either sync or async).
        _servicers: List of registered servicers.
        _service_names: List of service names for reflection.
        _health_servicer: Optional health check servicer.
    """

    def __init__(self, config: ServerConfig) -> None:
        """Initialize the base gRPC server.

        Args:
            config: The server configuration.
        """
        self.config = config
        self.server: GrpcServer | None = None
        self._servicers: list[Any] = []
        self._service_names: list[str] = []  # Track service names for reflection
        self._health_servicer: Any = None  # For health checking

    def register_servicer(
        self,
        servicer: T,
        add_to_server_fn: Callable[[T, GrpcServer], None],
        service_descriptor: ServiceDescriptor | None = None,
        service_names: list[str] | None = None,
    ) -> None:
        """Register a servicer with the gRPC server and track it for reflection.

        Args:
            servicer: The servicer implementation instance
            add_to_server_fn: The function to add the servicer to the server
            service_descriptor: Optional service descriptor (pb2 DESCRIPTOR)
            service_names: Optional explicit list of service full names

        Raises:
            ServicerError: If the server is not created before calling
        """
        if self.server is None:
            msg = "Server must be created before registering servicers"
            raise ServicerError(msg)

        # Register the servicer
        try:
            add_to_server_fn(servicer, self.server)
            self._servicers.append(servicer)
        except Exception as e:
            msg = f"Failed to register servicer: {e}"
            raise ServicerError(msg) from e

        # Add service names from explicit list
        if service_names:
            for name in service_names:
                if name not in self._service_names:
                    self._service_names.append(name)
                    logger.debug("Registered explicit service name for reflection: %s", name)

        # If a descriptor is provided, extract service names
        if service_descriptor and hasattr(service_descriptor, "services_by_name"):
            for service_name in service_descriptor.services_by_name:
                full_name = service_descriptor.services_by_name[service_name].full_name  # ignore: PLC0206
                if full_name not in self._service_names:
                    self._service_names.append(full_name)
                    logger.debug("Registered service name from descriptor: %s", full_name)

    @abc.abstractmethod
    def _register_servicers(self) -> None:
        """Register servicers with the gRPC server.

        This method should be implemented by subclasses to register
        the appropriate servicers for their specific functionality.

        Raises:
            ServicerError: If the server is not created before calling this method.
        """

    def _add_reflection(self) -> None:
        """Add reflection service to the gRPC server if enabled.

        Raises:
            ReflectionError: If reflection initialization fails.
        """
        if not self.config.enable_reflection or self.server is None or not self._service_names:
            return

        try:
            # Import here to avoid dependency if not used
            from grpc_reflection.v1alpha import reflection  # noqa: PLC0415

            # Get all registered service names
            service_names = self._service_names.copy()

            # Add the reflection service name
            reflection_service = reflection.SERVICE_NAME
            service_names.append(reflection_service)

            # Register services with the reflection service
            # This creates a dynamic file descriptor database that can respond to
            # reflection queries with detailed service information
            reflection.enable_server_reflection(service_names, self.server)

            logger.info("Added gRPC reflection service with services: %s", service_names)
        except ImportError:
            logger.warning("Could not enable reflection: grpcio-reflection package not installed")
        except Exception as e:
            error_msg = f"Failed to enable reflection: {e}"
            logger.warning(error_msg)
            raise ReflectionError(error_msg) from e

    def _add_health_service(self) -> None:
        """Add health checking service to the gRPC server.

        The health service allows clients to check server status.
        """
        if self.server is None:
            return

        try:
            # Import here to avoid dependency if not used
            from grpc_health.v1 import health_pb2, health_pb2_grpc  # noqa: PLC0415
            from grpc_health.v1.health import HealthServicer  # noqa: PLC0415

            # Create health servicer
            health_servicer = HealthServicer()

            # Register health servicer
            health_pb2_grpc.add_HealthServicer_to_server(health_servicer, self.server)

            # Add service name to reflection list
            if health_pb2.DESCRIPTOR.services_by_name:
                service_name = health_pb2.DESCRIPTOR.services_by_name["Health"].full_name
                if service_name not in self._service_names:
                    self._service_names.append(service_name)

            logger.info("Added gRPC health checking service")

            # Set all services as SERVING
            for service_name in self._service_names:
                health_servicer.set(service_name, health_pb2.HealthCheckResponse.SERVING)

            # Set overall service status
            health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

            # Store reference to health servicer
            self._health_servicer = health_servicer

        except ImportError:
            logger.warning("Could not enable health service: grpcio-health-checking package not installed")
        except Exception as e:
            logger.warning("Failed to enable health service: %s", e)

    def _create_server(self) -> GrpcServer:
        """Create a gRPC server instance based on the configuration.

        Returns:
            A configured gRPC server instance.

        Raises:
            ConfigurationError: If the server configuration is invalid.
        """
        try:
            # Create the server based on mode
            if self.config.mode == ServerMode.ASYNC:
                server = grpc_aio.server(options=self.config.server_options)
            else:
                server = grpc.server(
                    futures.ThreadPoolExecutor(max_workers=self.config.max_workers),
                    options=self.config.server_options,
                )

            # Add the appropriate port
            if self.config.security == SecurityMode.SECURE:
                self._add_secure_port(server)
            else:
                self._add_insecure_port(server)

        except Exception as e:
            msg = f"Failed to create server: {e}"
            raise ConfigurationError(msg) from e
        else:
            return server

    def _add_secure_port(self, server: GrpcServer) -> None:
        """Add a secure port to the server.

        Args:
            server: The gRPC server to add the port to.

        Raises:
            SecurityError: If credentials are not configured correctly.
        """
        if not self.config.credentials:
            msg = "Credentials must be provided for secure server"
            raise SecurityError(msg)

        try:
            # Read key and certificate files
            private_key = Path(self.config.credentials.server_key_path).read_bytes()
            certificate_chain = Path(self.config.credentials.server_cert_path).read_bytes()

            # Read root certificate if provided
            root_certificates = None
            if self.config.credentials.root_cert_path:
                root_certificates = Path(self.config.credentials.root_cert_path).read_bytes()
        except OSError as e:
            msg = f"Failed to read credential files: {e}"
            raise SecurityError(msg) from e

        try:
            # Create server credentials
            server_credentials = grpc.ssl_server_credentials(
                [(private_key, certificate_chain)],
                root_certificates=root_certificates,
                require_client_auth=(root_certificates is not None),
            )

            # Add secure port to server
            if self.config.mode == ServerMode.ASYNC:
                async_server = cast("grpc_aio.Server", server)
                async_server.add_secure_port(self.config.address, server_credentials)
            else:
                sync_server = cast("grpc.Server", server)
                sync_server.add_secure_port(self.config.address, server_credentials)

            logger.info("Added secure port %s", self.config.address)
        except Exception as e:
            msg = f"Failed to configure secure port: {e}"
            raise SecurityError(msg) from e

    def _add_insecure_port(self, server: GrpcServer) -> None:
        """Add an insecure port to the server.

        Args:
            server: The gRPC server to add the port to.

        Raises:
            ConfigurationError: If adding the insecure port fails.
        """
        try:
            if self.config.mode == ServerMode.ASYNC:
                async_server = cast("grpc_aio.Server", server)
                async_server.add_insecure_port(self.config.address)
            else:
                sync_server = cast("grpc.Server", server)
                sync_server.add_insecure_port(self.config.address)

            logger.info("Added insecure port %s", self.config.address)
        except Exception as e:
            msg = f"Failed to add insecure port: {e}"
            raise ConfigurationError(msg) from e

    def start(self) -> None:
        """Start the gRPC server.

        If using async mode, this will use the event loop to start the server.
        If using sync mode, this will start the server in a non-blocking way.

        Raises:
            ServerStateError: If the server fails to start.
        """
        self.server = self._create_server()
        self._register_servicers()

        # Add health service
        self._add_health_service()

        # Add reflection if enabled
        self._add_reflection()

        # Start the server
        logger.info("Starting gRPC server on %s", self.config.address)
        try:
            if self.config.mode == ServerMode.ASYNC:
                # For async server, use the event loop
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                loop.run_until_complete(self._start_async())
            else:
                # For sync server, directly call start
                sync_server = cast("grpc.Server", self.server)
                sync_server.start()
            logger.info("✅ gRPC server started on %s", self.config.address)
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
        """Start the gRPC server asynchronously.

        This method should be used directly in an async context.

        Raises:
            ServerStateError: If the server fails to start.
        """
        self.server = self._create_server()
        self._register_servicers()

        # Add health service
        self._add_health_service()

        # Add reflection if enabled
        self._add_reflection()

        # Start the server
        logger.info("Starting gRPC server on %s", self.config.address)
        try:
            if self.config.mode == ServerMode.ASYNC:
                await self._start_async()
            else:
                # For sync server in async context
                sync_server = cast("grpc.Server", self.server)
                sync_server.start()
            logger.info("✅ gRPC server started on %s", self.config.address)
        except Exception as e:
            logger.exception("❎ Error starting server")
            msg = f"Failed to start server: {e}"
            raise ServerStateError(msg) from e

    def stop(self, grace: float | None = None) -> None:
        """Stop the gRPC server.

        Args:
            grace: Optional grace period in seconds for existing RPCs to complete.
        """
        if self.server is None:
            logger.warning("Attempted to stop server, but no server is running")
            return

        logger.info("Stopping gRPC server...")
        if self.config.mode == ServerMode.ASYNC:
            # We'll use a different approach that works whether we're in a running event loop or not
            try:
                # Get the current event loop
                loop = asyncio.get_event_loop()

                if loop.is_running():
                    # If we're in a running event loop, we can't run_until_complete
                    # Just warn the user they should use stop_async
                    logger.warning(
                        "Called stop() on async server from a running event loop. "
                        "This might not fully shut down the server. "
                        "Use await stop_async() in async contexts instead."
                    )
                    # Set server to None to avoid further operations
                    self.server = None
                    logger.info("✅ gRPC server marked as stopped")
                    return
                # If not in a running event loop, use run_until_complete
                loop.run_until_complete(self._stop_async(grace))
            except RuntimeError:
                # Event loop issues - try with a new loop
                logger.info("Creating new event loop for shutdown")
                try:
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    new_loop.run_until_complete(self._stop_async(grace))
                finally:
                    new_loop.close()
        else:
            # For sync server, we can just call stop
            sync_server = cast("grpc.Server", self.server)
            sync_server.stop(grace=grace)

        logger.info("✅ gRPC server stopped")
        self.server = None

    async def _stop_async(self, grace: float | None = None) -> None:
        """Stop the async gRPC server.

        Args:
            grace: Optional grace period in seconds for existing RPCs to complete.
        """
        if self.server is None:
            return

        async_server = cast("grpc_aio.Server", self.server)
        await async_server.stop(grace=grace)

    async def stop_async(self, grace: float | None = None) -> None:
        """Stop the gRPC server asynchronously.

        This method should be used in async contexts.

        Args:
            grace: Optional grace period in seconds for existing RPCs to complete.
        """
        if self.server is None:
            logger.warning("Attempted to stop server, but no server is running")
            return

        logger.info("Stopping gRPC server asynchronously...")
        if self.config.mode == ServerMode.ASYNC:
            await self._stop_async(grace)
        else:
            # For sync server, we can just call stop
            sync_server = cast("grpc.Server", self.server)
            sync_server.stop(grace=grace)

        logger.info("✅ gRPC server stopped")
        self.server = None

    def wait_for_termination(self) -> None:
        """Wait for the server to terminate.

        In synchronous mode, this blocks until the server is terminated.
        In asynchronous mode, a warning is logged suggesting to use `await_termination`.
        """
        if self.server is None:
            logger.warning("Attempted to wait for termination, but no server is running")
            return

        if self.config.mode == ServerMode.SYNC:
            # For sync server
            sync_server = cast("grpc.Server", self.server)
            sync_server.wait_for_termination()
        else:
            # For async server, the caller should use await_termination instead
            logger.warning(
                "Called wait_for_termination on async server. Use await_termination instead for async servers.",
            )

    async def await_termination(self) -> None:
        """Wait for the async server to terminate.

        This method should only be used with async servers.
        """
        if self.config.mode == ServerMode.SYNC:
            logger.warning(
                "Called await_termination on sync server. Use wait_for_termination instead for sync servers.",
            )
            return

        if self.server is None:
            logger.warning("Attempted to await termination, but no server is running")
            return

        # For async server
        async_server = cast("grpc_aio.Server", self.server)
        await async_server.wait_for_termination()
