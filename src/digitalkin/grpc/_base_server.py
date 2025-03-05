"""Base gRPC server implementation for DigitalKin."""

import abc
import asyncio
import logging
from concurrent import futures
from pathlib import Path
from typing import Any, cast

import grpc
from grpc import aio as grpc_aio

from digitalkin.grpc.utils.models import SecurityMode, ServerConfig, ServerMode

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
    """

    def __init__(self, config: ServerConfig) -> None:
        """Initialize the base gRPC server.

        Args:
            config: The server configuration.
        """
        self.config = config
        self.server: grpc.Server | grpc_aio.Server | None = None
        self._servicers: list[Any] = []

    def _create_server(self) -> grpc.Server | grpc_aio.Server:
        """Create a gRPC server instance based on the configuration.

        Returns:
            A configured gRPC server instance.
        """
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

        return server

    def _add_secure_port(self, server: grpc.Server | grpc_aio.Server) -> None:
        """Add a secure port to the server.

        Args:
            server: The gRPC server to add the port to.

        Raises:
            ValueError: If credentials are not configured correctly.
        """
        if not self.config.credentials:
            msg = "Credentials must be provided for secure server"
            raise ValueError(msg)

        # Read key and certificate files
        private_key = Path(self.config.credentials.server_key_path, "rb").read_bytes()

        certificate_chain = Path(self.config.credentials.server_cert_path, "rb").read_bytes()

        # Read root certificate if provided
        root_certificates = None
        if self.config.credentials.root_cert_path:
            root_certificates = Path(self.config.credentials.root_cert_path, "rb").read_bytes()

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

    def _add_insecure_port(self, server: grpc.Server | grpc_aio.Server) -> None:
        """Add an insecure port to the server.

        Args:
            server: The gRPC server to add the port to.
        """
        if self.config.mode == ServerMode.ASYNC:
            async_server = cast("grpc_aio.Server", server)
            async_server.add_insecure_port(self.config.address)
        else:
            sync_server = cast("grpc.Server", server)
            sync_server.add_insecure_port(self.config.address)

        logger.info("Added insecure port %s", self.config.address)

    @abc.abstractmethod
    def _register_servicers(self) -> None:
        """Register servicers with the gRPC server.

        This method should be implemented by subclasses to register
        the appropriate servicers for their specific functionality.

        Raises:
            RuntimeError: If the server is not created before calling this method.
        """

    def start(self) -> None:
        """Start the gRPC server.

        If using async mode, this will not block. If using sync mode,
        this will start the server in a non-blocking way.
        """
        self.server = self._create_server()
        self._register_servicers()

        # Start the server
        logger.info("Starting gRPC server on %s", self.config.address)
        try:
            self.server.start()
            logger.info("✅ gRPC server started on %s", self.config.address)
        except Exception:
            logger.exception("❎ Error starting server")

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
            # For async server, we need to use an async approach to stop
            async_server = cast("grpc_aio.Server", self.server)

            async def _stop_async_server() -> None:
                await async_server.stop(grace=grace)

            # Create a new event loop if needed
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                loop.run_until_complete(_stop_async_server())
            except Exception:
                logger.exception("❎ Error stopping async server")
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
