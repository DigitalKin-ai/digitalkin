"""Client wrapper to ease channel creation with specific ServerConfig."""

from pathlib import Path
from typing import Any

import grpc
import grpc.aio

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig, SecurityMode


class GrpcClientWrapper:
    """gRPC client shared by the different services.

    Subclasses should set the service_name class attribute to identify
    the gRPC service in logs (e.g., "SetupService", "RegistryService").
    """

    stub: Any
    service_name: str = "UnknownService"  # Override in subclasses for better logging
    _channel: grpc.Channel | None = None

    @staticmethod
    def _build_channel_credentials(config: ClientConfig) -> grpc.ChannelCredentials | None:
        """Build SSL channel credentials from config if secure mode.

        Args:
            config: Client configuration with security and credential settings.

        Returns:
            Channel credentials for secure mode, None for insecure.
        """
        if config.security != SecurityMode.SECURE or config.credentials is None:
            return None
        root_certificates = Path(config.credentials.root_cert_path).read_bytes()
        private_key = None
        certificate_chain = None
        if config.credentials.client_cert_path is not None and config.credentials.client_key_path is not None:
            private_key = Path(config.credentials.client_key_path).read_bytes()
            certificate_chain = Path(config.credentials.client_cert_path).read_bytes()
        return grpc.ssl_channel_credentials(
            root_certificates=root_certificates,
            certificate_chain=certificate_chain,
            private_key=private_key,
        )

    def _init_channel(self, config: ClientConfig) -> grpc.Channel:
        """Create a sync gRPC channel.

        Args:
            config: Client configuration for the channel.

        Returns:
            A sync gRPC channel.
        """
        credentials = self._build_channel_credentials(config)
        if credentials is not None:
            channel = grpc.secure_channel(config.address, credentials, options=config.grpc_options)
        else:
            channel = grpc.insecure_channel(config.address, options=config.grpc_options)
        self._channel = channel
        return channel

    def _init_aio_channel(self, config: ClientConfig) -> grpc.aio.Channel:
        """Create an async gRPC channel.

        Args:
            config: Client configuration for the channel.

        Returns:
            An async gRPC channel.
        """
        credentials = self._build_channel_credentials(config)
        if credentials is not None:
            return grpc.aio.secure_channel(config.address, credentials, options=config.grpc_options)
        return grpc.aio.insecure_channel(config.address, options=config.grpc_options)

    def close_channel(self) -> None:
        """Close the gRPC channel if it exists."""
        if self._channel is not None:
            self._channel.close()
            self._channel = None

    def exec_grpc_query(
        self,
        query_endpoint: str,
        request: Any,
    ) -> Any:
        """Execute a gRPC query with from the query's rpc endpoint name.

        Arguments:
            query_endpoint: rpc query name (e.g., "GetSetup", "CreateSetupVersion")
            request: gRPC protobuf request object

        Returns:
            gRPC protobuf response object.

        Raises:
            ServerError: gRPC error with status code and details for caller to handle.
        """
        try:
            logger.debug(
                "gRPC request: %s.%s - sending request to remote service",
                self.service_name,
                query_endpoint,
                extra={
                    "service_name": self.service_name,
                    "endpoint": query_endpoint,
                    "request_type": type(request).__name__,
                    "request_preview": str(request)[:200],  # Truncate for log readability
                },
            )
            # getattr unavoidable: gRPC stubs expose RPC methods as dynamic attributes by endpoint name
            response = getattr(self.stub, query_endpoint)(request)
            logger.debug(
                "gRPC response: %s.%s - received response from remote service",
                self.service_name,
                query_endpoint,
                extra={
                    "service_name": self.service_name,
                    "endpoint": query_endpoint,
                    "response_type": type(response).__name__,
                    "response_preview": str(response)[:200],  # Truncate for log readability
                },
            )
        except grpc.RpcError as e:
            status_code = e.code().name
            status_value = e.code().value[0]
            details = e.details()

            error_msg = f"[gRPC-client:{self.service_name}.{query_endpoint}] [{status_code}] {details}"

            logger.error(
                "gRPC call failed: %s.%s returned [%s] %s. "
                "The remote gRPC service returned an error or is unreachable. "
                "Check the remote service logs for more details.",
                self.service_name,
                query_endpoint,
                status_code,
                details,
                extra={
                    "service_name": self.service_name,
                    "endpoint": query_endpoint,
                    "grpc_status_code": status_code,
                    "grpc_status_value": status_value,
                    "grpc_details": details,
                    "request_type": type(request).__name__,
                },
            )
            raise ServerError(error_msg) from e
        else:
            return response
