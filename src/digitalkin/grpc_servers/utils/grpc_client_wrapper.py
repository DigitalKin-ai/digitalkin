"""Client wrapper to ease channel creation with specific ServerConfig."""

from pathlib import Path
from typing import Any

import grpc

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

    def _init_channel(self, config: ClientConfig) -> grpc.Channel:
        """Create an appropriate channel to the registry server.

        Returns:
            A gRPC channel for communication with the registry.

        Raises:
            ValueError: If credentials are required but not provided.
        """
        if config.security == SecurityMode.SECURE and config.credentials is not None:
            # Secure channel
            root_certificates = Path(config.credentials.root_cert_path).read_bytes()

            # mTLS channel
            private_key = None
            certificate_chain = None
            if config.credentials.client_cert_path is not None and config.credentials.client_key_path is not None:
                private_key = Path(config.credentials.client_key_path).read_bytes()
                certificate_chain = Path(config.credentials.client_cert_path).read_bytes()

            # Create channel credentials
            channel_credentials = grpc.ssl_channel_credentials(
                root_certificates=root_certificates,
                certificate_chain=certificate_chain,
                private_key=private_key,
            )

            channel = grpc.secure_channel(config.address, channel_credentials, options=config.grpc_options)
            self._channel = channel
            return channel
        # Insecure channel
        channel = grpc.insecure_channel(config.address, options=config.grpc_options)
        self._channel = channel
        return channel

    def close_channel(self) -> None:
        """Close the gRPC channel if it exists."""
        if self._channel is not None:
            self._channel.close()
            self._channel = None

    def exec_grpc_query(self, query_endpoint: str, request: Any) -> Any:  # noqa: ANN401
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
            return response  # noqa: TRY300
        except grpc.RpcError as e:
            status_code = e.code().name if hasattr(e, "code") else "UNKNOWN"
            status_value = e.code().value[0] if hasattr(e, "code") else -1
            details = e.details() if hasattr(e, "details") else str(e)

            # Build comprehensive error message for the caller
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
