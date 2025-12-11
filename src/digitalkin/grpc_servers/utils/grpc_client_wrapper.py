"""Client wrapper to ease channel creation with specific ServerConfig."""

from pathlib import Path
from typing import Any

import grpc

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig, SecurityMode


class GrpcClientWrapper:
    """gRPC client shared by the different services."""

    stub: Any

    @staticmethod
    def _init_channel(config: ClientConfig) -> grpc.Channel:
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

            return grpc.secure_channel(config.address, channel_credentials, options=config.channel_options)
        # Insecure channel
        return grpc.insecure_channel(config.address, options=config.channel_options)

    def exec_grpc_query(self, query_endpoint: str, request: Any) -> Any:  # noqa: ANN401
        """Execute a gRPC query with from the query's rpc endpoint name.

        Arguments:
            query_endpoint: rpc query name
            request: gRPC object to match the rpc query

        Returns:
            corresponding gRPC reponse.

        Raises:
            ServerError: gRPC error catching with status code and details
        """
        service_name = getattr(self, "service_name", "unknown")
        try:
            logger.debug(
                "Sending gRPC request to %s",
                query_endpoint,
                extra={"request": str(request), "service": service_name},
            )
            response = getattr(self.stub, query_endpoint)(request)
            logger.debug(
                "Received gRPC response from %s",
                query_endpoint,
                extra={"response": str(response), "service": service_name},
            )
        except grpc.RpcError as e:
            status_code = e.code().name if hasattr(e, "code") else "UNKNOWN"
            details = e.details() if hasattr(e, "details") else str(e)
            msg = f"[{status_code}] {details}"
            logger.error(
                "gRPC %s failed: %s",
                query_endpoint,
                msg,
                extra={"service": service_name},
            )
            raise ServerError(msg) from e
        else:
            return response
