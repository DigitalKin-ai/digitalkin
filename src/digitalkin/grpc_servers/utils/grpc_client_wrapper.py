"""Client wrapper to ease channel creation with specific ServerConfig."""

import logging
from typing import Any

import grpc

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.models import SecurityMode, ServerConfig

logger = logging.getLogger(__name__)


class GrpcClientWrapper:
    """gRPC client shared by the different services."""

    stub: Any

    @staticmethod
    def _init_channel(config: ServerConfig) -> grpc.Channel:
        """Create an appropriate channel to the registry server.

        Returns:
            A gRPC channel for communication with the registry.

        Raises:
            ValueError: If credentials are required but not provided.
        """
        if config.security == SecurityMode.SECURE and config.credentials:
            # Secure channel
            with open(config.credentials.server_cert_path, "rb") as cert_file:  # noqa: FURB101
                certificate_chain = cert_file.read()

            root_certificates = None
            if config.credentials.root_cert_path:
                with open(config.credentials.root_cert_path, "rb") as root_cert_file:  # noqa: FURB101
                    root_certificates = root_cert_file.read()

            # Create channel credentials
            channel_credentials = grpc.ssl_channel_credentials(root_certificates=root_certificates or certificate_chain)

            return grpc.secure_channel(f"{config.host}:{config.port}", channel_credentials)
        # Insecure channel
        return grpc.insecure_channel(f"{config.host}:{config.port}")

    def exec_grpc_query(self, query_endpoint: str, request: Any) -> Any:  # noqa: ANN401
        """Execute a gRPC query with from the query's rpc endpoint name.

        Arguments:
            query_endpoint: rpc query name
            request: gRPC object to match the rpc query

        Returns:
            corresponding gRPC reponse.

        Raises:
            ServerError: gRPC error catching
        """
        try:
            # Call the register method
            logger.warning("send request to %s", query_endpoint)
            response = getattr(self.stub, query_endpoint)(request)
            logger.warning("recive response from request to registry: %s", response)
        except grpc.RpcError:
            logger.exception("RPC error during registration:")
            raise ServerError
        else:
            return response
