"""Client wrapper to ease channel creation with specific ServerConfig."""

import logging
from pathlib import Path
from typing import Any

import grpc

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.grpc_servers.utils.models import ClientConfig, SecurityMode

logger = logging.getLogger(__name__)


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

            return grpc.secure_channel(config.address, channel_credentials)
        # Insecure channel
        return grpc.insecure_channel(config.address)

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
