"""Client wrapper to ease channel creation with specific ServerConfig."""

import asyncio
from pathlib import Path
from typing import Any, ClassVar

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
    _channel: grpc.aio.Channel | None = None

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

    def _init_channel(self, config: ClientConfig) -> grpc.aio.Channel:
        """Create an async gRPC channel.

        Args:
            config: Client configuration for the channel.

        Returns:
            An async gRPC channel.
        """
        credentials = self._build_channel_credentials(config)
        grpc_compression = config.compression.to_grpc()
        if credentials is not None:
            channel = grpc.aio.secure_channel(
                config.address, credentials, options=config.grpc_options, compression=grpc_compression
            )
        else:
            channel = grpc.aio.insecure_channel(
                config.address, options=config.grpc_options, compression=grpc_compression
            )
        self._channel = channel
        return channel

    async def close_channel(self) -> None:
        """Close the gRPC channel if it exists."""
        if self._channel is not None:
            await self._channel.close()
            self._channel = None

    _RETRYABLE_CODES: ClassVar[set[grpc.StatusCode]] = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.INTERNAL}

    async def exec_grpc_query(
        self,
        query_endpoint: str,
        request: Any,
    ) -> Any:
        """Execute a gRPC query with from the query's rpc endpoint name.

        Retries up to 2 times on transient errors (UNAVAILABLE, INTERNAL)
        with exponential backoff (50ms, 100ms).

        Arguments:
            query_endpoint: rpc query name (e.g., "GetSetup", "CreateSetupVersion")
            request: gRPC protobuf request object

        Returns:
            gRPC protobuf response object.

        Raises:
            ServerError: gRPC error with status code and details for caller to handle.
        """
        max_retries = 2
        backoff_delays = (0.05, 0.1)
        last_error: grpc.RpcError | None = None

        for attempt in range(max_retries + 1):
            if attempt > 0:
                await asyncio.sleep(backoff_delays[attempt - 1])

            try:
                logger.debug(
                    "gRPC request: %s.%s - sending request to remote service",
                    self.service_name,
                    query_endpoint,
                    extra={
                        "service_name": self.service_name,
                        "endpoint": query_endpoint,
                        "request_type": type(request).__name__,
                        "request_preview": str(request)[:200],
                    },
                )
                # getattr unavoidable: gRPC stubs expose RPC methods as dynamic attributes
                response = await getattr(self.stub, query_endpoint)(request)
                logger.debug(
                    "gRPC response: %s.%s - received response from remote service",
                    self.service_name,
                    query_endpoint,
                    extra={
                        "service_name": self.service_name,
                        "endpoint": query_endpoint,
                        "response_type": type(response).__name__,
                        "response_preview": str(response)[:200],
                    },
                )
            except grpc.RpcError as e:
                last_error = e
                if e.code() not in self._RETRYABLE_CODES or attempt == max_retries:
                    break
                logger.warning(
                    "gRPC transient error on %s.%s [%s] (attempt %d/%d), retrying in %.0fms",
                    self.service_name,
                    query_endpoint,
                    e.code().name,
                    attempt + 1,
                    max_retries + 1,
                    backoff_delays[attempt] * 1000,
                )
            else:
                return response

        if last_error is None:
            msg = f"[gRPC-client:{self.service_name}.{query_endpoint}] Retry loop exited without response or error"
            raise ServerError(msg)
        status_code = last_error.code().name
        status_value = last_error.code().value[0]
        details = last_error.details()
        retried = last_error.code() in self._RETRYABLE_CODES
        suffix = f" (after {max_retries + 1} attempts)" if retried else ""

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
        error_msg = f"[gRPC-client:{self.service_name}.{query_endpoint}] [{status_code}] {details}{suffix}"
        raise ServerError(error_msg) from last_error
