"""Client wrapper to ease channel creation with specific ServerConfig."""

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, ClassVar

import grpc
import grpc.aio

from digitalkin.grpc_servers.utils.exceptions import ServerError
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.settings.utils.channel import SecurityMode


class GrpcClientWrapper:
    """gRPC client shared by the different services.

    Subclasses should set the service_name class attribute to identify
    the gRPC service in logs (e.g., "SetupService", "RegistryService").

    Channels are cached at the class level and ref-counted. gRPC HTTP/2
    channels natively multiplex concurrent streams on a single connection,
    so sharing a channel across tasks is safe and efficient.
    """

    stub: Any
    service_name: str = "UnknownService"  # Override in subclasses for better logging
    _channel: grpc.aio.Channel | None = None
    _channel_cache_key: str | None = None
    _channel_cache: ClassVar[dict[str, grpc.aio.Channel]] = {}
    _ref_counts: ClassVar[dict[str, int]] = {}
    _RETRYABLE_CODES: ClassVar[set[grpc.StatusCode]] = {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    }
    _QUERY_MAX_RETRIES: ClassVar[int] = int(os.environ.get("DIGITALKIN_GRPC_QUERY_MAX_RETRIES", "2"))
    _QUERY_BACKOFF_BASE_MS: ClassVar[float] = float(os.environ.get("DIGITALKIN_GRPC_QUERY_BACKOFF_BASE_MS", "50"))

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
        """Get or create a cached async gRPC channel.

        Channels are keyed by (address, security, compression) and ref-counted.
        Multiple instances sharing the same config reuse one HTTP/2 connection.

        Args:
            config: Client configuration for the channel.

        Returns:
            An async gRPC channel (may be shared with other instances).
        """
        cache_key = f"{config.address}:{config.security.value}:{config.compression.value}"
        if cache_key in GrpcClientWrapper._channel_cache:
            GrpcClientWrapper._ref_counts[cache_key] += 1
            channel = GrpcClientWrapper._channel_cache[cache_key]
            self._channel = channel
            self._channel_cache_key = cache_key
            return channel

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
        GrpcClientWrapper._channel_cache[cache_key] = channel
        GrpcClientWrapper._ref_counts[cache_key] = 1
        self._channel = channel
        self._channel_cache_key = cache_key
        return channel

    async def close(self) -> None:
        """Release this instance's gRPC channel ref. Subclasses override to release extra resources."""
        await self.close_channel()

    async def close_channel(self) -> None:
        """Release this instance's ref on the cached channel.

        The underlying channel is only closed when the last ref is released.
        """
        if self._channel is None:
            return
        if (key := self._channel_cache_key) is not None and key in GrpcClientWrapper._ref_counts:
            GrpcClientWrapper._ref_counts[key] -= 1
            if GrpcClientWrapper._ref_counts[key] <= 0:
                GrpcClientWrapper._ref_counts.pop(key, None)
                GrpcClientWrapper._channel_cache.pop(key, None)
                await self._channel.close()
        else:
            await self._channel.close()
        self._channel = None

    @classmethod
    async def release_cached_channel(cls, key: str) -> None:
        """Decrement refcount for a cache key and close channel when last ref is released.

        Args:
            key: Channel cache key to release.
        """
        if key not in cls._ref_counts:
            return
        cls._ref_counts[key] -= 1
        if cls._ref_counts[key] <= 0:
            cls._ref_counts.pop(key, None)
            channel = cls._channel_cache.pop(key, None)
            if channel is not None:
                await channel.close()

    @classmethod
    async def close_all_cached_channels(cls) -> None:
        """Close all cached channels and reset the cache.

        Intended for server shutdown to ensure clean resource release.
        """
        for channel in cls._channel_cache.values():
            await channel.close()
        cls._channel_cache.clear()
        cls._ref_counts.clear()

    async def exec_grpc_query(
        self,
        query_endpoint: str,
        request: Any,
        timeout: float | None = None,
    ) -> Any:
        """Execute a gRPC query with from the query's rpc endpoint name.

        Retries on transient errors (UNAVAILABLE, INTERNAL, DEADLINE_EXCEEDED)
        with exponential backoff. Retry count and backoff base are configurable
        via DIGITALKIN_GRPC_QUERY_MAX_RETRIES and DIGITALKIN_GRPC_QUERY_BACKOFF_BASE_MS.

        Arguments:
            query_endpoint: rpc query name (e.g., "GetSetup", "CreateSetupVersion")
            request: gRPC protobuf request object
            timeout: Optional per-call timeout in seconds (passed to gRPC stub call)

        Returns:
            gRPC protobuf response object.

        Raises:
            ServerError: gRPC error with status code and details for caller to handle.
        """
        max_retries = self._QUERY_MAX_RETRIES
        backoff_delays = tuple(self._QUERY_BACKOFF_BASE_MS / 1000 * (2**i) for i in range(max_retries))
        last_error: grpc.RpcError | None = None

        for attempt in range(max_retries + 1):
            if attempt > 0:
                await asyncio.sleep(backoff_delays[attempt - 1])

            try:
                # getattr unavoidable: gRPC stubs expose RPC methods as dynamic attributes
                response = await getattr(self.stub, query_endpoint)(request, timeout=timeout)
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
        details = last_error.details()
        retried = last_error.code() in self._RETRYABLE_CODES
        suffix = f" (after {max_retries + 1} attempts)" if retried else ""

        log_level = logging.DEBUG if last_error.code() == grpc.StatusCode.NOT_FOUND else logging.ERROR
        logger.log(
            log_level,
            "gRPC call failed: %s.%s [%s] %s (%s)",
            self.service_name,
            query_endpoint,
            status_code,
            details,
            type(request).__name__,
        )
        error_msg = f"[gRPC-client:{self.service_name}.{query_endpoint}] [{status_code}] {details}{suffix}"
        raise ServerError(error_msg) from last_error

    async def poll_grpc(self, endpoint: str, request: Any, *, timeout: float) -> Any | None:
        """Execute a single polling RPC. Returns None on DEADLINE_EXCEEDED (expected empty poll).

        Unlike exec_grpc_query, DEADLINE_EXCEEDED is not an error for polling-style RPCs
        where the server holds the connection until a result is available or timeout occurs.
        No retry is performed — the caller is responsible for the retry loop.

        Args:
            endpoint: RPC method name on self.stub.
            request: gRPC request protobuf.
            timeout: Seconds before treating as 'no result available'.

        Returns:
            gRPC response, or None if DEADLINE_EXCEEDED.

        Raises:
            ServerError: For any non-DEADLINE_EXCEEDED gRPC error.
        """
        try:
            # getattr unavoidable: gRPC stubs expose RPC methods as dynamic attributes
            return await getattr(self.stub, endpoint)(request, timeout=timeout)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                return None
            msg = f"[{self.service_name}.{endpoint}] [{e.code().name}] {e.details()}"
            raise ServerError(msg) from e
