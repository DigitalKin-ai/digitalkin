"""Client wrapper to ease channel creation with specific ServerConfig.

Includes per-service circuit breaker protection: when a downstream service
fails repeatedly, subsequent calls fail fast with ``CircuitOpenError``
instead of waiting for the full timeout. This prevents cascade failure
amplification across the mesh.
"""

import asyncio
import logging
from pathlib import Path
from typing import Any, ClassVar

import grpc
import grpc.aio

from digitalkin.core.resilience.bulkhead import Bulkhead
from digitalkin.grpc_servers.exceptions import CircuitOpenError, ServerError
from digitalkin.grpc_servers.interceptors.permission import PermissionClientInterceptor
from digitalkin.grpc_servers.interceptors.request_ids import RequestIdClientInterceptor
from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.settings.grpc_client import get_grpc_client_settings
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
    _stub_cache: ClassVar[dict[tuple[str, type], Any]] = {}

    _RETRYABLE_CODES: ClassVar[set[grpc.StatusCode]] = {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.INTERNAL,
        grpc.StatusCode.DEADLINE_EXCEEDED,
    }

    # Codes that count toward opening the circuit (service-health failures).
    # Application-level codes (NOT_FOUND, INVALID_ARGUMENT, …) mean the service
    # responded, so they never trip the breaker.
    _CIRCUIT_FAILURE_CODES: ClassVar[set[grpc.StatusCode]] = _RETRYABLE_CODES | {
        grpc.StatusCode.UNKNOWN,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    }

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
        interceptors = [RequestIdClientInterceptor(), PermissionClientInterceptor()]
        if credentials is not None:
            channel = grpc.aio.secure_channel(
                config.address,
                credentials,
                options=config.grpc_options,
                compression=grpc_compression,
                interceptors=interceptors,
            )
        else:
            channel = grpc.aio.insecure_channel(
                config.address,
                options=config.grpc_options,
                compression=grpc_compression,
                interceptors=interceptors,
            )
        GrpcClientWrapper._channel_cache[cache_key] = channel
        GrpcClientWrapper._ref_counts[cache_key] = 1
        self._channel = channel
        self._channel_cache_key = cache_key
        return channel

    def _get_or_create_stub(self, stub_class: type) -> Any:
        """Get a cached stub or create one for the current channel.

        Stubs are stateless wrappers — same class on same channel is identical.
        Caching avoids per-request object allocation.

        Args:
            stub_class: gRPC stub class (e.g., StorageServiceStub).

        Returns:
            Cached or newly created stub instance.
        """
        cache_key = self._channel_cache_key
        if cache_key is not None:
            key = (cache_key, stub_class)
            cached = GrpcClientWrapper._stub_cache.get(key)
            if cached is not None:
                return cached
            stub = stub_class(self._channel)
            GrpcClientWrapper._stub_cache[key] = stub
            return stub
        return stub_class(self._channel)

    async def close(self) -> None:
        """Release this instance's gRPC channel ref. Subclasses override to release extra resources."""
        await self.close_channel()

    async def close_channel(self) -> None:
        """Release this instance's ref on the cached channel.

        The underlying channel is only closed when the last ref is released.
        When the last ref is released, the corresponding circuit breaker
        singleton is also removed to prevent unbounded accumulation.
        """
        if self._channel is None:
            return
        if (key := self._channel_cache_key) is not None and key in GrpcClientWrapper._ref_counts:
            GrpcClientWrapper._ref_counts[key] -= 1
            if GrpcClientWrapper._ref_counts[key] <= 0:
                GrpcClientWrapper._ref_counts.pop(key, None)
                GrpcClientWrapper._channel_cache.pop(key, None)
                GrpcClientWrapper._stub_cache = {k: v for k, v in GrpcClientWrapper._stub_cache.items() if k[0] != key}
                await self._channel.close()
                CircuitBreaker.remove(self.service_name)
                Bulkhead.remove(self.service_name)
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
            # Purge stubs bound to the closing channel.
            cls._stub_cache = {k: v for k, v in cls._stub_cache.items() if k[0] != key}
            if channel is not None:
                await channel.close()

    @classmethod
    async def evict_cached_channel(cls, key: str) -> None:
        """Force-close and remove a cached channel regardless of refcount.

        Guarantees a fresh connection on re-dial: a channel left cached after
        a peer died can be wedged mid-reconnect, so a resume must not reuse it.
        A missing key is a no-op.

        Args:
            key: Channel cache key to evict.
        """
        cls._ref_counts.pop(key, None)
        channel = cls._channel_cache.pop(key, None)
        cls._stub_cache = {k: v for k, v in cls._stub_cache.items() if k[0] != key}
        if channel is not None:
            await channel.close()

    @classmethod
    async def close_all_cached_channels(cls) -> None:
        """Close all cached channels, reset cache, and clear circuit breakers.

        Intended for server shutdown to ensure clean resource release.
        Clears circuit breaker singletons to prevent unbounded growth
        from dynamically discovered services.
        """
        for channel in cls._channel_cache.values():
            await channel.close()
        cls._channel_cache.clear()
        cls._ref_counts.clear()
        cls._stub_cache.clear()
        CircuitBreaker.clear_all()

    async def exec_grpc_query(  # noqa: C901, PLR0912, PLR0914, PLR0915
        self,
        query_endpoint: str,
        request: Any,
        timeout: float | None = None,
    ) -> Any:
        """Execute a gRPC query with circuit breaker protection and retry.

        The circuit breaker is per-service (keyed on ``service_name``).
        When the circuit is OPEN, calls fail immediately with ``CircuitOpenError``
        wrapped in ``ServerError`` — no network round-trip, no timeout wait.

        Retries on transient errors (UNAVAILABLE, INTERNAL, DEADLINE_EXCEEDED)
        with exponential backoff. Retry count and backoff base are configurable
        via DIGITALKIN_GRPC_QUERY_MAX_RETRIES and DIGITALKIN_GRPC_QUERY_BACKOFF_BASE_MS.

        Arguments:
            query_endpoint: rpc query name (e.g., "GetSetup", "CreateSetupVersion")
            request: gRPC protobuf request object
            timeout: Per-call timeout in seconds. ``None`` applies no client-side deadline.

        Returns:
            gRPC protobuf response object.

        Raises:
            ServerError: gRPC error with status code and details for caller to handle.
        """
        rpc_method = getattr(self.stub, query_endpoint, None)
        if rpc_method is None:
            # M3: validate the method before claiming the half-open probe lock,
            # so a missing method can't escape cb.check() and wedge the breaker.
            logger.info(
                "[VALIDATE M3] RPC method missing, raised before breaker probe: %s.%s",
                self.service_name,
                query_endpoint,
            )  # TODO(validate): remove after prod validation
            msg = f"[gRPC-client:{self.service_name}] RPC method '{query_endpoint}' not found on stub"
            raise ServerError(msg)

        cb = CircuitBreaker.get_or_create(self.service_name)
        try:
            cb.check()
        except CircuitOpenError as e:
            error_msg = f"[gRPC-client:{self.service_name}.{query_endpoint}] {e}"
            raise ServerError(error_msg) from e

        grpc_settings = get_grpc_client_settings()
        eff_timeout = timeout if timeout is not None else grpc_settings.timeout
        if timeout is None:
            logger.info(
                "[VALIDATE H1] default gRPC deadline applied: %.1fs (%s.%s)",
                eff_timeout,
                self.service_name,
                query_endpoint,
            )  # TODO(validate): remove after prod validation
        max_retries = grpc_settings.max_retries
        backoff_base_ms = grpc_settings.backoff_base_ms
        backoff_delays = tuple(backoff_base_ms / 1000 * (2**i) for i in range(max_retries))
        last_error: grpc.RpcError | None = None

        try:
            for attempt in range(max_retries + 1):
                if attempt > 0:
                    await asyncio.sleep(backoff_delays[attempt - 1])

                try:
                    response = await rpc_method(request, timeout=eff_timeout)
                except grpc.RpcError as e:
                    last_error = e
                    if e.code() in self._RETRYABLE_CODES and attempt < max_retries:
                        logger.warning(
                            "gRPC transient error on %s.%s [%s] (attempt %d/%d), retrying in %.0fms",
                            self.service_name,
                            query_endpoint,
                            e.code().name,
                            attempt + 1,
                            max_retries + 1,
                            backoff_delays[attempt] * 1000,
                        )
                        continue
                    if e.code() in self._CIRCUIT_FAILURE_CODES:
                        logger.warning(
                            "circuit-breaker tick: %s.%s [%s]",
                            self.service_name,
                            query_endpoint,
                            e.code().name,
                        )
                        cb.record_failure()
                    else:
                        cb.record_success()
                    break
                else:
                    cb.record_success()
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
        finally:
            # Free a half-open probe the loop never resolved (e.g. CancelledError mid-call or backoff).
            if cb.release_probe():
                logger.info(
                    "[VALIDATE R1] released abandoned circuit probe: %s.%s",
                    self.service_name,
                    query_endpoint,
                )  # TODO(validate): remove after prod validation
