"""gRPC client implementation for Communication service."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import TYPE_CHECKING

import grpc.aio
from agentic_mesh_protocol.gateway.v1 import gateway_pb2, gateway_service_pb2_grpc
from agentic_mesh_protocol.module.v1 import (
    information_pb2,
    module_service_pb2_grpc,
)
from google.protobuf import json_format, struct_pb2

from digitalkin.core.profiling.step_timer import StepTimer
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.circuit_breaker import CBState
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.services.base_strategy import BaseStrategy
from digitalkin.services.communication.communication_strategy import CommunicationStrategy
from digitalkin.services.communication.exceptions import (
    InvalidConsumerAddressError,
    M2MCallTimeout,
    M2MTargetUnavailable,
)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Awaitable, Callable

    from digitalkin.grpc_servers.m2m_call_registry import M2MCallRegistry


class GrpcCommunication(CommunicationStrategy, GrpcClientWrapper):
    """gRPC client for module-to-module communication.

    This class provides methods to communicate with remote modules
    using the Module Service gRPC protocol.
    """

    service_name: str = "CommunicationService"

    # Set once at ``ModuleServer._register_gateway_servicer`` startup so that
    # every per-task ``GrpcCommunication`` (built by ``services_config``)
    # can find the local M2M call registry without threading the reference
    # through every construction site.
    _shared_m2m_calls: M2MCallRegistry | None = None

    @classmethod
    def set_m2m_call_registry(cls, registry: M2MCallRegistry | None) -> None:
        """Register the process-singleton ``M2MCallRegistry`` for ``call_module`` to use.

        Called by ``ModuleServer`` exactly once at startup. ``call_module``
        then reaches the registry via this class-level slot without any
        per-task plumbing.
        """
        cls._shared_m2m_calls = registry

    @staticmethod
    def _protocol_name(data: struct_pb2.Struct) -> str:
        """Extract ``data.root.protocol`` (empty string if absent)."""
        root = data.fields.get("root")
        if root is None:
            return ""
        proto = root.struct_value.fields.get("protocol")
        return proto.string_value if proto is not None else ""

    @staticmethod
    def stream_error(data: struct_pb2.Struct) -> tuple[str, str] | None:
        """Decode a ``stream.error`` Struct yielded by :meth:`call_module`.

        The gateway emits ``stream.error`` sentinels in-band when the
        dial-back, dispatcher, or module pipeline fails. They flow through
        :meth:`call_module` as regular Structs so the caller can branch.

        Args:
            data: A Struct yielded by ``async for ... in comm.call_module(...)``.

        Returns:
            ``(code, message)`` if ``data`` is a ``stream.error`` sentinel,
            otherwise ``None``.
        """
        root = data.fields.get("root")
        if root is None:
            return None
        fields = root.struct_value.fields
        proto = fields.get("protocol")
        if proto is None or proto.string_value != "stream.error":
            return None
        code_v = fields.get("code")
        msg_v = fields.get("message")
        return (
            code_v.string_value if code_v is not None else "",
            msg_v.string_value if msg_v is not None else "",
        )

    def __init__(
        self,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        client_config: ClientConfig,
        m2m_calls: M2MCallRegistry | None = None,
    ) -> None:
        """Initialize the gRPC communication client.

        Args:
            mission_id: Mission identifier
            setup_id: Setup identifier
            setup_version_id: Setup version identifier
            client_config: Client configuration for gRPC connection
            m2m_calls: ``M2MCallRegistry`` used as the dial-back rendezvous for
                ``call_module``. ``call_module`` requires this; if ``None`` is
                passed, the class-level slot from
                :meth:`set_m2m_call_registry` is used. If neither is set,
                ``call_module`` raises ``RuntimeError``.
        """
        BaseStrategy.__init__(self, mission_id, setup_id, setup_version_id)
        self.client_config = client_config
        # Per-instance explicit override wins; otherwise fall back to the
        # class-level slot wired by ``set_m2m_call_registry``.
        self._m2m_calls = m2m_calls if m2m_calls is not None else type(self)._shared_m2m_calls
        # Track cache keys this instance owns refs on, for cleanup
        self._pool_keys: set[str] = set()

        logger.debug(
            "Initialized GrpcCommunication",
            extra={"security": client_config.security},
        )

    def _get_or_create_channel(self, module_address: str, module_port: int) -> grpc.aio.Channel:
        """Get or create a shared cached channel for the target module.

        Uses GrpcClientWrapper._channel_cache for ref-counted sharing so
        multiple tasks calling the same remote module reuse one HTTP/2 connection.

        Args:
            module_address: Module host address
            module_port: Module port

        Returns:
            Async gRPC channel for the target module
        """
        config = ClientConfig(
            host=module_address,
            port=module_port,
            mode=self.client_config.mode,
            security=self.client_config.security,
            credentials=self.client_config.credentials,
            compression=self.client_config.compression,
            channel_options=self.client_config.channel_options,
        )
        channel = self._init_channel(config)
        if self._channel_cache_key is not None:
            self._pool_keys.add(self._channel_cache_key)
        return channel

    async def close_all_channels(self) -> None:
        """Release refs on all pooled gRPC channels."""
        for key in self._pool_keys:
            await GrpcClientWrapper.release_cached_channel(key)
        self._pool_keys.clear()

    async def close(self) -> None:
        """Release all pooled gRPC channels."""
        await self.close_all_channels()

    def dial_consumer_stream(
        self,
        address: str,
    ) -> tuple[gateway_service_pb2_grpc.GatewayServiceStub, Callable[[], Awaitable[None]]]:
        """Open (or reuse) a pooled channel to a consumer's GatewayService.

        External clients (chainlit, web UI) run their own GatewayService
        gRPC server. This returns a stub for the consumer's Stream RPC
        plus a release closure to drop the cached channel ref when done.

        Args:
            address: ``"host:port"`` of the consumer's GatewayService.

        Returns:
            ``(stub, release_channel)`` — call ``await release_channel()``
            after the BiDi is fully drained.

        Raises:
            InvalidConsumerAddressError: If ``address`` is not ``host:port``
                with a port in 1-65535.
        """
        host, sep, port_str = address.partition(":")
        if not host or not sep or not port_str:
            msg = f"address must be host:port, got {address!r}"
            raise InvalidConsumerAddressError(msg)
        try:
            port = int(port_str)
        except ValueError as exc:
            msg = f"port must be integer, got {address!r}"
            raise InvalidConsumerAddressError(msg) from exc
        if not (1 <= port <= 65535):  # noqa: PLR2004 — TCP port range
            msg = f"port out of range, got {address!r}"
            raise InvalidConsumerAddressError(msg)
        self._get_or_create_channel(host, port)
        stub = self._get_or_create_stub(gateway_service_pb2_grpc.GatewayServiceStub)
        cache_key = self._channel_cache_key

        async def _release() -> None:
            if cache_key:
                await GrpcClientWrapper.release_cached_channel(cache_key)
                self._pool_keys.discard(cache_key)

        return stub, _release

    def _create_stub(self, module_address: str, module_port: int) -> module_service_pb2_grpc.ModuleServiceStub:
        """Create a new stub for the target module.

        Args:
            module_address: Module host address
            module_port: Module port

        Returns:
            ModuleServiceStub for the target module
        """
        self._get_or_create_channel(module_address, module_port)
        return self._get_or_create_stub(module_service_pb2_grpc.ModuleServiceStub)

    async def get_module_schemas(
        self,
        module_address: str,
        module_port: int,
        *,
        llm_format: bool = False,
    ) -> dict[str, dict]:
        """Get module schemas via gRPC.

        Args:
            module_address: Target module address
            module_port: Target module port
            llm_format: Return LLM-friendly format

        Returns:
            Dictionary containing schemas: input, output, setup, secret, cost
        """
        stub = self._create_stub(module_address, module_port)

        # Create requests
        # Note: cost always uses llm_format=False to get actual config data (rates, units)
        # No LLM are allowed to set costs
        input_request = information_pb2.GetModuleInputRequest(llm_format=llm_format)
        output_request = information_pb2.GetModuleOutputRequest(llm_format=llm_format)
        setup_request = information_pb2.GetModuleSetupRequest(llm_format=llm_format)
        secret_request = information_pb2.GetModuleSecretRequest(llm_format=llm_format)
        cost_request = information_pb2.GetModuleCostRequest(llm_format=False)

        # Get all schemas in parallel
        input_response, output_response, setup_response, secret_response, cost_response = await asyncio.gather(
            stub.GetModuleInput(input_request),
            stub.GetModuleOutput(output_request),
            stub.GetModuleSetup(setup_request),
            stub.GetModuleSecret(secret_request),
            stub.GetModuleCost(cost_request),
        )

        logger.debug(
            "Retrieved module schemas",
            extra={
                "module_address": module_address,
                "module_port": module_port,
                "llm_format": llm_format,
            },
        )

        return {
            "input": json_format.MessageToDict(input_response.input_schema),
            "output": json_format.MessageToDict(output_response.output_schema),
            "setup": json_format.MessageToDict(setup_response.setup_schema),
            "secret": json_format.MessageToDict(secret_response.secret_schema),
            "cost": json_format.MessageToDict(cost_response.cost_schema),
        }

    async def call_module(  # noqa: C901, PLR0912, PLR0915
        self,
        module_address: str,
        module_port: int,
        input_data: dict | struct_pb2.Struct,
        setup_id: str,
        mission_id: str,
        callback: Callable[[struct_pb2.Struct], Awaitable[None]] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> AsyncGenerator[struct_pb2.Struct, None]:
        """Invoke a remote module through its GatewayService and stream output.

        Wraps the call in resilience belts sourced from
        :class:`GatewayM2MSettings`: concurrency cap, per-target circuit
        breaker, per-call deadline, TTL on the in-flight registry entry,
        and CANCEL-signal propagation on cancellation. See plan
        "Resilience requirements".

        Args:
            module_address: Target module's gateway host.
            module_port: Target module's gateway port.
            input_data: First input delivered to the remote module. Either
                a dict (will be loaded into a Struct) or a proto Struct.
            setup_id: Setup configuration ID.
            mission_id: Mission context ID.
            callback: Optional async callback invoked with each output Struct.
            metadata: Optional gRPC metadata propagated on the StartStream
                request (tenant headers, trace context, etc.).

        Yields:
            ``google.protobuf.Struct`` per remote module output.

        Raises:
            RuntimeError: When no GatewayServicer is wired (M2M unavailable).
            M2MAtCapacityError: When the concurrency semaphore times out.
            M2MTargetUnavailable: When the target's circuit breaker is open.
            M2MCallTimeout: When the output queue stalls past
                ``call_timeout_s``.
        """
        if self._m2m_calls is None:
            msg = (
                "call_module needs an M2MCallRegistry wired into GrpcCommunication. "
                "Call GrpcCommunication.set_m2m_call_registry(registry) at process startup "
                "(ModuleServer does this automatically) or pass m2m_calls=… to __init__."
            )
            raise RuntimeError(msg)
        m2m = self._m2m_calls
        m2m_settings = m2m._settings.m2m  # noqa: SLF001 — registry exposes its settings for in-package use

        if isinstance(input_data, struct_pb2.Struct):
            query = input_data
        else:
            query = struct_pb2.Struct()
            json_format.ParseDict(input_data, query)

        target_key = f"{module_address}:{module_port}"
        timer = StepTimer()
        log_extra = {
            "setup_id": setup_id,
            "mission_id": mission_id,
            "target_key": target_key,
        }

        # Fast-fail if the breaker for this target is open.
        breaker = m2m.breaker_for(target_key)
        if breaker.state == CBState.OPEN:
            logger.warning("[m2m] breaker OPEN — fast-failing target=%s", target_key, extra=log_extra)
            msg = f"circuit breaker open for {target_key}"
            raise M2MTargetUnavailable(msg)
        timer.mark("breaker_check")

        await m2m.acquire_slot()
        timer.mark("acquire_slot")

        task_id = str(uuid.uuid4())
        log_extra["task_id"] = task_id
        output_queue: asyncio.Queue[struct_pb2.Struct | None] = asyncio.Queue(
            maxsize=m2m_settings.call_queue_maxsize,
        )
        from digitalkin.models.grpc_servers.m2m import _M2MCallEntry

        entry = _M2MCallEntry(
            task_id=task_id,
            query=query,
            output_queue=output_queue,
            expires_at=time.monotonic() + m2m_settings.call_ttl_s,
            target_key=target_key,
            setup_id=setup_id,
            mission_id=mission_id,
        )
        m2m.register(entry)
        timer.mark("register")

        # Build the StartStream stub against the target. Channel is pooled by
        # GrpcClientWrapper (same as get_module_schemas / dial_consumer_stream).
        self._get_or_create_channel(module_address, module_port)
        stub = self._get_or_create_stub(gateway_service_pb2_grpc.GatewayServiceStub)

        cancelled = False
        try:
            grpc_metadata: list[tuple[str, str]] = []
            if metadata:
                grpc_metadata.extend((k, v) for k, v in metadata.items() if k != "x-client-address")
            grpc_metadata.append(("x-client-address", m2m.effective_advertise_address()))

            try:
                start_resp = await stub.StartStream(
                    gateway_pb2.StartStreamRequest(task_id=task_id, setup_id=setup_id, mission_id=mission_id),
                    metadata=tuple(grpc_metadata),
                )
            except grpc.aio.AioRpcError as exc:
                breaker.record_failure()
                logger.warning(
                    "[m2m] StartStream failed: [%s] %s",
                    exc.code().name,
                    exc.details() or "",
                    extra=log_extra,
                )
                raise
            timer.mark("start_stream")

            if not start_resp.accepted:
                breaker.record_failure()
                msg = f"target {target_key} rejected StartStream task_id={task_id}"
                raise RuntimeError(msg)
            logger.info("[m2m] StartStream accepted task_id=%s", task_id, extra=log_extra)

            # Drain outputs from the queue until None (dial-back finished) or
            # a fatal stream.error sentinel arrives. Each get() is bounded by
            # call_timeout_s so a silent producer can't pin us forever.
            first_seen = False
            error_observed = False
            while True:
                try:
                    item = await asyncio.wait_for(
                        output_queue.get(),
                        timeout=m2m_settings.call_timeout_s,
                    )
                except asyncio.TimeoutError as exc:
                    breaker.record_failure()
                    msg = (
                        f"call_module timed out after {m2m_settings.call_timeout_s}s "
                        f"waiting for output target={target_key} task_id={task_id}"
                    )
                    raise M2MCallTimeout(msg) from exc

                if item is None:
                    break

                if not first_seen:
                    timer.mark("first_output")
                    first_seen = True

                # In-band classification: stream.error+fatal counts as failure;
                # stream.end is the success terminator. Both are also pushed to
                # the caller so the application can inspect.
                root_field = item.fields.get("root") if item.fields else None
                if root_field is not None:
                    proto_field = root_field.struct_value.fields.get("protocol")
                    protocol_value = proto_field.string_value if proto_field is not None else ""
                    if protocol_value == "stream.error":
                        fatal_field = root_field.struct_value.fields.get("fatal")
                        if fatal_field is not None and fatal_field.bool_value:
                            error_observed = True
                if callback:
                    await callback(item)
                yield item

            if error_observed:
                breaker.record_failure()
            else:
                breaker.record_success()
            timer.mark("stream_end")
            timer.log("[m2m] call_module", task_id)

        except asyncio.CancelledError:
            cancelled = True
            raise
        finally:
            if cancelled:
                # Best-effort CANCEL signal to the target — fire-and-forget with
                # a short deadline. Swallow any error: cancellation propagation
                # must not raise out of the finally.
                try:
                    await asyncio.wait_for(
                        stub.SendSignal(
                            gateway_pb2.ClientSignalRequest(task_id=task_id, action=gateway_pb2.SignalAction.CANCEL),
                        ),
                        timeout=m2m_settings.call_cancel_signal_timeout_s,
                    )
                except (asyncio.TimeoutError, grpc.aio.AioRpcError, Exception):
                    logger.warning(
                        "[m2m] best-effort SendSignal(CANCEL) failed for task_id=%s",
                        task_id,
                        extra=log_extra,
                    )
            m2m.unregister(task_id)
            m2m.release_slot()
