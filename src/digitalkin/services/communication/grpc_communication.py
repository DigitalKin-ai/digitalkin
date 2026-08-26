"""gRPC client implementation for Communication service."""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import TYPE_CHECKING, Any

import grpc.aio
from agentic_mesh_protocol.gateway.v1 import gateway_pb2, gateway_service_pb2_grpc
from agentic_mesh_protocol.module.v1 import (
    information_pb2,
    module_service_pb2_grpc,
)
from google.protobuf import json_format, struct_pb2

from digitalkin.core.profiling.step_timer import StepTimer
from digitalkin.grpc_servers.interceptors.request_ids import RequestContext
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.grpc_servers.utils.validators import GatewayValidator
from digitalkin.logger import logger
from digitalkin.models.grpc_servers.circuit_breaker import CBState
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.settings.gateway import get_gateway_settings
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


class _GatewayBackendClient(GrpcClientWrapper):
    """Resilient client to the backend GatewayService (own circuit breaker) for AssociateTask."""

    service_name: str = "GatewayBackendService"

    def __init__(self, client_config: ClientConfig) -> None:
        """Dial the backend GatewayService and cache its stub.

        Args:
            client_config: Backend services-provider config (same host as user_profile).
        """
        self._init_channel(client_config)
        self.stub = self._get_or_create_stub(gateway_service_pb2_grpc.GatewayServiceStub)


class GrpcCommunication(CommunicationStrategy, GrpcClientWrapper):
    """gRPC client for module-to-module communication."""

    service_name: str = "CommunicationService"

    _shared_m2m_calls: M2MCallRegistry | None = None

    @classmethod
    def set_m2m_call_registry(cls, registry: M2MCallRegistry | None) -> None:
        """Register the process-singleton ``M2MCallRegistry`` for ``call_module``."""
        cls._shared_m2m_calls = registry

    @staticmethod
    def _protocol_name(data: struct_pb2.Struct) -> str:
        """Return ``data.root.protocol`` or ``""``.

        Args:
            data: A wire Struct from the gateway stream.

        Returns:
            The protocol sentinel string, or empty if absent.
        """
        root = data.fields.get("root")
        if root is None:
            return ""
        proto = root.struct_value.fields.get("protocol")
        return proto.string_value if proto is not None else ""

    @staticmethod
    def stream_error(data: struct_pb2.Struct) -> tuple[str, str] | None:
        """Decode a ``stream.error`` Struct from :meth:`call_module`.

        Args:
            data: A Struct yielded by ``call_module``.

        Returns:
            ``(code, message)`` if ``data`` is a ``stream.error``, else ``None``.
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
        gateway_backend_config: ClientConfig | None = None,
    ) -> None:
        """Initialize the gRPC communication client.

        Args:
            mission_id: Mission identifier.
            setup_id: Setup identifier.
            setup_version_id: Setup version identifier.
            client_config: gRPC client config.
            m2m_calls: Optional ``M2MCallRegistry``; falls back to the
                class-level slot from :meth:`set_m2m_call_registry`.
            gateway_backend_config: Backend GatewayService config for AssociateTask
                (same host as user_profile). Required for M2M tool calls.
        """
        BaseStrategy.__init__(self, mission_id, setup_id, setup_version_id)
        self.client_config = client_config
        self._m2m_calls = m2m_calls if m2m_calls is not None else self._shared_m2m_calls
        self._pool_keys: set[str] = set()
        self._gateway_backend = (
            _GatewayBackendClient(gateway_backend_config) if gateway_backend_config is not None else None
        )

        logger.debug("Initialized GrpcCommunication (security=%s)", client_config.security)

    def _get_or_create_channel(self, module_address: str, module_port: int) -> grpc.aio.Channel:
        """Return a shared, ref-counted gRPC channel to the target module.

        Args:
            module_address: Module host.
            module_port: Module port.

        Returns:
            Async gRPC channel.
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
        if self._gateway_backend is not None:
            await self._gateway_backend.close_channel()

    def dial_consumer_stream(
        self,
        address: str,
    ) -> tuple[gateway_service_pb2_grpc.GatewayServiceStub, Callable[[], Awaitable[None]]]:
        """Open (or reuse) a pooled channel to a consumer's GatewayService.

        Args:
            address: ``host:port`` of the consumer's GatewayService.

        Returns:
            ``(stub, release_channel)`` — await ``release_channel()`` when done.

        Raises:
            InvalidConsumerAddressError: If ``address`` is not ``host:port``.
        """
        err = GatewayValidator.validate_address(address, "address")
        if err is not None:
            raise InvalidConsumerAddressError(err)
        host, _, port_str = address.partition(":")
        port = int(port_str)
        self._get_or_create_channel(host, port)
        stub = self._get_or_create_stub(gateway_service_pb2_grpc.GatewayServiceStub)
        cache_key = self._channel_cache_key

        async def _release() -> None:
            if cache_key:
                await GrpcClientWrapper.release_cached_channel(cache_key)
                self._pool_keys.discard(cache_key)

        return stub, _release

    async def evict_consumer_channel(self, address: str) -> None:
        """Force a fresh channel on the next dial to ``address``.

        Removes any cached (possibly wedged) channel so a resume re-dial does
        not reuse a connection left broken by a peer that died. No-op if the
        address is malformed or no channel is cached.

        Args:
            address: ``host:port`` of the consumer's GatewayService.
        """
        host, _, port_str = address.partition(":")
        if not port_str.isdigit():
            return
        key = f"{host}:{int(port_str)}:{self.client_config.security.value}:{self.client_config.compression.value}"
        await GrpcClientWrapper.evict_cached_channel(key)

    def _create_stub(self, module_address: str, module_port: int) -> module_service_pb2_grpc.ModuleServiceStub:
        """Return a ModuleServiceStub for the target module.

        Args:
            module_address: Module host.
            module_port: Module port.

        Returns:
            ModuleServiceStub.
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

        # Cost always uses llm_format=False — rates/units must come from config.
        input_request = information_pb2.GetModuleInputRequest(llm_format=llm_format)
        output_request = information_pb2.GetModuleOutputRequest(llm_format=llm_format)
        setup_request = information_pb2.GetModuleSetupRequest(llm_format=llm_format)
        secret_request = information_pb2.GetModuleSecretRequest(llm_format=llm_format)
        cost_request = information_pb2.GetModuleCostRequest(llm_format=False)

        input_response, output_response, setup_response, secret_response, cost_response = await asyncio.gather(
            stub.GetModuleInput(input_request),
            stub.GetModuleOutput(output_request),
            stub.GetModuleSetup(setup_request),
            stub.GetModuleSecret(secret_request),
            stub.GetModuleCost(cost_request),
        )

        logger.debug(
            "Retrieved module schemas from %s:%d (llm_format=%s)",
            module_address,
            module_port,
            llm_format,
        )

        return {
            "input": json_format.MessageToDict(input_response.input_schema),
            "output": json_format.MessageToDict(output_response.output_schema),
            "setup": json_format.MessageToDict(setup_response.setup_schema),
            "secret": json_format.MessageToDict(secret_response.secret_schema),
            "cost": json_format.MessageToDict(cost_response.cost_schema),
        }

    async def get_module_config_schema(
        self,
        module_address: str,
        module_port: int,
        *,
        llm_format: bool = False,
    ) -> dict[str, Any]:
        """Get the module's config-setup JSON schema via gRPC (``GetConfigSetupModule``).

        Args:
            module_address: Target module address.
            module_port: Target module port.
            llm_format: Return the LLM-friendly schema format.

        Returns:
            The config-setup JSON schema (the fields a caller fills at setup/update).
        """
        stub = self._create_stub(module_address, module_port)
        response = await stub.GetConfigSetupModule(information_pb2.GetConfigSetupModuleRequest(llm_format=llm_format))
        return json_format.MessageToDict(response.config_setup_schema)

    async def call_module(  # noqa: C901, PLR0912, PLR0914, PLR0915
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

        Resilience belts (concurrency cap, per-target breaker, deadline,
        TTL, CANCEL propagation) come from :class:`GatewayM2MSettings`.

        Args:
            module_address: Target module's gateway host.
            module_port: Target module's gateway port.
            input_data: First input (dict or Struct).
            setup_id: Setup configuration ID.
            mission_id: Mission context ID.
            callback: Optional async callback per output Struct.
            metadata: Optional gRPC metadata for StartStream.

        Yields:
            ``google.protobuf.Struct`` per remote output.

        Raises:
            CancelledError: Task cancelled.
            AioRpcError: gRPC errors.
            RuntimeError: No GatewayServicer wired.
            M2MAtCapacityError: Concurrency semaphore timed out.
            M2MTargetUnavailable: Target's breaker is open.
            M2MCallTimeout: Output queue stalled past ``call_timeout_s``.
        """
        if self._m2m_calls is None:
            msg = (
                "call_module needs an M2MCallRegistry wired into GrpcCommunication. "
                "Call GrpcCommunication.set_m2m_call_registry(registry) at process startup "
                "(ModuleServer does this automatically) or pass m2m_calls=… to __init__."
            )
            raise RuntimeError(msg)
        m2m = self._m2m_calls
        m2m_settings = get_gateway_settings().m2m

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

        task_id = ""
        breaker = m2m.breaker_for(target_key)

        last_mark = "init"
        chunks_seen = 0
        max_qdepth = 0
        gaps_ns: list[int] = []
        last_chunk_ns = 0
        cancelled = False
        registered = False
        slot_acquired = False
        stub: Any = None

        try:  # noqa: PLR1702, PLW0717
            if breaker.state == CBState.OPEN:
                logger.warning("[m2m] breaker OPEN — fast-failing target=%s", target_key, extra=log_extra)
                msg = f"circuit breaker open for {target_key}"
                raise M2MTargetUnavailable(msg)  # noqa: TRY301
            timer.mark("breaker_check")
            last_mark = "breaker_check"

            await m2m.acquire_slot()
            slot_acquired = True
            timer.mark("acquire_slot")
            last_mark = "acquire_slot"

            # The BACKEND mints + registers the sub-task (linked to the running parent's
            # mission), so it is a real task the tool module's CheckResourceAccess accepts.
            # Resilient: deadline + retry + own breaker via exec_grpc_query; retries are safe
            # because the idempotency nonce dedupes them backend-side. Fail-closed on error.
            if self._gateway_backend is None:
                msg = "gateway_backend_config is required for M2M AssociateTask"
                raise RuntimeError(msg)  # noqa: TRY301
            parent_task_id = RequestContext.current().get("task_id", "")
            idem_key = uuid.uuid4().hex  # idempotency nonce, NOT a task_id (backend mints the id)
            assoc = await self._gateway_backend.exec_grpc_query(
                "AssociateTask",
                gateway_pb2.AssociateTaskRequest(parent_task_id=parent_task_id),
                timeout=m2m_settings.call_associate_timeout_s,
                metadata=(("x-idempotency-key", idem_key),),
            )
            task_id = assoc.task_id
            if not task_id:
                msg = f"backend returned no task_id from AssociateTask (parent={parent_task_id})"
                raise RuntimeError(msg)  # noqa: TRY301
            logger.info(
                "[VALIDATE AT2] AssociateTask minted: parent=%s child=%s target=%s",
                parent_task_id,
                task_id,
                target_key,
                extra=log_extra,
            )  # TODO(validate): remove after prod validation
            log_extra["task_id"] = task_id
            timer.mark("associate_task")
            last_mark = "associate_task"

            self._get_or_create_channel(module_address, module_port)
            timer.mark("channel_create")
            last_mark = "channel_create"
            stub = self._get_or_create_stub(gateway_service_pb2_grpc.GatewayServiceStub)
            timer.mark("stub_create")
            last_mark = "stub_create"

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
            registered = True
            timer.mark("register")
            last_mark = "register"

            try:  # noqa: PLW0717
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
                last_mark = "start_stream"

                if not start_resp.accepted:
                    breaker.record_failure()
                    msg = f"target {target_key} rejected StartStream task_id={task_id}"
                    raise RuntimeError(msg)
                logger.info("[m2m] StartStream accepted task_id=%s", task_id, extra=log_extra)

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

                    now_ns = time.perf_counter_ns()
                    chunks_seen += 1
                    depth = output_queue.qsize()
                    max_qdepth = max(max_qdepth, depth)
                    if not first_seen:
                        timer.mark("first_output")
                        last_mark = "first_output"
                        first_seen = True
                    else:
                        gaps_ns.append(now_ns - last_chunk_ns)
                    last_chunk_ns = now_ns

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
                last_mark = "stream_end"

                gaps_ms_sorted = sorted(g / 1e6 for g in gaps_ns)
                max_gap_ms = gaps_ms_sorted[-1] if gaps_ms_sorted else 0.0
                p95_gap_ms = gaps_ms_sorted[int(0.95 * (len(gaps_ms_sorted) - 1))] if gaps_ms_sorted else 0.0
                logger.debug(
                    "[perf] [m2m] call_module: %s chunks=%d max_gap_ms=%.2f "
                    "p95_gap_ms=%.2f max_qdepth=%d total=%.2fms task_id=%s",
                    timer.format_steps(),
                    chunks_seen,
                    max_gap_ms,
                    p95_gap_ms,
                    max_qdepth,
                    timer.total_ms(),
                    task_id,
                    extra=log_extra,
                )

            except asyncio.CancelledError:
                cancelled = True
                raise
            finally:
                if cancelled and stub is not None and task_id:
                    sig_t0 = time.perf_counter_ns()
                    sig_failure = ""
                    try:
                        await asyncio.wait_for(
                            stub.SendSignal(
                                gateway_pb2.ClientSignalRequest(
                                    task_id=task_id,
                                    action=gateway_pb2.SignalAction.CANCEL,
                                ),
                            ),
                            timeout=m2m_settings.call_cancel_signal_timeout_s,
                        )
                    except (asyncio.TimeoutError, grpc.aio.AioRpcError, Exception) as exc:
                        sig_failure = type(exc).__name__
                    sig_ms = (time.perf_counter_ns() - sig_t0) / 1e6
                    if not sig_failure:
                        logger.debug(
                            "[perf] [m2m] send_signal: action=CANCEL rpc_ms=%.2f task_id=%s",
                            sig_ms,
                            task_id,
                            extra=log_extra,
                        )
                    else:
                        logger.warning(
                            "[m2m] send_signal_failed: failure=%s action=CANCEL elapsed_ms=%.2f task_id=%s",
                            sig_failure,
                            sig_ms,
                            task_id,
                            extra=log_extra,
                        )

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "[m2m] call_module_failed: failure=%s at_step=%s elapsed_ms=%.2f breaker=%s chunks_seen=%d task_id=%s",
                type(exc).__name__,
                last_mark,
                timer.elapsed_now_ms(),
                breaker.state.name,
                chunks_seen,
                task_id,
                extra=log_extra,
            )
            raise
        finally:
            if registered:
                m2m.unregister(task_id)
            if slot_acquired:
                m2m.release_slot()
