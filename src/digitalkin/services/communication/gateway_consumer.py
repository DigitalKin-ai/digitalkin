"""Consumer-side helper for the gateway dial-back protocol.

A *consumer* (UI, dev tool, eval harness, M2M caller) calls
``StartStream`` on the gateway with ``x-client-address`` metadata, then
serves a local ``GatewayService.Stream`` server that the gateway dials
back into. The gateway delivers ``stream.init`` first, the consumer
replies with the query as the first ``StreamServer``, and the gateway
then forwards module outputs as subsequent ``StreamClient`` messages.

Two construction modes:

- ``GatewayConsumer.standalone(config)`` — owns its own gRPC server
  (chainlit, dev tool, eval harness).
- ``GatewayConsumer.attached(config, host_server)`` — registers the
  dial-back servicer on an existing ``grpc.aio.Server`` (M2M: a module
  reuses its own gateway server, no second port).

Both expose the same ``call(query, setup_id, mission_id)`` async iterator
yielding raw ``google.protobuf.Struct`` outputs.
"""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import grpc
from agentic_mesh_protocol.gateway.v1 import gateway_pb2, gateway_service_pb2_grpc
from pydantic import BaseModel, Field, NonNegativeInt, PositiveInt
from typing_extensions import Self

from digitalkin.logger import logger
from digitalkin.models.settings.consumer import ConsumerSettings
from digitalkin.models.settings.server.grpc import GrpcServerSettings

# Singletons — read env vars once at import. Field defaults below pull
# from `_CONSUMER_DEFAULTS` so any caller building `ConsumerConfig()`
# picks up env overrides; `_GRPC_SERVER_OPTIONS` is reused by every
# `GatewayConsumer` instance so we don't re-instantiate the settings
# on every consumer build (L5).
_CONSUMER_DEFAULTS = ConsumerSettings()
_GRPC_SERVER_OPTIONS = GrpcServerSettings().options

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator
    from types import TracebackType

    from google.protobuf import struct_pb2


class ConsumerConfig(BaseModel):
    """Settings for a :class:`GatewayConsumer`.

    Field defaults are sourced from :class:`ConsumerSettings`
    (env-var-backed). Caller-supplied values always win. Only
    ``gateway_address`` has no default — it must be supplied.
    """

    gateway_address: str = Field(description="host:port of the gateway's GatewayService.")
    listen: str = Field(
        default=_CONSUMER_DEFAULTS.listen,
        description="Bind interface for the standalone dial-back server.",
    )
    port: PositiveInt = Field(
        default=_CONSUMER_DEFAULTS.port,
        description="Bind port for the standalone dial-back server.",
    )
    advertise_address: str = Field(
        default=_CONSUMER_DEFAULTS.advertise_address,
        description=(
            "host:port the gateway will dial. Sent as x-client-address metadata. Defaults to listen:port when empty."
        ),
    )
    secure_mode: bool = Field(
        default=_CONSUMER_DEFAULTS.secure_mode,
        description="Use TLS for the outbound gateway channel.",
    )
    cert_path: str = Field(
        default=_CONSUMER_DEFAULTS.cert_path,
        description="Directory containing ca.crt (when secure_mode).",
    )
    queue_maxsize: NonNegativeInt = Field(
        default=_CONSUMER_DEFAULTS.queue_maxsize,
        description="Per-task output backpressure ceiling. 0 disables the bound.",
    )

    @property
    def effective_advertise(self) -> str:
        return self.advertise_address or f"{self.listen}:{self.port}"


class GatewayConsumerError(Exception):
    """Base class for gateway-consumer-side failures."""


class StartStreamRejected(GatewayConsumerError):
    """Gateway returned ``accepted=False`` on ``StartStream``."""

    def __init__(self, task_id: str) -> None:
        super().__init__(f"Gateway rejected task {task_id}")
        self.task_id = task_id


class StartStreamRpcError(GatewayConsumerError):
    """gRPC transport failure during ``StartStream``.

    Wraps the underlying :class:`grpc.aio.AioRpcError` and exposes the
    status code as a typed attribute so callers can switch on it without
    parsing the ``__cause__`` chain.
    """

    def __init__(self, task_id: str, cause: grpc.aio.AioRpcError) -> None:
        code = cause.code()
        details = cause.details() or ""
        super().__init__(f"StartStream failed for {task_id}: [{code.name}] {details}")
        self.task_id = task_id
        self.code = code
        self.details = details
        self.__cause__ = cause


@dataclass
class _TaskHandle:
    task_id: str
    query: struct_pb2.Struct
    output_queue: asyncio.Queue
    extra: dict[str, Any] = field(default_factory=dict)


class _DialBackServicer(gateway_service_pb2_grpc.GatewayServiceServicer):
    """Serves the gateway-initiated ``Stream`` BiDi.

    Reads the first ``StreamClient`` (always ``stream.init``), looks up
    the matching task, replies with the query as the first
    ``StreamServer``, then forwards every subsequent ``StreamClient``
    payload onto the per-task output queue.
    """

    def __init__(self, registry: dict[str, _TaskHandle]) -> None:
        self._registry = registry

    async def Stream(
        self,
        request_iterator: AsyncIterator[gateway_pb2.StreamClient],
        context: grpc.aio.ServicerContext,  # noqa: ARG002
    ) -> AsyncGenerator[gateway_pb2.StreamServer, None]:
        handle: _TaskHandle | None = None
        try:
            async for upstream in request_iterator:
                if handle is None:
                    handle = self._registry.get(upstream.task_id)
                    if handle is None:
                        logger.warning(
                            "Dial-back for unknown task — closing",
                            extra={"task_id": upstream.task_id},
                        )
                        return
                    yield gateway_pb2.StreamServer(task_id=handle.task_id, seq=0, data=handle.query)
                    continue
                if upstream.data and len(upstream.data.fields) > 0:
                    await handle.output_queue.put(upstream.data)
        finally:
            if handle is not None:
                await handle.output_queue.put(None)


class GatewayConsumer:
    """End-to-end consumer for the gateway StartStream + dial-back flow."""

    def __init__(
        self,
        config: ConsumerConfig,
        *,
        host_server: grpc.aio.Server | None = None,
    ) -> None:
        """Use :meth:`standalone` or :meth:`attached` instead of calling directly."""
        self._config = config
        self._host_server = host_server
        self._owns_server = host_server is None
        self._channel: grpc.aio.Channel | None = None
        self._stub: gateway_service_pb2_grpc.GatewayServiceStub | None = None
        self._server: grpc.aio.Server | None = host_server
        self._registry: dict[str, _TaskHandle] = {}
        self._grpc_options = _GRPC_SERVER_OPTIONS

    @classmethod
    def standalone(cls, config: ConsumerConfig) -> GatewayConsumer:
        """Build a consumer that owns its own dial-back gRPC server."""
        return cls(config)

    @classmethod
    def attached(cls, config: ConsumerConfig, host_server: grpc.aio.Server) -> GatewayConsumer:
        """Register the dial-back servicer on an existing gRPC server.

        Use this from inside a module that already runs its own gateway
        server — no second port, TLS posture inherited from the host.
        """
        return cls(config, host_server=host_server)

    @property
    def pending_tasks(self) -> int:
        """Number of in-flight tasks awaiting output (observability)."""
        return len(self._registry)

    async def start(self) -> None:
        """Open the outbound channel and (if standalone) start the dial-back server."""
        cfg = self._config
        if cfg.secure_mode and cfg.cert_path:
            from anyio import Path as AnyioPath

            ca = await AnyioPath(f"{cfg.cert_path}/ca.crt").read_bytes()
            credentials = grpc.ssl_channel_credentials(root_certificates=ca)
            self._channel = grpc.aio.secure_channel(cfg.gateway_address, credentials, options=self._grpc_options)
        else:
            self._channel = grpc.aio.insecure_channel(cfg.gateway_address, options=self._grpc_options)
        self._stub = gateway_service_pb2_grpc.GatewayServiceStub(self._channel)

        servicer = _DialBackServicer(self._registry)
        if self._owns_server:
            self._server = grpc.aio.server(options=self._grpc_options)
            gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(servicer, self._server)
            self._server.add_insecure_port(f"{cfg.listen}:{cfg.port}")
            await self._server.start()
            logger.info(
                "GatewayConsumer ready (standalone) — gateway=%s listen=%s:%d advertised=%s",
                cfg.gateway_address,
                cfg.listen,
                cfg.port,
                cfg.effective_advertise,
            )
        else:
            assert self._server is not None  # noqa: S101
            gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(servicer, self._server)
            logger.info(
                "GatewayConsumer ready (attached) — gateway=%s advertised=%s",
                cfg.gateway_address,
                cfg.effective_advertise,
            )

    async def stop(self) -> None:
        """Stop the dial-back server (if owned) and close the outbound channel."""
        if self._owns_server and self._server is not None:
            await self._server.stop(grace=1.0)
        self._server = None if self._owns_server else self._server
        if self._channel is not None:
            await self._channel.close()
            self._channel = None
        self._stub = None

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.stop()

    async def call(
        self,
        query: struct_pb2.Struct,
        setup_id: str,
        mission_id: str = "missions:default",
        *,
        task_id: str | None = None,
    ) -> AsyncIterator[struct_pb2.Struct]:
        """Run one task end-to-end and yield raw output Structs.

        Filters out ``stream.start`` and stops cleanly on ``stream.end``.
        ``stream.error`` payloads are yielded so the caller can surface
        them — inspect the ``root.protocol`` field.

        Args:
            query: The first input delivered to the module (proto Struct).
            setup_id: Setup/Kin identifier.
            mission_id: Mission identifier.
            task_id: Optional explicit task id (defaults to a fresh UUID4).

        Yields:
            ``google.protobuf.Struct`` for every module output.

        Raises:
            RuntimeError: If :meth:`start` was not called.
            ValueError: If ``task_id`` collides with an in-flight task.
            StartStreamRejected: If the gateway returned ``accepted=False``.
            StartStreamRpcError: If the ``StartStream`` RPC itself failed
                (network error, server crash, etc.). The original
                :class:`grpc.aio.AioRpcError` is available as ``__cause__``
                and the status code as the ``.code`` attribute.
        """
        if self._stub is None:
            msg = "GatewayConsumer.start() must be called before call()"
            raise RuntimeError(msg)

        tid = task_id or str(uuid.uuid4())
        if tid in self._registry:
            msg = f"task_id {tid} is already in flight"
            raise ValueError(msg)

        handle = _TaskHandle(
            task_id=tid,
            query=query,
            output_queue=asyncio.Queue(maxsize=self._config.queue_maxsize),
        )
        self._registry[tid] = handle
        log_extra = {"task_id": tid, "setup_id": setup_id, "mission_id": mission_id}

        try:
            try:
                resp = await self._stub.StartStream(
                    gateway_pb2.StartStreamRequest(task_id=tid, setup_id=setup_id, mission_id=mission_id),
                    metadata=(("x-client-address", self._config.effective_advertise),),
                )
            except grpc.aio.AioRpcError as e:
                logger.warning(
                    "StartStream RPC failed: [%s] %s",
                    e.code().name,
                    e.details() or "",
                    extra=log_extra,
                )
                raise StartStreamRpcError(tid, e) from e

            logger.info(
                "StartStream response: accepted=%s task_id=%s (advertised dial-back addr=%s)",
                resp.accepted,
                resp.task_id,
                self._config.effective_advertise,
                extra=log_extra,
            )
            if not resp.accepted:
                raise StartStreamRejected(tid)
            logger.info("Awaiting gateway dial-back", extra=log_extra)

            while True:
                data = await handle.output_queue.get()
                if data is None:
                    return
                proto_name = self._protocol_name(data)
                if proto_name == "stream.start":
                    continue
                if proto_name == "stream.end":
                    return
                yield data
        finally:
            self._registry.pop(tid, None)

    @staticmethod
    def _protocol_name(data: struct_pb2.Struct) -> str:
        root = data.fields.get("root")
        if root is None:
            return ""
        proto = root.struct_value.fields.get("protocol")
        return proto.string_value if proto is not None else ""

    @staticmethod
    def stream_error(data: struct_pb2.Struct) -> tuple[str, str] | None:
        """Decode a ``stream.error`` Struct yielded by :meth:`call`.

        The gateway emits ``stream.error`` sentinels in-band when the
        dial-back, dispatcher, or module pipeline fails (codes from
        :class:`StreamErrorCode`). They flow through :meth:`call` as
        regular Structs so the caller can branch on the failure.

        Args:
            data: A Struct yielded by ``async for ... in consumer.call(...)``.

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
