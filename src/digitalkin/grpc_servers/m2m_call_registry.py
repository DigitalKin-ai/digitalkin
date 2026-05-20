"""Process-singleton state for in-flight M2M outbound calls.

Owns:
- The registry of in-flight call entries (keyed by ``task_id``).
- The per-target circuit-breaker map.
- The concurrency semaphore.
- The TTL sweeper task.
- The dial-back-receive branch driven from ``GatewayServicer.Stream``.

The registry is shared by two distinct users in the process:
- **Writer** — ``GrpcCommunication.call_module`` (many per-task instances)
  registers on the way in, unregisters on the way out.
- **Reader** — ``GatewayServicer.Stream`` dispatches on
  ``data.root.protocol == "stream.init"`` and hands off to
  :meth:`handle_dial_back_receive` here.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from typing import TYPE_CHECKING, Any

from agentic_mesh_protocol.gateway.v1 import gateway_pb2

from digitalkin.grpc_servers.exceptions import M2MAtCapacityError
from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
from digitalkin.logger import logger
from digitalkin.models.settings.server.channel import ServerChannelSettings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

    from digitalkin.models.grpc_servers.m2m import _M2MCallEntry
    from digitalkin.models.settings.gateway import GatewaySettings


class M2MCallRegistry:
    """In-flight outbound-call state + dial-back-receive driver.

    Constructor:
        settings: ``GatewaySettings``; reads ``settings.m2m.*`` for TTL,
            breaker thresholds, concurrency limit, and queue sizing.
    """

    def __init__(self, settings: GatewaySettings) -> None:
        """Module to module communication."""
        self._settings = settings
        self._entries: dict[str, _M2MCallEntry] = {}
        self._semaphore = asyncio.Semaphore(settings.m2m.call_max_concurrent)
        self._breakers: dict[str, CircuitBreaker] = {}
        self._sweeper: asyncio.Task[None] | None = None
        self._channel_settings: ServerChannelSettings | None = None

    # ------------------------------------------------------------------
    # Registry surface
    # ------------------------------------------------------------------

    def register(self, entry: _M2MCallEntry) -> None:
        """Register a fresh outbound call. Caller must hold a slot via :meth:`acquire_slot`."""
        self._entries[entry.task_id] = entry

    def unregister(self, task_id: str) -> _M2MCallEntry | None:
        """Remove a call. Returns the entry if it was present."""
        return self._entries.pop(task_id, None)

    def get(self, task_id: str) -> _M2MCallEntry | None:
        """Look up an in-flight call."""
        return self._entries.get(task_id)

    def has(self, task_id: str) -> bool:
        return task_id in self._entries

    @property
    def entries(self) -> dict[str, _M2MCallEntry]:
        """The live entries dict — used by tests and introspection."""
        return self._entries

    # ------------------------------------------------------------------
    # Breaker
    # ------------------------------------------------------------------

    def breaker_for(self, target_key: str) -> CircuitBreaker:
        """Lazy-create a per-target circuit breaker."""
        breaker = self._breakers.get(target_key)
        if breaker is None:
            breaker = CircuitBreaker(
                service_id=f"m2m:{target_key}",
                fail_max=self._settings.m2m.call_breaker_fail_max,
                reset_timeout=self._settings.m2m.call_breaker_reset_timeout_s,
            )
            self._breakers[target_key] = breaker
        return breaker

    # ------------------------------------------------------------------
    # Concurrency
    # ------------------------------------------------------------------

    async def acquire_slot(self) -> None:
        """Acquire one concurrency slot or raise :class:`M2MAtCapacityError` on timeout."""
        try:
            await asyncio.wait_for(
                self._semaphore.acquire(),
                timeout=self._settings.m2m.call_acquire_timeout_s,
            )
        except asyncio.TimeoutError as exc:
            msg = (
                f"call slot not acquired within "
                f"{self._settings.m2m.call_acquire_timeout_s}s (max_concurrent="
                f"{self._settings.m2m.call_max_concurrent})"
            )
            raise M2MAtCapacityError(msg) from exc

    def release_slot(self) -> None:
        """Release one outbound concurrency slot."""
        self._semaphore.release()

    # ------------------------------------------------------------------
    # Advertise address (for x-client-address metadata on StartStream)
    # ------------------------------------------------------------------

    def effective_advertise_address(self) -> str:
        """``host:port`` the local gateway advertises as its dial-back target."""
        if self._channel_settings is None:
            self._channel_settings = ServerChannelSettings()
        host = self._channel_settings.advertise_host or self._channel_settings.host
        return f"{host}:{self._channel_settings.port}"

    # ------------------------------------------------------------------
    # Sweeper lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Spawn the TTL sweeper task."""
        if self._sweeper is None:
            self._sweeper = asyncio.create_task(self._sweep_loop(), name="m2m_call_sweeper")

    async def stop(self) -> None:
        """Cancel the TTL sweeper task."""
        if self._sweeper is not None:
            self._sweeper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sweeper
            self._sweeper = None

    async def _sweep_loop(self) -> None:
        interval = self._settings.m2m.call_sweeper_interval_s
        while True:
            try:
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                return
            now = time.monotonic()
            for tid, entry in list(self._entries.items()):
                if entry.expires_at >= now:
                    continue
                self._entries.pop(tid, None)
                try:
                    entry.output_queue.put_nowait(None)
                except asyncio.QueueFull:
                    logger.warning(
                        "[m2m-sweeper] queue full while reaping task_id=%s target=%s",
                        tid,
                        entry.target_key,
                    )
                self.breaker_for(entry.target_key).record_failure()
                logger.warning(
                    "[m2m-sweeper] reaped task_id=%s target=%s (TTL %.1fs exceeded)",
                    tid,
                    entry.target_key,
                    self._settings.m2m.call_ttl_s,
                    extra={
                        "task_id": tid,
                        "setup_id": entry.setup_id,
                        "mission_id": entry.mission_id,
                    },
                )

    # ------------------------------------------------------------------
    # Dial-back-receive — invoked from GatewayServicer.Stream
    # ------------------------------------------------------------------

    async def handle_dial_back_receive(
        self,
        task_id: str,
        request_iterator: AsyncIterator[Any],
    ) -> AsyncGenerator[Any, None]:
        """Serve a dial-back BiDi initiated by a remote gateway.

        Caller (``GatewayServicer.Stream``) has already consumed the
        ``stream.init`` first message and confirmed the entry exists via
        :meth:`has`. Reply with the cached query, then push every subsequent
        inbound Struct onto the registered ``output_queue`` until
        ``stream.end`` or a fatal ``stream.error`` is observed.

        Yields:
            StreamClient — first the cached query, then nothing else.
        """
        handle = self._entries[task_id]  # caller pre-checked via has()
        log_extra = {
            "task_id": handle.task_id,
            "setup_id": handle.setup_id,
            "mission_id": handle.mission_id,
            "target_key": handle.target_key,
        }
        logger.info("[m2m-dialback] dial-back received, replying with query", extra=log_extra)
        yield gateway_pb2.StreamClient(from_seq=0, task_id=task_id, data=handle.query)

        breaker = self.breaker_for(handle.target_key)
        try:
            async for upstream in request_iterator:
                if not (upstream.data and len(upstream.data.fields) > 0):
                    continue

                root_field = upstream.data.fields.get("root")
                if root_field is not None:
                    protocol_field = root_field.struct_value.fields.get("protocol")
                    protocol = protocol_field.string_value if protocol_field is not None else ""
                else:
                    protocol = ""

                try:
                    handle.output_queue.put_nowait(upstream.data)
                except asyncio.QueueFull:
                    logger.warning(
                        "[m2m-dialback] output_queue full task_id=%s — dropping output",
                        task_id,
                        extra=log_extra,
                    )

                if protocol == "stream.end":
                    breaker.record_success()
                    return
                if protocol == "stream.error":
                    fatal_field = root_field.struct_value.fields.get("fatal")
                    is_fatal = fatal_field is not None and fatal_field.bool_value
                    if is_fatal:
                        breaker.record_failure()
                        return
        finally:
            with contextlib.suppress(asyncio.QueueFull):
                handle.output_queue.put_nowait(None)
