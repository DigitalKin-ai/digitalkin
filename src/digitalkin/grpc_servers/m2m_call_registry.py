"""Process-singleton state for in-flight M2M outbound calls."""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

from agentic_mesh_protocol.gateway.v1 import gateway_pb2

from digitalkin.grpc_servers.exceptions import M2MAtCapacityError
from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
from digitalkin.logger import logger
from digitalkin.models.settings.gateway import get_gateway_settings
from digitalkin.models.settings.server.channel import get_server_channel_settings

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

    from digitalkin.models.grpc_servers.m2m import _M2MCallEntry


class M2MCallRegistry:
    """In-flight outbound-call state and dial-back-receive driver."""

    def __init__(self) -> None:
        """Initialize the registry."""
        self._entries: dict[str, _M2MCallEntry] = {}
        self._semaphore = asyncio.Semaphore(get_gateway_settings().m2m.call_max_concurrent)
        self._breakers: OrderedDict[str, CircuitBreaker] = OrderedDict()
        self._sweeper: asyncio.Task[None] | None = None

    def register(self, entry: _M2MCallEntry) -> None:
        """Register a fresh outbound call (caller must hold a slot)."""
        self._entries[entry.task_id] = entry

    def unregister(self, task_id: str) -> _M2MCallEntry | None:
        """Remove the call entry.

        Returns:
            The removed entry, or ``None`` if absent.
        """
        return self._entries.pop(task_id, None)

    def get(self, task_id: str) -> _M2MCallEntry | None:
        """Look up an in-flight call.

        Returns:
            The entry, or ``None``.
        """
        return self._entries.get(task_id)

    def has(self, task_id: str) -> bool:
        """Whether ``task_id`` has an in-flight entry.

        Returns:
            True if present.
        """
        return task_id in self._entries

    @property
    def entries(self) -> dict[str, _M2MCallEntry]:
        """The live entries dict."""
        return self._entries

    def breaker_for(self, target_key: str) -> CircuitBreaker:
        """Lazy-create the per-target circuit breaker.

        Returns:
            The circuit breaker.
        """
        breaker = self._breakers.get(target_key)
        if breaker is not None:
            self._breakers.move_to_end(target_key)
            return breaker
        m2m = get_gateway_settings().m2m
        breaker = CircuitBreaker(
            service_id=f"m2m:{target_key}",
            fail_max=m2m.call_breaker_fail_max,
            reset_timeout=m2m.call_breaker_reset_timeout_s,
        )
        self._breakers[target_key] = breaker
        if len(self._breakers) > 256:  # noqa: PLR2004
            self._breakers.popitem(last=False)
        return breaker

    async def acquire_slot(self) -> None:
        """Acquire one concurrency slot.

        Raises:
            M2MAtCapacityError: When the semaphore times out.
        """
        m2m = get_gateway_settings().m2m
        try:
            await asyncio.wait_for(
                self._semaphore.acquire(),
                timeout=m2m.call_acquire_timeout_s,
            )
        except asyncio.TimeoutError as exc:
            msg = (
                f"call slot not acquired within "
                f"{m2m.call_acquire_timeout_s}s (max_concurrent="
                f"{m2m.call_max_concurrent})"
            )
            raise M2MAtCapacityError(msg) from exc

    def release_slot(self) -> None:
        """Release one outbound concurrency slot."""
        self._semaphore.release()

    def effective_advertise_address(self) -> str:  # noqa: PLR6301
        """``host:port`` the local gateway advertises as its dial-back target.

        Returns:
            ``host:port`` string from channel settings (``advertise_host`` falls back to ``host``).
        """
        ch = get_server_channel_settings()
        host = ch.advertise_host or ch.host
        return f"{host}:{ch.port}"

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
        """Reap entries past their TTL and unblock waiting consumers."""
        while True:
            m2m = get_gateway_settings().m2m
            try:
                await asyncio.sleep(m2m.call_sweeper_interval_s)
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
                    m2m.call_ttl_s,
                    extra={
                        "task_id": tid,
                        "setup_id": entry.setup_id,
                        "mission_id": entry.mission_id,
                    },
                )

    async def handle_dial_back_receive(
        self,
        task_id: str,
        request_iterator: AsyncIterator[Any],
    ) -> AsyncGenerator[Any, None]:
        """Serve a dial-back BiDi initiated by a remote gateway.

        Yields the cached query first, then pushes inbound Structs onto
        the registered ``output_queue`` until ``stream.end`` or fatal
        ``stream.error``.

        Yields:
            StreamClient (the cached query).
        """
        handle = self._entries[task_id]
        log_extra = {
            "task_id": handle.task_id,
            "setup_id": handle.setup_id,
            "mission_id": handle.mission_id,
            "target_key": handle.target_key,
        }
        logger.info("[m2m-dialback] dial-back received, replying with query", extra=log_extra)
        yield gateway_pb2.StreamClient(from_seq=0, task_id=task_id, data=handle.query)

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
                        "[m2m-dialback] output_queue full — DROPPED output (content=%s)",
                        str(upstream.data)[:2048],
                        extra=log_extra,
                    )

                if protocol == "stream.end":
                    return
                if protocol == "stream.error":
                    fatal_field = root_field.struct_value.fields.get("fatal")
                    is_fatal = fatal_field is not None and fatal_field.bool_value
                    if is_fatal:
                        # M2: breaker outcome recorded only in call_module (was double-counted here).
                        return
        finally:
            with contextlib.suppress(asyncio.QueueFull):
                handle.output_queue.put_nowait(None)
