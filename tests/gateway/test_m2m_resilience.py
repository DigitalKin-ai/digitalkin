"""Resilience belts for ``GrpcCommunication.call_module``.

Covers the four safety mechanisms from ``GatewayM2MSettings``:
- TTL sweeper drops stuck registry entries.
- Per-target circuit breaker fast-fails after consecutive failures.
- Concurrency semaphore caps in-flight outbound calls.
- Per-call output queue deadline.

Also pins cancellation propagation: cancelling ``call_module`` sends a
best-effort ``SendSignal(CANCEL)`` to the target.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import grpc.aio
import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_pb2
from google.protobuf import struct_pb2

from digitalkin.grpc_servers.gateway_servicer import GatewayServicer, _M2MCallEntry
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.settings.utils.channel import SecurityMode
from digitalkin.services.communication.grpc_communication import (
    GrpcCommunication,
    M2MAtCapacityError,
    M2MCallTimeout,
    M2MTargetUnavailable,
)

pytestmark = [pytest.mark.timeout(15)]


def _struct(d: dict[str, Any]) -> struct_pb2.Struct:
    s = struct_pb2.Struct()
    s.update(d)
    return s


def _gw() -> GatewayServicer:
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock()
    fake_redis.xlen = AsyncMock(return_value=0)
    runner = MagicMock()
    runner.run = AsyncMock()
    return GatewayServicer(
        redis_client=fake_redis,
        max_streams=10,
        client_config=ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE),
        module_runner=runner,
    )


def _comm(gw: GatewayServicer) -> GrpcCommunication:
    return GrpcCommunication(
        mission_id="missions:test",
        setup_id="setups:test",
        setup_version_id="setup_versions:test",
        client_config=ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE),
        m2m_calls=gw._m2m,
    )


class TestTTLSweeper:
    """Expired registry entries are reaped, queues signaled, breaker bumped."""

    async def test_sweeper_reaps_expired_entries(self) -> None:
        gw = _gw()
        gw._settings.m2m.call_sweeper_interval_s = 0.05
        await gw.start()
        try:
            queue: asyncio.Queue[struct_pb2.Struct | None] = asyncio.Queue()
            gw._m2m.register(
                _M2MCallEntry(
                    task_id="stuck",
                    query=_struct({"root": {"protocol": "x"}}),
                    output_queue=queue,
                    expires_at=time.monotonic() - 1.0,  # already expired
                    target_key="bad-target:1",
                ),
            )
            # Wait a few sweeper ticks.
            await asyncio.sleep(0.2)
            assert gw._m2m.entries.get("stuck") is None
            # Queue received a None sentinel so any caller awaiting unblocks.
            assert queue.get_nowait() is None
            # Breaker for the target now has at least one recorded failure.
            breaker = gw._m2m.breaker_for("bad-target:1")
            assert breaker._failure_count >= 1  # noqa: SLF001 — test-only introspection
        finally:
            await gw.stop()


class TestCircuitBreaker:
    """Open breaker fast-fails ``call_module``; closes on success."""

    async def test_open_breaker_fast_fails(self) -> None:
        gw = _gw()
        gw._settings.m2m.call_breaker_fail_max = 1
        comm = _comm(gw)
        # Force the breaker open by recording a failure (fail_max=1).
        gw._m2m.breaker_for("127.0.0.1:9999").record_failure()
        assert gw._m2m.breaker_for("127.0.0.1:9999").state.value == "open"

        with pytest.raises(M2MTargetUnavailable):
            async for _ in comm.call_module(
                module_address="127.0.0.1",
                module_port=9999,
                input_data={"root": {"protocol": "x"}},
                setup_id="setups:test",
                mission_id="missions:test",
            ):
                pass


class TestMaxConcurrent:
    """Concurrency cap rejects calls past ``outbound_max_concurrent``."""

    async def test_third_call_raises_at_capacity(self) -> None:
        gw = _gw()
        gw._settings.m2m.call_max_concurrent = 2
        gw._settings.m2m.call_acquire_timeout_s = 0.2
        # Rebuild the semaphore with the new limit.
        gw._m2m._semaphore = asyncio.Semaphore(2)

        # Hold both slots.
        await gw._m2m.acquire_slot()
        await gw._m2m.acquire_slot()

        comm = _comm(gw)
        with pytest.raises(M2MAtCapacityError):
            async for _ in comm.call_module(
                module_address="127.0.0.1",
                module_port=9999,
                input_data={"root": {"protocol": "x"}},
                setup_id="setups:test",
                mission_id="missions:test",
            ):
                pass

        # Release for hygiene.
        gw._m2m.release_slot()
        gw._m2m.release_slot()


class TestCallTimeout:
    """Silent target → ``M2MCallTimeout`` after the deadline."""

    async def test_output_queue_silence_raises(self) -> None:
        gw = _gw()
        gw._settings.m2m.call_timeout_s = 0.15
        comm = _comm(gw)

        # Stub StartStream to succeed but never push to the queue.
        stub_mock = MagicMock()
        stub_mock.StartStream = AsyncMock(
            return_value=gateway_pb2.StartStreamResponse(accepted=True, task_id="tid"),
        )
        stub_mock.SendSignal = AsyncMock()
        comm._get_or_create_channel = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        comm._get_or_create_stub = MagicMock(return_value=stub_mock)  # type: ignore[method-assign]

        with pytest.raises(M2MCallTimeout):
            async for _ in comm.call_module(
                module_address="127.0.0.1",
                module_port=9999,
                input_data={"root": {"protocol": "x"}},
                setup_id="setups:test",
                mission_id="missions:test",
            ):
                pass


class TestCancellation:
    """Cancelled ``call_module`` sends a best-effort ``SendSignal(CANCEL)``."""

    async def test_cancel_sends_signal_and_cleans_up(self) -> None:
        gw = _gw()
        comm = _comm(gw)

        stub_mock = MagicMock()
        stub_mock.StartStream = AsyncMock(
            return_value=gateway_pb2.StartStreamResponse(accepted=True, task_id="tid"),
        )
        stub_mock.SendSignal = AsyncMock(
            return_value=gateway_pb2.ClientSignalResponse(success=True, task_id="tid"),
        )
        comm._get_or_create_channel = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        comm._get_or_create_stub = MagicMock(return_value=stub_mock)  # type: ignore[method-assign]

        async def _drive() -> None:
            async for _ in comm.call_module(
                module_address="127.0.0.1",
                module_port=9999,
                input_data={"root": {"protocol": "x"}},
                setup_id="setups:test",
                mission_id="missions:test",
            ):
                pass

        task = asyncio.create_task(_drive())
        await asyncio.sleep(0.05)  # let call_module register and call StartStream
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        # SendSignal(CANCEL) was best-effort dispatched.
        assert stub_mock.SendSignal.await_count >= 1
        sent_request = stub_mock.SendSignal.await_args.args[0]
        assert sent_request.action == gateway_pb2.SignalAction.CANCEL
        # Registry + semaphore cleaned up.
        assert not gw._m2m.entries
        assert gw._m2m._semaphore._value == gw._settings.m2m.call_max_concurrent
