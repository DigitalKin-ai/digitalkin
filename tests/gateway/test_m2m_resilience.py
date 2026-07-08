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
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_pb2
from google.protobuf import struct_pb2

from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
from digitalkin.models.grpc_servers.m2m import _M2MCallEntry
from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.settings.utils.channel import SecurityMode
from digitalkin.grpc_servers.exceptions import M2MAtCapacityError, PermissionDeniedError
from digitalkin.models.settings.gateway import get_gateway_settings
from digitalkin.services.communication.exceptions import M2MCallTimeout, M2MTargetUnavailable
from digitalkin.services.communication.grpc_communication import GrpcCommunication

pytestmark = [pytest.mark.timeout(15)]


def _struct(d: dict[str, Any]) -> struct_pb2.Struct:
    s = struct_pb2.Struct()
    s.update(d)
    return s


def _gw() -> GatewayServicer:
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock()
    fake_redis.xlen = AsyncMock(return_value=0)
    fake_redis.verify = AsyncMock(return_value=True)
    fake_redis.close = AsyncMock()
    runner = MagicMock()
    runner.run = AsyncMock()
    return GatewayServicer(
        redis_client=fake_redis,
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

    async def test_sweeper_reaps_expired_entries(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_M2M_CALL_SWEEPER_INTERVAL_S", "0.05")
        get_gateway_settings.cache_clear()
        gw = _gw()
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

    async def test_open_breaker_fast_fails(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_M2M_CALL_BREAKER_FAIL_MAX", "1")
        get_gateway_settings.cache_clear()
        gw = _gw()
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

    @pytest.mark.chaos
    async def test_permission_denied_passes_through_and_keeps_breaker_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The permission middleware raises PermissionDeniedError; call_module lets it pass, breaker untouched."""
        monkeypatch.setenv("DIGITALKIN_M2M_CALL_BREAKER_FAIL_MAX", "1")
        get_gateway_settings.cache_clear()
        gw = _gw()
        comm = _comm(gw)

        stub_mock = MagicMock()
        stub_mock.StartStream = AsyncMock(side_effect=PermissionDeniedError("[/gateway/StartStream] denied"))
        stub_mock.SendSignal = AsyncMock()
        stub_mock.AssociateTask = AsyncMock(
            return_value=gateway_pb2.AssociateTaskResponse(task_id="tid"),
        )
        comm._get_or_create_channel = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        comm._get_or_create_stub = MagicMock(return_value=stub_mock)  # type: ignore[method-assign]

        with pytest.raises(PermissionDeniedError):
            async for _ in comm.call_module(
                module_address="127.0.0.1",
                module_port=9998,
                input_data={"root": {"protocol": "x"}},
                setup_id="setups:test",
                mission_id="missions:test",
            ):
                pass

        # Permission is not a health signal: the breaker must stay closed (record_failure not reached).
        assert gw._m2m.breaker_for("127.0.0.1:9998").state.value == "closed"


class TestBreakerSingleCount:
    """M2: a failure is recorded exactly once in ``call_module`` and never doubled.

    The fix removed the ``breaker.record_failure()`` from
    ``M2MCallRegistry.handle_dial_back_receive`` (the dial-back serving side):
    in embedded mode that path and ``call_module`` share one process, so a fatal
    dial-back used to count twice and the breaker opened at ``fail_max // 2``.
    """

    async def test_dial_back_receive_does_not_record_breaker(self) -> None:
        gw = _gw()
        task_id = "dialtask"
        queue: asyncio.Queue[struct_pb2.Struct | None] = asyncio.Queue()
        gw._m2m.register(
            _M2MCallEntry(
                task_id=task_id,
                query=_struct({"root": {"protocol": "x"}}),
                output_queue=queue,
                expires_at=time.monotonic() + 100.0,
                target_key="dial-tgt:1",
            ),
        )
        breaker = gw._m2m.breaker_for("dial-tgt:1")
        before = breaker._failure_count  # noqa: SLF001 — test-only introspection

        async def _req_iter() -> AsyncIterator[gateway_pb2.StreamServer]:
            yield gateway_pb2.StreamServer(
                seq=0,
                task_id=task_id,
                data=_struct({"root": {"protocol": "stream.error", "fatal": True}}),
            )

        seen = [item async for item in gw._m2m.handle_dial_back_receive(task_id, _req_iter())]
        # The cached query is replayed first.
        assert seen and seen[0].task_id == task_id
        # M2: serving a fatal dial-back must NOT touch the breaker — call_module owns that.
        assert breaker._failure_count == before  # noqa: SLF001

    async def test_breaker_opens_at_fail_max_not_half(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_M2M_CALL_BREAKER_FAIL_MAX", "4")
        monkeypatch.setenv("DIGITALKIN_M2M_CALL_TIMEOUT_S", "0.05")
        get_gateway_settings.cache_clear()
        gw = _gw()
        comm = _comm(gw)

        stub_mock = MagicMock()
        stub_mock.StartStream = AsyncMock(
            return_value=gateway_pb2.StartStreamResponse(accepted=True, task_id="tid"),
        )
        stub_mock.SendSignal = AsyncMock()
        stub_mock.AssociateTask = AsyncMock(
            return_value=gateway_pb2.AssociateTaskResponse(task_id="tid"),
        )
        comm._get_or_create_channel = MagicMock(return_value=MagicMock())  # type: ignore[method-assign]
        comm._get_or_create_stub = MagicMock(return_value=stub_mock)  # type: ignore[method-assign]

        target = "127.0.0.1:9999"

        async def _one_failed_call() -> None:
            with pytest.raises(M2MCallTimeout):
                async for _ in comm.call_module(
                    module_address="127.0.0.1",
                    module_port=9999,
                    input_data={"root": {"protocol": "x"}},
                    setup_id="setups:test",
                    mission_id="missions:test",
                ):
                    pass

        for _ in range(3):
            await _one_failed_call()
        # Three real failures, each counted once → below fail_max(4) → still closed.
        assert gw._m2m.breaker_for(target)._failure_count == 3  # noqa: SLF001
        assert gw._m2m.breaker_for(target).state.value == "closed"

        await _one_failed_call()
        # Exactly fail_max real failures → open. With the old double-count it would
        # have opened at the 2nd call.
        assert gw._m2m.breaker_for(target).state.value == "open"


class TestMaxConcurrent:
    """Concurrency cap rejects calls past ``outbound_max_concurrent``."""

    async def test_third_call_raises_at_capacity(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_M2M_CALL_MAX_CONCURRENT", "2")
        monkeypatch.setenv("DIGITALKIN_M2M_CALL_ACQUIRE_TIMEOUT_S", "0.2")
        get_gateway_settings.cache_clear()
        gw = _gw()  # semaphore sized to 2 from settings

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

    async def test_output_queue_silence_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DIGITALKIN_M2M_CALL_TIMEOUT_S", "0.15")
        get_gateway_settings.cache_clear()
        gw = _gw()
        comm = _comm(gw)

        # Stub StartStream to succeed but never push to the queue.
        stub_mock = MagicMock()
        stub_mock.StartStream = AsyncMock(
            return_value=gateway_pb2.StartStreamResponse(accepted=True, task_id="tid"),
        )
        stub_mock.SendSignal = AsyncMock()
        stub_mock.AssociateTask = AsyncMock(
            return_value=gateway_pb2.AssociateTaskResponse(task_id="tid"),
        )
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
        stub_mock.AssociateTask = AsyncMock(
            return_value=gateway_pb2.AssociateTaskResponse(task_id="tid"),
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
        assert gw._m2m._semaphore._value == get_gateway_settings().m2m.call_max_concurrent
