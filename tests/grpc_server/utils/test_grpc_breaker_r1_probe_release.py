"""R1 regression: an abandoned half-open probe must not wedge the circuit breaker.

``exec_grpc_query`` acquires a HALF_OPEN probe slot via ``cb.check()``. If the
underlying RPC escapes with a non-``RpcError`` (e.g. ``asyncio.CancelledError``
from signal-driven cancellation, or a cancel during the backoff sleep), the
outcome is never recorded — so the ``finally`` must release the probe, else
``check()`` raises "probe in progress" forever for that service.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.models.grpc_servers.circuit_breaker import CBState

pytestmark = [pytest.mark.timeout(15), pytest.mark.regression]


def _half_open_wrapper(service: str) -> tuple[CircuitBreaker, GrpcClientWrapper]:
    """Build a wrapper whose breaker sits in HALF_OPEN with the probe slot free."""
    CircuitBreaker.remove(service)
    cb = CircuitBreaker.get_or_create(service)
    cb._state = CBState.HALF_OPEN
    cb._half_open_lock = False
    wrapper = GrpcClientWrapper.__new__(GrpcClientWrapper)
    wrapper.stub = MagicMock()
    wrapper.service_name = service
    return cb, wrapper


def test_release_probe_frees_only_a_held_lock() -> None:
    cb = CircuitBreaker("rp_unit", fail_max=2, reset_timeout=1.0)
    cb._state = CBState.HALF_OPEN
    cb._half_open_lock = True
    assert cb.release_probe() is True
    assert cb._half_open_lock is False
    # Idempotent: nothing left to release.
    assert cb.release_probe() is False
    # No-op when not HALF_OPEN.
    cb._state = CBState.CLOSED
    assert cb.release_probe() is False


@pytest.mark.asyncio
async def test_cancelled_probe_releases_half_open_lock() -> None:
    service = "r1_cancel_service"
    cb, wrapper = _half_open_wrapper(service)
    wrapper.stub.CallModule = AsyncMock(side_effect=asyncio.CancelledError())
    try:
        with pytest.raises(asyncio.CancelledError):
            await wrapper.exec_grpc_query("CallModule", MagicMock())
        assert cb._half_open_lock is False
        # A fresh probe is admitted again — the breaker is not wedged.
        cb.check()
    finally:
        CircuitBreaker.remove(service)


@pytest.mark.asyncio
async def test_generic_exception_releases_half_open_lock() -> None:
    service = "r1_runtime_service"
    cb, wrapper = _half_open_wrapper(service)
    wrapper.stub.CallModule = AsyncMock(side_effect=RuntimeError("boom"))
    try:
        with pytest.raises(RuntimeError):
            await wrapper.exec_grpc_query("CallModule", MagicMock())
        assert cb._half_open_lock is False
        cb.check()
    finally:
        CircuitBreaker.remove(service)


@pytest.mark.asyncio
async def test_successful_probe_closes_breaker_without_spurious_release() -> None:
    service = "r1_success_service"
    cb, wrapper = _half_open_wrapper(service)
    wrapper.stub.CallModule = AsyncMock(return_value="OK")
    try:
        result = await wrapper.exec_grpc_query("CallModule", MagicMock())
        assert result == "OK"
        assert cb.state == CBState.CLOSED
        assert cb._half_open_lock is False
    finally:
        CircuitBreaker.remove(service)
