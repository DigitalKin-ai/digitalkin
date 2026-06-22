"""M3 regression: a missing RPC method must not wedge the half-open circuit breaker.

The method-existence check runs BEFORE the breaker probe, so a missing method
raises ``ServerError`` without claiming (and leaking) the HALF_OPEN probe lock.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from digitalkin.grpc_servers.exceptions import ServerError
from digitalkin.grpc_servers.utils.circuit_breaker import CircuitBreaker
from digitalkin.grpc_servers.utils.grpc_client_wrapper import GrpcClientWrapper
from digitalkin.models.grpc_servers.circuit_breaker import CBState


@pytest.mark.asyncio
async def test_missing_method_does_not_wedge_half_open_lock() -> None:
    service = "m3_test_service"
    CircuitBreaker.remove(service)
    cb = CircuitBreaker.get_or_create(service)
    cb._state = CBState.HALF_OPEN  # noqa: SLF001
    cb._half_open_lock = False  # noqa: SLF001

    wrapper = GrpcClientWrapper.__new__(GrpcClientWrapper)
    wrapper.stub = MagicMock(spec=[])  # spec=[] → getattr(any name, None) returns None
    wrapper.service_name = service

    try:
        with pytest.raises(ServerError, match="not found on stub"):
            await wrapper.exec_grpc_query("NonExistentMethod", MagicMock())

        # M3: the probe lock was never claimed — the method check preceded cb.check().
        assert cb._half_open_lock is False  # noqa: SLF001
    finally:
        CircuitBreaker.remove(service)
