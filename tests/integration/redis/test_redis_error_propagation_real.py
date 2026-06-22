"""R2 integration: StartStream declines gracefully when real Redis is unreachable.

Pairs the fake-based unit tests in ``tests/gateway/test_redis_error_propagation.py``.
Uses a real ``RedisClient`` pointed at a closed port so the idempotency claim
hits redis-py's actual ``ConnectionError`` path — the gateway must return
``accepted=False`` rather than aborting the RPC.
"""

from __future__ import annotations

import socket
from unittest.mock import MagicMock

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.timeout(20)]


def _closed_port() -> int:
    """Return a localhost port with no listener (connections are refused)."""
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


async def test_startstream_not_accepted_when_redis_unreachable() -> None:
    from agentic_mesh_protocol.gateway.v1 import gateway_pb2

    from digitalkin.core.task_manager.redis.redis_client import RedisClient
    from digitalkin.grpc_servers.gateway_servicer import GatewayServicer

    client = RedisClient(f"redis://127.0.0.1:{_closed_port()}/0")
    servicer = GatewayServicer(redis_client=client)
    ctx = MagicMock()
    ctx.invocation_metadata.return_value = [("x-client-address", "127.0.0.1:50057")]
    req = gateway_pb2.StartStreamRequest(task_id="task_int_r2", setup_id="setups:s", mission_id="missions:m")
    try:
        resp = await servicer.StartStream(req, ctx)
        assert resp.accepted is False
        assert resp.task_id == "task_int_r2"
    finally:
        await client.close()
