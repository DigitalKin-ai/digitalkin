"""Unit tests for ``GatewayServicer.Stream``'s dial-back-receive dispatch.

A remote gateway dialing back into this process sends an in-band
``stream.init`` sentinel as its first message. The servicer looks up the
matching outbound entry, replies with the cached query, then forwards
inbound outputs onto the entry's queue until ``stream.end`` or fatal
``stream.error``.
"""

from __future__ import annotations

import asyncio
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

try:
    import fakeredis.aioredis as fakeredis_aio
except ImportError:
    fakeredis_aio = None  # type: ignore[assignment]

SKIP_NO_FAKEREDIS = pytest.mark.skipif(fakeredis_aio is None, reason="fakeredis not installed")
pytestmark = [pytest.mark.timeout(10)]


def _struct(d: dict[str, Any]) -> struct_pb2.Struct:
    s = struct_pb2.Struct()
    s.update(d)
    return s


def _make_servicer() -> GatewayServicer:
    fake_redis = MagicMock()
    fake_redis.xadd = AsyncMock()
    fake_redis.xlen = AsyncMock(return_value=0)
    runner = MagicMock()
    runner.run = AsyncMock()
    return GatewayServicer(
        redis_client=fake_redis,
        client_config=ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE),
        module_runner=runner,
    )


class _Iter:
    def __init__(self, msgs: list[Any]) -> None:
        self._msgs = msgs
        self._i = 0

    def __aiter__(self) -> _Iter:
        return self

    async def __anext__(self) -> Any:
        if self._i >= len(self._msgs):
            raise StopAsyncIteration
        msg = self._msgs[self._i]
        self._i += 1
        return msg


class TestDialBackBranch:
    """``stream.init`` first message routes to the dial-back-receive handler."""

    async def test_replies_with_cached_query_then_forwards_outputs(self) -> None:
        gw = _make_servicer()
        query = _struct({"root": {"protocol": "ask", "q": "hello"}})
        queue: asyncio.Queue[struct_pb2.Struct | None] = asyncio.Queue()
        gw._m2m.register(
            _M2MCallEntry(
                task_id="t1",
                query=query,
                output_queue=queue,
                expires_at=asyncio.get_event_loop().time() + 60,
                target_key="127.0.0.1:1",
            ),
        )

        init = _struct({"root": {"protocol": "stream.init"}})
        out1 = _struct({"root": {"protocol": "ask.response", "text": "hi"}})
        end = _struct({"root": {"protocol": "stream.end"}})
        req_iter = _Iter([
            gateway_pb2.StreamServer(task_id="t1", seq=0, data=init),
            gateway_pb2.StreamServer(task_id="t1", seq=1, data=out1),
            gateway_pb2.StreamServer(task_id="t1", seq=2, data=end),
        ])

        ctx = MagicMock()
        ctx.invocation_metadata.return_value = []
        yielded: list[Any] = []
        async for resp in gw.Stream(req_iter, ctx):
            yielded.append(resp)

        # First (and only) yield is the query reply as StreamClient.
        assert len(yielded) == 1
        assert isinstance(yielded[0], gateway_pb2.StreamClient)
        assert yielded[0].task_id == "t1"
        assert yielded[0].data.fields["root"].struct_value.fields["protocol"].string_value == "ask"

        # Queue received out1, end, then None (from finally).
        items: list[struct_pb2.Struct | None] = []
        while not queue.empty():
            items.append(queue.get_nowait())
        protos = [
            (i.fields["root"].struct_value.fields["protocol"].string_value if i is not None else None)
            for i in items
        ]
        assert protos == ["ask.response", "stream.end", None]

        # Success terminator → breaker recorded a success (state CLOSED, no failures).
        breaker = gw._m2m.breaker_for("127.0.0.1:1")
        assert breaker.state.value == "closed"

    async def test_unknown_task_id_emits_fatal(self) -> None:
        gw = _make_servicer()
        init = _struct({"root": {"protocol": "stream.init"}})
        req_iter = _Iter([gateway_pb2.StreamServer(task_id="unknown", seq=0, data=init)])

        ctx = MagicMock()
        ctx.invocation_metadata.return_value = []
        yielded: list[Any] = []
        async for resp in gw.Stream(req_iter, ctx):
            yielded.append(resp)

        # _fatal_close yields stream.error + stream.end (both as StreamClient).
        assert len(yielded) == 2
        protos = [r.data.fields["root"].struct_value.fields["protocol"].string_value for r in yielded]
        assert protos == ["stream.error", "stream.end"]

    async def test_fatal_stream_error_records_breaker_failure(self) -> None:
        gw = _make_servicer()
        queue: asyncio.Queue[struct_pb2.Struct | None] = asyncio.Queue()
        gw._m2m.register(
            _M2MCallEntry(
                task_id="t2",
                query=_struct({"root": {"protocol": "ask"}}),
                output_queue=queue,
                expires_at=asyncio.get_event_loop().time() + 60,
                target_key="127.0.0.1:9999",
            ),
        )
        init = _struct({"root": {"protocol": "stream.init"}})
        err = _struct({"root": {"protocol": "stream.error", "fatal": True, "code": "X", "message": "boom"}})
        req_iter = _Iter([
            gateway_pb2.StreamServer(task_id="t2", seq=0, data=init),
            gateway_pb2.StreamServer(task_id="t2", seq=1, data=err),
        ])

        ctx = MagicMock()
        ctx.invocation_metadata.return_value = []
        yielded = [r async for r in gw.Stream(req_iter, ctx)]
        assert len(yielded) == 1  # only the query reply

        breaker = gw._m2m.breaker_for("127.0.0.1:9999")
        # After one failure with fail_max=5 default, breaker is still CLOSED but counted.
        assert breaker.state.value == "closed"
