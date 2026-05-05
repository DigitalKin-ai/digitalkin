"""Phase 2.C — full-duplex BiDi: unbounded input + unbounded output.

The dial-back BiDi handles both directions concurrently for the lifetime
of the task:

- Consumer → Gateway: unlimited follow-up `StreamServer` messages land
  on `session.input_queue` after the first reply (which goes to the
  ModuleRunner).
- Gateway → Consumer: unlimited `StreamClient` messages drain from
  `task:{task_id}:stream` until the EOS marker.

Both sides use the same in-process gRPC + fakeredis fixtures already
exercised by `test_dial_consumer.py`.
"""

from __future__ import annotations

import asyncio
from typing import Any

import grpc
import grpc.aio
import pytest
from agentic_mesh_protocol.gateway.v1 import gateway_pb2, gateway_service_pb2_grpc
from google.protobuf import struct_pb2

from tests.gateway.test_dial_consumer import (
    SKIP_NO_FAKEREDIS,
    _FakeConsumerServicer,
    _FakeModuleRunner,
    _FakeRedisClient,
    _mock_context,
    _start_request,
)

pytestmark = [pytest.mark.timeout(30)]


def _protocol_of(stream_msg: Any) -> str:
    root = stream_msg.data.fields.get("root")
    if root is None:
        return ""
    pf = root.struct_value.fields.get("protocol")
    return pf.string_value if pf is not None else ""


@pytest.fixture
async def gateway_with_runner():
    from digitalkin.grpc_servers.gateway_servicer import GatewayServicer
    from digitalkin.models.grpc_servers.models import ClientConfig
    from digitalkin.models.settings.utils.channel import SecurityMode

    redis = _FakeRedisClient()
    cfg = ClientConfig(host="127.0.0.1", port=1, security=SecurityMode.INSECURE)
    runner = _FakeModuleRunner()
    servicer = GatewayServicer(
        redis_client=redis,  # type: ignore[arg-type]
        max_streams=100,
        client_config=cfg,
        module_runner=runner,  # type: ignore[arg-type]
    )
    servicer._fake_runner = runner  # type: ignore[attr-defined]
    try:
        yield servicer, redis
    finally:
        await redis.close()


@SKIP_NO_FAKEREDIS
class TestFullDuplex:
    async def test_unbounded_upstream_inputs(self, gateway_with_runner) -> None:
        """5 follow-up StreamServer messages all land on session.input_queue."""
        gateway, _redis = gateway_with_runner
        n_followups = 5
        servicer = _FakeConsumerServicer(
            query_data={"q": "first"},
            extra_upstream=[{"q": f"turn-{i}"} for i in range(1, n_followups + 1)],
        )
        server = grpc.aio.server()
        gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(servicer, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            task_id = "task_unbounded_in"
            ctx = _mock_context({"x-client-address": f"127.0.0.1:{port}"})
            await gateway.StartStream(_start_request(task_id), ctx)

            session = None
            for _ in range(80):
                session = gateway._registry.get(task_id)
                if session is not None and session.input_queue.qsize() >= n_followups:
                    break
                await asyncio.sleep(0.05)

            assert session is not None
            assert session.input_queue.qsize() >= n_followups

            # First reply went to ModuleRunner (not the queue).
            assert len(gateway._fake_runner.calls) == 1
            assert gateway._fake_runner.calls[0]["query"].fields["q"].string_value == "first"

            # Follow-ups landed on the queue in order.
            payloads = []
            while not session.input_queue.empty():
                item = session.input_queue.get_nowait()
                payloads.append(item["_proto"].fields["q"].string_value)
            assert payloads == [f"turn-{i}" for i in range(1, n_followups + 1)]
        finally:
            await server.stop(grace=0.1)

    async def test_unbounded_outputs(self, gateway_with_runner) -> None:
        """100 outputs pumped through task:{id}:stream all reach the consumer."""
        gateway, redis = gateway_with_runner
        n_outputs = 100
        servicer = _FakeConsumerServicer(query_data={"q": "go"})
        server = grpc.aio.server()
        gateway_service_pb2_grpc.add_GatewayServiceServicer_to_server(servicer, server)
        port = server.add_insecure_port("127.0.0.1:0")
        await server.start()
        try:
            task_id = "task_unbounded_out"

            # Pre-load the Redis stream with 100 outputs + EOS, using the
            # canonical `{"root": {"protocol": ...}}` shape.
            from digitalkin.core.task_manager.redis.proto_streams import ProtoStreamWriter
            writer = ProtoStreamWriter(task_id, redis)  # type: ignore[arg-type]
            for i in range(n_outputs):
                s = struct_pb2.Struct()
                s.update({"root": {"protocol": "tick", "i": i}})
                await writer.write_struct(s)
            await writer.write_eos()

            ctx = _mock_context({"x-client-address": f"127.0.0.1:{port}"})
            await gateway.StartStream(_start_request(task_id), ctx)

            # Wait until the consumer sees stream.end on the wire.
            for _ in range(200):
                if any(_protocol_of(m) == "stream.end" for m in servicer.received):
                    break
                await asyncio.sleep(0.1)

            ticks = [
                int(m.data.fields["root"].struct_value.fields["i"].number_value)
                for m in servicer.received
                if _protocol_of(m) == "tick"
            ]
            assert ticks == list(range(n_outputs))
            protos = [_protocol_of(m) for m in servicer.received]
            assert protos[-1] == "stream.end"
        finally:
            await server.stop(grace=0.1)
