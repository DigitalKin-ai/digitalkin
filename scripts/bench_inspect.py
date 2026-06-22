#!/usr/bin/env python3
"""Run one Stream call, dump every StreamServer message received."""

import asyncio
import time
import uuid

import grpc
import grpc.aio
from agentic_mesh_protocol.gateway.v1 import gateway_pb2, gateway_service_pb2_grpc
from google.protobuf import json_format, struct_pb2


async def main(host: str, setup_id: str, protocol: str) -> None:
    channel = grpc.aio.insecure_channel(host)
    stub = gateway_service_pb2_grpc.GatewayServiceStub(channel)

    task_id = str(uuid.uuid4())
    mission_id = f"missions:inspect_{uuid.uuid4().hex[:8]}"

    # Build the query Struct.
    data = struct_pb2.Struct()
    if protocol.startswith("healthcheck"):
        data.update({"root": {"protocol": protocol}})
    else:
        data.update({"root": {"protocol": protocol, "user_prompt": "ping"}})

    t0 = time.monotonic()
    ack = await stub.StartStream(gateway_pb2.StartStreamRequest(
        task_id=task_id, setup_id=setup_id, mission_id=mission_id,
    ), timeout=10)
    t_ack = (time.monotonic() - t0) * 1000
    print(f"StartStream  accepted={ack.accepted}  task_id={ack.task_id}  ({t_ack:.2f} ms)\n")

    first_client = gateway_pb2.StreamClient(task_id=task_id, from_seq=0, data=data)

    async def _iter():
        yield first_client

    print("StreamServer messages:")
    print(f"{'idx':>3} {'t (ms)':>10} {'seq':>4}  protocol")
    print("-" * 60)

    idx = 0
    async for msg in stub.Stream(_iter(), timeout=30):
        idx += 1
        t_ms = (time.monotonic() - t0) * 1000
        proto_name = "(no root.protocol)"
        root = msg.data.fields.get("root")
        if root is not None:
            pf = root.struct_value.fields.get("protocol")
            if pf is not None:
                proto_name = pf.string_value
        print(f"{idx:>3} {t_ms:>10.2f} {msg.seq:>4}  {proto_name}")

        # Dump full payload
        as_dict = json_format.MessageToDict(msg.data)
        for line in str(as_dict).split("\n"):
            print(f"      {line}")
        print()

        if proto_name == "stream.end":
            break

    print(f"\nTotal messages: {idx}")
    await channel.close()


if __name__ == "__main__":
    import sys
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost:50056"
    setup_id = sys.argv[2] if len(sys.argv) > 2 else "setups:ada_setup"
    protocol = sys.argv[3] if len(sys.argv) > 3 else "healthcheck_ping"
    asyncio.run(main(host, setup_id, protocol))
