#!/usr/bin/env python3
"""Profile one Stream call end-to-end.

Captures:
  - TT1R (first server message arrival, gateway-emitted stream.start).
  - TT2R (second server message arrival — the module's first domain output).
  - Total wall time.
  - One pyinstrument flame graph of the whole call (HTML).
  - Per-message timings printed inline (idx, t_ms, seq, protocol).

Usage:
    uv run python scripts/profile_one.py [host:port] [setup_id] [protocol]

Defaults:
    host:port = localhost:50056
    setup_id  = setups:01kh6qkgfbkraz3fyrmg8d3rvt
    protocol  = healthcheck_ping

Output:
    bench_results/profile/profile_<task_id>.html  (open in a browser)
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from pathlib import Path

import grpc
import grpc.aio
from agentic_mesh_protocol.gateway.v1 import gateway_pb2, gateway_service_pb2_grpc
from google.protobuf import json_format, struct_pb2


GRPC_OPTIONS = [
    ("grpc.max_receive_message_length", 100 * 1024 * 1024),
    ("grpc.keepalive_time_ms", 60_000),
    ("grpc.keepalive_timeout_ms", 20_000),
    ("grpc.keepalive_permit_without_calls", True),
]


async def _run_one(host: str, setup_id: str, protocol: str, mission_id: str) -> dict:
    """Open StartStream + Stream once. Return per-message timings."""
    channel = grpc.aio.insecure_channel(host, options=GRPC_OPTIONS)
    stub = gateway_service_pb2_grpc.GatewayServiceStub(channel)
    task_id = str(uuid.uuid4())

    # Build query Struct.
    data = struct_pb2.Struct()
    if protocol.startswith("healthcheck"):
        data.update({"root": {"protocol": protocol}})
    elif protocol == "agui_stream":
        data.update({
            "root": {
                "protocol": "agui_stream",
                "thread_id": str(uuid.uuid4()),
                "run_id": str(uuid.uuid4()),
                "messages": [{"role": "user", "id": str(uuid.uuid4()), "content": "ping"}],
                "tools": [],
                "context": [],
            },
        })
    else:
        data.update({"root": {"protocol": protocol, "user_prompt": "ping"}})

    t0 = time.monotonic_ns()
    ack = await stub.StartStream(
        gateway_pb2.StartStreamRequest(task_id=task_id, setup_id=setup_id, mission_id=mission_id),
        timeout=10,
    )
    t_ack = (time.monotonic_ns() - t0) / 1e6  # ms

    if not ack.accepted:
        await channel.close()
        return {"task_id": task_id, "ok": False, "error": "not_accepted", "t_ack_ms": t_ack}

    first_msg = gateway_pb2.StreamClient(task_id=task_id, from_seq=0, data=data)

    async def _iter():
        yield first_msg

    msgs: list[dict] = []
    tt1r = 0.0
    tt2r = 0.0
    async for m in stub.Stream(_iter(), timeout=30):
        t_ms = (time.monotonic_ns() - t0) / 1e6
        proto_name = "(no root.protocol)"
        root = m.data.fields.get("root")
        if root is not None:
            pf = root.struct_value.fields.get("protocol")
            if pf is not None:
                proto_name = pf.string_value
        msgs.append({
            "idx": len(msgs) + 1,
            "t_ms": t_ms,
            "seq": m.seq,
            "task_id": m.task_id,
            "protocol": proto_name,
            "data": json_format.MessageToDict(m.data),
        })
        if len(msgs) == 1:
            tt1r = t_ms
        elif len(msgs) == 2:
            tt2r = t_ms
        if proto_name == "stream.end":
            break

    total_ms = (time.monotonic_ns() - t0) / 1e6
    await channel.close()
    return {
        "task_id": task_id,
        "ok": True,
        "t_ack_ms": t_ack,
        "tt1r_ms": tt1r,
        "tt2r_ms": tt2r,
        "total_ms": total_ms,
        "n_messages": len(msgs),
        "messages": msgs,
    }


async def main(host: str, setup_id: str, protocol: str) -> int:
    try:
        from pyinstrument import Profiler
    except ImportError:
        print("pyinstrument is not installed; running without flame graph", file=sys.stderr)
        Profiler = None  # type: ignore[assignment]

    mission_id = f"missions:profile_{uuid.uuid4().hex[:8]}"
    out_dir = Path("bench_results/profile")
    out_dir.mkdir(parents=True, exist_ok=True)

    profiler = Profiler(async_mode="enabled") if Profiler is not None else None
    if profiler is not None:
        profiler.start()

    result = await _run_one(host, setup_id, protocol, mission_id)

    if profiler is not None:
        profiler.stop()
        html_path = out_dir / f"profile_{result['task_id']}.html"
        html_path.write_text(profiler.output_html())
        print(f"\nflame graph: {html_path}")

    print("\n=== Result ===")
    print(f"task_id    : {result['task_id']}")
    print(f"ok         : {result['ok']}")
    if not result["ok"]:
        print(f"error      : {result.get('error')}")
        return 1

    print(f"t_ack      : {result['t_ack_ms']:.2f} ms  (StartStream unary roundtrip)")
    print(f"TT1R       : {result['tt1r_ms']:.2f} ms  (first server message — gateway stream.start)")
    print(f"TT2R       : {result['tt2r_ms']:.2f} ms  (second server message — module's first output)")
    print(f"total      : {result['total_ms']:.2f} ms  (stream.end received + clean close)")
    print(f"messages   : {result['n_messages']}")

    print("\n=== Wire trace ===")
    print(f"{'idx':>3} {'t (ms)':>10} {'seq':>4}  protocol")
    print("-" * 60)
    for m in result["messages"]:
        print(f"{m['idx']:>3} {m['t_ms']:>10.2f} {m['seq']:>4}  {m['protocol']}")
        for k, v in m["data"].items():
            print(f"      {k}: {v}")

    return 0


if __name__ == "__main__":
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost:50056"
    setup_id = sys.argv[2] if len(sys.argv) > 2 else "setups:01kh6qkgfbkraz3fyrmg8d3rvt"
    protocol = sys.argv[3] if len(sys.argv) > 3 else "healthcheck_ping"
    sys.exit(asyncio.run(main(host, setup_id, protocol)))
