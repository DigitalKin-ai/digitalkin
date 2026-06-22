#!/usr/bin/env python3
"""Demo client for the Gateway gRPC service with per-endpoint testing.

Subcommands:
    full      Full pipeline: StartStream → ConsumeStream → SendSignal
    start     StartStream only (unary)
    consume   ConsumeStream on existing task (requires --task-id)
    produce   StartStream + ProduceStream (act as Module A)
    signal    SendSignal on existing task (requires --task-id)
    inspect   Dump Redis keys for a task (no gRPC)

Usage:
    python client.py full --prompt "Hello world"
    python client.py full --prompt "Test" --setup '{"uppercase": true, "repeat": 5}'
    python client.py start --prompt "Hello"
    python client.py consume --task-id <uuid>
    python client.py produce --task-id <uuid> --chunks 3
    python client.py signal --task-id <uuid> --action cancel
    python client.py inspect --task-id <uuid>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from typing import Any, AsyncGenerator

import grpc
import redis.asyncio as aioredis
from google.protobuf import json_format, struct_pb2

from agentic_mesh_protocol.gateway.v1 import gateway_pb2, gateway_service_pb2_grpc

# ── ANSI colors ─────────────────────────────────────────────────────

RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
CYAN = "\033[36m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"

GRPC_OPTIONS = [
    ("grpc.max_receive_message_length", 50 * 1024 * 1024),
    ("grpc.max_send_message_length", 50 * 1024 * 1024),
    ("grpc.keepalive_time_ms", 30_000),
    ("grpc.keepalive_timeout_ms", 10_000),
    ("grpc.keepalive_permit_without_calls", True),
]


# ══════════════════════════════════════════════════════════════════
# Redis inspector
# ══════════════════════════════════════════════════════════════════


class RedisTracker:
    """Snapshots all Redis keys matching a task_id."""

    _redis: aioredis.Redis
    _snapshots: dict[str, dict[str, dict[str, Any]]]

    def __init__(self, redis_url: str) -> None:
        self._redis = aioredis.from_url(redis_url, decode_responses=False)
        self._snapshots = {}

    async def close(self) -> None:
        await self._redis.aclose()

    async def snapshot(self, label: str, task_id: str) -> dict[str, dict[str, Any]]:
        """Scan Redis for all keys containing task_id and capture their content.

        Args:
            label: Name for this snapshot.
            task_id: The task UUID to scan for.

        Returns:
            Dict of {key: {type, data}} for every matching key.
        """
        state: dict[str, dict[str, Any]] = {}
        patterns = [f"*{task_id}*", f"gateway:session:{task_id}"]
        seen: set[bytes] = set()
        for pattern in patterns:
            cursor: int = 0
            while True:
                cursor, keys = await self._redis.scan(cursor, match=pattern, count=500)
                seen.update(keys)
                if cursor == 0:
                    break

        for raw_key in sorted(seen):
            key = raw_key.decode()
            key_type = (await self._redis.type(raw_key)).decode()  # type: ignore[union-attr]
            entry: dict[str, Any] = {"type": key_type}

            if key_type == "stream":
                entries = await self._redis.xrange(raw_key)
                entry["len"] = len(entries)
                entry["entries"] = []
                for eid, fields in entries:
                    decoded: dict[str, str] = {}
                    for fk, fv in fields.items():
                        fname = fk.decode()
                        if fname == "pb" and fv:
                            s = struct_pb2.Struct()
                            s.ParseFromString(fv)
                            decoded[fname] = json_format.MessageToDict(s)
                        elif fname == "pb":
                            decoded[fname] = ""
                        else:
                            decoded[fname] = fv.decode()
                    entry["entries"].append({
                        "id": eid.decode() if isinstance(eid, bytes) else eid,
                        "fields": decoded,
                    })
                ttl = await self._redis.ttl(raw_key)
                if ttl > 0:
                    entry["ttl"] = ttl

            elif key_type == "hash":
                raw = await self._redis.hgetall(raw_key)
                entry["data"] = {k.decode(): v.decode() for k, v in raw.items()}
                ttl = await self._redis.ttl(raw_key)
                if ttl > 0:
                    entry["ttl"] = ttl

            elif key_type == "string":
                raw_val = await self._redis.get(raw_key)
                entry["data"] = raw_val.decode() if raw_val else ""
                ttl = await self._redis.ttl(raw_key)
                if ttl > 0:
                    entry["ttl"] = ttl

            elif key_type == "set":
                members = await self._redis.smembers(raw_key)
                entry["data"] = sorted(m.decode() for m in members)

            state[key] = entry

        self._snapshots[label] = state
        return state

    def diff(self, label_a: str, label_b: str) -> dict[str, str]:
        """Compare two snapshots and return per-key change description."""
        old = self._snapshots.get(label_a, {})
        new = self._snapshots.get(label_b, {})
        changes: dict[str, str] = {}
        for key in sorted(set(old) | set(new)):
            if key not in old:
                changes[key] = "NEW"
            elif key not in new:
                changes[key] = "DEL"
            elif old[key] != new[key]:
                changes[key] = "MOD"
            else:
                changes[key] = "---"
        return changes

    @property
    def labels(self) -> list[str]:
        return list(self._snapshots)

    def get(self, label: str) -> dict[str, dict[str, Any]]:
        return self._snapshots.get(label, {})


# ══════════════════════════════════════════════════════════════════
# Pretty printing
# ══════════════════════════════════════════════════════════════════


def _short_key(key: str, task_id: str) -> str:
    return key.replace(task_id, "{id}")


def _format_stream_entry(entry: dict[str, Any]) -> str:
    fields = entry["fields"]
    pb = fields.get("pb", "")
    seq = fields.get("seq", "?")
    eos = fields.get("eos", "")
    if eos == "true":
        return f"seq={seq} [EOS]"
    if isinstance(pb, dict):
        protocol = pb.get("root", {}).get("protocol", "")
        text = pb.get("root", {}).get("text", "")
        return f"seq={seq} {text or protocol}"
    return f"seq={seq} (empty)"


def _print_snapshot(snap: dict[str, dict[str, Any]], task_id: str) -> None:
    """Print a single snapshot's Redis state."""
    if not snap:
        print(f"  {DIM}(no keys found){RESET}")  # noqa: T201
        return

    for key in sorted(snap):
        short = _short_key(key, task_id)
        info = snap[key]
        ktype = info.get("type", "?")
        ttl_str = f"  ttl={info['ttl']}s" if "ttl" in info else ""

        if ktype == "stream":
            print(f"  {CYAN}{short:<42}{RESET} {ktype}{ttl_str}  ({info.get('len', 0)} entries)")  # noqa: T201
            for entry in info.get("entries", []):
                print(f"    {DIM}{entry['id']:>15}{RESET}  {_format_stream_entry(entry)}")  # noqa: T201

        elif ktype == "hash":
            data = info.get("data", {})
            print(f"  {CYAN}{short:<42}{RESET} {ktype}{ttl_str}")  # noqa: T201
            for hk in sorted(data):
                print(f"    {hk}: {data[hk]}")  # noqa: T201

        elif ktype == "string":
            data = info.get("data", "")
            print(f"  {CYAN}{short:<42}{RESET} {ktype}{ttl_str}  = {data}")  # noqa: T201

        else:
            print(f"  {CYAN}{short:<42}{RESET} {ktype}{ttl_str}")  # noqa: T201


def _print_diff_report(tracker: RedisTracker, task_id: str) -> None:
    """Print the full comparison report across snapshots."""
    labels = tracker.labels
    print()  # noqa: T201
    print(f"{BOLD}{'=' * 70}{RESET}")  # noqa: T201
    print(f"{BOLD}  REDIS STATE TRACKING — per-step diff{RESET}")  # noqa: T201
    print(f"  task_id = {task_id}")  # noqa: T201
    print(f"{BOLD}{'=' * 70}{RESET}")  # noqa: T201

    for i, label in enumerate(labels):
        if i == 0:
            continue

        prev_label = labels[i - 1]
        changes = tracker.diff(prev_label, label)
        snap = tracker.get(label)

        print()  # noqa: T201
        print(f"  {BOLD}[{i}] After {label}{RESET}")  # noqa: T201
        print(f"  {'─' * 60}")  # noqa: T201

        if all(v == "---" for v in changes.values()):
            print(f"      {DIM}(no Redis changes){RESET}")  # noqa: T201
            continue

        for key in sorted(changes):
            status = changes[key]
            short = _short_key(key, task_id)
            info = snap.get(key, {})
            ktype = info.get("type", "?")
            ttl_str = f"  ttl={info['ttl']}s" if "ttl" in info else ""

            color = GREEN if status == "NEW" else YELLOW if status == "MOD" else RED if status == "DEL" else DIM
            print(f"    {color}{status:>3}{RESET}  {short:<40} {ktype}{ttl_str}")  # noqa: T201

            if ktype == "stream":
                prev_snap = tracker.get(prev_label)
                prev_ids = {e["id"] for e in prev_snap.get(key, {}).get("entries", [])}
                for entry in info.get("entries", []):
                    marker = f"{GREEN} *{RESET}" if entry["id"] not in prev_ids else f"{DIM}  {RESET}"
                    print(f"        {marker} {entry['id']:>15}  {_format_stream_entry(entry)}")  # noqa: T201

            elif ktype == "hash":
                data = info.get("data", {})
                prev_data = tracker.get(prev_label).get(key, {}).get("data", {})
                for hk in sorted(set(data) | set(prev_data)):
                    old_v = prev_data.get(hk)
                    new_v = data.get(hk)
                    if old_v == new_v:
                        print(f"           {hk}: {new_v}")  # noqa: T201
                    elif old_v is None:
                        print(f"         {GREEN}+ {hk}: {new_v}{RESET}")  # noqa: T201
                    elif new_v is None:
                        print(f"         {RED}- {hk}: {old_v}{RESET}")  # noqa: T201
                    else:
                        print(f"         {YELLOW}~ {hk}: {old_v} -> {new_v}{RESET}")  # noqa: T201


# ══════════════════════════════════════════════════════════════════
# gRPC endpoint calls
# ══════════════════════════════════════════════════════════════════


async def cmd_start(
    stub: gateway_service_pb2_grpc.GatewayServiceStub,
    task_id: str,
    prompt: str,
    setup: dict[str, Any] | None,
) -> bool:
    """StartStream — create a task session.

    Returns:
        True if accepted.
    """
    input_struct = struct_pb2.Struct()
    payload: dict[str, Any] = {
        "root": {"protocol": "message", "text": prompt},
    }
    if setup:
        payload["setup"] = setup
    input_struct.update(payload)

    t0 = time.monotonic()
    resp = await stub.StartStream(
        gateway_pb2.StartStreamRequest(
            task_id=task_id,
            input=input_struct,
            setup_id="demo-setup",
            mission_id="demo-mission",
        ),
    )
    elapsed = (time.monotonic() - t0) * 1000

    status_color = GREEN if resp.accepted else RED
    print(f"  {status_color}accepted{RESET} = {resp.accepted}  ({elapsed:.1f}ms)")  # noqa: T201
    print(f"  task_id  = {resp.task_id}")  # noqa: T201
    return resp.accepted


async def cmd_consume(
    stub: gateway_service_pb2_grpc.GatewayServiceStub,
    task_id: str,
    verbose: bool,
) -> list[dict[str, Any]]:
    """ConsumeStream — read module output from Redis.

    Returns:
        List of received items.
    """
    received: list[dict[str, Any]] = []

    async def _requests() -> AsyncGenerator:
        yield gateway_pb2.ConsumeStreamRequest(
            init=gateway_pb2.ConsumeStreamInit(task_id=task_id, from_seq=0),
        )

    t0 = time.monotonic()
    resp_stream = stub.ConsumeStream(_requests())
    async for resp in resp_stream:
        elapsed = (time.monotonic() - t0) * 1000
        payload_type = resp.WhichOneof("payload")

        if payload_type == "output":
            data_dict = json_format.MessageToDict(resp.output.data)
            text = data_dict.get("root", {}).get("text", data_dict.get("root", {}).get("protocol", ""))
            received.append({"seq": resp.output.seq, "text": text})
            print(f"  {GREEN}seq={resp.output.seq:>2}{RESET}  {text}")  # noqa: T201
            if verbose:
                print(f"         {DIM}{json.dumps(data_dict, ensure_ascii=False)}{RESET}")  # noqa: T201

        elif payload_type == "status":
            state_name = gateway_pb2.StreamState.Name(resp.status.state)
            received.append({"status": state_name})
            color = GREEN if "COMPLETED" in state_name else YELLOW
            print(f"  {color}status: {state_name}{RESET}  ({elapsed:.1f}ms total)")  # noqa: T201
            break

        elif payload_type == "error":
            received.append({"error": resp.error.message})
            print(f"  {RED}error: code={resp.error.code} msg={resp.error.message}{RESET}")  # noqa: T201
            break

        elif payload_type == "heartbeat":
            if verbose:
                print(f"  {DIM}heartbeat{RESET}")  # noqa: T201

    return received


async def cmd_produce(
    stub: gateway_service_pb2_grpc.GatewayServiceStub,
    task_id: str,
    prompt: str,
    num_chunks: int,
) -> int:
    """ProduceStream — act as Module A, push output chunks.

    Returns:
        Number of server responses.
    """
    async def _requests() -> AsyncGenerator:
        yield gateway_pb2.ProduceStreamRequest(
            init=gateway_pb2.ProduceStreamInit(task_id=task_id),
        )
        for i in range(num_chunks):
            data = struct_pb2.Struct()
            data.update({
                "root": {
                    "protocol": "message",
                    "text": f"[{i + 1}/{num_chunks}] {prompt}",
                },
            })
            yield gateway_pb2.ProduceStreamRequest(
                output=gateway_pb2.ProduceStreamOutput(task_id=task_id, data=data),
            )
            await asyncio.sleep(0.05)

    t0 = time.monotonic()
    resp_stream = stub.ProduceStream(_requests())
    count = 0
    async for resp in resp_stream:
        count += 1
        payload = resp.WhichOneof("payload")
        print(f"  response #{count}: {payload}")  # noqa: T201

    elapsed = (time.monotonic() - t0) * 1000
    print(f"  {DIM}stream closed ({count} responses, {elapsed:.1f}ms){RESET}")  # noqa: T201
    return count


async def cmd_signal(
    stub: gateway_service_pb2_grpc.GatewayServiceStub,
    task_id: str,
    action: str,
) -> bool:
    """SendSignal — send a control signal.

    Returns:
        True if accepted.
    """
    action_enum = (
        gateway_pb2.SIGNAL_ACTION_CANCEL if action == "cancel"
        else gateway_pb2.SIGNAL_ACTION_PAUSE
    )

    t0 = time.monotonic()
    resp = await stub.SendSignal(
        gateway_pb2.ClientSignalRequest(task_id=task_id, action=action_enum),
    )
    elapsed = (time.monotonic() - t0) * 1000

    status_color = GREEN if resp.success else RED
    print(f"  {status_color}success{RESET} = {resp.success}  ({elapsed:.1f}ms)")  # noqa: T201
    return resp.success


# ══════════════════════════════════════════════════════════════════
# Subcommand handlers
# ══════════════════════════════════════════════════════════════════


async def run_full(args: argparse.Namespace) -> None:
    """Full pipeline: StartStream → ConsumeStream → SendSignal."""
    task_id = args.task_id or str(uuid.uuid4())
    setup = json.loads(args.setup) if args.setup else None
    tracker = RedisTracker(args.redis) if args.verbose else None

    print(f"\n{BOLD}Gateway{RESET} : {args.gateway}")  # noqa: T201
    print(f"{BOLD}Redis{RESET}   : {args.redis}")  # noqa: T201
    print(f"{BOLD}task_id{RESET} : {task_id}")  # noqa: T201
    if setup:
        print(f"{BOLD}setup{RESET}   : {json.dumps(setup)}")  # noqa: T201

    try:
        async with grpc.aio.insecure_channel(args.gateway, options=GRPC_OPTIONS) as channel:
            stub = gateway_service_pb2_grpc.GatewayServiceStub(channel)

            if tracker:
                await tracker.snapshot("baseline", task_id)

            # 1. StartStream
            print(f"\n{BOLD}[1] StartStream{RESET}")  # noqa: T201
            accepted = await cmd_start(stub, task_id, args.prompt, setup)
            if not accepted:
                print(f"  {RED}Server rejected the task — aborting.{RESET}")  # noqa: T201
                return

            if tracker:
                await asyncio.sleep(0.1)
                await tracker.snapshot("StartStream", task_id)

            # 2. ConsumeStream — wait for module output
            print(f"\n{BOLD}[2] ConsumeStream{RESET}")  # noqa: T201
            await asyncio.sleep(0.2)  # let module start
            received = await cmd_consume(stub, task_id, args.verbose)

            if tracker:
                await asyncio.sleep(0.1)
                await tracker.snapshot("ConsumeStream", task_id)

            # 3. SendSignal
            print(f"\n{BOLD}[3] SendSignal (cancel){RESET}")  # noqa: T201
            await cmd_signal(stub, task_id, "cancel")

            if tracker:
                await asyncio.sleep(0.1)
                await tracker.snapshot("SendSignal", task_id)

        # Print Redis diff report
        if tracker:
            _print_diff_report(tracker, task_id)

        # JSON output
        if args.json:
            print(f"\n{BOLD}JSON:{RESET}")  # noqa: T201
            print(json.dumps({  # noqa: T201
                "task_id": task_id,
                "accepted": accepted,
                "received": received,
            }, indent=2, ensure_ascii=False))
    finally:
        if tracker:
            await tracker.close()


async def run_start(args: argparse.Namespace) -> None:
    """StartStream only."""
    task_id = args.task_id or str(uuid.uuid4())
    setup = json.loads(args.setup) if args.setup else None

    print(f"\n{BOLD}[StartStream]{RESET}  gateway={args.gateway}  task_id={task_id}")  # noqa: T201

    async with grpc.aio.insecure_channel(args.gateway, options=GRPC_OPTIONS) as channel:
        stub = gateway_service_pb2_grpc.GatewayServiceStub(channel)
        await cmd_start(stub, task_id, args.prompt, setup)


async def run_consume(args: argparse.Namespace) -> None:
    """ConsumeStream on existing task."""
    if not args.task_id:
        print(f"{RED}--task-id is required for consume{RESET}")  # noqa: T201
        sys.exit(1)

    print(f"\n{BOLD}[ConsumeStream]{RESET}  gateway={args.gateway}  task_id={args.task_id}")  # noqa: T201

    async with grpc.aio.insecure_channel(args.gateway, options=GRPC_OPTIONS) as channel:
        stub = gateway_service_pb2_grpc.GatewayServiceStub(channel)
        await cmd_consume(stub, args.task_id, args.verbose)


async def run_produce(args: argparse.Namespace) -> None:
    """StartStream + ProduceStream (act as Module A)."""
    task_id = args.task_id or str(uuid.uuid4())
    setup = json.loads(args.setup) if args.setup else None

    print(f"\n{BOLD}[Produce]{RESET}  gateway={args.gateway}  task_id={task_id}")  # noqa: T201

    async with grpc.aio.insecure_channel(args.gateway, options=GRPC_OPTIONS) as channel:
        stub = gateway_service_pb2_grpc.GatewayServiceStub(channel)

        # StartStream first (registers the session)
        print(f"\n  {BOLD}StartStream{RESET}")  # noqa: T201
        accepted = await cmd_start(stub, task_id, args.prompt, setup)
        if not accepted:
            print(f"  {RED}Rejected{RESET}")  # noqa: T201
            return

        # ProduceStream (act as Module A)
        print(f"\n  {BOLD}ProduceStream ({args.chunks} chunks){RESET}")  # noqa: T201
        await cmd_produce(stub, task_id, args.prompt, args.chunks)


async def run_signal(args: argparse.Namespace) -> None:
    """SendSignal on existing task."""
    if not args.task_id:
        print(f"{RED}--task-id is required for signal{RESET}")  # noqa: T201
        sys.exit(1)

    print(f"\n{BOLD}[SendSignal]{RESET}  gateway={args.gateway}  task_id={args.task_id}  action={args.action}")  # noqa: T201

    async with grpc.aio.insecure_channel(args.gateway, options=GRPC_OPTIONS) as channel:
        stub = gateway_service_pb2_grpc.GatewayServiceStub(channel)
        await cmd_signal(stub, args.task_id, args.action)


async def run_inspect(args: argparse.Namespace) -> None:
    """Dump Redis keys for a task (no gRPC)."""
    if not args.task_id:
        print(f"{RED}--task-id is required for inspect{RESET}")  # noqa: T201
        sys.exit(1)

    print(f"\n{BOLD}[Inspect]{RESET}  redis={args.redis}  task_id={args.task_id}")  # noqa: T201
    print()  # noqa: T201

    tracker = RedisTracker(args.redis)
    try:
        snap = await tracker.snapshot("current", args.task_id)
        if not snap:
            print(f"  {YELLOW}No keys found for task_id={args.task_id}{RESET}")  # noqa: T201
            return

        _print_snapshot(snap, args.task_id)

        if args.json:
            print(f"\n{BOLD}JSON:{RESET}")  # noqa: T201
            print(json.dumps(snap, indent=2, ensure_ascii=False, default=str))  # noqa: T201
    finally:
        await tracker.close()


# ══════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════


def _build_parser() -> argparse.ArgumentParser:
    default_gateway = os.environ.get("GATEWAY_ADDR", "localhost:50051")
    default_redis = os.environ.get("DIGITALKIN_REDIS_URL", "redis://localhost:6379/0")

    p = argparse.ArgumentParser(
        description="Demo client for the Gateway gRPC service",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
examples:
  %(prog)s full --prompt "Hello world"
  %(prog)s full --prompt "Test" --setup '{"uppercase": true, "repeat": 5}'
  %(prog)s start --prompt "Hello"
  %(prog)s consume --task-id <uuid>
  %(prog)s produce --chunks 5 --prompt "Manual"
  %(prog)s signal --task-id <uuid> --action cancel
  %(prog)s inspect --task-id <uuid>
""",
    )

    # Common flags
    p.add_argument("--gateway", default=default_gateway, help=f"Gateway address (default: {default_gateway})")
    p.add_argument("--redis", default=default_redis, help=f"Redis URL (default: {default_redis})")
    p.add_argument("--task-id", default="", help="Task UUID (auto-generated if omitted)")
    p.add_argument("--prompt", default="Hello from demo client", help="Input text (default: %(default)s)")
    p.add_argument("--setup", default="", help='Module setup overrides as JSON (e.g. \'{"uppercase": true}\')')
    p.add_argument("-v", "--verbose", action="store_true", help="Show full Redis state and proto details")
    p.add_argument("--json", action="store_true", help="Print JSON output at the end")

    sub = p.add_subparsers(dest="command", help="Endpoint to test")

    # full (default)
    sub.add_parser("full", help="Full pipeline: StartStream -> ConsumeStream -> SendSignal")

    # start
    sub.add_parser("start", help="StartStream only (unary)")

    # consume
    sub.add_parser("consume", help="ConsumeStream on existing task (requires --task-id)")

    # produce
    sp_produce = sub.add_parser("produce", help="StartStream + ProduceStream (act as Module A)")
    sp_produce.add_argument("--chunks", type=int, default=3, help="Number of chunks to produce (default: 3)")

    # signal
    sp_signal = sub.add_parser("signal", help="SendSignal on existing task (requires --task-id)")
    sp_signal.add_argument("--action", choices=["cancel", "pause"], default="cancel", help="Signal action (default: cancel)")

    # inspect
    sub.add_parser("inspect", help="Dump Redis keys for a task (no gRPC)")

    return p


async def main() -> None:
    """Entry point."""
    parser = _build_parser()
    args = parser.parse_args()

    # Default to "full" if no subcommand
    if not args.command:
        args.command = "full"

    # Set defaults for subcommand-specific args
    if not hasattr(args, "chunks"):
        args.chunks = 3
    if not hasattr(args, "action"):
        args.action = "cancel"

    handlers = {
        "full": run_full,
        "start": run_start,
        "consume": run_consume,
        "produce": run_produce,
        "signal": run_signal,
        "inspect": run_inspect,
    }

    try:
        await handlers[args.command](args)
    except grpc.aio.AioRpcError as e:
        print(f"\n{RED}gRPC error: {e.code().name} — {e.details()}{RESET}")  # noqa: T201
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n{DIM}Interrupted{RESET}")  # noqa: T201


if __name__ == "__main__":
    asyncio.run(main())
