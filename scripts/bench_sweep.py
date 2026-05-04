#!/usr/bin/env python3
"""Multi-concurrency benchmark sweep — dial-back consumer (chainlit-style).

The bench is a *consumer*, the same role chainlit plays. It runs one
shared :class:`digitalkin.GatewayConsumer` for the whole sweep:

    1. Starts a local ``GatewayService.Stream`` server (the dial-back).
    2. Calls ``StartStream`` with ``x-client-address`` metadata.
    3. The module's gateway dials back; ``GatewayConsumer.call(...)``
       yields one ``google.protobuf.Struct`` per module output.

All concurrent calls multiplex through the single dial-back server.

Two modes:
- **wave** (--wave): fire N requests, wait for all, repeat.
- **sustained** (default): keep exactly N requests in flight via a
  semaphore; immediate replacement on completion.

Usage:
    uv run python scripts/bench_sweep.py
    uv run python scripts/bench_sweep.py --wave -c 1,10,50 -d 60 -a 5
    uv run python scripts/bench_sweep.py --consumer-advertise host.docker.internal:50057

Requires the target module's gateway to be reachable from the bench
process *and* the bench's dial-back address (``--consumer-advertise``)
to be reachable from the module's gateway. For a local module on
``localhost:50061``, the defaults work; for a docker-hosted module,
override ``--consumer-advertise`` to a host the container can resolve.
"""

import argparse
import asyncio
import json
import os
import random
import statistics
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import grpc
from digitalkin import ConsumerConfig, GatewayConsumer, StartStreamRejected, StartStreamRpcError
from google.protobuf import struct_pb2

# ── Defaults ──────────────────────────────────────────────────────────────────

DEFAULT_CONCURRENCY_LEVELS = [1, 5, 25]
DEFAULT_ATTEMPTS = 3
DEFAULT_DURATION_S = 30
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 50061
DEFAULT_SETUP_ID = "setups:template_tool_setup"
DEFAULT_PROMPT = "hello"
DEFAULT_OUTPUT_DIR = "bench_results/sweep"
DEFAULT_CONSUMER_LISTEN = "[::]"
DEFAULT_CONSUMER_PORT = 50057
DEFAULT_CONSUMER_ADVERTISE = "localhost:50057"

MISSION_IDS = [
    f"missions:sweep_{i:04d}_{''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=16))}"
    for i in range(5000)
]
_mission_cycle: Iterator[str] = iter(MISSION_IDS)


def _next_mission() -> str:
    global _mission_cycle
    try:
        return next(_mission_cycle)
    except StopIteration:
        _mission_cycle = iter(MISSION_IDS)
        return next(_mission_cycle)


# ── Data types ────────────────────────────────────────────────────────────────


@dataclass
class RequestResult:
    ok: bool
    latency_ms: float
    ttfr_ms: float = 0.0
    tt2r_ms: float = 0.0
    messages: int = 0
    # error is (code, message); code maps to StreamErrorCode for in-band
    # protocol failures, or to a synthetic bucket for client-side excs:
    # "STARTSTREAM_RPC", "STARTSTREAM_REJECTED", "GRPC_AIO", "BENCH",
    # "NO_MESSAGES". Empty tuple means success.
    error: tuple[str, str] = ("", "")

    @property
    def error_code(self) -> str:
        return self.error[0]

    @property
    def error_message(self) -> str:
        return self.error[1]


@dataclass
class AttemptResult:
    concurrency: int
    attempt: int
    mode: str = "wave"
    results: list[RequestResult] = field(default_factory=list)
    duration_s: float = 0.0

    @property
    def ok_latencies(self) -> list[float]:
        return sorted(r.latency_ms for r in self.results if r.ok)

    @property
    def ok_ttfr(self) -> list[float]:
        return sorted(r.ttfr_ms for r in self.results if r.ok and r.ttfr_ms > 0)

    @property
    def ok_tt2r(self) -> list[float]:
        return sorted(r.tt2r_ms for r in self.results if r.ok and r.tt2r_ms > 0)

    @property
    def ok_count(self) -> int:
        return sum(1 for r in self.results if r.ok)

    @property
    def err_count(self) -> int:
        return sum(1 for r in self.results if not r.ok)

    @property
    def throughput(self) -> float:
        return self.ok_count / self.duration_s if self.duration_s > 0 else 0

    def percentile(self, lats: list[float], p: float) -> float:
        if not lats:
            return 0.0
        k = (len(lats) - 1) * (p / 100)
        f_idx = int(k)
        c_idx = min(f_idx + 1, len(lats) - 1)
        d = k - f_idx
        return lats[f_idx] + d * (lats[c_idx] - lats[f_idx])


@dataclass
class LevelSummary:
    concurrency: int
    attempts: list[AttemptResult] = field(default_factory=list)

    @property
    def all_ok_latencies(self) -> list[float]:
        return sorted(lat for a in self.attempts for lat in a.ok_latencies)

    @property
    def total_ok(self) -> int:
        return sum(a.ok_count for a in self.attempts)

    @property
    def total_err(self) -> int:
        return sum(a.err_count for a in self.attempts)

    @property
    def avg_throughput(self) -> float:
        tputs = [a.throughput for a in self.attempts if a.throughput > 0]
        return statistics.mean(tputs) if tputs else 0

    def p(self, pct: float) -> float:
        lats = self.all_ok_latencies
        if not lats:
            return 0.0
        k = (len(lats) - 1) * (pct / 100)
        f_idx = int(k)
        c_idx = min(f_idx + 1, len(lats) - 1)
        d = k - f_idx
        return lats[f_idx] + d * (lats[c_idx] - lats[f_idx])


# ── Per-request via GatewayConsumer.call ──────────────────────────────────────


def _build_input(prompt: str, protocol: str) -> struct_pb2.Struct:
    s = struct_pb2.Struct()
    if protocol == "agui_stream":
        s.update({
            "root": {
                "protocol": "agui_stream",
                "thread_id": str(uuid.uuid4()),
                "run_id": str(uuid.uuid4()),
                "messages": [{"role": "user", "id": str(uuid.uuid4()), "content": prompt}],
                "tools": [],
                "context": [],
            },
        })
    elif protocol.startswith("healthcheck"):
        s.update({"root": {"protocol": protocol}})
    else:
        s.update({"root": {"protocol": protocol, "user_prompt": prompt}})
    return s


async def _run_one_request(
    consumer: GatewayConsumer, setup_id: str, prompt: str, protocol: str = "agui_stream",
) -> RequestResult:
    """One end-to-end task via the dial-back consumer.

    ``GatewayConsumer.call`` filters out ``stream.start`` and stops on
    ``stream.end``. We measure latency to the first and second yielded
    domain Struct. ``stream.error`` envelopes flow through the iterator
    and are decoded via :meth:`GatewayConsumer.stream_error`.
    """
    query = _build_input(prompt, protocol)
    mission_id = _next_mission()

    t0 = time.monotonic()
    first_msg_t = 0.0
    second_msg_t = 0.0
    msg_count = 0

    try:
        async for data in consumer.call(query, setup_id=setup_id, mission_id=mission_id):
            msg_count += 1
            now = (time.monotonic() - t0) * 1000
            if msg_count == 1:
                first_msg_t = now
            elif msg_count == 2:
                second_msg_t = now

            err = GatewayConsumer.stream_error(data)
            if err is not None:
                code, message = err
                return RequestResult(
                    False, (time.monotonic() - t0) * 1000,
                    first_msg_t, second_msg_t, msg_count,
                    error=(code or "STREAM_ERROR", message[:200]),
                )

        total = (time.monotonic() - t0) * 1000
        if msg_count < 1:
            return RequestResult(
                False, total, first_msg_t, second_msg_t, msg_count,
                error=("NO_MESSAGES", "no_messages_received"),
            )
        return RequestResult(True, total, first_msg_t, second_msg_t, msg_count)

    except StartStreamRpcError as e:
        total = (time.monotonic() - t0) * 1000
        return RequestResult(
            False, total, first_msg_t, second_msg_t, msg_count,
            error=("STARTSTREAM_RPC", f"[{e.code.name}] {e.details}"[:200]),
        )
    except StartStreamRejected as e:
        total = (time.monotonic() - t0) * 1000
        return RequestResult(
            False, total, first_msg_t, second_msg_t, msg_count,
            error=("STARTSTREAM_REJECTED", str(e)[:200]),
        )
    except grpc.aio.AioRpcError as e:
        total = (time.monotonic() - t0) * 1000
        return RequestResult(
            False, total, first_msg_t, second_msg_t, msg_count,
            error=("GRPC_AIO", f"[{e.code().name}] {e.details() or ''}"[:200]),
        )
    except Exception as e:  # noqa: BLE001 — bench surface
        total = (time.monotonic() - t0) * 1000
        return RequestResult(
            False, total, first_msg_t, second_msg_t, msg_count,
            error=("BENCH", str(e)[:200]),
        )


# ── Wave mode ─────────────────────────────────────────────────────────────────


async def _run_attempt_wave(
    consumer: GatewayConsumer, concurrency: int, attempt: int, duration_s: float,
    setup_id: str, prompt: str, protocol: str = "agui_stream",
) -> AttemptResult:
    """Fire N requests, wait for all, repeat until duration expires."""
    result = AttemptResult(concurrency=concurrency, attempt=attempt, mode="wave")
    t_start = time.monotonic()
    t_end = t_start + duration_s

    try:
        while time.monotonic() < t_end:
            tasks = [
                asyncio.create_task(_run_one_request(consumer, setup_id, prompt, protocol))
                for _ in range(concurrency)
            ]
            done = await asyncio.gather(*tasks, return_exceptions=True)
            for r in done:
                if isinstance(r, RequestResult):
                    if not r.ok:
                        msg = f"Request failed: {r.error}"
                        raise RuntimeError(msg)
                    result.results.append(r)
                else:
                    msg = f"Request exception: {r!s}"
                    raise RuntimeError(msg)
    finally:
        result.duration_s = time.monotonic() - t_start

    return result


# ── Sustained mode ────────────────────────────────────────────────────────────


async def _run_attempt_sustained(
    consumer: GatewayConsumer, concurrency: int, attempt: int, duration_s: float,
    setup_id: str, prompt: str, protocol: str = "agui_stream",
) -> AttemptResult:
    """Keep exactly N requests in flight via semaphore. Immediate replacement."""
    result = AttemptResult(concurrency=concurrency, attempt=attempt, mode="sustained")
    sem = asyncio.Semaphore(concurrency)
    t_start = time.monotonic()
    t_end = t_start + duration_s
    pending: set[asyncio.Task] = set()
    stop = False

    async def _worker() -> RequestResult:
        async with sem:
            if stop:
                return RequestResult(False, 0, error=("STOPPED", "stopped"))
            return await _run_one_request(consumer, setup_id, prompt, protocol)

    try:
        for _ in range(concurrency):
            if time.monotonic() >= t_end:
                break
            t = asyncio.create_task(_worker())
            pending.add(t)
            t.add_done_callback(pending.discard)

        while time.monotonic() < t_end:
            if not pending:
                break
            done, _ = await asyncio.wait(pending, timeout=0.1, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                r = t.result()
                if isinstance(r, RequestResult):
                    if not r.ok:
                        msg = f"Request failed: {r.error}"
                        raise RuntimeError(msg)
                    result.results.append(r)
                else:
                    msg = f"Request exception: {r!s}"
                    raise RuntimeError(msg)

                if time.monotonic() < t_end:
                    new_t = asyncio.create_task(_worker())
                    pending.add(new_t)
                    new_t.add_done_callback(pending.discard)

        stop = True
        if pending:
            done_final = await asyncio.gather(*pending, return_exceptions=True)
            for r in done_final:
                if isinstance(r, RequestResult):
                    if not r.ok and r.error_code != "STOPPED":
                        msg = f"Request failed during drain: {r.error}"
                        raise RuntimeError(msg)
                    result.results.append(r)
                elif isinstance(r, Exception):
                    msg = f"Request exception during drain: {r!s}"
                    raise RuntimeError(msg)

    finally:
        result.duration_s = time.monotonic() - t_start

    result.results = [r for r in result.results if r.error_code != "STOPPED"]
    return result


# ── Main loop ─────────────────────────────────────────────────────────────────


async def run_sweep(
    consumer: GatewayConsumer,
    concurrency_levels: list[int],
    attempts: int,
    duration_s: int,
    setup_id: str,
    prompt: str,
    sustained: bool,
    protocol: str = "agui_stream",
    abort_on_failure: bool = True,
) -> list[LevelSummary]:
    """Run all concurrency levels progressively against the shared consumer."""
    summaries: list[LevelSummary] = []
    mode = "sustained" if sustained else "wave"
    run_fn = _run_attempt_sustained if sustained else _run_attempt_wave

    for c in concurrency_levels:
        summary = LevelSummary(concurrency=c)
        print(f"\n{'=' * 60}")
        print(f"  Concurrency: {c}  ({attempts} attempts x {duration_s}s, {mode})")
        print(f"{'=' * 60}")

        level_failed = False
        for attempt in range(1, attempts + 1):
            print(f"  Attempt {attempt}/{attempts}...", end=" ", flush=True)
            ar = await run_fn(consumer, c, attempt, duration_s, setup_id, prompt, protocol)
            summary.attempts.append(ar)

            lats = ar.ok_latencies
            ttfr = ar.ok_ttfr
            tt2r = ar.ok_tt2r
            p50 = ar.percentile(lats, 50)
            p95 = ar.percentile(lats, 95)
            p99 = ar.percentile(lats, 99)
            ttfr_p50 = ar.percentile(ttfr, 50)
            tt2r_p50 = ar.percentile(tt2r, 50)
            print(
                f"OK={ar.ok_count} ERR={ar.err_count} "
                f"P50={p50:.0f}ms P95={p95:.0f}ms P99={p99:.0f}ms "
                f"TT1R={ttfr_p50:.0f}ms TT2R={tt2r_p50:.0f}ms "
                f"RPS={ar.throughput:.1f}",
                flush=True,
            )

            if ar.err_count > 0:
                bad = [r for r in ar.results if not r.ok][:3]
                for r in bad:
                    print(f"    error: {r.error}")
                level_failed = True

            if attempt < attempts:
                await asyncio.sleep(3)

        summaries.append(summary)

        if level_failed and abort_on_failure:
            print(
                f"\n  ✗ ABORT: concurrency={c} produced errors. "
                f"Skipping remaining levels (use --no-abort to override).",
                flush=True,
            )
            break

        if c != concurrency_levels[-1]:
            await asyncio.sleep(5)

    return summaries


def generate_report(
    summaries: list[LevelSummary],
    host: str,
    port: int,
    attempts: int,
    duration_s: int,
    concurrency_levels: list[int],
    mode: str,
) -> str:
    """Generate Markdown report."""
    lines = [
        "# Benchmark Sweep Report",
        "",
        f"**Date**: {time.strftime('%Y-%m-%d %H:%M:%S')}",
        f"**Target**: {host}:{port} (Gateway dial-back consumer)",
        f"**Mode**: {mode}",
        f"**Attempts**: {attempts} x {duration_s}s per concurrency level",
        f"**Concurrency levels**: {', '.join(str(c) for c in concurrency_levels)}",
        f"**STREAM_READ_BLOCK_MS**: {os.environ.get('DIGITALKIN_STREAM_READ_BLOCK_MS', '100')} (default)",
        "",
        "## Summary Table",
        "",
        "| Concurrency | Requests | Errors | Error% | P50 (ms) | P95 (ms) | P99 (ms) | TT2R P50 | Avg RPS | Min (ms) | Max (ms) |",
        "|------------|----------|--------|--------|----------|----------|----------|----------|---------|----------|----------|",
    ]

    for s in summaries:
        lats = s.all_ok_latencies
        tt2r_lats = sorted(r.tt2r_ms for a in s.attempts for r in a.results if r.ok and r.tt2r_ms > 0)
        total = s.total_ok + s.total_err
        err_pct = (s.total_err / total * 100) if total > 0 else 0
        min_l = min(lats) if lats else 0
        max_l = max(lats) if lats else 0
        tt2r_p50 = s.attempts[0].percentile(tt2r_lats, 50) if tt2r_lats else 0
        lines.append(
            f"| {s.concurrency} | {s.total_ok} | {s.total_err} | {err_pct:.1f}% "
            f"| {s.p(50):.1f} | {s.p(95):.1f} | {s.p(99):.1f} | {tt2r_p50:.1f} "
            f"| {s.avg_throughput:.1f} | {min_l:.1f} | {max_l:.1f} |"
        )

    lines.extend(["", "## Per-Attempt Breakdown", ""])
    lines.extend((
        "| Concurrency | Attempt | OK | ERR | P50 | P95 | P99 | RPS |",
        "|------------|---------|-----|-----|------|------|------|------|",
    ))
    for s in summaries:
        for a in s.attempts:
            lats = a.ok_latencies
            p50 = a.percentile(lats, 50)
            p95 = a.percentile(lats, 95)
            p99 = a.percentile(lats, 99)
            lines.append(
                f"| {a.concurrency} | {a.attempt} | {a.ok_count} | {a.err_count} "
                f"| {p50:.1f} | {p95:.1f} | {p99:.1f} | {a.throughput:.1f} |"
            )

    # Aggregate errors by stable code (StreamErrorCode + bench buckets);
    # within each code, aggregate the most common message snippet.
    errors: dict[str, dict[str, int]] = {}
    for s in summaries:
        for a in s.attempts:
            for r in a.results:
                if not r.ok:
                    code = r.error_code or "UNKNOWN"
                    msg = (r.error_message or "(no message)")[:100]
                    bucket = errors.setdefault(code, {})
                    bucket[msg] = bucket.get(msg, 0) + 1
    if errors:
        lines.extend(["", "## Errors", ""])
        lines.extend(("| Code | Count | Top message |", "|------|-------|-------------|"))
        ranked = sorted(errors.items(), key=lambda x: -sum(x[1].values()))
        for code, msgs in ranked:
            total = sum(msgs.values())
            top_msg, _ = max(msgs.items(), key=lambda x: x[1])
            lines.append(f"| `{code}` | {total} | `{top_msg}` |")

    return "\n".join(lines) + "\n"


def generate_graphs(summaries: list[LevelSummary], output_dir: str, mode: str) -> list[str]:
    """Generate PNG graphs with matplotlib."""
    try:
        import matplotlib as mpl

        mpl.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    paths = []
    concurrencies = [s.concurrency for s in summaries]
    p50s = [s.p(50) for s in summaries]
    p95s = [s.p(95) for s in summaries]
    p99s = [s.p(99) for s in summaries]
    throughputs = [s.avg_throughput for s in summaries]
    error_rates = [
        (s.total_err / (s.total_ok + s.total_err) * 100) if (s.total_ok + s.total_err) > 0 else 0 for s in summaries
    ]

    ts = time.strftime("%Y%m%d_%H%M%S")
    title = f"Gateway Dial-back Benchmark ({mode})"

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle(title, fontsize=14, fontweight="bold")

    ax = axes[0][0]
    ax.plot(concurrencies, p50s, "o-", label="P50", color="#2196F3", linewidth=2)
    ax.plot(concurrencies, p95s, "s-", label="P95", color="#FF9800", linewidth=2)
    ax.plot(concurrencies, p99s, "^-", label="P99", color="#F44336", linewidth=2)
    ax.set_xlabel("Concurrency")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Latency vs Concurrency")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_xscale("log")

    ax = axes[0][1]
    ax.bar(range(len(concurrencies)), throughputs, color="#4CAF50", alpha=0.8)
    ax.set_xticks(range(len(concurrencies)))
    ax.set_xticklabels([str(c) for c in concurrencies])
    ax.set_xlabel("Concurrency")
    ax.set_ylabel("Requests/sec")
    ax.set_title("Throughput")
    ax.grid(True, alpha=0.3, axis="y")

    ax = axes[1][0]
    ax.bar(range(len(concurrencies)), error_rates, color="#F44336", alpha=0.8)
    ax.set_xticks(range(len(concurrencies)))
    ax.set_xticklabels([str(c) for c in concurrencies])
    ax.set_xlabel("Concurrency")
    ax.set_ylabel("Error Rate (%)")
    ax.set_title("Error Rate")
    ax.grid(True, alpha=0.3, axis="y")

    ax = axes[1][1]
    for s in summaries:
        attempt_p50s = [a.percentile(a.ok_latencies, 50) for a in s.attempts]
        ax.plot(range(1, len(attempt_p50s) + 1), attempt_p50s, "o-", label=f"c={s.concurrency}")
    ax.set_xlabel("Attempt")
    ax.set_ylabel("P50 Latency (ms)")
    ax.set_title("P50 Stability Across Attempts")
    ax.legend(fontsize=7, ncol=2)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    path1 = os.path.join(output_dir, f"sweep_overview_{mode}_{ts}.png")
    fig.savefig(path1, dpi=150)
    plt.close(fig)
    paths.append(path1)
    print(f"  Graph: {path1}")

    fig2, ax2 = plt.subplots(figsize=(12, 6))
    box_data = [s.all_ok_latencies for s in summaries if s.all_ok_latencies]
    box_labels = [str(s.concurrency) for s in summaries if s.all_ok_latencies]
    if box_data:
        bp = ax2.boxplot(box_data, tick_labels=box_labels, patch_artist=True, showfliers=False)
        colors = plt.cm.Blues([0.3 + 0.7 * i / len(box_data) for i in range(len(box_data))])
        for patch, color in zip(bp["boxes"], colors):
            patch.set_facecolor(color)
        ax2.set_xlabel("Concurrency")
        ax2.set_ylabel("Latency (ms)")
        ax2.set_title(f"Latency Distribution — {mode} (box plot, outliers hidden)")
        ax2.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    path2 = os.path.join(output_dir, f"sweep_distribution_{mode}_{ts}.png")
    fig2.savefig(path2, dpi=150)
    plt.close(fig2)
    paths.append(path2)
    print(f"  Graph: {path2}")

    return paths


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    p = argparse.ArgumentParser(
        description="Gateway dial-back benchmark sweep with wave and sustained modes.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--wave", action="store_true",
        help="Wave mode: fire N requests, wait for all, repeat (default: sustained — keeps N in flight via semaphore).",
    )
    p.add_argument(
        "--no-abort", action="store_true",
        help="Continue sweep even if a level errors out (default: abort on first failing level).",
    )
    p.add_argument(
        "-c", "--concurrency", default=None,
        help=f"Comma-separated concurrency levels (default: {','.join(str(c) for c in DEFAULT_CONCURRENCY_LEVELS)})",
    )
    p.add_argument("-a", "--attempts", type=int, default=DEFAULT_ATTEMPTS, help=f"Attempts per level (default: {DEFAULT_ATTEMPTS})")
    p.add_argument("-d", "--duration", type=int, default=DEFAULT_DURATION_S, help=f"Seconds per attempt (default: {DEFAULT_DURATION_S})")
    p.add_argument("--host", default=DEFAULT_HOST, help=f"Gateway host (default: {DEFAULT_HOST})")
    p.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Gateway port (default: {DEFAULT_PORT})")
    p.add_argument("--setup-id", default=DEFAULT_SETUP_ID, help=f"Setup ID (default: {DEFAULT_SETUP_ID})")
    p.add_argument("--prompt", default=DEFAULT_PROMPT, help=f"Prompt text (default: {DEFAULT_PROMPT})")
    p.add_argument("--protocol", default="agui_stream", help="Protocol: agui_stream, healthcheck_ping, healthcheck_services (default: agui_stream)")
    p.add_argument("-o", "--output-dir", default=DEFAULT_OUTPUT_DIR, help=f"Output directory (default: {DEFAULT_OUTPUT_DIR})")
    p.add_argument(
        "--consumer-listen", default=DEFAULT_CONSUMER_LISTEN,
        help=f"Bind interface for the dial-back server (default: {DEFAULT_CONSUMER_LISTEN}).",
    )
    p.add_argument(
        "--consumer-port", type=int, default=DEFAULT_CONSUMER_PORT,
        help=f"Bind port for the dial-back server (default: {DEFAULT_CONSUMER_PORT}).",
    )
    p.add_argument(
        "--consumer-advertise", default=DEFAULT_CONSUMER_ADVERTISE,
        help=f"host:port the gateway will dial — must be reachable from the module's gateway "
        f"(default: {DEFAULT_CONSUMER_ADVERTISE}).",
    )
    return p.parse_args()


async def main() -> None:
    args = parse_args()
    concurrency_levels = [int(x) for x in args.concurrency.split(",")] if args.concurrency else DEFAULT_CONCURRENCY_LEVELS
    sustained = not args.wave
    mode = "sustained" if sustained else "wave"
    output_dir = args.output_dir

    os.makedirs(output_dir, exist_ok=True)

    total_time = len(concurrency_levels) * args.attempts * (args.duration + 5)
    print(f"\n  Gateway Dial-back Benchmark Sweep")
    print(f"  Target gateway: {args.host}:{args.port}")
    print(f"  Dial-back listen: {args.consumer_listen}:{args.consumer_port}")
    print(f"  Dial-back advertised: {args.consumer_advertise}")
    print(f"  Mode: {mode}")
    print(f"  Levels: {concurrency_levels}")
    print(f"  Attempts: {args.attempts} x {args.duration}s each")
    print(f"  Estimated time: ~{total_time // 60} min")

    consumer = GatewayConsumer.standalone(
        ConsumerConfig(
            gateway_address=f"{args.host}:{args.port}",
            listen=args.consumer_listen,
            port=args.consumer_port,
            advertise_address=args.consumer_advertise,
        ),
    )
    async with consumer:
        cold_result = await _run_one_request(consumer, args.setup_id, args.prompt, args.protocol)
        if cold_result.ok:
            print(f"\n  Cold request: {cold_result.latency_ms:.0f}ms (cache warming)")
        else:
            print(f"\n  Cold request FAILED: {cold_result.error}")

        await asyncio.sleep(1)

        summaries = await run_sweep(
            consumer, concurrency_levels, args.attempts, args.duration,
            args.setup_id, args.prompt,
            sustained, args.protocol, abort_on_failure=not args.no_abort,
        )

    if cold_result.ok:
        all_max = max(
            (max(r.latency_ms for r in a.results if r.ok) for s in summaries for a in s.attempts if a.ok_count > 0),
            default=0,
        )
        if all_max > cold_result.latency_ms * 1.5:
            print(f"\n  ⚠ WARNING: max warm latency ({all_max:.0f}ms) > 1.5x cold request ({cold_result.latency_ms:.0f}ms)")
            print(f"    This suggests a regression beyond cache warming.")
        else:
            print(f"\n  ✓ Max warm latency ({all_max:.0f}ms) within expected range of cold request ({cold_result.latency_ms:.0f}ms)")

    report = generate_report(summaries, args.host, args.port, args.attempts, args.duration, concurrency_levels, mode)
    report_path = os.path.join(output_dir, f"sweep_report_{mode}.md")
    Path(report_path).write_text(report, encoding="utf-8")
    print(f"\n  Report: {report_path}")

    generate_graphs(summaries, output_dir, mode)

    raw = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "mode": mode,
        "config": {
            "host": args.host,
            "port": args.port,
            "setup_id": args.setup_id,
            "consumer_listen": args.consumer_listen,
            "consumer_port": args.consumer_port,
            "consumer_advertise": args.consumer_advertise,
            "concurrency_levels": concurrency_levels,
            "attempts": args.attempts,
            "duration_s": args.duration,
        },
        "levels": [
            {
                "concurrency": s.concurrency,
                "total_ok": s.total_ok,
                "total_err": s.total_err,
                "p50": round(s.p(50), 2),
                "p95": round(s.p(95), 2),
                "p99": round(s.p(99), 2),
                "avg_rps": round(s.avg_throughput, 2),
                "attempts": [
                    {
                        "attempt": a.attempt,
                        "ok": a.ok_count,
                        "err": a.err_count,
                        "p50": round(a.percentile(a.ok_latencies, 50), 2),
                        "p95": round(a.percentile(a.ok_latencies, 95), 2),
                        "p99": round(a.percentile(a.ok_latencies, 99), 2),
                        "rps": round(a.throughput, 2),
                        "duration_s": round(a.duration_s, 2),
                    }
                    for a in s.attempts
                ],
            }
            for s in summaries
        ],
    }
    json_path = os.path.join(output_dir, f"sweep_raw_{mode}.json")
    Path(json_path).write_text(json.dumps(raw, indent=2), encoding="utf-8")
    print(f"  Raw JSON: {json_path}")

    print(f"\n{'=' * 105}")
    print(
        f"  {'C':>5} | {'OK':>6} | {'ERR':>5} | {'P50':>8} | {'P95':>8} | {'P99':>8} "
        f"| {'TT1R':>7} | {'TT2R':>7} | {'RPS':>7}"
    )
    print(
        f"  {'-' * 5}-+-{'-' * 6}-+-{'-' * 5}-+-{'-' * 8}-+-{'-' * 8}-+-{'-' * 8}"
        f"-+-{'-' * 7}-+-{'-' * 7}-+-{'-' * 7}"
    )
    for s in summaries:
        ttfr_lats = sorted(r.ttfr_ms for a in s.attempts for r in a.results if r.ok and r.ttfr_ms > 0)
        tt2r_lats = sorted(r.tt2r_ms for a in s.attempts for r in a.results if r.ok and r.tt2r_ms > 0)
        ttfr_p50 = s.attempts[0].percentile(ttfr_lats, 50) if ttfr_lats else 0
        tt2r_p50 = s.attempts[0].percentile(tt2r_lats, 50) if tt2r_lats else 0
        print(
            f"  {s.concurrency:>5} | {s.total_ok:>6} | {s.total_err:>5} "
            f"| {s.p(50):>7.1f} | {s.p(95):>7.1f} | {s.p(99):>7.1f} "
            f"| {ttfr_p50:>6.1f} | {tt2r_p50:>6.1f} | {s.avg_throughput:>6.1f}"
        )
    print(f"{'=' * 105}\n")


if __name__ == "__main__":
    asyncio.run(main())
