"""Scalability benchmark — config-driven, three-phase load test with LLM-readable report.

Based on digitalkin-sandbox/scripts/stress_test_grpc.py patterns.
Reads a JSON config, runs each scenario through three phases:
  1. Sequential — one request at a time (baseline latency)
  2. Concurrent — sustained concurrent workers
  3. Burst — all requests fired simultaneously

Produces a Markdown report optimized for LLM consumption.

Usage:
    uv run python scripts/scalability_bench.py scripts/bench_config_single.json
    uv run python scripts/scalability_bench.py scripts/bench_config.json --report report.md
    uv run python scripts/scalability_bench.py scripts/bench_config_single.json --only single_ping
"""

import argparse
import asyncio
import contextlib
import datetime
import json
import operator
import os
import pathlib
import random
import statistics
import time
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any

import grpc
import psutil
from agentic_mesh_protocol.module.v1 import lifecycle_pb2, module_service_pb2_grpc
from google.protobuf import json_format

# ── ANSI ──────────────────────────────────────────────────────────────────────

RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
CYAN = "\033[36m"
BOLD = "\033[1m"
DIM = "\033[2m"
RESET = "\033[0m"
_BAR = "\u2500" * 65
_DBAR = "\u2550" * 65

GRPC_OPTIONS = [
    ("grpc.max_receive_message_length", 100 * 1024 * 1024),
    ("grpc.max_send_message_length", 100 * 1024 * 1024),
    ("grpc.keepalive_time_ms", 30_000),
    ("grpc.keepalive_timeout_ms", 10_000),
    ("grpc.keepalive_permit_without_calls", True),
]

PHASE_TIMEOUT_S = 30 * 60  # 30 minutes per phase
REQUEST_TIMEOUT_S = 120  # per-request gRPC timeout
SLEEP_BETWEEN_REQUESTS_S = 0.5
SLEEP_BETWEEN_PHASES_S = 5.0
SLEEP_BETWEEN_BURSTS_S = 3.0
SLEEP_BETWEEN_SCENARIOS_S = 10.0

# ── Mission ID pool ──────────────────────────────────────────────────────────

MISSION_IDS = [
    f"missions:bench_{i:04d}_{''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=16))}"
    for i in range(2000)
]
_mission_iter: Iterator[str] = iter(MISSION_IDS)


def _gen_mission_id() -> str:
    try:
        return next(_mission_iter)
    except StopIteration:
        suffix = "".join(random.choices("abcdefghijklmnopqrstuvwxyz0123456789", k=20))
        return f"missions:{suffix}"


# ── Result types ─────────────────────────────────────────────────────────────


@dataclass
class Result:
    """Single request result."""

    status: str  # ok | error
    latency_ms: float
    messages: int = 0
    first_msg_ms: float = 0.0
    error: str = ""
    grpc_code: str = ""
    grpc_details_full: str = ""
    mission_id: str = ""
    t_offset_ms: float = 0.0


@dataclass
class PhaseResult:
    """Results for one phase (sequential/concurrent/burst)."""

    phase: str
    results: list[Result] = field(default_factory=list)
    duration_s: float = 0.0

    @property
    def ok_count(self) -> int:
        return sum(1 for r in self.results if r.status == "ok")

    @property
    def err_count(self) -> int:
        return sum(1 for r in self.results if r.status != "ok")

    @property
    def ok_latencies(self) -> list[float]:
        return [r.latency_ms for r in self.results if r.status == "ok"]

    @property
    def ok_first_msg(self) -> list[float]:
        return [r.first_msg_ms for r in self.results if r.status == "ok" and r.first_msg_ms > 0]


@dataclass
class ScenarioReport:
    """Full report for one scenario across all phases."""

    label: str
    config: dict[str, Any]
    phases: list[PhaseResult] = field(default_factory=list)
    memory_start_bytes: int = 0
    memory_peak_bytes: int = 0
    cpu_start: float = 0.0
    cpu_end: float = 0.0
    load_avg_start: list[float] = field(default_factory=list)
    load_avg_end: list[float] = field(default_factory=list)

    @property
    def all_results(self) -> list[Result]:
        return [r for p in self.phases for r in p.results]

    @property
    def all_errors(self) -> list[Result]:
        return [r for r in self.all_results if r.status != "ok"]

    @property
    def error_catalog(self) -> dict[str, dict[str, Any]]:
        """Deduplicated errors: message -> {count, codes, phases}."""
        catalog: dict[str, dict[str, Any]] = {}
        for phase in self.phases:
            for r in phase.results:
                if r.status != "ok":
                    key = r.grpc_details_full or r.error or "(no message)"
                    if key not in catalog:
                        catalog[key] = {"count": 0, "codes": [], "phases": []}
                    catalog[key]["count"] += 1
                    if r.grpc_code and r.grpc_code not in catalog[key]["codes"]:
                        catalog[key]["codes"].append(r.grpc_code)
                    if phase.phase not in catalog[key]["phases"]:
                        catalog[key]["phases"].append(phase.phase)
        return catalog


# ── Env var scoping ──────────────────────────────────────────────────────────


class ScopedEnv:
    """Set env vars, restore on exit."""

    def __init__(self, env: dict[str, str]) -> None:
        self._env = env
        self._saved: dict[str, str | None] = {}

    def __enter__(self) -> None:
        for key, val in self._env.items():
            self._saved[key] = os.environ.get(key)
            os.environ[key] = val

    def __exit__(self, *_: object) -> None:
        for key, original in self._saved.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original


# ── gRPC helpers ─────────────────────────────────────────────────────────────


def _make_request(setup_id: str, mission_id: str, input_data: dict) -> lifecycle_pb2.StartModuleRequest:
    """Build a StartModuleRequest from input dict."""
    req = lifecycle_pb2.StartModuleRequest()
    json_format.ParseDict(input_data, req.input)
    req.setup_id = setup_id
    req.mission_id = mission_id
    return req


async def _do_start_module(
    address: str,
    request: lifecycle_pb2.StartModuleRequest,
    t_global: float = 0.0,
    mission_id: str = "",
    timeout: float = REQUEST_TIMEOUT_S,
) -> Result:
    """Call StartModule (server-streaming), return Result."""
    channel = grpc.aio.insecure_channel(address, options=GRPC_OPTIONS)
    t0 = time.monotonic()
    first_msg_t = 0.0
    count = 0
    all_ok = True

    try:
        stub = module_service_pb2_grpc.ModuleServiceStub(channel)
        stream = stub.StartModule(request, timeout=timeout)

        async for resp in stream:
            count += 1
            ms = (time.monotonic() - t0) * 1000
            if count == 1:
                first_msg_t = ms
            if not resp.success:
                all_ok = False

        total = (time.monotonic() - t0) * 1000
        t_off = (t0 - t_global) * 1000 if t_global else 0.0

        if count == 0:
            return Result("error", total, error="empty stream", mission_id=mission_id, t_offset_ms=t_off)
        if not all_ok:
            return Result(
                "error", total, count, first_msg_t, error="success=false", mission_id=mission_id, t_offset_ms=t_off
            )
        return Result("ok", total, count, first_msg_t, mission_id=mission_id, t_offset_ms=t_off)

    except grpc.aio.AioRpcError as e:
        total = (time.monotonic() - t0) * 1000
        t_off = (t0 - t_global) * 1000 if t_global else 0.0
        details_full = str(e.details() or "")
        return Result(
            "error",
            total,
            count,
            first_msg_t,
            error=details_full[:200],
            grpc_code=e.code().name,
            mission_id=mission_id,
            grpc_details_full=details_full,
            t_offset_ms=t_off,
        )
    except Exception as e:
        total = (time.monotonic() - t0) * 1000
        t_off = (t0 - t_global) * 1000 if t_global else 0.0
        return Result("error", total, count, first_msg_t, error=str(e)[:200], mission_id=mission_id, t_offset_ms=t_off)
    finally:
        await channel.close()


# ── Three-phase runner ───────────────────────────────────────────────────────


def _log(msg: str, *, indent: int = 2) -> None:
    " " * indent


def _elapsed(t0: float) -> str:
    s = int(time.monotonic() - t0)
    m, s = divmod(s, 60)
    return f"{m}:{s:02d}" if m else f"{s}s"


def _live(msg: str) -> None:
    """Overwrite the current terminal line."""
    with contextlib.suppress(OSError):
        os.get_terminal_size().columns


def _live_end() -> None:
    """Finish live line — move to next line."""


async def _run_sequential(
    address: str, setup_id: str, input_data: dict, count: int, t_global: float, req_timeout: float = REQUEST_TIMEOUT_S
) -> PhaseResult:
    """Phase 1: one request at a time with sleep between each."""
    phase = PhaseResult(phase="sequential")
    t0 = time.monotonic()
    deadline = t0 + PHASE_TIMEOUT_S
    ok_n = 0
    err_n = 0
    last_err = ""
    for i in range(count):
        if time.monotonic() >= deadline:
            _live_end()
            _log(f"{YELLOW}Phase timeout reached after {i} requests{RESET}", indent=4)
            break
        mid = _gen_mission_id()
        req = _make_request(setup_id, mid, input_data)
        result = await _do_start_module(address, req, t_global=t_global, mission_id=mid, timeout=req_timeout)
        phase.results.append(result)
        if result.status == "ok":
            ok_n += 1
        else:
            err_n += 1
            code = f"[{result.grpc_code}] " if result.grpc_code else ""
            last_err = f"{code}{result.error[:60]}"

        err_part = f"  {RED}{err_n} err{RESET}" if err_n else ""
        last_err_part = f"  {DIM}last: {last_err}{RESET}" if err_n else ""
        _live(
            f"    [{_elapsed(t0)}] {i + 1}/{count}  "
            f"{GREEN}{ok_n} ok{RESET}{err_part}  "
            f"{result.latency_ms:.0f}ms{last_err_part}"
        )
        if i < count - 1:
            await asyncio.sleep(SLEEP_BETWEEN_REQUESTS_S)
    _live_end()
    phase.duration_s = time.monotonic() - t0
    return phase


async def _run_concurrent(
    address: str,
    setup_id: str,
    input_data: dict,
    concurrency: int,
    total: int,
    t_global: float,
    req_timeout: float = REQUEST_TIMEOUT_S,
) -> PhaseResult:
    """Phase 2: worker-queue sustained concurrency with sleep between requests."""
    phase = PhaseResult(phase="concurrent")
    queue: asyncio.Queue = asyncio.Queue()
    completed = 0
    ok_n = 0
    err_n = 0
    last_err = ""
    for i in range(total):
        queue.put_nowait(i)

    t0 = time.monotonic()
    deadline = t0 + PHASE_TIMEOUT_S

    async def worker(wid: int) -> None:
        nonlocal completed, ok_n, err_n, last_err
        while True:
            if time.monotonic() >= deadline:
                break
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            mid = _gen_mission_id()
            req = _make_request(setup_id, mid, input_data)
            result = await _do_start_module(address, req, t_global=t_global, mission_id=mid, timeout=req_timeout)
            phase.results.append(result)
            completed += 1
            if result.status == "ok":
                ok_n += 1
            else:
                err_n += 1
                code = f"[{result.grpc_code}] " if result.grpc_code else ""
                last_err = f"{code}{result.error[:60]}"

            err_part = f"  {RED}{err_n} err{RESET}" if err_n else ""
            last_err_part = f"  {DIM}last: {last_err}{RESET}" if err_n else ""
            _live(
                f"    [{_elapsed(t0)}] {completed}/{total}  "
                f"{GREEN}{ok_n} ok{RESET}{err_part}  "
                f"{result.latency_ms:.0f}ms{last_err_part}"
            )
            queue.task_done()
            await asyncio.sleep(SLEEP_BETWEEN_REQUESTS_S)

    tasks = [asyncio.create_task(worker(w)) for w in range(concurrency)]
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(queue.join(), timeout=PHASE_TIMEOUT_S)
    _live_end()
    if queue.qsize() > 0:
        _log(f"{YELLOW}Phase timeout — {completed}/{total} completed{RESET}", indent=4)
    for t in tasks:
        t.cancel()
    phase.duration_s = time.monotonic() - t0
    return phase


async def _run_burst(
    address: str,
    setup_id: str,
    input_data: dict,
    count: int,
    repeats: int,
    t_global: float,
    req_timeout: float = REQUEST_TIMEOUT_S,
) -> PhaseResult:
    """Phase 3: fire all at once, repeated `repeats` times with sleep between."""
    phase = PhaseResult(phase="burst")
    total_ok = 0
    total_err = 0

    async def _one() -> Result:
        mid = _gen_mission_id()
        req = _make_request(setup_id, mid, input_data)
        return await _do_start_module(address, req, t_global=t_global, mission_id=mid, timeout=req_timeout)

    t0 = time.monotonic()
    deadline = t0 + PHASE_TIMEOUT_S

    for wave in range(repeats):
        if time.monotonic() >= deadline:
            _live_end()
            _log(f"{YELLOW}Phase timeout after {wave} waves{RESET}", indent=4)
            break

        wave_t0 = time.monotonic()

        async def _tick() -> None:
            while True:
                await asyncio.sleep(2)
                _live(f"    [{_elapsed(t0)}] Wave {wave + 1}/{repeats} — waiting... ({_elapsed(wave_t0)} elapsed)")

        ticker = asyncio.create_task(_tick())
        _live(f"    [{_elapsed(t0)}] Wave {wave + 1}/{repeats} — firing {count} requests...")
        try:
            raw = await asyncio.gather(*[_one() for _ in range(count)], return_exceptions=True)
        finally:
            ticker.cancel()
        wave_ok = 0
        wave_err = 0
        wave_lats: list[float] = []
        for r in raw:
            if isinstance(r, Result):
                phase.results.append(r)
                if r.status == "ok":
                    wave_ok += 1
                    wave_lats.append(r.latency_ms)
                else:
                    wave_err += 1
            else:
                phase.results.append(Result("error", 0, error=str(r)))
                wave_err += 1

        total_ok += wave_ok
        total_err += wave_err
        err_part = f"  {RED}{total_err} err{RESET}" if total_err else ""
        lat_str = f"  avg={statistics.mean(wave_lats):.0f}ms" if wave_lats else ""
        _live(
            f"    [{_elapsed(t0)}] Wave {wave + 1}/{repeats}: "
            f"{GREEN}{wave_ok}{RESET}/{count}{lat_str}  "
            f"total: {GREEN}{total_ok} ok{RESET}{err_part}"
        )
        _live_end()

        if wave < repeats - 1:
            await asyncio.sleep(SLEEP_BETWEEN_BURSTS_S)

    phase.duration_s = time.monotonic() - t0
    return phase


async def run_scenario(scenario: dict[str, Any]) -> ScenarioReport:
    """Run all three phases for a scenario."""
    label = scenario["label"]
    address = scenario["target"]
    setup_id = scenario["setup_id"]
    input_data = scenario["input"]
    env_overrides = scenario.get("env", {})

    seq_count = scenario.get("sequential", 1000)
    conc_levels = scenario.get("concurrency", 10)
    conc_total = scenario.get("requests", 100)
    burst_levels = scenario.get("burst_size", 50)
    burst_repeats = scenario.get("burst_repeats", 10)
    req_timeout = scenario.get("request_timeout", REQUEST_TIMEOUT_S)

    # Normalize to lists for multi-level runs
    if not isinstance(conc_levels, list):
        conc_levels = [conc_levels]
    if not isinstance(burst_levels, list):
        burst_levels = [burst_levels]

    report = ScenarioReport(label=label, config=scenario)

    with ScopedEnv(env_overrides):
        proc = psutil.Process()
        report.memory_start_bytes = proc.memory_info().rss
        report.cpu_start = psutil.cpu_percent(interval=None)
        report.load_avg_start = list(os.getloadavg())

        t_global = time.monotonic()
        phase_num = 0

        # Phase 1: Sequential
        phase_num += 1
        _log(
            f"\n{BOLD}Phase {phase_num}: Sequential{RESET} ({seq_count} requests, req_timeout {req_timeout}s, phase_timeout {PHASE_TIMEOUT_S // 60}min)"
        )
        p1 = await _run_sequential(address, setup_id, input_data, seq_count, t_global, req_timeout)
        report.phases.append(p1)
        _print_phase_summary(p1)

        # Phase 2..N: Concurrent at each level
        for conc_workers in conc_levels:
            _log(f"{DIM}Sleeping {SLEEP_BETWEEN_PHASES_S}s between phases...{RESET}")
            await asyncio.sleep(SLEEP_BETWEEN_PHASES_S)

            phase_num += 1
            _log(
                f"\n{BOLD}Phase {phase_num}: Concurrent @{conc_workers}{RESET} ({conc_workers} workers, {conc_total} requests, req_timeout {req_timeout}s)"
            )
            p = await _run_concurrent(address, setup_id, input_data, conc_workers, conc_total, t_global, req_timeout)
            p.phase = f"concurrent_{conc_workers}"
            report.phases.append(p)
            _print_phase_summary(p)

        # Phase N+1..M: Burst at each level
        for burst_count in burst_levels:
            _log(f"{DIM}Sleeping {SLEEP_BETWEEN_PHASES_S}s between phases...{RESET}")
            await asyncio.sleep(SLEEP_BETWEEN_PHASES_S)

            phase_num += 1
            _log(
                f"\n{BOLD}Phase {phase_num}: Burst @{burst_count}{RESET} ({burst_count} simultaneous x {burst_repeats} waves, req_timeout {req_timeout}s)"
            )
            p = await _run_burst(address, setup_id, input_data, burst_count, burst_repeats, t_global, req_timeout)
            p.phase = f"burst_{burst_count}"
            report.phases.append(p)
            _print_phase_summary(p)

        report.memory_peak_bytes = proc.memory_info().rss
        report.cpu_end = psutil.cpu_percent(interval=None)
        report.load_avg_end = list(os.getloadavg())

    return report


def _print_phase_summary(phase: PhaseResult) -> None:
    ok = phase.ok_count
    err = phase.err_count
    lats = phase.ok_latencies
    total = len(phase.results)
    rps = total / phase.duration_s if phase.duration_s > 0 else 0

    status = f"{GREEN}{ok}{RESET}/{total} ok" + (f"  {RED}{err} err{RESET}" if err else "")
    avg = f"  avg {statistics.mean(lats):.0f}ms" if lats else ""
    p99 = ""
    if len(lats) >= 2:
        sorted_lats = sorted(lats)
        idx = min(int(len(sorted_lats) * 0.99), len(sorted_lats) - 1)
        p99 = f"  p99 {sorted_lats[idx]:.0f}ms"
    _log(f"{BOLD}=> {status}{avg}{p99}  {DIM}{rps:.1f} req/s  ({phase.duration_s:.1f}s){RESET}")


# ── Markdown report generation ───────────────────────────────────────────────


def _pct(sorted_lats: list[float], p: float) -> float:
    idx = min(int(len(sorted_lats) * p / 100), len(sorted_lats) - 1)
    return sorted_lats[idx]


def generate_report(scenarios: list[ScenarioReport]) -> str:
    """Generate LLM-optimized Markdown report."""
    lines: list[str] = []
    lines.append("# Scalability Benchmark Report")
    lines.append(f"\nGenerated: {datetime.datetime.now().isoformat(timespec='seconds')}")

    # ── Global summary table ─────────────────────────────────────────────
    lines.append("\n## Summary\n")
    lines.append("| Scenario | Phase | Requests | OK | Errors | Throughput | Avg Lat | P50 | P99 | TTFR Avg |")
    lines.append("|----------|-------|----------|----|--------|------------|---------|-----|-----|----------|")

    for sr in scenarios:
        for phase in sr.phases:
            total = len(phase.results)
            ok = phase.ok_count
            err = phase.err_count
            rps = total / phase.duration_s if phase.duration_s > 0 else 0
            lats = sorted(phase.ok_latencies)
            fm = sorted(phase.ok_first_msg)
            avg_lat = f"{statistics.mean(lats):.0f}" if lats else "-"
            p50 = f"{_pct(lats, 50):.0f}" if lats else "-"
            p99 = f"{_pct(lats, 99):.0f}" if lats else "-"
            ttfr = f"{statistics.mean(fm):.0f}" if fm else "-"
            lines.append(
                f"| {sr.label} | {phase.phase} | {total} | {ok} | {err} | "
                f"{rps:.1f} req/s | {avg_lat} | {p50} | {p99} | {ttfr} |"
            )

    # ── Per-scenario detail ──────────────────────────────────────────────
    for sr in scenarios:
        lines.append(f"\n## {sr.label}\n")

        # Config
        lines.append("### Configuration\n")
        lines.append(f"- **Target**: `{sr.config.get('target')}`")
        lines.append(f"- **Setup ID**: `{sr.config.get('setup_id')}`")
        lines.append(f"- **Input protocol**: `{_extract_protocol(sr.config.get('input', {}))}`")
        if sr.config.get("env"):
            lines.append(f"- **Env overrides**: {len(sr.config['env'])} vars")
            for k, v in sr.config["env"].items():
                lines.append(f"  - `{k}={v}`")

        # Per-phase latency
        for phase in sr.phases:
            lats = sorted(phase.ok_latencies)
            fm = sorted(phase.ok_first_msg)
            total = len(phase.results)
            rps = total / phase.duration_s if phase.duration_s > 0 else 0

            lines.append(
                f"\n### {phase.phase.capitalize()} — {total} requests, {phase.duration_s:.1f}s, {rps:.1f} req/s\n"
            )

            if lats:
                lines.extend((
                    "| Metric | Value |",
                    "|--------|-------|",
                    f"| OK | {phase.ok_count} |",
                    f"| Errors | {phase.err_count} |",
                    f"| Min latency | {min(lats):.0f}ms |",
                    f"| P50 latency | {_pct(lats, 50):.0f}ms |",
                    f"| P90 latency | {_pct(lats, 90):.0f}ms |",
                    f"| P95 latency | {_pct(lats, 95):.0f}ms |",
                    f"| P99 latency | {_pct(lats, 99):.0f}ms |",
                    f"| Max latency | {max(lats):.0f}ms |",
                    f"| Avg latency | {statistics.mean(lats):.0f}ms |",
                ))
                if len(lats) > 1:
                    lines.append(f"| Std dev | {statistics.stdev(lats):.0f}ms |")
            else:
                lines.append("_No successful requests._")

            if fm:
                lines.append(
                    f"\nFirst-message latency: min={min(fm):.0f}ms  p50={_pct(fm, 50):.0f}ms  max={max(fm):.0f}ms"
                )

        # Error catalog
        catalog = sr.error_catalog
        total_errs = sum(v["count"] for v in catalog.values())
        lines.append(f"\n### Errors — {total_errs} total, {len(catalog)} distinct\n")

        if not catalog:
            lines.append("_No errors._")
        else:
            # By gRPC code
            codes: dict[str, int] = defaultdict(int)
            for info in catalog.values():
                for code in info["codes"]:
                    codes[code] += info["count"]
            if codes:
                lines.extend(("| gRPC Code | Count |", "|-----------|-------|"))
                for code, n in sorted(codes.items(), key=lambda x: -x[1]):
                    lines.append(f"| `{code}` | {n} |")

            # Full error messages
            lines.append(f"\n#### Unique Error Messages ({len(catalog)})\n")
            for msg, info in sorted(catalog.items(), key=lambda x: -x[1]["count"]):
                codes_str = ", ".join(info["codes"]) if info["codes"] else "unknown"
                phases_str = ", ".join(info["phases"])
                lines.extend((f"**[{info['count']}x]** `{codes_str}` — phases: {phases_str}\n", "```", msg, "```\n"))

        # System metrics
        lines.extend((
            "### System Metrics\n",
            f"- CPU: {sr.cpu_start:.1f}% → {sr.cpu_end:.1f}%",
            f"- Load avg: {sr.load_avg_start} → {sr.load_avg_end}",
        ))
        delta_mb = (sr.memory_peak_bytes - sr.memory_start_bytes) / (1024 * 1024)
        lines.append(f"- Memory delta: {delta_mb:.1f}MB (peak RSS)")

    # ── Concurrent vs Burst comparison ──────────────────────────────────
    for sr in scenarios:
        conc_phases = {p.phase: p for p in sr.phases if p.phase.startswith("concurrent_")}
        burst_phases = {p.phase: p for p in sr.phases if p.phase.startswith("burst_")}
        # Find matching levels
        pairs: list[tuple[int, PhaseResult, PhaseResult]] = []
        for cp_name, cp in conc_phases.items():
            level = cp_name.split("_", 1)[1]
            bp_name = f"burst_{level}"
            if bp_name in burst_phases:
                pairs.append((int(level), cp, burst_phases[bp_name]))
        if not pairs:
            continue
        pairs.sort(key=operator.itemgetter(0))
        lines.extend((
            f"\n## {sr.label} — Concurrent vs Burst\n",
            "Concurrent uses a worker pool that sustains N in-flight requests with sleep between each. Burst fires all N requests simultaneously via `asyncio.gather`, waits for all to complete, repeats.\n",
            "| Level | | OK | Errors | Avg Lat | P50 | P99 | TTFR Avg | Throughput |",
            "|------:|----|---:|-------:|--------:|----:|----:|---------:|-----------:|",
        ))
        for level, cp, bp in pairs:
            for tag, p in [("conc", cp), ("burst", bp)]:
                lats = sorted(p.ok_latencies)
                fm = sorted(p.ok_first_msg)
                total = len(p.results)
                rps = total / p.duration_s if p.duration_s > 0 else 0
                avg_lat = f"{statistics.mean(lats):.0f}" if lats else "-"
                p50 = f"{_pct(lats, 50):.0f}" if lats else "-"
                p99 = f"{_pct(lats, 99):.0f}" if lats else "-"
                ttfr = f"{statistics.mean(fm):.0f}" if fm else "-"
                lines.append(
                    f"| {level} | {tag} | {p.ok_count} | {p.err_count} | "
                    f"{avg_lat}ms | {p50}ms | {p99}ms | {ttfr}ms | {rps:.1f} req/s |"
                )
            # Delta row
            c_lats = cp.ok_latencies
            b_lats = bp.ok_latencies
            if c_lats and b_lats:
                c_avg = statistics.mean(c_lats)
                b_avg = statistics.mean(b_lats)
                delta_pct = ((b_avg - c_avg) / c_avg) * 100 if c_avg else 0
                sign = "+" if delta_pct >= 0 else ""
                c_rps = len(cp.results) / cp.duration_s if cp.duration_s > 0 else 0
                b_rps = len(bp.results) / bp.duration_s if bp.duration_s > 0 else 0
                rps_delta = ((b_rps - c_rps) / c_rps) * 100 if c_rps else 0
                rps_sign = "+" if rps_delta >= 0 else ""
                lines.append(f"| | **delta** | | | **{sign}{delta_pct:.0f}%** | | | | **{rps_sign}{rps_delta:.0f}%** |")

    # ── Raw JSON ─────────────────────────────────────────────────────────
    lines.extend(("\n## Raw JSON\n", "```json"))
    raw = []
    for sr in scenarios:
        entry = {
            "label": sr.label,
            "config": sr.config,
            "phases": [
                {
                    "phase": p.phase,
                    "duration_s": round(p.duration_s, 3),
                    "ok": p.ok_count,
                    "errors": p.err_count,
                    "results": [
                        {
                            "status": r.status,
                            "latency_ms": round(r.latency_ms, 1),
                            "messages": r.messages,
                            "first_msg_ms": round(r.first_msg_ms, 1),
                            "error": r.error,
                            "grpc_code": r.grpc_code,
                            "grpc_details_full": r.grpc_details_full,
                            "mission_id": r.mission_id,
                        }
                        for r in p.results
                    ],
                }
                for p in sr.phases
            ],
        }
        raw.append(entry)
    lines.extend((json.dumps(raw, indent=2, ensure_ascii=False), "```"))

    return "\n".join(lines)


def _phase_level(phase_name: str) -> int | None:
    """Extract numeric level from phase name like 'concurrent_20' -> 20."""
    parts = phase_name.rsplit("_", 1)
    if len(parts) == 2 and parts[1].isdigit():
        return int(parts[1])
    return None


def generate_graphs(scenarios: list[ScenarioReport], output_dir: str, ts: str) -> list[str]:
    """Generate PNG charts for each scenario. Returns list of generated file paths."""
    try:
        import matplotlib as mpl

        mpl.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        return []

    generated: list[str] = []

    for sr in scenarios:
        # Collect phases by type
        seq = next((p for p in sr.phases if p.phase == "sequential"), None)
        conc_phases = sorted(
            [
                (p, _phase_level(p.phase))
                for p in sr.phases
                if p.phase.startswith("concurrent_") and _phase_level(p.phase) is not None
            ],
            key=operator.itemgetter(1),
        )
        burst_phases = sorted(
            [
                (p, _phase_level(p.phase))
                for p in sr.phases
                if p.phase.startswith("burst_") and _phase_level(p.phase) is not None
            ],
            key=operator.itemgetter(1),
        )

        if not conc_phases and not burst_phases:
            continue

        conc_levels = [lv for _, lv in conc_phases]
        burst_levels = [lv for _, lv in burst_phases]

        def _stats(p: PhaseResult) -> dict[str, float]:
            lats = sorted(p.ok_latencies)
            fm = sorted(p.ok_first_msg)
            total = len(p.results)
            return {
                "avg": statistics.mean(lats) if lats else 0,
                "p50": _pct(lats, 50) if lats else 0,
                "p99": _pct(lats, 99) if lats else 0,
                "ttfr": statistics.mean(fm) if fm else 0,
                "rps": total / p.duration_s if p.duration_s > 0 else 0,
                "err_pct": (p.err_count / total * 100) if total else 0,
            }

        conc_stats = [_stats(p) for p, _ in conc_phases]
        burst_stats = [_stats(p) for p, _ in burst_phases]
        seq_stats = _stats(seq) if seq else None

        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle(f"{sr.label} — Scaling Analysis", fontsize=14, fontweight="bold")

        # ── 1. Latency vs Concurrency ────────────────────────────────
        ax = axes[0][0]
        if conc_levels and conc_stats:
            ax.plot(conc_levels, [s["avg"] for s in conc_stats], "o-", color="#2196F3", label="conc avg", linewidth=2)
            ax.plot(conc_levels, [s["p99"] for s in conc_stats], "s--", color="#2196F3", alpha=0.5, label="conc p99")
        if burst_levels and burst_stats:
            ax.plot(
                burst_levels, [s["avg"] for s in burst_stats], "o-", color="#F44336", label="burst avg", linewidth=2
            )
            ax.plot(burst_levels, [s["p99"] for s in burst_stats], "s--", color="#F44336", alpha=0.5, label="burst p99")
        if seq_stats:
            ax.axhline(
                y=seq_stats["avg"],
                color="#4CAF50",
                linestyle=":",
                alpha=0.7,
                label=f"seq baseline ({seq_stats['avg']:.0f}ms)",
            )
        ax.set_xlabel("Concurrency Level")
        ax.set_ylabel("Latency (ms)")
        ax.set_title("Latency vs Concurrency")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
        if conc_levels or burst_levels:
            ax.set_xticks(conc_levels or burst_levels)

        # ── 2. Throughput vs Concurrency ─────────────────────────────
        ax = axes[0][1]
        if conc_levels and conc_stats:
            ax.plot(conc_levels, [s["rps"] for s in conc_stats], "o-", color="#2196F3", label="concurrent", linewidth=2)
        if burst_levels and burst_stats:
            ax.plot(burst_levels, [s["rps"] for s in burst_stats], "o-", color="#F44336", label="burst", linewidth=2)
        if seq_stats:
            ax.axhline(
                y=seq_stats["rps"],
                color="#4CAF50",
                linestyle=":",
                alpha=0.7,
                label=f"seq baseline ({seq_stats['rps']:.1f})",
            )
        ax.set_xlabel("Concurrency Level")
        ax.set_ylabel("Throughput (req/s)")
        ax.set_title("Throughput vs Concurrency")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
        if conc_levels or burst_levels:
            ax.set_xticks(conc_levels or burst_levels)

        # ── 3. TTFR vs Concurrency ──────────────────────────────────
        ax = axes[1][0]
        if conc_levels and conc_stats:
            ax.plot(
                conc_levels, [s["ttfr"] for s in conc_stats], "o-", color="#2196F3", label="concurrent", linewidth=2
            )
        if burst_levels and burst_stats:
            ax.plot(burst_levels, [s["ttfr"] for s in burst_stats], "o-", color="#F44336", label="burst", linewidth=2)
        if seq_stats:
            ax.axhline(
                y=seq_stats["ttfr"],
                color="#4CAF50",
                linestyle=":",
                alpha=0.7,
                label=f"seq baseline ({seq_stats['ttfr']:.0f}ms)",
            )
        ax.set_xlabel("Concurrency Level")
        ax.set_ylabel("TTFR (ms)")
        ax.set_title("Time to First Response vs Concurrency")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3)
        if conc_levels or burst_levels:
            ax.set_xticks(conc_levels or burst_levels)

        # ── 4. Error Rate vs Concurrency ─────────────────────────────
        ax = axes[1][1]
        if conc_levels and conc_stats:
            ax.bar(
                [x - 1.5 for x in conc_levels],
                [s["err_pct"] for s in conc_stats],
                width=3,
                color="#2196F3",
                alpha=0.7,
                label="concurrent",
            )
        if burst_levels and burst_stats:
            ax.bar(
                [x + 1.5 for x in burst_levels],
                [s["err_pct"] for s in burst_stats],
                width=3,
                color="#F44336",
                alpha=0.7,
                label="burst",
            )
        ax.set_xlabel("Concurrency Level")
        ax.set_ylabel("Error Rate (%)")
        ax.set_title("Error Rate vs Concurrency")
        ax.legend(fontsize=8)
        ax.grid(True, alpha=0.3, axis="y")
        if conc_levels or burst_levels:
            ax.set_xticks(conc_levels or burst_levels)
        ax.set_ylim(bottom=0)

        plt.tight_layout()
        path = os.path.join(output_dir, f"{sr.label}_scaling_{ts}.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)
        generated.append(path)

        # ── 5. Latency Distribution (box plot) ──────────────────────
        all_leveled = [(p, lv, "conc") for p, lv in conc_phases] + [(p, lv, "burst") for p, lv in burst_phases]
        if all_leveled:
            box_data = []
            box_labels = []
            box_colors = []
            if seq and seq.ok_latencies:
                box_data.append(seq.ok_latencies)
                box_labels.append("seq")
                box_colors.append("#4CAF50")
            for p, lv, kind in all_leveled:
                lats = p.ok_latencies
                if lats:
                    box_data.append(lats)
                    box_labels.append(f"{'c' if kind == 'conc' else 'b'}{lv}")
                    box_colors.append("#2196F3" if kind == "conc" else "#F44336")

            if box_data:
                fig2, ax2 = plt.subplots(figsize=(max(10, len(box_data) * 1.2), 6))
                fig2.suptitle(f"{sr.label} — Latency Distribution", fontsize=14, fontweight="bold")
                bp = ax2.boxplot(box_data, labels=box_labels, patch_artist=True, showfliers=False, widths=0.6)
                for patch, color in zip(bp["boxes"], box_colors):
                    patch.set_facecolor(color)
                    patch.set_alpha(0.6)
                ax2.set_ylabel("Latency (ms)")
                ax2.set_xlabel("Phase (c=concurrent, b=burst)")
                ax2.grid(True, alpha=0.3, axis="y")
                plt.tight_layout()
                path2 = os.path.join(output_dir, f"{sr.label}_distribution_{ts}.png")
                fig2.savefig(path2, dpi=150)
                plt.close(fig2)
                generated.append(path2)

    return generated


def _extract_protocol(input_data: dict) -> str:
    if "protocol" in input_data:
        return input_data["protocol"]
    payload = input_data.get("payload", input_data.get("root", {}))
    if isinstance(payload, dict):
        return payload.get("protocol", payload.get("payload_type", "unknown"))
    return "unknown"


# ── Main ─────────────────────────────────────────────────────────────────────


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Config-driven scalability benchmark with three-phase load testing.",
    )
    parser.add_argument("config", help="Path to JSON config file")
    parser.add_argument("-o", "--output-dir", default=None, help="Override output_dir from config")
    parser.add_argument("--only", nargs="*", default=None, help="Run only these labels")
    parser.add_argument("--report", "-r", default="", help="Write Markdown report to FILE")
    args = parser.parse_args()

    with open(args.config, encoding="utf-8") as f:
        config = json.load(f)

    output_dir = args.output_dir or config.get("output_dir", "bench_results")
    scenarios = config["scenarios"]

    if args.only:
        only_set = set(args.only)
        scenarios = [s for s in scenarios if s["label"] in only_set]
        if not scenarios:
            return

    os.makedirs(output_dir, exist_ok=True)
    all_reports: list[ScenarioReport] = []
    ts = time.strftime("%Y%m%d_%H%M%S")

    for i, scenario in enumerate(scenarios, 1):
        label = scenario["label"]
        _extract_protocol(scenario.get("input", {}))
        scenario.get("env")

        sr = await run_scenario(scenario)
        all_reports.append(sr)

        if i < len(scenarios):
            await asyncio.sleep(SLEEP_BETWEEN_SCENARIOS_S)

        # Per-scenario JSON
        out_path = os.path.join(output_dir, f"{label}_{ts}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "label": sr.label,
                    "phases": [
                        {
                            "phase": p.phase,
                            "ok": p.ok_count,
                            "errors": p.err_count,
                            "duration_s": round(p.duration_s, 3),
                        }
                        for p in sr.phases
                    ],
                    "total_errors": len(sr.all_errors),
                    "error_catalog": dict(sr.error_catalog.items()),
                },
                f,
                indent=2,
            )

    # Summary
    for sr in all_reports:
        len(sr.all_errors)
        for p in sr.phases:
            f"{GREEN}{p.ok_count}{RESET}/{len(p.results)}"
            lats = p.ok_latencies
            f"  avg {statistics.mean(lats):.0f}ms" if lats else ""
            len(p.results) / p.duration_s if p.duration_s > 0 else 0

    # Report
    report_path = args.report
    if not report_path:
        report_path = os.path.join(output_dir, f"report_{ts}.md")

    md = generate_report(all_reports)
    pathlib.Path(report_path).write_text(md, encoding="utf-8")

    # Graphs
    graph_paths = generate_graphs(all_reports, output_dir, ts)
    if graph_paths:
        for gp in graph_paths:
            pass


if __name__ == "__main__":
    asyncio.run(main())
