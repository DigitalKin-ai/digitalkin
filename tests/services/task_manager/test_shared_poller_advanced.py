"""Advanced correctness and stress tests for the _SharedPoller signal delivery pipeline.

Scenario coverage:
    - Poll interval integrity: new task registration must not trigger early polls
    - Terminal signal exit latency: poison pill lets consumer exit without timeout
    - Concurrent tasks: 50 tasks, partial cancellations, cross-task signal fidelity
    - Poller lifecycle: empty → running → empty → restart
    - Backpressure: queue-full drops with warning log, no cross-task interference
    - Exponential backoff: interval growth and reset after signal
    - Race conditions: dispatch-while-unregistered, close-during-consume, dedup under load
"""

from __future__ import annotations

import asyncio
import contextlib
from collections import defaultdict
from itertools import count
from unittest.mock import Mock

import pytest
from agentic_mesh_protocol.task_manager.v1 import (
    task_manager_dto_pb2,
    task_manager_message_pb2,
)
from google.protobuf.struct_pb2 import Struct
from google.protobuf.timestamp_pb2 import Timestamp

from digitalkin.models.grpc_servers.models import ClientConfig
from digitalkin.models.settings.utils.channel import ControlFlow, SecurityMode
from digitalkin.services.task_manager.grpc_task_manager import GrpcTaskManager, _SharedPoller, _SharedSendBuffer

pytestmark = pytest.mark.timeout(30)

_TS_SEQ = count(1_000_000)  # Monotonically increasing, collision-free timestamps
_MISSION = "missions:adv"
_SETUP = "setups:adv"
_VERSION = "versions:adv"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _proto(task_id: str, action: str, ts: int | None = None) -> task_manager_message_pb2.Task:
    """Build a Task proto with a guaranteed-unique (or explicit) timestamp."""
    p = task_manager_message_pb2.Task(
        task_id=task_id,
        mission_id=_MISSION,
        setup_id=_SETUP,
        setup_version_id=_VERSION,
        action=action,
        cancellation_reason="none",
    )
    stamp = Timestamp()
    stamp.seconds = ts if ts is not None else next(_TS_SEQ)
    p.created_at.CopyFrom(stamp)
    p.payload.CopyFrom(Struct())
    return p


def _client(poll_interval: float = 0.1, initial: float = 0.05) -> GrpcTaskManager:
    cfg = ClientConfig(host="[::]", port=50051, mode=ControlFlow.ASYNC, security=SecurityMode.INSECURE)
    c = GrpcTaskManager(
        mission_id=_MISSION,
        setup_id=_SETUP,
        setup_version_id=_VERSION,
        client_config=cfg,
        poll_interval=poll_interval,
        initial_poll_interval=initial,
    )
    c.stub = Mock()
    return c


def _poller(poll_fn=None, poll_interval: float = 0.2, initial: float = 0.05) -> _SharedPoller:
    async def _noop(task_ids: list[str]) -> list:  # noqa: RUF029
        return []

    return _SharedPoller(poll_fn or _noop, poll_interval=poll_interval, initial_poll_interval=initial)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
async def _reset():
    _SharedPoller._instances.clear()
    _SharedSendBuffer._instances.clear()
    yield
    for p in list(_SharedPoller._instances.values()):
        with contextlib.suppress(Exception):
            await p.close()
    _SharedPoller._instances.clear()
    _SharedSendBuffer._instances.clear()


# ===========================================================================
# 1. Poll Interval Integrity
# ===========================================================================


class TestPollerIntervalIntegrity:
    """A new register() while the poller is sleeping must not cut the sleep short."""

    @pytest.mark.asyncio
    async def test_second_registration_does_not_trigger_early_poll(self) -> None:
        """Before the _wake_event removal, calling register() while the poller slept would
        set _wake_event, cutting the sleep short and producing an unscheduled poll.
        Verify that the second registration is silently absorbed into the next natural poll.
        """
        poll_count = 0
        poll_batches: list[list[str]] = []

        async def poll_fn(task_ids: list[str]) -> list:
            nonlocal poll_count
            poll_count += 1
            poll_batches.append(sorted(task_ids))
            return []

        # initial=0.2 → after first poll (no signals) backoff to 0.4s sleep (+jitter ≤ 0.2s)
        poller = _poller(poll_fn, poll_interval=0.8, initial=0.2)
        poller.register("task_a")

        await asyncio.sleep(0.02)  # First poll fires immediately
        assert poll_count == 1, "Expected exactly 1 poll after poller started"

        # Register second task during the 0.4–0.6s sleep window
        poller.register("task_b")
        await asyncio.sleep(0.05)  # Well within the backoff window
        assert poll_count == 1, (
            f"Second register() triggered spurious early poll (count={poll_count}). "
            "The _wake_event removal should prevent this."
        )

        # Natural second poll: backoff is 0.4s + jitter ≤ 0.2s → wait 0.8s to be safe
        await asyncio.sleep(0.8)
        assert poll_count >= 2

        # Both tasks appear in the batched call — the shared poller's key value
        assert "task_a" in poll_batches[1]
        assert "task_b" in poll_batches[1], (
            "task_b registered before the second poll must be included in it"
        )

        await poller.close()

    @pytest.mark.asyncio
    async def test_N_simultaneous_registrations_produce_one_batched_poll(self) -> None:
        """N tasks registered with no await between them must produce exactly 1 RPC,
        not N.  This is the core purpose of _SharedPoller.
        """
        N = 30
        poll_count = 0
        poll_batches: list[list[str]] = []

        async def poll_fn(task_ids: list[str]) -> list:
            nonlocal poll_count
            poll_count += 1
            poll_batches.append(sorted(task_ids))
            return []

        poller = _poller(poll_fn, poll_interval=0.5, initial=0.2)

        for i in range(N):
            poller.register(f"task_{i}")

        await asyncio.sleep(0.02)  # Yield to let the single poll fire

        assert poll_count == 1, (
            f"Expected 1 batched poll for {N} tasks, got {poll_count}"
        )
        assert len(poll_batches[0]) == N, (
            f"Expected all {N} task_ids in one poll, got {len(poll_batches[0])}"
        )

        await poller.close()


# ===========================================================================
# 2. Terminal Signal Exit Latency
# ===========================================================================


class TestTerminalSignalExitLatency:
    """The consumer generator must exhaust quickly after a 'stop'/'cancel' signal.

    Without the poison-pill fix the consumer blocked on queue.get() for up to
    poll_interval * 2 seconds after the terminal signal was already yielded.
    """

    @pytest.mark.asyncio
    async def test_stop_signal_exhausts_consumer_without_unsubscribe(self) -> None:
        """Generator must exhaust by itself after 'stop' — no explicit unsubscribe needed.

        Key metric: with poll_interval=2.0s the OLD code would stall ~4s waiting for
        queue.get() to time out.  With the poison pill the exit is near-instant.
        """
        stop_proto = _proto("t1", "stop")

        async def mock_signals(req, timeout=None):
            return task_manager_dto_pb2.GetSignalsResponse(tasks=[stop_proto])

        client = _client(poll_interval=2.0, initial=0.04)
        client.stub.GetSignals = mock_signals

        _, gen = await client.subscribe_signals("t1")

        received = []
        t0 = asyncio.get_event_loop().time()
        async for sig in gen:
            received.append(sig)
        elapsed = asyncio.get_event_loop().time() - t0

        assert len(received) == 1
        assert received[0]["action"] == "stop"
        assert elapsed < 0.5, (
            f"Consumer took {elapsed:.3f}s to exit after 'stop' "
            f"(poll_interval=2s — without poison pill fix this would be ~4s)"
        )

    @pytest.mark.asyncio
    async def test_cancel_signal_exhausts_consumer_without_unsubscribe(self) -> None:
        """Same guarantee for 'cancel' action."""
        cancel_proto = _proto("t2", "cancel")

        async def mock_signals(req, timeout=None):
            return task_manager_dto_pb2.GetSignalsResponse(tasks=[cancel_proto])

        client = _client(poll_interval=2.0, initial=0.04)
        client.stub.GetSignals = mock_signals

        _, gen = await client.subscribe_signals("t2")

        t0 = asyncio.get_event_loop().time()
        received = [sig async for sig in gen]
        elapsed = asyncio.get_event_loop().time() - t0

        assert received[0]["action"] == "cancel"
        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_signal_delivered_before_poison_pill(self) -> None:
        """The actual stop/cancel payload must be yielded BEFORE the generator terminates.

        task_session.py's listen_signals() depends on receiving the signal so it can
        call _handle_stop() / _handle_cancel() before the generator is exhausted.
        """
        stop_proto = _proto("t3", "stop")

        async def mock_signals(req, timeout=None):
            return task_manager_dto_pb2.GetSignalsResponse(tasks=[stop_proto])

        client = _client(poll_interval=1.0, initial=0.04)
        client.stub.GetSignals = mock_signals

        _, gen = await client.subscribe_signals("t3")

        # Give the poller one cycle to dispatch
        await asyncio.sleep(0.1)

        # Drain generator completely — must yield exactly 1 item (the stop signal)
        items = [sig async for sig in gen]

        assert len(items) == 1, f"Expected 1 signal before exhaustion, got {len(items)}"
        assert items[0]["action"] == "stop"

    @pytest.mark.asyncio
    async def test_non_terminal_signal_does_not_close_consumer(self) -> None:
        """A 'start' signal is NOT terminal; the consumer must remain open after receiving it."""
        call_count = 0
        start_proto = _proto("t4", "start")

        async def mock_signals(req, timeout=None):
            nonlocal call_count
            call_count += 1
            return task_manager_dto_pb2.GetSignalsResponse(
                tasks=[start_proto] if call_count == 1 else []
            )

        client = _client(poll_interval=0.1, initial=0.04)
        client.stub.GetSignals = mock_signals

        _, gen = await client.subscribe_signals("t4")

        # Consume the start signal
        sig = await asyncio.wait_for(gen.__anext__(), timeout=1.0)
        assert sig["action"] == "start"

        # Task must still be in the poller (not auto-removed)
        key = client._channel_cache_key or "default"
        poller = _SharedPoller._instances.get(key)
        assert poller is not None
        assert "t4" in poller._task_queues, "Non-terminal signal must NOT remove the task"

        await client.close()

    @pytest.mark.asyncio
    async def test_terminal_signal_removes_task_from_poller_synchronously(self) -> None:
        """_dispatch_signal must remove the task from _task_queues before returning,
        so subsequent polls never include a terminated task_id.
        """
        poller = _poller()
        queue = poller.register("victim")

        stop_p = _proto("victim", "stop")
        poller._dispatch_signal(stop_p)

        # Synchronous — no await needed
        assert "victim" not in poller._task_queues, (
            "Task must be removed from _task_queues synchronously inside _dispatch_signal"
        )
        assert queue.qsize() == 2  # signal + poison pill


# ===========================================================================
# 3. Concurrent Tasks — Signal Fidelity
# ===========================================================================


class TestConcurrentTasksFidelity:
    """Many concurrent subscribers; signals must be routed to the correct consumer."""

    @pytest.mark.asyncio
    async def test_50_tasks_each_receives_exactly_its_own_stop_signal(self) -> None:
        """50 concurrent tasks, each gets one 'stop' signal for its own task_id.
        No signal must be delivered to the wrong consumer.
        """
        N = 50
        protos = {f"task_{i}": _proto(f"task_{i}", "stop") for i in range(N)}
        delivered = False

        async def mock_signals(req, timeout=None):
            nonlocal delivered
            if not delivered:
                delivered = True
                return task_manager_dto_pb2.GetSignalsResponse(tasks=list(protos.values()))
            return task_manager_dto_pb2.GetSignalsResponse(tasks=[])

        client = _client(poll_interval=1.0, initial=0.05)
        client.stub.GetSignals = mock_signals

        generators = {}
        for i in range(N):
            tid = f"task_{i}"
            _, gen = await client.subscribe_signals(tid)
            generators[tid] = gen

        results: dict[str, list] = defaultdict(list)

        async def consume(tid: str, gen):
            async for sig in gen:
                results[tid].append(sig)

        await asyncio.gather(*[
            asyncio.wait_for(consume(tid, gen), timeout=5.0)
            for tid, gen in generators.items()
        ])

        for i in range(N):
            tid = f"task_{i}"
            assert len(results[tid]) == 1, f"{tid}: expected 1 signal, got {len(results[tid])}"
            assert results[tid][0]["task_id"] == tid, f"{tid}: received signal for wrong task"
            assert results[tid][0]["action"] == "stop"

    @pytest.mark.asyncio
    async def test_partial_cancels_leave_other_tasks_registered(self) -> None:
        """5 of 20 tasks get 'cancel'; the other 15 must remain in _task_queues."""
        N = 20
        cancel_ids = {f"task_{i}" for i in range(5)}
        delivered = False

        async def mock_signals(req, timeout=None):
            nonlocal delivered
            if not delivered:
                delivered = True
                protos = [
                    _proto(tid, "cancel") if tid in cancel_ids else _proto(tid, "start")
                    for tid in req.task_ids
                ]
                return task_manager_dto_pb2.GetSignalsResponse(tasks=protos)
            return task_manager_dto_pb2.GetSignalsResponse(tasks=[])

        client = _client(poll_interval=2.0, initial=0.05)
        client.stub.GetSignals = mock_signals

        for i in range(N):
            await client.subscribe_signals(f"task_{i}")

        await asyncio.sleep(0.15)

        key = client._channel_cache_key or "default"
        poller = _SharedPoller._instances[key]

        for tid in cancel_ids:
            assert tid not in poller._task_queues, f"{tid} should have been auto-removed after 'cancel'"

        for i in range(5, N):
            tid = f"task_{i}"
            assert tid in poller._task_queues, f"{tid} should still be registered"

        await client.close()

    @pytest.mark.asyncio
    async def test_all_tasks_terminal_poller_self_terminates(self) -> None:
        """When every task receives a terminal signal the poll loop must stop itself."""
        N = 10
        delivered = False

        async def mock_signals(req, timeout=None):
            nonlocal delivered
            if not delivered:
                delivered = True
                return task_manager_dto_pb2.GetSignalsResponse(
                    tasks=[_proto(tid, "stop") for tid in req.task_ids]
                )
            return task_manager_dto_pb2.GetSignalsResponse(tasks=[])

        client = _client(poll_interval=5.0, initial=0.05)
        client.stub.GetSignals = mock_signals

        for i in range(N):
            await client.subscribe_signals(f"t_{i}")

        await asyncio.sleep(0.2)

        key = client._channel_cache_key or "default"
        poller = _SharedPoller._instances.get(key)

        assert not poller._task_queues, "All queues should be cleared after terminal dispatch"
        assert poller._stop_event.is_set(), "stop_event must be set when all tasks removed"

        await asyncio.sleep(0.1)
        assert poller._task is None or poller._task.done(), "Poll loop task must have exited"

        await client.close()


# ===========================================================================
# 4. Poller Lifecycle
# ===========================================================================


class TestPollerLifecycle:
    """Poller starts, stops, and restarts correctly across multiple task waves."""

    @pytest.mark.asyncio
    async def test_poller_restarts_for_new_registration_after_idle(self) -> None:
        """After all tasks unregister (loop exits), a new register() must start a fresh loop."""
        poll_count = 0

        async def poll_fn(task_ids: list[str]) -> list:
            nonlocal poll_count
            poll_count += 1
            return []

        poller = _poller(poll_fn, poll_interval=0.5, initial=0.05)

        # Wave 1
        poller.register("wave1")
        await asyncio.sleep(0.02)
        assert poll_count >= 1
        poller.unregister("wave1")
        await asyncio.sleep(0.1)
        assert poller._task is None or poller._task.done(), "Poller should have stopped after wave 1"

        count_after_wave1 = poll_count

        # Wave 2 — must create a new asyncio.Task (not reuse the dead one)
        poller.register("wave2")
        assert poller._task is not None and not poller._task.done(), (
            "New register() must restart the poll loop"
        )
        await asyncio.sleep(0.02)
        assert poll_count > count_after_wave1, "New registration must trigger at least one poll"
        assert "wave2" in poller._task_queues

        await poller.close()

    @pytest.mark.asyncio
    async def test_stop_event_set_on_last_task_removed(self) -> None:
        """_stop_event must fire exactly when the last task is unregistered."""
        poller = _poller()
        poller.register("a")
        poller.register("b")

        poller.unregister("a")
        assert not poller._stop_event.is_set(), "stop_event must NOT fire while tasks remain"

        poller.unregister("b")
        assert poller._stop_event.is_set(), "stop_event must fire when last task unregisters"

    @pytest.mark.asyncio
    async def test_close_sends_poison_pill_to_every_queue(self) -> None:
        """close() must deliver a None sentinel to every registered queue."""
        poller = _poller()
        queues = [poller.register(f"t{i}") for i in range(8)]

        await poller.close()

        for i, q in enumerate(queues):
            item = q.get_nowait()
            assert item is None, f"Queue t{i}: expected None sentinel, got {item!r}"

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self) -> None:
        """Calling close() twice must not raise."""
        poller = _poller()
        poller.register("x")
        await poller.close()
        await poller.close()  # Must be silent

    @pytest.mark.asyncio
    async def test_unregister_inside_dispatch_via_terminal_does_not_corrupt_iteration(self) -> None:
        """_dispatch_signal modifies _task_queues (via unregister) mid-loop in _poll_loop.
        Since asyncio is single-threaded, this is safe — but verify it does not skip
        dispatching to sibling tasks that come after the terminal task in the same poll.
        """
        N = 5
        # task_0 gets "stop" (terminal), tasks 1-4 get "start" (non-terminal)
        # All returned in a single batch
        delivered = False

        received_by: dict[str, list] = defaultdict(list)
        queues: dict[str, asyncio.Queue] = {}

        async def poll_fn(task_ids: list[str]) -> list:
            nonlocal delivered
            if not delivered:
                delivered = True
                result = [_proto("task_0", "stop")]
                result += [_proto(f"task_{i}", "start") for i in range(1, N)]
                return result
            return []

        poller = _poller(poll_fn, poll_interval=1.0, initial=0.05)
        for i in range(N):
            queues[f"task_{i}"] = poller.register(f"task_{i}")

        await asyncio.sleep(0.15)

        # task_0 should have been removed (terminal)
        assert "task_0" not in poller._task_queues

        # task_0's queue: [stop_proto, None]
        q0 = queues["task_0"]
        assert q0.qsize() == 2

        # tasks 1-4 should have received their "start" signals (no removal)
        for i in range(1, N):
            tid = f"task_{i}"
            assert tid in poller._task_queues, f"{tid} should still be registered"
            assert not queues[tid].empty(), f"{tid} should have received its 'start' signal"

        await poller.close()


# ===========================================================================
# 5. Backpressure — Queue Full
# ===========================================================================


class TestBackpressureAndQueueFull:
    """Full queues must produce warnings and not stall sibling task delivery."""

    @pytest.mark.asyncio
    async def test_overflow_drops_signal_and_logs_warning(self) -> None:
        """When a task's queue is at maxsize, the next dispatch logs a warning and drops.

        The project uses structlog, which bypasses pytest's caplog. We mock the module
        logger directly to assert the warning call without relying on log propagation.
        """
        from unittest.mock import patch

        poller = _poller()
        queue = poller.register("t1")
        maxsize = queue.maxsize

        # Fill queue completely
        for i in range(maxsize):
            queue.put_nowait(_proto("t1", "start", ts=i + 1))

        overflow_proto = _proto("t1", "start", ts=maxsize + 1)
        with patch("digitalkin.services.task_manager.grpc_task_manager.logger") as mock_logger:
            result = poller._dispatch_signal(overflow_proto)

        assert result is True  # Dispatch attempted (True), signal itself was dropped
        assert queue.qsize() == maxsize, "Queue size must not grow beyond maxsize"
        mock_logger.warning.assert_called_once()
        warning_msg = mock_logger.warning.call_args[0][0]
        assert "queue full" in warning_msg.lower() or "dropping" in warning_msg.lower(), (
            f"Unexpected warning message: {warning_msg!r}"
        )

        await poller.close()

    @pytest.mark.asyncio
    async def test_full_queue_on_task1_does_not_block_task2_dispatch(self) -> None:
        """_dispatch_signal uses put_nowait (non-blocking); a full queue on task_1
        must never delay signal delivery to task_2.
        """
        poller = _poller()
        q1 = poller.register("task_1")
        q2 = poller.register("task_2")

        # Saturate task_1's queue
        for i in range(q1.maxsize):
            q1.put_nowait(_proto("task_1", "start", ts=i + 1))

        # Dispatch to task_2 — must succeed instantly despite task_1 being full
        p2 = _proto("task_2", "cancel")
        result = poller._dispatch_signal(p2)

        assert result is True
        assert not q2.empty(), "task_2 must receive its signal regardless of task_1's queue state"
        item = q2.get_nowait()
        assert item is p2

        await poller.close()


# ===========================================================================
# 6. Exponential Backoff
# ===========================================================================


class TestExponentialBackoff:
    """Poll intervals must double each cycle without signals; reset after a signal."""

    @pytest.mark.asyncio
    async def test_gaps_grow_monotonically_without_signals(self) -> None:
        """Wall-clock time between consecutive polls must grow (within jitter tolerance).

        Sequence: initial=0.05s → 0.1 → 0.2 → 0.4 (capped at poll_interval=0.4).
        """
        poll_times: list[float] = []

        async def poll_fn(task_ids: list[str]) -> list:
            poll_times.append(asyncio.get_event_loop().time())
            return []

        poller = _poller(poll_fn, poll_interval=0.4, initial=0.05)
        poller.register("t1")

        # Wait long enough for 5 polls
        await asyncio.sleep(2.5)
        assert len(poll_times) >= 4, f"Expected ≥4 polls, got {len(poll_times)}"

        gaps = [poll_times[i + 1] - poll_times[i] for i in range(len(poll_times) - 1)]

        # Each gap must not be smaller than 70% of the previous (allows for jitter variance)
        for i in range(min(3, len(gaps) - 1)):
            assert gaps[i + 1] >= gaps[i] * 0.7, (
                f"Gap[{i+1}]={gaps[i+1]:.3f}s < 0.7 × Gap[{i}]={gaps[i]:.3f}s — "
                "backoff is not growing (or is shrinking)"
            )

        # Steady-state gap must not exceed max interval + 50% jitter
        if len(gaps) >= 4:
            assert gaps[-1] < 0.7, (
                f"Steady-state gap {gaps[-1]:.3f}s exceeds poll_interval=0.4s + 50% jitter"
            )

        await poller.close()

    @pytest.mark.asyncio
    async def test_interval_resets_to_initial_after_signal(self) -> None:
        """After a signal is dispatched, current_interval must reset to initial_poll_interval,
        producing a shorter gap after the signal than the gaps accumulated before it.
        """
        call_count = 0
        poll_times: list[float] = []
        signal_proto = _proto("t1", "start")

        async def poll_fn(task_ids: list[str]) -> list:
            nonlocal call_count
            call_count += 1
            poll_times.append(asyncio.get_event_loop().time())
            # Return the signal only on call 3, so calls 1 and 2 produce backoff
            return [signal_proto] if call_count == 3 else []

        poller = _poller(poll_fn, poll_interval=0.4, initial=0.05)
        poller.register("t1")

        await asyncio.sleep(1.5)
        assert len(poll_times) >= 5, f"Expected ≥5 polls, got {len(poll_times)}"

        # Gap before the signal (calls 2→3): should be the backed-off interval (~0.2s)
        # Gap after the signal (calls 3→4): should be the reset interval (~0.05s)
        gap_before = poll_times[2] - poll_times[1]  # sleep between call 2 and call 3
        gap_after = poll_times[3] - poll_times[2]   # sleep between call 3 (signal) and call 4

        assert gap_after < gap_before, (
            f"Interval did not reset after signal: "
            f"gap before={gap_before:.3f}s, gap after={gap_after:.3f}s"
        )
        # After reset the gap must be close to initial (≤ initial + 50% jitter = 0.075s)
        assert gap_after < 0.12, (
            f"Post-signal gap {gap_after:.3f}s > initial_poll_interval × 1.5 — reset failed"
        )

        await poller.close()


# ===========================================================================
# 7. Race Conditions and Safety
# ===========================================================================


class TestRaceConditionsAndSafety:
    """Concurrent and edge-case operations must never crash or corrupt state."""

    @pytest.mark.asyncio
    async def test_dispatch_to_unregistered_task_returns_false(self) -> None:
        """_dispatch_signal for an unknown task_id must return False without raising."""
        poller = _poller()
        ghost = _proto("ghost", "cancel")
        assert poller._dispatch_signal(ghost) is False

    @pytest.mark.asyncio
    async def test_interleaved_register_unregister_does_not_corrupt_state(self) -> None:
        """20 tasks staggered-register and then unregister concurrently.
        After all complete, _task_queues must be empty and the poller must not crash.
        """
        poll_count = 0

        async def poll_fn(task_ids: list[str]) -> list:
            nonlocal poll_count
            poll_count += 1
            await asyncio.sleep(0)
            return []

        poller = _poller(poll_fn, poll_interval=0.05, initial=0.02)

        async def _wave(tid: str, delay: float) -> None:
            await asyncio.sleep(delay)
            poller.register(tid)
            await asyncio.sleep(0.04)
            poller.unregister(tid)

        await asyncio.gather(*[_wave(f"t_{i}", i * 0.005) for i in range(20)])
        await asyncio.sleep(0.1)

        assert not poller._task_queues, (
            f"Leaked task queues after all unregisters: {list(poller._task_queues)}"
        )
        assert poll_count >= 1

    @pytest.mark.asyncio
    async def test_signal_stop_instance_unblocks_blocked_consumer(self) -> None:
        """signal_stop_instance must deliver a None sentinel to a consumer already
        blocked on queue.get(), waking it up instantly.
        """
        _SharedPoller._instances["adv_key"] = _poller(
            poll_interval=60.0, initial=60.0  # Effectively never polls organically
        )
        poller = _SharedPoller._instances["adv_key"]
        queue = poller.register("victim")

        blocked_get = asyncio.create_task(queue.get())
        await asyncio.sleep(0.01)  # Confirm the get is truly blocked

        _SharedPoller.signal_stop_instance("adv_key", "victim")

        item = await asyncio.wait_for(blocked_get, timeout=0.5)
        assert item is None, "signal_stop_instance must deliver None to unblock the consumer"
        assert "victim" not in poller._task_queues, "victim must be unregistered"

    @pytest.mark.asyncio
    async def test_close_unblocks_all_blocked_consumers(self) -> None:
        """close() must unblock every consumer currently waiting on queue.get()."""
        N = 10
        poller = _poller()
        queues = [poller.register(f"t{i}") for i in range(N)]

        blocked = [asyncio.create_task(q.get()) for q in queues]
        await asyncio.sleep(0.01)

        await poller.close()

        for i, task in enumerate(blocked):
            result = await asyncio.wait_for(task, timeout=0.5)
            assert result is None, f"Queue t{i}: close() must deliver None, got {result!r}"

    @pytest.mark.asyncio
    async def test_same_signal_not_delivered_twice_across_many_polls(self) -> None:
        """Timestamp-based dedup must prevent re-delivery of the same signal even
        when the poll_fn returns it on every call (simulating a slow-to-advance server).
        """
        fixed_ts = 99_999
        repeated_proto = _proto("t1", "start", ts=fixed_ts)
        call_count = 0

        async def poll_fn(task_ids: list[str]) -> list:
            nonlocal call_count
            call_count += 1
            # Always return the same proto — dedup must suppress all but the first
            return [repeated_proto] if call_count <= 6 else []

        poller = _poller(poll_fn, poll_interval=0.05, initial=0.02)
        queue = poller.register("t1")

        await asyncio.sleep(0.35)  # Enough for 6+ polls

        delivered = []
        while not queue.empty():
            item = queue.get_nowait()
            if item is not None:
                delivered.append(item)

        assert len(delivered) == 1, (
            f"Dedup failed: {len(delivered)} copies of the same signal delivered "
            f"across {call_count} polls (expected exactly 1)"
        )

        await poller.close()

    @pytest.mark.asyncio
    async def test_terminal_dispatch_followed_by_second_terminal_is_noop(self) -> None:
        """Dispatching a second terminal signal for an already-removed task must be safe
        (_dispatch_signal returns False, no crash, no duplicate poison pill).
        """
        poller = _poller()
        queue = poller.register("t1")

        stop1 = _proto("t1", "stop", ts=1)
        stop2 = _proto("t1", "cancel", ts=2)

        r1 = poller._dispatch_signal(stop1)
        assert r1 is True
        assert "t1" not in poller._task_queues  # auto-removed

        r2 = poller._dispatch_signal(stop2)
        assert r2 is False  # task is gone — must return False, not crash

        # Queue must have exactly 2 items: the stop signal + the poison pill
        assert queue.qsize() == 2
        assert queue.get_nowait() is stop1
        assert queue.get_nowait() is None

    @pytest.mark.asyncio
    async def test_poll_fn_exception_does_not_kill_poller(self) -> None:
        """If poll_fn raises, the poller must log a warning and continue polling."""
        call_count = 0
        poll_times: list[float] = []

        async def flaky_poll_fn(task_ids: list[str]) -> list:
            nonlocal call_count
            call_count += 1
            poll_times.append(asyncio.get_event_loop().time())
            if call_count <= 3:
                msg = f"Simulated transient failure on call {call_count}"
                raise RuntimeError(msg)
            return []

        poller = _poller(flaky_poll_fn, poll_interval=0.3, initial=0.05)
        poller.register("t1")

        await asyncio.sleep(1.5)

        assert call_count >= 4, (
            f"Poller died after exception — only {call_count} calls made, expected ≥4"
        )
        assert poller._task is not None and not poller._task.done(), (
            "Poller task must still be alive after recovering from exceptions"
        )

        await poller.close()

    @pytest.mark.asyncio
    async def test_signal_without_created_at_always_dispatched(self) -> None:
        """A signal with no created_at field has ts_key=None, bypassing dedup entirely.
        It must always be dispatched regardless of prior signals seen.
        """
        poller = _poller()
        queue = poller.register("t1")

        # Build a proto with NO created_at
        p = task_manager_message_pb2.Task(
            task_id="t1",
            mission_id=_MISSION,
            setup_id=_SETUP,
            setup_version_id=_VERSION,
            action="start",
            cancellation_reason="none",
        )
        p.payload.CopyFrom(Struct())
        # Intentionally do NOT set created_at

        r1 = poller._dispatch_signal(p)
        r2 = poller._dispatch_signal(p)  # Same proto, no timestamp — must dispatch twice

        assert r1 is True
        assert r2 is True
        assert queue.qsize() == 2, "Both no-timestamp signals must be dispatched (no dedup)"

        await poller.close()
