"""Memory-related fixtures for testing.

This module provides reusable fixtures for memory testing, profiling,
and leak detection across the test suite.
"""

import asyncio
import gc
import sys
import tracemalloc
import weakref
from typing import Any, Callable, Dict, List, Optional, Tuple

import pytest
import pytest_asyncio


@pytest.fixture
def memory_tracker():
    """Fixture for tracking memory usage during tests."""

    class MemoryTracker:
        def __init__(self):
            self.snapshots: List[Tuple[str, int]] = []
            self.start_memory: Optional[int] = None
            self.tracemalloc_started = False

        def start(self):
            """Start memory tracking."""
            if not tracemalloc.is_tracing():
                tracemalloc.start()
                self.tracemalloc_started = True

            gc.collect()
            self.start_memory = self._get_current_memory()
            self.snapshots = [("start", self.start_memory)]

        def snapshot(self, label: str):
            """Take a memory snapshot with label."""
            gc.collect()
            current = self._get_current_memory()
            self.snapshots.append((label, current))
            return current

        def _get_current_memory(self) -> int:
            """Get current memory usage in bytes."""
            if sys.platform == "linux":
                try:
                    import resource
                    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
                except ImportError:
                    pass

            # Fallback to tracemalloc
            current, peak = tracemalloc.get_traced_memory()
            return current

        def get_growth(self) -> int:
            """Get memory growth since start."""
            if not self.snapshots:
                return 0
            current = self._get_current_memory()
            return current - self.start_memory

        def get_peak(self) -> int:
            """Get peak memory usage."""
            if not self.snapshots:
                return 0
            return max(memory for _, memory in self.snapshots)

        def assert_no_leak(self, threshold_kb: int = 1024):
            """Assert no memory leak above threshold."""
            growth = self.get_growth()
            growth_kb = growth / 1024

            assert growth_kb < threshold_kb, \
                f"Memory leak detected: {growth_kb:.2f} KB growth (threshold: {threshold_kb} KB)"

        def report(self) -> str:
            """Generate memory usage report."""
            if not self.snapshots:
                return "No memory snapshots taken"

            lines = ["Memory Usage Report:"]
            lines.append(f"Start: {self.start_memory / 1024 / 1024:.2f} MB")

            for label, memory in self.snapshots[1:]:
                growth = memory - self.start_memory
                lines.append(
                    f"{label}: {memory / 1024 / 1024:.2f} MB "
                    f"(+{growth / 1024:.2f} KB)"
                )

            peak = self.get_peak()
            total_growth = self.get_growth()
            lines.append(f"Peak: {peak / 1024 / 1024:.2f} MB")
            lines.append(f"Total Growth: {total_growth / 1024:.2f} KB")

            return "\n".join(lines)

        def cleanup(self):
            """Clean up tracking."""
            if self.tracemalloc_started:
                tracemalloc.stop()

    tracker = MemoryTracker()
    yield tracker
    tracker.cleanup()


@pytest.fixture
def weak_ref_tracker():
    """Fixture for tracking object lifecycle with weak references."""

    class WeakRefTracker:
        def __init__(self):
            self.refs: Dict[str, weakref.ReferenceType] = {}

        def track(self, name: str, obj: Any) -> weakref.ReferenceType:
            """Track an object with a weak reference."""
            ref = weakref.ref(obj)
            self.refs[name] = ref
            return ref

        def is_alive(self, name: str) -> bool:
            """Check if tracked object is still alive."""
            if name not in self.refs:
                return False
            return self.refs[name]() is not None

        def get(self, name: str) -> Optional[Any]:
            """Get tracked object if still alive."""
            if name not in self.refs:
                return None
            return self.refs[name]()

        def assert_garbage_collected(self, name: str, force_collect: bool = True):
            """Assert that object has been garbage collected."""
            if force_collect:
                gc.collect()

            assert not self.is_alive(name), \
                f"Object '{name}' was not garbage collected"

        def assert_all_collected(self, force_collect: bool = True):
            """Assert all tracked objects have been garbage collected."""
            if force_collect:
                gc.collect()

            alive = [name for name in self.refs if self.is_alive(name)]
            assert not alive, \
                f"Objects not garbage collected: {', '.join(alive)}"

        def clear(self):
            """Clear all references."""
            self.refs.clear()

    return WeakRefTracker()


@pytest.fixture
def queue_memory_monitor():
    """Fixture for monitoring asyncio.Queue memory usage."""

    class QueueMemoryMonitor:
        def __init__(self):
            self.queues: Dict[str, asyncio.Queue] = {}
            self.max_sizes: Dict[str, int] = {}

        def create_queue(self, name: str, maxsize: int = 0) -> asyncio.Queue:
            """Create a monitored queue."""
            queue = asyncio.Queue(maxsize=maxsize)
            self.queues[name] = queue
            self.max_sizes[name] = 0
            return queue

        async def put(self, name: str, item: Any):
            """Put item and track size."""
            if name not in self.queues:
                raise KeyError(f"Queue '{name}' not found")

            await self.queues[name].put(item)
            current_size = self.queues[name].qsize()
            self.max_sizes[name] = max(self.max_sizes[name], current_size)

        def get_max_size(self, name: str) -> int:
            """Get maximum size reached by queue."""
            return self.max_sizes.get(name, 0)

        def assert_queue_bounded(self, name: str, max_expected: int):
            """Assert queue never exceeded expected size."""
            actual_max = self.get_max_size(name)
            assert actual_max <= max_expected, \
                f"Queue '{name}' exceeded bounds: {actual_max} > {max_expected}"

        async def drain_all(self):
            """Drain all queues."""
            for queue in self.queues.values():
                while not queue.empty():
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

        def clear(self):
            """Clear all queues."""
            self.queues.clear()
            self.max_sizes.clear()

    return QueueMemoryMonitor()


@pytest_asyncio.fixture
async def connection_leak_detector():
    """Fixture for detecting connection leaks."""

    class ConnectionLeakDetector:
        def __init__(self):
            self.connections: List[weakref.ReferenceType] = []
            self.connection_count = 0

        def track_connection(self, conn: Any) -> Any:
            """Track a connection object."""
            self.connections.append(weakref.ref(conn))
            self.connection_count += 1
            return conn

        def get_alive_connections(self) -> List[Any]:
            """Get list of connections still alive."""
            gc.collect()
            return [ref() for ref in self.connections if ref() is not None]

        def assert_no_leaks(self):
            """Assert no connection leaks."""
            alive = self.get_alive_connections()
            assert not alive, \
                f"Connection leak detected: {len(alive)} connections still alive"

        def get_stats(self) -> Dict[str, int]:
            """Get connection statistics."""
            alive = self.get_alive_connections()
            return {
                "total_created": self.connection_count,
                "still_alive": len(alive),
                "properly_closed": self.connection_count - len(alive)
            }

    detector = ConnectionLeakDetector()
    yield detector

    # Check for leaks at fixture teardown
    detector.assert_no_leaks()


@pytest.fixture
def memory_stress_test():
    """Fixture for memory stress testing."""

    class MemoryStressTester:
        def __init__(self):
            self.allocations: List[Any] = []

        def allocate_mb(self, size_mb: int) -> bytes:
            """Allocate specified amount of memory."""
            data = b"x" * (size_mb * 1024 * 1024)
            self.allocations.append(data)
            return data

        def allocate_objects(self, count: int, size_bytes: int = 1024) -> List[bytes]:
            """Allocate many small objects."""
            objects = []
            for _ in range(count):
                obj = b"x" * size_bytes
                objects.append(obj)
                self.allocations.append(obj)
            return objects

        def clear(self):
            """Clear all allocations."""
            self.allocations.clear()
            gc.collect()

        async def stress_test_coroutine(
            self,
            coro: Callable,
            memory_mb: int = 100,
            duration_seconds: float = 1.0
        ):
            """Run coroutine under memory pressure."""
            # Allocate memory to create pressure
            self.allocate_mb(memory_mb)

            # Run coroutine
            try:
                result = await asyncio.wait_for(coro(), timeout=duration_seconds)
                return result
            finally:
                self.clear()

    return MemoryStressTester()


@pytest.fixture
def gc_monitor():
    """Fixture for monitoring garbage collection."""

    class GCMonitor:
        def __init__(self):
            self.initial_stats = gc.get_stats()
            self.collection_counts = gc.get_count()

        def force_collect(self) -> int:
            """Force garbage collection and return objects collected."""
            return gc.collect()

        def get_collection_delta(self) -> Tuple[int, int, int]:
            """Get change in collection counts for each generation."""
            current = gc.get_count()
            return tuple(
                current[i] - self.collection_counts[i]
                for i in range(len(current))
            )

        def assert_collected(self, min_objects: int = 1):
            """Assert that garbage collection occurred."""
            collected = self.force_collect()
            assert collected >= min_objects, \
                f"Expected at least {min_objects} objects collected, got {collected}"

        def disable(self):
            """Temporarily disable garbage collection."""
            gc.disable()

        def enable(self):
            """Re-enable garbage collection."""
            gc.enable()

        @property
        def is_enabled(self) -> bool:
            """Check if garbage collection is enabled."""
            return gc.isenabled()

    monitor = GCMonitor()
    yield monitor

    # Ensure GC is enabled at teardown
    if not monitor.is_enabled:
        monitor.enable()


@pytest.fixture
def task_memory_limiter():
    """Fixture for limiting memory usage in async tasks."""

    class TaskMemoryLimiter:
        def __init__(self):
            self.limits: Dict[str, int] = {}
            self.usage: Dict[str, int] = {}

        def set_limit(self, task_name: str, limit_mb: int):
            """Set memory limit for a task."""
            self.limits[task_name] = limit_mb * 1024 * 1024

        async def run_with_limit(
            self,
            task_name: str,
            coro: Callable,
            limit_mb: int = 100
        ):
            """Run coroutine with memory limit checking."""
            self.set_limit(task_name, limit_mb)

            # Get baseline memory
            gc.collect()
            if tracemalloc.is_tracing():
                baseline, _ = tracemalloc.get_traced_memory()
            else:
                baseline = 0

            try:
                result = await coro()

                # Check memory usage
                if tracemalloc.is_tracing():
                    current, peak = tracemalloc.get_traced_memory()
                    usage = peak - baseline
                    self.usage[task_name] = usage

                    if task_name in self.limits:
                        assert usage <= self.limits[task_name], \
                            f"Task '{task_name}' exceeded memory limit: " \
                            f"{usage / 1024 / 1024:.2f} MB > {limit_mb} MB"

                return result
            except Exception:
                raise

        def get_usage(self, task_name: str) -> int:
            """Get memory usage for a task."""
            return self.usage.get(task_name, 0)

    return TaskMemoryLimiter()


@pytest.fixture
def session_memory_tracker():
    """Fixture specifically for tracking TaskSession memory."""

    class SessionMemoryTracker:
        def __init__(self):
            self.sessions: Dict[str, weakref.ReferenceType] = {}
            self.session_queues: Dict[str, weakref.ReferenceType] = {}

        def track_session(self, session_id: str, session: Any):
            """Track a TaskSession instance."""
            self.sessions[session_id] = weakref.ref(session)
            if hasattr(session, 'queue'):
                self.session_queues[session_id] = weakref.ref(session.queue)

        def assert_session_cleaned(self, session_id: str):
            """Assert session and its resources are cleaned up."""
            gc.collect()

            # Check session object
            if session_id in self.sessions:
                assert self.sessions[session_id]() is None, \
                    f"Session '{session_id}' not garbage collected"

            # Check session queue
            if session_id in self.session_queues:
                queue_ref = self.session_queues[session_id]()
                if queue_ref is not None:
                    assert queue_ref.empty(), \
                        f"Session '{session_id}' queue not empty"

        def get_active_sessions(self) -> List[str]:
            """Get list of sessions still in memory."""
            gc.collect()
            return [
                sid for sid, ref in self.sessions.items()
                if ref() is not None
            ]

    return SessionMemoryTracker()