"""Memory profiling tests for performance monitoring.

This module contains tests that profile memory usage of critical components
to detect performance regressions and memory inefficiencies.
"""

import asyncio
import builtins
import contextlib
import gc
import sys
import tracemalloc
from typing import Any, ClassVar, NoReturn
from unittest.mock import AsyncMock, patch

import pytest

from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
from digitalkin.core.task_manager.remote_task_manager import RemoteTaskManager
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_config import ServicesConfig
from digitalkin.services.services_models import ServicesMode, ServicesStrategy

# Set timeout for all tests in this file (120 seconds)
pytestmark = pytest.mark.timeout(120)


class MockModule(BaseModule):
    """Mock module for memory profiling."""

    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
    services_config_params: ClassVar[dict[str, dict[str, str | None] | None]] = {}
    services_config: ClassVar[ServicesConfig] = ServicesConfig(
        services_config_strategies={}, services_config_params={}, mode=ServicesMode.LOCAL
    )

    def __init__(self, job_id: str, mission_id: str, setup_id: str, setup_version_id: str) -> None:
        super().__init__(job_id, mission_id, setup_id, setup_version_id)
        self.job_id = job_id
        self.mission_id = mission_id
        self.setup_id = setup_id
        self.setup_version_id = setup_version_id

    def _init_strategies(self, mission_id: str, setup_id: str, setup_version_id: str) -> dict[str, Any]:
        """Override to skip service initialization in tests."""
        return {
            "agent": None,
            "cost": None,
            "filesystem": None,
            "identity": None,
            "registry": None,
            "snapshot": None,
            "storage": None,
        }

    async def initialize(self, context: Any, setup_data: Any) -> None:
        """Initialize the module."""

    async def run(self) -> None:
        """Run the module."""
        await asyncio.sleep(0.01)

    async def cleanup(self) -> None:
        """Clean up the module."""


def get_memory_usage() -> int:
    """Get current memory usage in bytes."""
    gc.collect()
    if sys.platform == "linux":
        # Try to use resource module for more accurate memory measurement
        try:
            import resource

            return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        except ImportError:
            pass
    # Fallback to tracemalloc
    current, _peak = tracemalloc.get_traced_memory()
    return current


class TestTaskManagerMemoryProfile:
    """Memory profiling tests for task managers."""

    @pytest.mark.asyncio
    async def test_local_task_manager_memory_per_task(self):
        """Profile memory usage per task in LocalTaskManager."""
        tracemalloc.start()
        manager = LocalTaskManager(max_concurrent_tasks=100)

        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = AsyncMock()
            mock_conn.return_value = mock_db

            # Baseline memory
            gc.collect()
            baseline = get_memory_usage()

            # Create tasks and measure memory
            memory_per_task = []
            num_tasks = 20

            for i in range(num_tasks):
                module = MockModule(f"job-{i}", "mission", "setup", "version")

                async def task() -> None:
                    await asyncio.sleep(0.01)

                await manager.create_task(f"task-{i}", "mission", module, task())

                # Measure memory after each task
                gc.collect()
                current_memory = get_memory_usage()
                memory_increment = current_memory - baseline
                memory_per_task.append(memory_increment)

            # Calculate average memory per task
            avg_memory_per_task = sum(memory_per_task) / len(memory_per_task)

            # Clean up
            await manager.shutdown("mission")
            tracemalloc.stop()

            # Assert reasonable memory usage (< 1MB per task)
            assert avg_memory_per_task < 1024 * 1024, f"Average memory per task: {avg_memory_per_task / 1024:.2f} KB"

            # Check for memory leaks - memory should stabilize
            if len(memory_per_task) > 10:
                early_avg = sum(memory_per_task[:5]) / 5
                late_avg = sum(memory_per_task[-5:]) / 5
                growth_rate = (late_avg - early_avg) / early_avg if early_avg > 0 else 0

                # Memory growth should be minimal (< 20%)
                assert growth_rate < 0.2, f"Memory growth rate: {growth_rate * 100:.2f}%"

    @pytest.mark.asyncio
    async def test_remote_task_manager_memory_profile(self):
        """Profile memory usage in RemoteTaskManager."""
        tracemalloc.start()
        manager = RemoteTaskManager(max_concurrent_tasks=100)

        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = AsyncMock()
            mock_conn.return_value = mock_db

            # Baseline
            gc.collect()
            baseline = get_memory_usage()

            # Create remote tasks
            for i in range(10):
                module = MockModule(f"job-{i}", "mission", "setup", "version")

                async def dummy() -> None:
                    pass

                await manager.create_task(f"task-{i}", "mission", module, dummy())

            # Measure peak memory
            gc.collect()
            peak_memory = get_memory_usage()
            memory_used = peak_memory - baseline

            # Clean up
            await manager.shutdown("mission")
            tracemalloc.stop()

            # Remote task manager should use less memory (no actual task execution)
            assert memory_used < 500 * 1024, f"Memory used: {memory_used / 1024:.2f} KB"

    @pytest.mark.asyncio
    async def test_task_session_queue_memory_scaling(self):
        """Test memory scaling with queue size."""
        tracemalloc.start()

        with patch("digitalkin.core.task_manager.surrealdb_repository.SurrealDBConnection"):
            mock_db = AsyncMock()
            module = MockModule("job-1", "mission", "setup", "version")

            memory_by_queue_size: dict[int, int] = {}

            for queue_items in [10, 50, 100, 500, 1000]:
                gc.collect()
                baseline = get_memory_usage()

                session = TaskSession(task_id="task-1", mission_id="mission", db=mock_db, module=module)

                # Fill queue with items
                for i in range(queue_items):
                    await session.queue.put({"index": i, "data": "x" * 100})

                gc.collect()
                memory_used = get_memory_usage() - baseline
                memory_by_queue_size[queue_items] = memory_used

                # Clean up
                while not session.queue.empty():
                    session.queue.get_nowait()
                del session

            tracemalloc.stop()

            # Memory should scale roughly linearly with queue size
            if len(memory_by_queue_size) >= 2:
                sizes = sorted(memory_by_queue_size.keys())
                for i in range(1, len(sizes)):
                    size_ratio = sizes[i] / sizes[0]
                    memory_ratio = memory_by_queue_size[sizes[i]] / memory_by_queue_size[sizes[0]]

                    # Memory ratio should be within reasonable bounds of size ratio
                    # Allow for some overhead
                    assert memory_ratio < size_ratio * 2, (
                        f"Memory scaling issue: {sizes[i]} items uses {memory_ratio:.2f}x memory (expected ~{size_ratio}x)"
                    )


class TestJobManagerMemoryProfile:
    """Memory profiling tests for job managers."""

    @pytest.mark.asyncio
    async def test_single_job_manager_memory_baseline(self):
        """Test baseline memory usage of SingleJobManager."""
        tracemalloc.start()

        with patch("digitalkin.core.job_manager.single_job_manager.ConnectionFactory") as mock_factory:
            mock_conn = AsyncMock()
            mock_factory.create_surreal_connection.return_value = mock_conn

            gc.collect()
            baseline = get_memory_usage()

            manager = SingleJobManager(MockModule, ServicesMode.LOCAL)
            await manager.start()

            gc.collect()
            memory_after_init = get_memory_usage()
            init_memory = memory_after_init - baseline

            # Clean up
            await manager.stop_all_modules()

            gc.collect()
            memory_after_cleanup = get_memory_usage()
            cleanup_memory = memory_after_cleanup - baseline

            tracemalloc.stop()

            # Initial memory should be reasonable (< 10MB)
            assert init_memory < 10 * 1024 * 1024, f"Init memory: {init_memory / 1024 / 1024:.2f} MB"

            # Memory should be mostly freed after cleanup
            assert cleanup_memory < init_memory * 0.5, (
                f"Cleanup memory ({cleanup_memory / 1024:.2f} KB) > 50% of init ({init_memory / 1024:.2f} KB)"
            )

    @pytest.mark.asyncio
    async def test_taskiq_job_manager_stream_memory(self):
        """Test memory usage of TaskiqJobManager stream processing."""
        with patch("digitalkin.core.job_manager.taskiq_job_manager.TASKIQ_BROKER"):
            with patch("digitalkin.core.job_manager.taskiq_job_manager.TaskiqJobManager._start"):
                from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

                tracemalloc.start()
                gc.collect()
                baseline = get_memory_usage()

                manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

                # Simulate stream data
                stream_data = []
                for i in range(1000):
                    data = {"job_id": f"job-{i % 10}", "output_data": {"index": i, "payload": "x" * 1000}}
                    stream_data.append(data)

                # Process stream data
                for data in stream_data:
                    job_id = data["job_id"]
                    if job_id not in manager.job_queues:
                        manager.job_queues[job_id] = asyncio.Queue(maxsize=100)

                    # Simulate adding to queue (with overflow protection)
                    if not manager.job_queues[job_id].full():
                        manager.job_queues[job_id].put_nowait(data["output_data"])

                gc.collect()
                peak_memory = get_memory_usage() - baseline

                # Clear queues
                manager.job_queues.clear()

                gc.collect()
                after_clear = get_memory_usage() - baseline

                tracemalloc.stop()

                # Memory should be bounded by queue size limits
                assert peak_memory < 50 * 1024 * 1024, f"Peak memory: {peak_memory / 1024 / 1024:.2f} MB"

                # Memory should be freed after clearing
                assert after_clear < peak_memory * 0.1, f"Memory not freed: {after_clear / 1024:.2f} KB still used"


class TestMemoryLeakDetection:
    """Tests specifically designed to detect memory leaks."""

    @pytest.mark.asyncio
    async def test_repeated_task_creation_deletion(self):
        """Test for memory leaks in repeated task creation/deletion cycles."""
        tracemalloc.start()
        manager = LocalTaskManager()

        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = AsyncMock()
            mock_conn.return_value = mock_db

            memory_samples = []
            num_cycles = 10
            tasks_per_cycle = 5

            for cycle in range(num_cycles):
                # Create tasks
                for i in range(tasks_per_cycle):
                    module = MockModule(f"job-{cycle}-{i}", "mission", "setup", "version")

                    async def task() -> None:
                        await asyncio.sleep(0.01)

                    await manager.create_task(f"task-{cycle}-{i}", "mission", module, task())

                # Wait for tasks to complete
                await asyncio.sleep(0.1)

                # Cancel all tasks
                await manager.cancel_all_tasks("mission", timeout=0.1)

                # Measure memory
                gc.collect()
                memory_samples.append(get_memory_usage())

            tracemalloc.stop()

            # Check for memory leak pattern
            if len(memory_samples) > 5:
                # Compare early vs late samples
                early_avg = sum(memory_samples[:3]) / 3
                late_avg = sum(memory_samples[-3:]) / 3

                # Memory shouldn't grow significantly
                growth = (late_avg - early_avg) / early_avg if early_avg > 0 else 0
                assert growth < 0.5, f"Memory leak detected: {growth * 100:.2f}% growth over {num_cycles} cycles"

    @pytest.mark.asyncio
    async def test_exception_handling_memory_leak(self):
        """Test that exceptions don't cause memory leaks."""
        tracemalloc.start()
        manager = LocalTaskManager()

        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = AsyncMock()
            mock_conn.return_value = mock_db

            gc.collect()
            baseline = get_memory_usage()

            # Create tasks that fail
            for i in range(20):
                module = MockModule(f"job-{i}", "mission", "setup", "version")

                async def failing_task() -> NoReturn:
                    await asyncio.sleep(0.01)
                    msg = f"Task {i} failed"
                    raise ValueError(msg)

                with contextlib.suppress(builtins.BaseException):
                    await manager.create_task(f"task-{i}", "mission", module, failing_task())

                # Wait and cancel
                await asyncio.sleep(0.05)
                await manager.cancel_task(f"task-{i}", "mission", timeout=0.1)

            gc.collect()
            final_memory = get_memory_usage()
            memory_leaked = final_memory - baseline

            tracemalloc.stop()

            # Should not leak significant memory despite exceptions
            assert memory_leaked < 5 * 1024 * 1024, f"Memory leaked: {memory_leaked / 1024 / 1024:.2f} MB"


class TestMemoryOptimizations:
    """Tests to verify memory optimizations are working."""

    @pytest.mark.asyncio
    async def test_queue_memory_clearing_optimization(self):
        """Test that queue clearing optimization reduces memory."""
        with patch("digitalkin.core.task_manager.surrealdb_repository.SurrealDBConnection"):
            mock_db = AsyncMock()
            mock_db.close = AsyncMock()

            manager = LocalTaskManager()

            # Create task with full queue
            module = MockModule("job-1", "mission", "setup", "version")
            session = TaskSession("task-1", "mission", mock_db, module)
            manager.tasks_sessions["task-1"] = session

            # Fill queue with large items
            large_items = []
            for i in range(100):
                item = {"data": "x" * 10000}  # 10KB per item
                await session.queue.put(item)
                large_items.append(item)

            gc.collect()
            memory_before_cleanup = get_memory_usage()

            # Run cleanup (should clear queue)
            await manager._cleanup_task("task-1", "mission")

            # Force garbage collection
            del large_items
            gc.collect()
            memory_after_cleanup = get_memory_usage()

            # Memory should be significantly reduced
            memory_freed = memory_before_cleanup - memory_after_cleanup
            assert memory_freed > 500 * 1024, f"Only {memory_freed / 1024:.2f} KB freed, expected > 500 KB"

    @pytest.mark.asyncio
    async def test_connection_pooling_memory(self):
        """Test memory efficiency of connection pooling."""
        tracemalloc.start()

        with patch("digitalkin.core.common.factories.SurrealDBConnection") as mock_conn_class:
            connections = []

            def create_mock_conn(*args, **kwargs):
                conn = AsyncMock()
                conn.init_surreal_instance = AsyncMock()
                connections.append(conn)
                return conn

            mock_conn_class.side_effect = create_mock_conn

            from digitalkin.core.common import ConnectionFactory

            gc.collect()
            baseline = get_memory_usage()

            # Create multiple connections
            created_connections = []
            for i in range(20):
                conn = await ConnectionFactory.create_surreal_connection(database=f"db-{i}", auto_init=False)
                created_connections.append(conn)

            gc.collect()
            memory_with_connections = get_memory_usage() - baseline

            # Clear connections
            created_connections.clear()
            connections.clear()
            gc.collect()

            memory_after_clear = get_memory_usage() - baseline

            tracemalloc.stop()

            # Average memory per connection should be reasonable
            avg_memory_per_conn = memory_with_connections / 20
            assert avg_memory_per_conn < 100 * 1024, (
                f"Average memory per connection: {avg_memory_per_conn / 1024:.2f} KB"
            )

            # Memory should be freed
            assert memory_after_clear < memory_with_connections * 0.2, "Connections not properly garbage collected"


class TestMemoryBenchmarks:
    """Benchmark tests for memory usage."""

    @pytest.mark.asyncio
    async def test_benchmark_1000_tasks_memory(self, request):
        """Benchmark memory usage with 1000 tasks."""
        if not request.config.getoption("--run-slow", default=False):
            pytest.skip("Slow test - use --run-slow to run")

        tracemalloc.start()
        manager = LocalTaskManager(max_concurrent_tasks=1000)

        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = AsyncMock()
            mock_conn.return_value = mock_db

            gc.collect()
            baseline = get_memory_usage()

            # Create 1000 tasks
            for i in range(1000):
                module = MockModule(f"job-{i}", "mission", "setup", "version")

                async def task() -> None:
                    await asyncio.sleep(0.001)

                await manager.create_task(f"task-{i}", "mission", module, task())

                if i % 100 == 0:
                    gc.collect()
                    get_memory_usage() - baseline

            gc.collect()
            peak_memory = get_memory_usage() - baseline

            # Shutdown and cleanup
            await manager.shutdown("mission", timeout=5.0)

            gc.collect()
            final_memory = get_memory_usage() - baseline

            tracemalloc.stop()

            # Report results

            # Assert reasonable limits
            assert peak_memory < 100 * 1024 * 1024, "Peak memory > 100MB for 1000 tasks"
            assert final_memory < peak_memory * 0.1, "Memory not properly cleaned up"
