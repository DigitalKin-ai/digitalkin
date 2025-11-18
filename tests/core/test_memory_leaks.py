"""Memory leak detection tests for critical components.

This module contains tests specifically designed to detect and prevent
memory leaks in task managers, job managers, and related components.
"""

import asyncio
import gc
import weakref
from typing import Any, ClassVar, NoReturn
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
from digitalkin.core.task_manager.remote_task_manager import RemoteTaskManager
from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.core.task_manager.task_session import TaskSession
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_config import ServicesConfig
from digitalkin.services.services_models import ServicesMode, ServicesStrategy

# Set timeout for all tests in this file (30 seconds)
pytestmark = pytest.mark.timeout(30)


class MockModule(BaseModule):
    """Mock module for memory leak testing."""

    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
    services_config_params: ClassVar[dict[str, dict[str, str | None] | None]] = {}
    services_config: ClassVar[ServicesConfig] = ServicesConfig(
        services_config_strategies={}, services_config_params={}, mode=ServicesMode.LOCAL
    )

    def __init__(self, job_id: str, mission_id: str, setup_id: str, setup_version_id: str) -> None:
        # Skip service initialization for tests
        self.job_id = job_id
        self.mission_id = mission_id
        self.setup_id = setup_id
        self.setup_version_id = setup_version_id
        self.large_data = b"x" * (1024 * 1024)  # 1MB of data for memory tracking

    def _init_strategies(self, mission_id: str, setup_id: str, setup_version_id: str) -> dict[str, Any]:
        """Override to skip service initialization in tests."""
        return {}

    async def initialize(self, context: Any, setup_data: Any) -> None:
        """Initialize the module."""

    async def run(self) -> None:
        """Run the module."""
        await asyncio.sleep(0.01)

    async def cleanup(self) -> None:
        """Clean up the module."""

    async def stop(self) -> None:
        """Stop the module."""


class TestTaskManagerMemoryLeaks:
    """Test memory leaks in task managers."""

    @pytest.mark.asyncio
    async def test_local_task_manager_cleanup_on_cancel(self):
        """Test that LocalTaskManager properly cleans up resources on task cancellation."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.merge = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_123", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = LocalTaskManager()
            weak_refs = []

            # Helper function to create tasks without persisting loop variables
            async def create_task_and_track(task_idx: int) -> None:
                module = MockModule(f"job-{task_idx}", "mission", "setup", "version")
                weak_refs.append(weakref.ref(module))

                async def task_coro() -> None:
                    await asyncio.sleep(0.5)

                await manager.create_task(f"task-{task_idx}", "mission", module, task_coro())
                # module and task_coro go out of scope here

            # Create multiple tasks
            for i in range(5):
                await create_task_and_track(i)

            # Cancel all tasks
            await manager.cancel_all_tasks("mission", timeout=1.0)

            # Verify sessions are cleaned up
            assert len(manager.tasks_sessions) == 0
            assert len(manager.tasks) == 0

            # Force garbage collection
            gc.collect()

            # Verify all modules can be garbage collected
            # Since sessions are cleaned up, modules should be collectible
            for weak_ref in weak_refs:
                assert weak_ref() is None, "Module not garbage collected after cancellation"

    @pytest.mark.asyncio
    async def test_remote_task_manager_cleanup_on_shutdown(self):
        """Test that RemoteTaskManager properly cleans up on shutdown."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.merge = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_123", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = RemoteTaskManager()

            # Create tasks
            for i in range(3):
                module = MockModule(f"job-{i}", "mission", "setup", "version")

                async def dummy_coro() -> None:
                    pass

                await manager.create_task(f"task-{i}", "mission", module, dummy_coro())

            # Shutdown should clean everything
            await manager.shutdown("mission")

            assert len(manager.tasks_sessions) == 0
            assert len(manager.tasks) == 0

    @pytest.mark.asyncio
    async def test_task_session_queue_memory_cleanup(self):
        """Test that TaskSession properly cleans up queue memory."""
        mock_db = MagicMock(spec=SurrealDBConnection)
        mock_db.create = AsyncMock(return_value={"id": "signal_123"})
        mock_db.merge = AsyncMock()
        mock_db.update = AsyncMock()
        mock_db.close = AsyncMock()

        module = MockModule("job-1", "mission", "setup", "version")

        session = TaskSession(task_id="task-1", mission_id="mission", db=mock_db, module=module)

        # Fill queue with large items
        for i in range(100):
            await session.queue.put({"data": b"x" * 10000})  # 10KB each

        # Track queue with weak reference
        queue_ref = weakref.ref(session.queue)

        # Clean up session properly (clears queue, stops module, closes DB connection)
        await session.cleanup()

        # Delete session
        del session
        gc.collect()

        # Queue should be garbage collectible
        assert queue_ref() is None or queue_ref().empty()

    @pytest.mark.asyncio
    async def test_context_manager_cleanup(self):
        """Test that context managers properly clean up resources."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.merge = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_123", AsyncMock()))
            mock_conn.return_value = mock_db

            # Track if cleanup was called
            cleanup_called = False

            async def track_cleanup(*args, **kwargs) -> None:
                nonlocal cleanup_called
                cleanup_called = True

            # Use context manager
            async with LocalTaskManager() as manager:
                manager.shutdown = track_cleanup  # type: ignore

                # Create a task
                module = MockModule("job-1", "mission", "setup", "version")

                async def task() -> None:
                    await asyncio.sleep(0.01)

                await manager.create_task("task-1", "mission", module, task())

            # Verify cleanup was called on exit
            assert cleanup_called

    @pytest.mark.asyncio
    async def test_circular_reference_prevention(self):
        """Test that circular references don't prevent garbage collection."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.merge = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_123", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = LocalTaskManager()

            module = MockModule("job-1", "mission", "setup", "version")

            # Create circular reference scenario
            module.manager_ref = manager  # type: ignore
            manager.module_ref = module  # type: ignore

            weak_module = weakref.ref(module)
            weak_manager = weakref.ref(manager)

            async def task() -> None:
                pass

            await manager.create_task("task-1", "mission", module, task())
            await manager.shutdown("mission")

            # Break circular references
            del module.manager_ref  # type: ignore
            del manager.module_ref  # type: ignore
            del module
            del manager

            gc.collect()

            # Both should be collectible after breaking circular refs
            assert weak_module() is None
            assert weak_manager() is None


class TestJobManagerMemoryLeaks:
    """Test memory leaks in job managers."""

    @pytest.mark.asyncio
    async def test_single_job_manager_queue_cleanup(self):
        """Test that SingleJobManager cleans up queues properly."""
        with (
            patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn,
            patch(
                "digitalkin.core.job_manager.single_job_manager.ConnectionFactory.create_surreal_connection"
            ) as mock_conn_factory,
        ):
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.merge = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_123", AsyncMock()))
            mock_conn.return_value = mock_db
            mock_conn_factory.return_value = AsyncMock()

            manager = SingleJobManager(MockModule, ServicesMode.LOCAL)
            await manager.start()

            # Create jobs with queues
            job_ids = []
            for i in range(5):
                job_id = f"job-{i}"
                job_ids.append(job_id)

                # Simulate job creation
                module = MockModule(job_id, "mission", "setup", "version")
                session = TaskSession(job_id, "mission", mock_db, module)
                manager.tasks_sessions[job_id] = session

                # Fill queue with data
                for j in range(10):
                    await session.queue.put({"data": f"item-{j}"})

            # Stop all modules should clean queues
            await manager.stop_all_modules()

            # Verify all sessions and queues cleaned
            assert len(manager.tasks_sessions) == 0

    @pytest.mark.asyncio
    async def test_taskiq_job_manager_stream_consumer_cleanup(self):
        """Test that TaskiqJobManager cleans up stream consumers."""
        with (
            patch("digitalkin.core.job_manager.taskiq_job_manager.TASKIQ_BROKER") as mock_broker,
            patch(
                "digitalkin.core.job_manager.taskiq_job_manager.TaskiqJobManager._define_consumer"
            ) as mock_consumer_factory,
            patch(
                "digitalkin.core.job_manager.taskiq_job_manager.ConnectionFactory.create_surreal_connection"
            ) as mock_conn_factory,
        ):
            mock_broker.startup = AsyncMock()
            mock_consumer = AsyncMock()
            mock_consumer_factory.return_value = mock_consumer

            # Create mock connection
            mock_conn = AsyncMock()
            mock_conn.init_surreal_instance = AsyncMock()
            mock_conn.close = AsyncMock()
            mock_conn.create = AsyncMock(return_value={"id": "signal_123"})
            mock_conn.update = AsyncMock()
            mock_conn.merge = AsyncMock()
            mock_conn.start_live = AsyncMock(return_value=("live_123", AsyncMock()))

            async def create_connection(*args, **kwargs):
                return mock_conn

            mock_conn_factory.side_effect = create_connection

            # Import here to avoid issues with patches
            from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

            manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)
            await manager.start()

            # Track consumer task
            weakref.ref(manager.stream_consumer_task)

            # Stop should clean up consumer
            await manager._stop()

            # Verify consumer cleaned up
            mock_consumer.close.assert_awaited_once()
            assert manager.stream_consumer_task.cancelled()

            # Verify job queues cleared
            assert len(manager.job_queues) == 0

    @pytest.mark.asyncio
    async def test_job_registry_cleanup(self):
        """Test that job registries (task sessions) are properly cleaned up."""
        with patch("digitalkin.core.job_manager.taskiq_job_manager.TASKIQ_BROKER"):
            with patch("digitalkin.core.job_manager.taskiq_job_manager.TaskiqJobManager._start"):
                with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection"):
                    from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager
                    from digitalkin.core.task_manager.task_session import TaskSession

                    manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)
                    mock_db = Mock()

                    # Populate task sessions and job queues
                    for i in range(10):
                        job_id = f"job-{i}"
                        mock_module = Mock(spec=BaseModule)
                        manager.tasks_sessions[job_id] = TaskSession(job_id, "test_mission", mock_db, mock_module)
                        manager.job_queues[job_id] = asyncio.Queue()

                    # Track memory
                    sessions_size_before = len(manager.tasks_sessions)
                    queues_size_before = len(manager.job_queues)

                    # Clear specific job
                    job_to_remove = "job-5"
                    manager.tasks_sessions.pop(job_to_remove, None)
                    manager.job_queues.pop(job_to_remove, None)

                    # Verify removal
                    assert len(manager.tasks_sessions) == sessions_size_before - 1
                    assert len(manager.job_queues) == queues_size_before - 1
                    assert job_to_remove not in manager.tasks_sessions
                    assert job_to_remove not in manager.job_queues


class TestLiveQueryCleanup:
    """Test cleanup of SurrealDB live queries with real database."""

    @pytest.mark.asyncio
    async def test_live_query_cleanup_on_task_cancel(self):
        """Test that live queries are properly closed when tasks are cancelled."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.merge = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_123", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = LocalTaskManager()
            module = MockModule("job-1", "mission", "setup", "version")

            async def task_with_live_query() -> None:
                await asyncio.sleep(0.5)

            await manager.create_task("task-1", "mission", module, task_with_live_query())

            # Cancel should close DB connection (which kills live queries)
            await manager.cancel_task("task-1", "mission", timeout=1.0)

            # Verify task session was cleaned up
            assert "task-1" not in manager.tasks_sessions

    @pytest.mark.asyncio
    async def test_multiple_live_queries_cleanup(self):
        """Test cleanup of multiple concurrent live queries."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.merge = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_123", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = LocalTaskManager()

            # Create multiple tasks with live queries
            for i in range(5):
                module = MockModule(f"job-{i}", "mission", "setup", "version")

                async def task() -> None:
                    await asyncio.sleep(0.01)

                await manager.create_task(f"task-{i}", "mission", module, task())

            # Shutdown should close all connections
            await manager.shutdown("mission")

            # Verify all tasks cleaned up
            assert len(manager.tasks_sessions) == 0
            assert len(manager.tasks) == 0


class TestAsyncTaskCleanup:
    """Test cleanup of asyncio tasks."""

    @pytest.mark.asyncio
    async def test_fire_and_forget_task_prevention(self):
        """Test that fire-and-forget tasks are properly managed."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.merge = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_123", AsyncMock()))
            mock_conn.return_value = mock_db

            uncaught_exceptions = []

            def exception_handler(loop, context) -> None:
                uncaught_exceptions.append(context)

            loop = asyncio.get_event_loop()
            old_handler = loop.get_exception_handler()
            loop.set_exception_handler(exception_handler)

            try:
                manager = LocalTaskManager()

                module = MockModule("job-1", "mission", "setup", "version")

                async def failing_task() -> NoReturn:
                    await asyncio.sleep(0.01)
                    msg = "Task failed"
                    raise ValueError(msg)

                # This should be properly managed, not fire-and-forget
                await manager.create_task("task-1", "mission", module, failing_task())

                # Wait for task to fail
                await asyncio.sleep(0.1)

                # Cancel to clean up
                await manager.cancel_task("task-1", "mission", timeout=0.1)

                # No uncaught exceptions should occur
                assert len(uncaught_exceptions) == 0, f"Uncaught exceptions: {uncaught_exceptions}"

            finally:
                loop.set_exception_handler(old_handler)

    @pytest.mark.asyncio
    async def test_background_task_cleanup(self):
        """Test that background tasks are properly cleaned up."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.merge = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_123", AsyncMock()))
            mock_conn.return_value = mock_db

            all_tasks_before = asyncio.all_tasks()

            manager = LocalTaskManager()

            # Create several background tasks
            for i in range(3):
                module = MockModule(f"job-{i}", "mission", "setup", "version")

                async def background_work() -> None:
                    try:
                        while True:
                            await asyncio.sleep(0.1)
                    except asyncio.CancelledError:
                        # Properly handle cancellation
                        raise

                await manager.create_task(f"task-{i}", "mission", module, background_work())

            # Get tasks after creation
            all_tasks_during = asyncio.all_tasks()
            new_tasks = all_tasks_during - all_tasks_before

            # Should have created new tasks
            assert len(new_tasks) > 0

            # Shutdown should cancel all tasks
            await manager.shutdown("mission", timeout=2.0)

            # Give time for cleanup
            await asyncio.sleep(0.1)

            # All manager tasks should be cancelled
            for task in manager.tasks.values():
                assert task.cancelled() or task.done()


class TestMemoryGrowthPrevention:
    """Test prevention of memory growth in long-running scenarios."""

    @pytest.mark.asyncio
    async def test_queue_memory_growth_prevention(self):
        """Test that queues don't grow unbounded."""
        with (
            patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn,
            patch(
                "digitalkin.core.job_manager.single_job_manager.ConnectionFactory.create_surreal_connection"
            ) as mock_conn_factory,
        ):
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.merge = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_123", AsyncMock()))
            mock_conn.return_value = mock_db
            mock_conn_factory.return_value = AsyncMock()

            manager = SingleJobManager(MockModule, ServicesMode.LOCAL)
            await manager.start()

            # Create a job with a bounded queue
            module = MockModule("job-1", "mission", "setup", "version")
            session = TaskSession("job-1", "mission", mock_db, module)
            manager.tasks_sessions["job-1"] = session

            # Try to overflow the queue
            queue_size = session.queue.maxsize if session.queue.maxsize > 0 else 1000

            # Fill queue to capacity
            for i in range(queue_size):
                session.queue.put_nowait({"data": f"item-{i}"})

            # Queue should be full
            assert session.queue.full() or session.queue.qsize() == queue_size

            # Further puts should not increase memory (would block or raise)
            with pytest.raises((asyncio.QueueFull, AttributeError)):
                session.queue.put_nowait({"overflow": "data"})

            # Clean up resources
            await manager.stop_all_modules()

    @pytest.mark.asyncio
    async def test_session_dict_memory_growth_prevention(self):
        """Test that session dictionaries don't grow unbounded."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.merge = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_123", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = LocalTaskManager(max_concurrent_tasks=5)

            # Try to exceed max concurrent tasks
            for i in range(5):
                module = MockModule(f"job-{i}", "mission", "setup", "version")

                async def task() -> None:
                    await asyncio.sleep(0.01)

                await manager.create_task(f"task-{i}", "mission", module, task())

            # Should not be able to create more tasks
            module = MockModule("job-overflow", "mission", "setup", "version")

            async def overflow_task() -> None:
                pass

            with pytest.raises(RuntimeError, match="Maximum concurrent tasks"):
                await manager.create_task("task-overflow", "mission", module, overflow_task())

            # Clean up
            await manager.shutdown("mission")
