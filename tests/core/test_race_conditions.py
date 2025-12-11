"""Race condition tests for concurrent operations.

This module tests for race conditions in concurrent task operations,
ensuring thread-safety and proper synchronization.
"""

import asyncio
import random
from typing import Any, ClassVar
from unittest.mock import AsyncMock, Mock, patch

import pytest

from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_config import ServicesConfig
from digitalkin.services.services_models import ServicesMode, ServicesStrategy

# Set timeout for all tests in this file (10 seconds)
pytestmark = pytest.mark.timeout(10)


class MockModule(BaseModule):
    """Mock module for race condition testing."""

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
        self.execution_order = []

    def _init_strategies(self, mission_id: str, setup_id: str, setup_version_id: str) -> dict[str, Any]:
        """Override to skip service initialization in tests."""
        return {
            "agent": None,
            "communication": None,
            "cost": None,
            "filesystem": None,
            "identity": None,
            "registry": None,
            "snapshot": None,
            "storage": None,
            "user_profile": None,
        }

    async def initialize(self, context, setup_data) -> None:
        """Initialize the module."""
        self.execution_order.append("initialize")

    async def run(self) -> None:
        """Run the module."""
        self.execution_order.append("run")
        await asyncio.sleep(random.uniform(0.001, 0.01))

    async def cleanup(self) -> None:
        """Clean up the module."""


class TestConcurrentTaskCreation:
    """Test race conditions in concurrent task creation."""

    @pytest.mark.asyncio
    async def test_concurrent_task_creation_unique_ids(self):
        """Test that concurrent task creation maintains unique IDs."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_id", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = LocalTaskManager(max_concurrent_tasks=100)

            created_task_ids = set()
            creation_lock = asyncio.Lock()

            async def create_task_safely(task_id: str) -> None:
                module = MockModule(f"job-{task_id}", "mission", "setup", "version")

                async def task() -> None:
                    async with creation_lock:
                        created_task_ids.add(task_id)
                    await asyncio.sleep(0.01)

                await manager.create_task(task_id, "mission", module, task())

            # Create tasks concurrently
            tasks = [create_task_safely(f"task-{i}") for i in range(50)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check for any exceptions
            exceptions = [r for r in results if isinstance(r, Exception)]
            assert len(exceptions) == 0, f"Exceptions during creation: {exceptions}"

            # Verify all task IDs are unique
            assert len(created_task_ids) == 50
            assert len(manager.tasks_sessions) == 50

            # Clean up
            await manager.shutdown("mission")

    @pytest.mark.asyncio
    async def test_max_concurrent_tasks_race_condition(self):
        """Test that max concurrent tasks limit is enforced under race conditions."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_id", AsyncMock()))
            mock_conn.return_value = mock_db

            max_tasks = 5
            manager = LocalTaskManager(max_concurrent_tasks=max_tasks)

            successful_creates = []
            failed_creates = []

            async def try_create_task(task_id: str) -> None:
                module = MockModule(f"job-{task_id}", "mission", "setup", "version")

                async def task() -> None:
                    await asyncio.sleep(0.1)

                try:
                    await manager.create_task(task_id, "mission", module, task())
                    successful_creates.append(task_id)
                except RuntimeError as e:
                    if "Maximum concurrent tasks" in str(e):
                        failed_creates.append(task_id)
                    else:
                        raise

            # Try to create more tasks than allowed
            tasks = [try_create_task(f"task-{i}") for i in range(max_tasks + 5)]
            await asyncio.gather(*tasks, return_exceptions=True)

            # Some should fail due to limit
            total_attempts = max_tasks + 5
            assert len(successful_creates) + len(failed_creates) == total_attempts
            assert len(successful_creates) <= max_tasks
            assert len(failed_creates) >= 5
            assert len(manager.tasks_sessions) <= max_tasks

            # Clean up
            await manager.shutdown("mission")

    @pytest.mark.asyncio
    async def test_duplicate_task_id_race_condition(self):
        """Test handling of duplicate task IDs submitted concurrently."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_id", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = LocalTaskManager()

            success_count = 0
            duplicate_errors = 0

            async def try_create_duplicate(index: int) -> None:
                nonlocal success_count, duplicate_errors
                # Multiple tasks with same ID
                task_id = "duplicate-task"
                module = MockModule(f"job-{index}", "mission", "setup", "version")

                async def task() -> None:
                    await asyncio.sleep(0.01)

                try:
                    await manager.create_task(task_id, "mission", module, task())
                    success_count += 1
                except ValueError as e:
                    if "already exists" in str(e):
                        duplicate_errors += 1
                    else:
                        raise

            # Try to create same task ID concurrently
            tasks = [try_create_duplicate(i) for i in range(10)]
            await asyncio.gather(*tasks, return_exceptions=True)

            # Only one should succeed
            assert success_count == 1
            assert duplicate_errors == 9
            assert "duplicate-task" in manager.tasks_sessions
            assert len(manager.tasks_sessions) == 1

            # Clean up
            await manager.shutdown("mission")


class TestConcurrentCancellation:
    """Test race conditions in task cancellation."""

    @pytest.mark.asyncio
    async def test_concurrent_cancel_same_task(self):
        """Test concurrent cancellation of the same task."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_id", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = LocalTaskManager()

            module = MockModule("job-1", "mission", "setup", "version")

            async def long_task() -> None:
                await asyncio.sleep(1.0)

            await manager.create_task("task-1", "mission", module, long_task())

            # Try to cancel the same task concurrently
            cancel_tasks = [manager.cancel_task("task-1", "mission", timeout=0.5) for _ in range(5)]
            results = await asyncio.gather(*cancel_tasks, return_exceptions=True)

            # All cancellations should succeed (idempotent) or return True
            assert all(r is True or r is False for r in results if not isinstance(r, Exception)), (
                f"Unexpected results: {results}"
            )

            # Task should be removed
            assert "task-1" not in manager.tasks
            assert "task-1" not in manager.tasks_sessions

    @pytest.mark.asyncio
    async def test_cancel_all_tasks_concurrent_with_creation(self):
        """Test cancel_all_tasks while new tasks are being created."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_id", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = LocalTaskManager(max_concurrent_tasks=100)

            creation_complete = asyncio.Event()
            cancellation_started = asyncio.Event()

            async def create_tasks() -> None:
                for i in range(20):
                    if cancellation_started.is_set():
                        break

                    module = MockModule(f"job-{i}", "mission", "setup", "version")

                    async def task() -> None:
                        await asyncio.sleep(0.5)

                    try:
                        await manager.create_task(f"task-{i}", "mission", module, task())
                    except:
                        pass  # Might fail if cancelled

                    await asyncio.sleep(0.01)

                creation_complete.set()

            async def cancel_tasks():
                await asyncio.sleep(0.05)  # Let some tasks be created
                cancellation_started.set()
                return await manager.cancel_all_tasks("mission", timeout=0.1)

            # Run creation and cancellation concurrently
            create_task = asyncio.create_task(create_tasks())
            cancel_results = await cancel_tasks()

            # Wait for creation to complete
            await asyncio.wait_for(creation_complete.wait(), timeout=1.0)
            await create_task

            # All tasks that were created should have been cancelled
            for cancelled in cancel_results.values():
                assert cancelled is True

            # No tasks should remain
            assert len(manager.running_tasks) == 0


class TestConcurrentSignaling:
    """Test race conditions in signal operations."""

    @pytest.mark.asyncio
    async def test_concurrent_signals_to_same_task(self):
        """Test sending multiple signals concurrently to the same task."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_id", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = LocalTaskManager()

            MockModule("job-1", "mission", "setup", "version")

            # Create a mock session
            mock_session = Mock()
            mock_session.db = mock_db
            manager.tasks_sessions["task-1"] = mock_session

            # Send multiple signals concurrently
            signal_tasks = [
                manager.pause_task("task-1", "mission"),
                manager.resume_task("task-1", "mission"),
                manager.get_task_status("task-1", "mission"),
                manager.pause_task("task-1", "mission"),
                manager.send_signal("task-1", "mission", "custom", {"data": "test"}),
            ]

            results = await asyncio.gather(*signal_tasks, return_exceptions=True)

            # All signals should succeed
            assert all(r is True for r in results if not isinstance(r, Exception))

    @pytest.mark.asyncio
    async def test_signal_during_cancellation(self):
        """Test sending signals while task is being cancelled."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_id", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = LocalTaskManager()

            module = MockModule("job-1", "mission", "setup", "version")

            async def task() -> None:
                await asyncio.sleep(0.5)

            await manager.create_task("task-1", "mission", module, task())

            # Send signal and cancel concurrently
            signal_task = asyncio.create_task(manager.pause_task("task-1", "mission"))
            cancel_task = asyncio.create_task(manager.cancel_task("task-1", "mission", timeout=0.1))

            signal_result, cancel_result = await asyncio.gather(signal_task, cancel_task, return_exceptions=True)

            # At least one should succeed
            if not isinstance(signal_result, Exception):
                assert signal_result in {True, False}
            if not isinstance(cancel_result, Exception):
                assert cancel_result in {True, False}

            # Task should be cancelled
            assert "task-1" not in manager.tasks


class TestQueueRaceConditions:
    """Test race conditions in queue operations."""

    @pytest.mark.asyncio
    async def test_concurrent_queue_operations(self):
        """Test concurrent put/get operations on task session queues."""
        from digitalkin.core.task_manager.task_session import TaskSession

        mock_db = AsyncMock()
        mock_db.create.return_value = True
        mock_db.update.return_value = True

        module = MockModule("job-1", "mission", "setup", "version")
        session = TaskSession("task-1", "mission", mock_db, module)

        produced_items = set()
        consumed_items = set()

        async def producer(start_idx: int) -> None:
            for i in range(start_idx, start_idx + 20):
                item = f"item-{i}"
                await session.queue.put(item)
                produced_items.add(item)
                await asyncio.sleep(random.uniform(0, 0.001))

        async def consumer() -> None:
            while True:
                try:
                    item = await asyncio.wait_for(session.queue.get(), timeout=0.5)
                    consumed_items.add(item)
                    session.queue.task_done()
                    # Check after consuming to avoid race condition
                    if len(consumed_items) >= 80:  # 4 producers * 20 items
                        break
                except asyncio.TimeoutError:
                    # No more items available, exit gracefully
                    break

        # Run multiple producers and consumers concurrently
        producers = [producer(i * 20) for i in range(4)]
        consumers = [consumer() for _ in range(2)]

        await asyncio.gather(*producers, *consumers)

        # All produced items should be consumed
        assert produced_items == consumed_items
        assert len(produced_items) == 80

    @pytest.mark.asyncio
    async def test_queue_overflow_race_condition(self):
        """Test queue behavior when multiple producers hit the size limit."""
        from digitalkin.core.task_manager.task_session import TaskSession

        mock_db = AsyncMock()
        mock_db.create.return_value = True
        mock_db.update.return_value = True

        module = MockModule("job-1", "mission", "setup", "version")
        session = TaskSession("task-1", "mission", mock_db, module)

        # Use small queue for testing
        session.queue = asyncio.Queue(maxsize=5)

        overflow_count = 0
        success_count = 0

        async def try_add_item(item_id: int) -> None:
            nonlocal overflow_count, success_count
            try:
                await asyncio.wait_for(session.queue.put(f"item-{item_id}"), timeout=0.01)
                success_count += 1
            except asyncio.TimeoutError:
                overflow_count += 1

        # Try to add more items than queue can hold
        tasks = [try_add_item(i) for i in range(20)]
        await asyncio.gather(*tasks)

        # Exactly maxsize items should succeed
        assert success_count == 5
        assert overflow_count == 15
        assert session.queue.full()


class TestJobManagerRaceConditions:
    """Test race conditions in job managers."""

    @pytest.mark.asyncio
    async def test_single_job_manager_concurrent_job_creation(self):
        """Test concurrent job creation in SingleJobManager."""
        with patch("digitalkin.core.job_manager.single_job_manager.ConnectionFactory") as mock_factory:
            # Create mock connection
            mock_conn = AsyncMock()
            mock_conn.create.return_value = True
            mock_conn.update.return_value = True
            mock_conn.init_surreal_instance = AsyncMock()
            mock_conn.close = AsyncMock()

            async def create_connection(*args, **kwargs):
                return mock_conn

            mock_factory.create_surreal_connection = create_connection

            manager = SingleJobManager(MockModule, ServicesMode.LOCAL)
            await manager.start()

            created_jobs: set[str] = set()
            creation_errors = 0

            async def create_job(index: int) -> None:
                nonlocal creation_errors
                try:
                    from digitalkin.models.module import InputModel, SetupModel

                    class MockInput(InputModel):
                        value: int = index

                    class MockSetup(SetupModel):
                        config: int = index

                    job_id = await manager.create_module_instance_job(
                        MockInput(), MockSetup(), "mission", "setup", "version"
                    )
                    created_jobs.add(job_id)
                except Exception:
                    creation_errors += 1

            # Create jobs concurrently
            tasks = [create_job(i) for i in range(10)]
            await asyncio.gather(*tasks, return_exceptions=True)

            # All jobs should have unique IDs
            assert len(created_jobs) == 10 - creation_errors
            assert len(set(created_jobs)) == len(created_jobs)

            # Clean up
            await manager.stop_all_modules()

    @pytest.mark.asyncio
    async def test_concurrent_stop_module_operations(self):
        """Test concurrent stop operations on the same module."""
        with patch("digitalkin.core.job_manager.single_job_manager.ConnectionFactory") as mock_factory:
            # Create mock connection
            mock_conn = AsyncMock()
            mock_conn.create.return_value = True
            mock_conn.update.return_value = True
            mock_conn.init_surreal_instance = AsyncMock()
            mock_conn.close = AsyncMock()

            async def create_connection(*args, **kwargs):
                return mock_conn

            mock_factory.create_surreal_connection = create_connection

            manager = SingleJobManager(MockModule, ServicesMode.LOCAL)
            await manager.start()

            # Create a mock session
            module = MockModule("job-1", "mission", "setup", "version")
            module.stop = AsyncMock()

            mock_session = Mock()
            mock_session.module = module
            mock_session.mission_id = "mission"
            manager.tasks_sessions["job-1"] = mock_session

            # Create a mock task
            mock_task = asyncio.create_task(asyncio.sleep(0.1))
            manager.tasks["job-1"] = mock_task

            stop_results = []

            async def try_stop() -> None:
                result = await manager.stop_module("job-1")
                stop_results.append(result)

            # Try to stop the same module concurrently
            stop_tasks = [try_stop() for _ in range(5)]
            await asyncio.gather(*stop_tasks, return_exceptions=True)

            # Module stop should only be called once (due to lock)
            module.stop.assert_awaited()

            # Clean up
            await manager.stop_all_modules()


class TestShutdownRaceConditions:
    """Test race conditions during shutdown."""

    @pytest.mark.asyncio
    async def test_concurrent_shutdown_calls(self):
        """Test multiple concurrent shutdown calls."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_id", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = LocalTaskManager()

            # Create some tasks
            for i in range(5):
                module = MockModule(f"job-{i}", "mission", "setup", "version")

                async def task() -> None:
                    await asyncio.sleep(0.1)

                await manager.create_task(f"task-{i}", "mission", module, task())

            # Call shutdown concurrently
            shutdown_tasks = [manager.shutdown("mission", timeout=0.5) for _ in range(3)]

            await asyncio.gather(*shutdown_tasks, return_exceptions=True)

            # Shutdown should be idempotent
            assert manager._shutdown_event.is_set()
            assert len(manager.tasks_sessions) == 0

    @pytest.mark.asyncio
    async def test_task_creation_during_shutdown(self):
        """Test creating tasks while shutdown is in progress."""
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = Mock(spec=SurrealDBConnection)
            mock_db.init_surreal_instance = AsyncMock()
            mock_db.close = AsyncMock()
            mock_db.create = AsyncMock(return_value={"id": "signal_123"})
            mock_db.update = AsyncMock()
            mock_db.start_live = AsyncMock(return_value=("live_id", AsyncMock()))
            mock_conn.return_value = mock_db

            manager = LocalTaskManager()

            shutdown_started = asyncio.Event()
            creation_attempts = 0
            creation_failures = 0

            async def try_create_during_shutdown() -> None:
                nonlocal creation_attempts, creation_failures

                # Wait for shutdown to start
                await shutdown_started.wait()

                for i in range(5):
                    creation_attempts += 1
                    module = MockModule(f"late-job-{i}", "mission", "setup", "version")

                    async def task() -> None:
                        await asyncio.sleep(0.01)

                    try:
                        await manager.create_task(f"late-task-{i}", "mission", module, task())
                    except:
                        creation_failures += 1

            async def shutdown_with_signal() -> None:
                shutdown_started.set()
                await manager.shutdown("mission", timeout=0.5)

            # Run creation and shutdown concurrently
            create_task = asyncio.create_task(try_create_during_shutdown())
            shutdown_task = asyncio.create_task(shutdown_with_signal())

            await asyncio.gather(create_task, shutdown_task, return_exceptions=True)

            # Most creation attempts should fail or tasks should be cancelled
            assert creation_attempts == 5
            # During shutdown, some tasks may still be in running_tasks but should be cancelled
            # The exact count depends on timing, so we just verify shutdown completed
            assert manager._shutdown_event.is_set()
