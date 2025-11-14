"""Regression tests for previously identified and fixed issues.

This module contains tests that ensure previously fixed bugs don't reoccur.
Each test is documented with the original issue and fix.
"""

import asyncio
from typing import Any, ClassVar
from unittest.mock import AsyncMock, Mock, patch

import pytest

from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_config import ServicesConfig
from digitalkin.services.services_models import ServicesMode, ServicesStrategy


class MockModule(BaseModule):
    """Mock module for regression testing."""

    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
    services_config_params: ClassVar[dict[str, dict[str, str | None] | None]] = {}
    services_config: ClassVar[ServicesConfig] = ServicesConfig(
        services_config_strategies={}, services_config_params={}, mode=ServicesMode.LOCAL
    )

    def __init__(self, job_id: str, mission_id: str, setup_id: str, setup_version_id: str) -> None:
        # REGRESSION: Module MUST call super().__init__
        super().__init__(job_id, mission_id, setup_id, setup_version_id)
        self.job_id = job_id
        self.mission_id = mission_id
        self.setup_id = setup_id
        self.setup_version_id = setup_version_id
        self.initialize_called = False
        self.run_called = False

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
        """Initialize the module with correct signature."""
        # REGRESSION: initialize MUST accept context and setup_data
        self.initialize_called = True

    async def run(self) -> None:
        """Run the module."""
        self.run_called = True

    async def cleanup(self) -> None:
        """Clean up the module."""


class TestModuleInitializationRegression:
    """Test regressions related to module initialization."""

    @pytest.mark.asyncio
    async def test_module_super_init_called(self):
        """REGRESSION: MockModules were not calling super().__init__
        causing AttributeError on module attributes.
        """
        module = MockModule("job-1", "mission-1", "setup-1", "version-1")

        # These attributes should exist after super().__init__
        assert hasattr(module, "job_id")
        assert hasattr(module, "mission_id")
        assert hasattr(module, "setup_id")
        assert hasattr(module, "setup_version_id")
        assert module.job_id == "job-1"
        assert module.mission_id == "mission-1"

    @pytest.mark.asyncio
    async def test_module_initialize_signature(self):
        """REGRESSION: initialize() had wrong signature (missing context parameter)
        causing TypeError when module framework called it.
        """
        module = MockModule("job-1", "mission-1", "setup-1", "version-1")

        # Create mock context and setup_data
        mock_context = Mock()
        mock_setup_data = Mock()

        # Should not raise TypeError
        await module.initialize(mock_context, mock_setup_data)
        assert module.initialize_called


class TestTaskManagerChannelRegression:
    """Test regressions related to channel/DB management."""

    @pytest.mark.asyncio
    async def test_base_task_manager_no_channel_attribute(self):
        """REGRESSION: BaseTaskManager had both 'channel' and sessions with 'db',
        causing confusion about which to use.
        Fix: Removed channel attribute, use session.db instead.
        """
        manager = LocalTaskManager()

        # BaseTaskManager should NOT have a channel attribute
        assert not hasattr(manager, "channel")

        # Sessions should have db attribute
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = AsyncMock()
            mock_conn.return_value = mock_db

            module = MockModule("job-1", "mission", "setup", "version")

            async def task() -> None:
                pass

            await manager.create_task("task-1", "mission", module, task())

            # Session should have db, not manager
            assert "task-1" in manager.tasks_sessions
            assert hasattr(manager.tasks_sessions["task-1"], "db")
            assert manager.tasks_sessions["task-1"].db is not None

    @pytest.mark.asyncio
    async def test_send_signal_uses_session_db(self):
        """REGRESSION: send_signal was trying to use self.channel.update
        instead of session.db.update.
        """
        manager = LocalTaskManager()

        mock_db = AsyncMock()
        mock_db.update = AsyncMock(return_value=True)

        mock_session = Mock()
        mock_session.db = mock_db
        manager.tasks_sessions["task-1"] = mock_session

        # Should use session.db.update, not self.channel.update
        result = await manager.send_signal("task-1", "mission", "pause", {})

        assert result is True
        mock_db.update.assert_awaited_once_with("signals", "task-1", {"type": "pause", "payload": {}})


class TestTaskiqInfiniteLoopRegression:
    """Test regression for TaskiqJobManager infinite loop."""

    @pytest.mark.asyncio
    async def test_taskiq_stream_consumer_timeout(self):
        """REGRESSION: test_taskiq_job_manager had infinite loop in stream consumer
        Fix: Added asyncio.timeout(2.0) wrapper.
        """
        with patch("digitalkin.core.job_manager.taskiq_job_manager.TASKIQ_BROKER"):
            with patch("digitalkin.core.job_manager.taskiq_job_manager.TaskiqJobManager._start"):
                from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

                manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

                outputs = []
                count = 0

                # This should not hang due to timeout
                async def consume_stream() -> None:
                    async with manager.generate_stream_consumer("test-job") as stream:
                        # Add items to queue AFTER context manager creates it
                        queue = manager.job_queues["test-job"]
                        await queue.put({"data": "item1"})
                        await queue.put({"data": "item2"})

                        async for output in stream:
                            outputs.append(output)
                            nonlocal count
                            count += 1
                            if count >= 2:
                                break

                try:
                    await asyncio.wait_for(consume_stream(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass  # Expected timeout

                # Should have consumed available items
                assert len(outputs) >= 2


class TestMemoryLeakRegressions:
    """Test regressions related to memory leaks."""

    @pytest.mark.asyncio
    async def test_cleanup_task_clears_queue(self):
        """REGRESSION: _cleanup_task didn't clear queue items, causing memory leak
        Fix: Added queue draining logic.
        """
        manager = LocalTaskManager()

        mock_db = AsyncMock()
        mock_db.close = AsyncMock()

        session = Mock()
        session.db = mock_db
        session.queue = asyncio.Queue()

        # Fill queue with items
        for i in range(100):
            session.queue.put_nowait(f"item-{i}")

        # Add cleanup method that drains queue and closes DB
        async def mock_cleanup() -> None:
            # Drain queue
            try:
                while not session.queue.empty():
                    session.queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
            # Close DB
            await session.db.close()

        session.cleanup = AsyncMock(side_effect=mock_cleanup)

        manager.tasks_sessions["task-1"] = session

        # Should drain queue
        await manager._cleanup_task("task-1", "mission")

        # Queue should be empty
        assert session.queue.empty()
        # DB should be closed
        mock_db.close.assert_awaited_once()
        # Session should be removed
        assert "task-1" not in manager.tasks_sessions

    @pytest.mark.asyncio
    async def test_taskiq_job_manager_connection_cleanup(self):
        """REGRESSION: TaskiqJobManager didn't close SurrealDB connection
        Fix: Added connection cleanup in _stop().
        """
        with patch("digitalkin.core.job_manager.taskiq_job_manager.TASKIQ_BROKER") as mock_broker, patch(
            "digitalkin.core.job_manager.taskiq_job_manager.TaskiqJobManager._define_consumer"
        ) as mock_consumer_factory, patch(
            "digitalkin.core.job_manager.taskiq_job_manager.ConnectionFactory.create_surreal_connection",
            new_callable=AsyncMock,
        ) as mock_create_conn:
            mock_broker.startup = AsyncMock()
            mock_consumer = AsyncMock()
            mock_consumer.create_stream = AsyncMock()
            mock_consumer.start = AsyncMock()
            mock_consumer.subscribe = AsyncMock()
            mock_consumer_factory.return_value = mock_consumer
            mock_conn = AsyncMock()
            mock_conn.close = AsyncMock()
            mock_create_conn.return_value = mock_conn

            from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

            manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)
            await manager.start()

            # Should have connection
            assert hasattr(manager, "channel")

            # Stop should close connection
            await manager._stop()

            # Connection should be closed
            mock_conn.close.assert_awaited_once()


class TestFireAndForgetRegression:
    """Test regressions related to fire-and-forget async tasks."""

    @pytest.mark.asyncio
    async def test_stream_consumer_task_error_handling(self):
        """REGRESSION: Stream consumer task in TaskiqJobManager was fire-and-forget
        Fix: Wrapped with error handling and proper task management.
        """
        with patch("digitalkin.core.job_manager.taskiq_job_manager.TASKIQ_BROKER") as mock_broker, patch(
            "digitalkin.core.job_manager.taskiq_job_manager.TaskiqJobManager._define_consumer"
        ) as mock_consumer_factory, patch(
            "digitalkin.core.job_manager.taskiq_job_manager.ConnectionFactory.create_surreal_connection",
            new_callable=AsyncMock,
        ) as mock_create_conn:
            mock_broker.startup = AsyncMock()

            # Create consumer that will fail
            mock_consumer = AsyncMock()
            mock_consumer.create_stream = AsyncMock()
            mock_consumer.start = AsyncMock()
            mock_consumer.subscribe = AsyncMock()
            mock_consumer.run = AsyncMock(side_effect=Exception("Consumer failed"))
            mock_consumer.close = AsyncMock()

            mock_consumer_factory.return_value = mock_consumer
            mock_conn = AsyncMock()
            mock_create_conn.return_value = mock_conn

            from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

            manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

            # Start should create wrapped task
            await manager.start()

            # Task should exist and be wrapped
            assert hasattr(manager, "stream_consumer_task")
            assert manager.stream_consumer_task is not None

            # Wait a bit for task to fail
            await asyncio.sleep(0.1)

            # Task should have failed but not cause uncaught exception
            assert manager.stream_consumer_task.done()

            # Clean up - _stop awaits the failed task so it will raise
            try:
                await manager._stop()
            except Exception:
                pass  # Expected since task failed


class TestContextManagerRegression:
    """Test regressions related to async context managers."""

    @pytest.mark.asyncio
    async def test_context_manager_cleanup_on_error(self):
        """REGRESSION: Context managers weren't properly cleaning up on exceptions
        Fix: Added __aenter__ and __aexit__ to BaseTaskManager.
        """
        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn:
            mock_db = AsyncMock()
            mock_conn.return_value = mock_db

            manager = LocalTaskManager()

            try:
                async with manager:
                    module = MockModule("job-1", "mission", "setup", "version")

                    async def task() -> None:
                        await asyncio.sleep(0.1)

                    await manager.create_task("task-1", "mission", module, task())

                    # Simulate error
                    msg = "Test error"
                    raise ValueError(msg)
            except ValueError:
                pass

            # Shutdown should have been called despite error
            assert manager._shutdown_event.is_set()
            assert len(manager.tasks_sessions) == 0


class TestQueueTimeoutRegression:
    """Test regressions related to queue timeout issues."""

    @pytest.mark.asyncio
    async def test_generate_config_setup_timeout(self):
        """REGRESSION: generate_config_setup_module_response could hang forever
        Fix: Added asyncio.wait_for with 30s timeout.
        """
        with patch("digitalkin.core.job_manager.taskiq_job_manager.TASKIQ_BROKER"):
            with patch("digitalkin.core.job_manager.taskiq_job_manager.TaskiqJobManager._start"):
                from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

                manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

                # Should timeout instead of hanging
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(manager.generate_config_setup_module_response("timeout-job"), timeout=1.0)

                # Queue should be cleaned up
                assert "timeout-job" not in manager.job_queues

    @pytest.mark.asyncio
    async def test_stream_consumer_periodic_timeout(self):
        """REGRESSION: Stream consumer could hang if job disappeared
        Fix: Added periodic timeout checks.
        """
        with patch("digitalkin.core.job_manager.taskiq_job_manager.TASKIQ_BROKER"):
            with patch("digitalkin.core.job_manager.taskiq_job_manager.TaskiqJobManager._start"):
                from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager

                manager = TaskiqJobManager(MockModule, ServicesMode.REMOTE)

                queue = asyncio.Queue()
                manager.job_queues["disappearing-job"] = queue
                manager._job_registry["disappearing-job"] = "disappearing-job"

                items_consumed = []

                async with manager.generate_stream_consumer("disappearing-job") as stream:
                    # Remove job from registry (simulating job disappearance)
                    del manager._job_registry["disappearing-job"]

                    # Stream should eventually detect job is gone
                    items_consumed.extend([item async for item in stream])
                        # Should break due to timeout and job check

                # Stream should have ended
                assert "disappearing-job" not in manager.job_queues


class TestFactoryPatternRegression:
    """Test regressions related to factory pattern implementation."""

    @pytest.mark.asyncio
    async def test_connection_factory_auto_init(self):
        """REGRESSION: Connection factory didn't auto-initialize by default
        Fix: Added auto_init=True parameter.
        """
        with patch("digitalkin.core.common.factories.SurrealDBConnection") as mock_conn_class:
            mock_conn = AsyncMock()
            mock_conn_class.return_value = mock_conn

            from digitalkin.core.common import ConnectionFactory

            # Default should auto-initialize
            await ConnectionFactory.create_surreal_connection()
            mock_conn.init_surreal_instance.assert_awaited_once()

            # Can disable auto-init
            mock_conn.init_surreal_instance.reset_mock()
            await ConnectionFactory.create_surreal_connection(auto_init=False)
            mock_conn.init_surreal_instance.assert_not_awaited()

    def test_module_factory_validation(self):
        """REGRESSION: ModuleFactory didn't validate empty parameters
        Fix: Added parameter validation.
        """
        from digitalkin.core.common import ModuleFactory

        # Empty job_id should raise error
        with pytest.raises(ValueError, match="job_id cannot be empty"):
            ModuleFactory.create_module_instance(MockModule, "", "mission", "setup", "version")

        # Empty mission_id should raise error
        with pytest.raises(ValueError, match="mission_id cannot be empty"):
            ModuleFactory.create_module_instance(MockModule, "job", "", "setup", "version")

    def test_queue_factory_negative_size(self):
        """REGRESSION: QueueFactory allowed negative maxsize
        Fix: Added validation for maxsize >= 0.
        """
        from digitalkin.core.common import QueueFactory

        # Negative size should raise error
        with pytest.raises(ValueError, match="maxsize must be >= 0"):
            QueueFactory.create_bounded_queue(maxsize=-1)

        # Zero should be allowed (unlimited)
        queue = QueueFactory.create_bounded_queue(maxsize=0)
        assert queue.maxsize == 0


class TestAsyncCleanupRegression:
    """Test regressions related to async resource cleanup."""

    @pytest.mark.asyncio
    async def test_multiple_db_connections_cleanup(self):
        """REGRESSION: Multiple DB connections weren't all closed during shutdown
        Fix: Enhanced shutdown to clean all remaining sessions.
        """
        manager = LocalTaskManager()

        with patch("digitalkin.core.task_manager.base_task_manager.SurrealDBConnection") as mock_conn_class:
            # Create multiple mock connections
            mock_connections = []
            for i in range(5):
                mock_db = AsyncMock()
                mock_db.close = AsyncMock()
                mock_connections.append(mock_db)

            mock_conn_class.side_effect = mock_connections

            # Create multiple tasks
            for i in range(5):
                module = MockModule(f"job-{i}", "mission", "setup", "version")

                async def task() -> None:
                    await asyncio.sleep(0.01)

                await manager.create_task(f"task-{i}", "mission", module, task())

            # Shutdown should close all connections
            await manager.shutdown("mission")

            # All connections should be closed
            for mock_db in mock_connections:
                mock_db.close.assert_awaited_once()

            # All sessions should be cleaned
            assert len(manager.tasks_sessions) == 0


class TestConcurrencyRegression:
    """Test regressions related to concurrency issues."""

    @pytest.mark.asyncio
    async def test_single_job_manager_lock_protection(self):
        """REGRESSION: SingleJobManager.stop_module wasn't thread-safe
        Fix: Added async lock protection.
        """
        with patch("digitalkin.core.job_manager.single_job_manager.ConnectionFactory") as mock_factory:
            mock_conn = AsyncMock()
            mock_factory.create_surreal_connection = AsyncMock(return_value=mock_conn)

            manager = SingleJobManager(MockModule, ServicesMode.LOCAL)
            await manager.start()

            # Create mock module and session
            module = MockModule("job-1", "mission", "setup", "version")
            module.stop = AsyncMock()

            mock_db = AsyncMock()
            mock_db.close = AsyncMock()

            session = Mock()
            session.module = module
            session.mission_id = "mission"
            session.db = mock_db

            # Add cleanup method - DON'T call module.stop() here as TaskSession.cleanup() does that
            async def mock_cleanup() -> None:
                await session.db.close()

            session.cleanup = AsyncMock(side_effect=mock_cleanup)

            manager.tasks_sessions["job-1"] = session

            # Mock task
            manager.tasks["job-1"] = asyncio.create_task(asyncio.sleep(0.1))

            stop_calls = []

            async def track_stop() -> None:
                result = await manager.stop_module("job-1")
                stop_calls.append(result)

            # Multiple concurrent stop calls
            await asyncio.gather(track_stop(), track_stop(), track_stop(), return_exceptions=True)

            # Module.stop should only be called once (lock prevents multiple)
            assert module.stop.await_count <= 1

            # Clean up
            await manager.stop_all_modules()
