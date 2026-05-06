"""Regression tests for previously identified and fixed issues.

This module contains tests that ensure previously fixed bugs don't reoccur.
Each test is documented with the original issue and fix.
"""

import asyncio
from collections.abc import AsyncGenerator
from enum import Enum
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from pydantic import BaseModel, Field

from digitalkin.core.job_manager.single_job_manager import SingleJobManager
from digitalkin.core.task_manager.local_task_manager import LocalTaskManager
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_config import ServicesConfig
from digitalkin.services.services_models import ServicesMode, ServicesStrategy
from digitalkin.services.task_manager.task_manager_strategy import TaskManagerStrategy


async def _empty_signals() -> AsyncGenerator[dict, None]:
    """Async generator that blocks until cancelled, yielding nothing."""
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        return
    yield  # pragma: no cover


class MockModule(BaseModule):
    """Mock module for regression testing."""

    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
    services_config_params: ClassVar[dict[str, dict[str, str | None] | None]] = {}
    services_config: ClassVar[ServicesConfig] = ServicesConfig(
        services_config_strategies={}, services_config_params={}, mode=ServicesMode.LOCAL
    )

    def __init__(
        self,
        job_id: str,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        request_metadata: dict[str, str] | None = None,
        tool_cache=None,
    ) -> None:
        # REGRESSION: Module MUST call super().__init__
        super().__init__(job_id, mission_id, setup_id, setup_version_id, request_metadata=request_metadata, tool_cache=tool_cache)
        self.job_id = job_id
        self.mission_id = mission_id
        self.setup_id = setup_id
        self.setup_version_id = setup_version_id
        self.initialize_called = False
        self.run_called = False
        # Wire a mock task_manager so ModuleContext is fully functional
        task_mgr = Mock(spec=TaskManagerStrategy)
        task_mgr.send_signal = AsyncMock(return_value={})
        task_mgr.subscribe_signals = AsyncMock(return_value=("sub", _empty_signals()))
        task_mgr.unsubscribe_signals = AsyncMock()
        task_mgr.close = AsyncMock()
        self.context.task_manager = task_mgr

    def _init_strategies(self, mission_id: str, setup_id: str, setup_version_id: str) -> dict[str, Any]:
        """Override to skip service initialization in tests."""
        return {
            "communication": None,
            "cost": None,
            "filesystem": None,
            "identity": None,
            "registry": None,
            "storage": None,
            "user_profile": None,
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
        """REGRESSION: BaseTaskManager had a 'channel' attribute causing confusion.
        Fix: Removed channel attribute entirely; signal service lives in TaskSession.
        """
        manager = LocalTaskManager()

        # BaseTaskManager should NOT have a channel attribute
        assert not hasattr(manager, "channel")

        module = MockModule("job-1", "mission", "setup", "version")

        async def task() -> None:
            pass

        await manager.create_task("task-1", "mission", module, task())

        # Session is created and tracked
        assert "task-1" in manager.tasks_sessions
        # Signal service comes from the module context, not the manager
        assert hasattr(manager.tasks_sessions["task-1"], "signal_service")

    @pytest.mark.asyncio
    async def test_send_signal_uses_session_service(self):
        """REGRESSION: send_signal was using self.channel instead of the session's
        signal service. Fix: delegated to session.signal_service.send_signal().
        """
        manager = LocalTaskManager()

        mock_signal_svc = AsyncMock()
        mock_signal_svc.send_signal = AsyncMock(return_value={})

        mock_session = Mock()
        mock_session.status = "pending"
        mock_session.setup_id = "setup:test"
        mock_session.setup_version_id = "setup_version:test"
        mock_session.signal_service = mock_signal_svc
        manager.tasks_sessions["task-1"] = mock_session

        result = await manager.send_signal("task-1", "mission", "cancel", {})

        assert result is True
        mock_signal_svc.send_signal.assert_awaited_once()


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
        session._write_lock = asyncio.Lock()

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

class TestContextManagerRegression:
    """Test regressions related to async context managers."""

    @pytest.mark.asyncio
    async def test_context_manager_cleanup_on_error(self):
        """REGRESSION: Context managers weren't properly cleaning up on exceptions
        Fix: Added __aenter__ and __aexit__ to BaseTaskManager.
        """
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


class TestFactoryPatternRegression:
    """Test regressions related to factory pattern implementation."""

    def test_module_factory_creates_independent_instances(self):
        """REGRESSION: Factory was reusing module instances instead of creating new ones.
        Fix: ModuleFactory.create_module_instance always calls the constructor.
        """
        from digitalkin.core.common import ModuleFactory

        m1 = ModuleFactory.create_module_instance(MockModule, "job-1", "mission", "setup", "v1")
        m2 = ModuleFactory.create_module_instance(MockModule, "job-2", "mission", "setup", "v1")

        assert m1 is not m2
        assert m1.job_id == "job-1"
        assert m2.job_id == "job-2"

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
    async def test_multiple_sessions_cleanup_on_shutdown(self):
        """REGRESSION: Multiple sessions weren't all cleaned during shutdown.
        Fix: Enhanced shutdown to clean all remaining sessions.
        """
        manager = LocalTaskManager()

        for i in range(5):
            module = MockModule(f"job-{i}", "mission", "setup", "version")

            async def task() -> None:
                await asyncio.sleep(0.01)

            await manager.create_task(f"task-{i}", "mission", module, task())

        await manager.shutdown("mission")

        # All sessions should be cleaned
        assert len(manager.tasks_sessions) == 0


class _MockBackend(Enum):
    """Test enum for serialization regression."""

    AUTO = "auto"
    CUSTOM = "custom"


class _MockEnumSetupModel(BaseModel):
    """Test model with enum field for serialization regression."""

    backend: _MockBackend = Field(default=_MockBackend.AUTO)
    name: str = "test"


class TestEnumSerializationRegression:
    """Test regression for enum serialization in job manager queues."""

    @pytest.mark.asyncio
    async def test_add_to_queue_serializes_enums(self):
        """REGRESSION: model_dump() without mode='json' left raw Python enums in dict,
        causing json_format.ParseDict to fail with ParseError.
        Fix: Changed model_dump() to model_dump(mode='json') in add_to_queue.
        """
        manager = SingleJobManager(MockModule, ServicesMode.LOCAL, MagicMock())
        await manager.start()

        session = Mock()
        session.queue = asyncio.Queue()
        session.stream_closed = False
        session._write_lock = asyncio.Lock()
        manager.tasks_sessions["job-enum"] = session

        output = _MockEnumSetupModel(backend=_MockBackend.CUSTOM, name="test")
        await manager.add_to_queue("job-enum", output)

        result = session.queue.get_nowait()

        # Enum must be serialized as string, not raw enum object
        assert result["backend"] == "custom"
        assert isinstance(result["backend"], str)


class TestTaskAccumulationRegression:
    """Test regression for task accumulation blocking new task creation."""

    @pytest.mark.asyncio
    async def test_completed_tasks_dont_block_creation(self):
        """REGRESSION: Completed/failed sessions in cleanup window blocked new task creation.

        Under high throughput, sessions in terminal states (completed/failed/cancelled)
        accumulated in tasks_sessions faster than cleanup removed them, eventually hitting
        the max_concurrent_tasks limit even though few tasks were truly active.

        Fix: _validate_task_creation counts only pending/running sessions.
        """
        manager = LocalTaskManager()
        manager.max_concurrent_tasks = 3

        # Simulate 3 sessions that have completed but haven't been cleaned up yet
        for i in range(3):
            manager.tasks_sessions[f"old-{i}"] = Mock(status="completed")

        async def work() -> None:
            pass

        coro = work()

        # Should succeed despite 3 sessions in dict (all are completed, active count is 0)
        await manager._validate_task_creation("new-task", "mission", coro)
        coro.close()
