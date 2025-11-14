"""Test factory pattern implementations for resource creation.

This module tests the factory patterns used throughout the codebase
to ensure proper resource creation, error handling, and memory management.
"""

import asyncio
import datetime
from typing import Any, ClassVar
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from digitalkin.core.common import ConnectionFactory, ModuleFactory, QueueFactory
from digitalkin.core.task_manager.surrealdb_repository import SurrealDBConnection
from digitalkin.models.module.module_types import DataModel, SetupModel, DataTrigger
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_config import ServicesConfig
from digitalkin.services.services_models import ServicesMode, ServicesStrategy


# Create mock model classes
class MockDataTrigger(DataTrigger):
    """Mock data trigger."""
    protocol: str = "mock"
    value: str = "test"


class MockInputModel(DataModel[MockDataTrigger]):
    """Mock input model."""
    pass


class MockOutputModel(DataModel[MockDataTrigger]):
    """Mock output model."""
    pass


class MockSetupModel(SetupModel):
    """Mock setup model."""
    config_value: str = "default"


class MockModule(BaseModule[MockInputModel, MockOutputModel, MockSetupModel, None]):
    """Mock module for testing ModuleFactory."""

    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
    services_config_params: ClassVar[dict[str, dict[str, str | None] | None]] = {}
    services_config: ClassVar[ServicesConfig] = ServicesConfig(
        services_config_strategies={},
        services_config_params={},
        mode=ServicesMode.LOCAL
    )

    def __init__(self, job_id: str, mission_id: str, setup_id: str, setup_version_id: str):
        super().__init__(job_id, mission_id, setup_id, setup_version_id)
        self.job_id = job_id
        self.mission_id = mission_id
        self.setup_id = setup_id
        self.setup_version_id = setup_version_id
        self.initialized = False

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
        self.initialized = True

    async def run(self) -> None:
        """Run the module."""
        pass

    async def cleanup(self) -> None:
        """Clean up the module."""
        pass


class TestConnectionFactory:
    """Test ConnectionFactory for SurrealDB connection creation."""

    @pytest.mark.asyncio
    async def test_create_surreal_connection_default_params(self):
        """Test creating connection with default parameters."""
        with patch('digitalkin.core.common.factories.SurrealDBConnection') as mock_conn_class:
            mock_conn = AsyncMock(spec=SurrealDBConnection)
            mock_conn_class.return_value = mock_conn

            connection = await ConnectionFactory.create_surreal_connection()

            # Verify default parameters
            mock_conn_class.assert_called_once_with("task_manager", datetime.timedelta(seconds=5))
            mock_conn.init_surreal_instance.assert_awaited_once()
            assert connection == mock_conn

    @pytest.mark.asyncio
    async def test_create_surreal_connection_custom_params(self):
        """Test creating connection with custom parameters."""
        with patch('digitalkin.core.common.factories.SurrealDBConnection') as mock_conn_class:
            mock_conn = AsyncMock(spec=SurrealDBConnection)
            mock_conn_class.return_value = mock_conn

            custom_db = "custom_db"
            custom_timeout = datetime.timedelta(seconds=10)

            connection = await ConnectionFactory.create_surreal_connection(
                database=custom_db,
                timeout=custom_timeout,
                auto_init=False
            )

            # Verify custom parameters
            mock_conn_class.assert_called_once_with(custom_db, custom_timeout)
            mock_conn.init_surreal_instance.assert_not_awaited()
            assert connection == mock_conn

    @pytest.mark.asyncio
    async def test_create_surreal_connection_init_failure(self):
        """Test handling of initialization failure."""
        with patch('digitalkin.core.common.factories.SurrealDBConnection') as mock_conn_class:
            mock_conn = AsyncMock(spec=SurrealDBConnection)
            mock_conn.init_surreal_instance.side_effect = ConnectionError("Failed to connect")
            mock_conn_class.return_value = mock_conn

            with pytest.raises(ConnectionError, match="Failed to connect"):
                await ConnectionFactory.create_surreal_connection()

    @pytest.mark.asyncio
    async def test_create_surreal_connection_memory_cleanup(self):
        """Test that connections are properly cleaned up on error."""
        with patch('digitalkin.core.common.factories.SurrealDBConnection') as mock_conn_class:
            mock_conn = AsyncMock(spec=SurrealDBConnection)
            mock_conn.init_surreal_instance.side_effect = Exception("Init failed")
            mock_conn_class.return_value = mock_conn

            # Track if connection is properly handled
            connections_created = []

            def track_connection(*args, **kwargs):
                conn = AsyncMock(spec=SurrealDBConnection)
                conn.init_surreal_instance.side_effect = Exception("Init failed")
                connections_created.append(conn)
                return conn

            mock_conn_class.side_effect = track_connection

            with pytest.raises(Exception, match="Init failed"):
                await ConnectionFactory.create_surreal_connection()

            # Verify connection was created but exception propagated
            assert len(connections_created) == 1

    @pytest.mark.asyncio
    async def test_create_surreal_connection_timeout_handling(self):
        """Test timeout handling during connection creation."""
        with patch('digitalkin.core.common.factories.SurrealDBConnection') as mock_conn_class:
            mock_conn = AsyncMock(spec=SurrealDBConnection)

            async def slow_init():
                await asyncio.sleep(10)  # Simulate slow connection

            mock_conn.init_surreal_instance = slow_init
            mock_conn_class.return_value = mock_conn

            # This should timeout if the factory properly handles timeouts
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    ConnectionFactory.create_surreal_connection(
                        timeout=datetime.timedelta(seconds=0.05)
                    ),
                    timeout=0.1,
                )


class TestModuleFactory:
    """Test ModuleFactory for module instance creation."""

    def test_create_module_instance_basic(self):
        """Test basic module instance creation."""
        job_id = "test-job-123"
        mission_id = "mission-456"
        setup_id = "setup-789"
        setup_version_id = "version-001"

        module = ModuleFactory.create_module_instance(
            MockModule,
            job_id,
            mission_id,
            setup_id,
            setup_version_id
        )

        assert isinstance(module, MockModule)
        assert module.job_id == job_id
        assert module.mission_id == mission_id
        assert module.setup_id == setup_id
        assert module.setup_version_id == setup_version_id

    def test_create_module_instance_invalid_class(self):
        """Test creation with invalid module class."""
        class NotAModule:
            """Not a valid module class."""
            pass

        with pytest.raises(TypeError):
            ModuleFactory.create_module_instance(
                NotAModule,  # type: ignore
                "job-id",
                "mission-id",
                "setup-id",
                "version-id"
            )

    def test_create_module_instance_memory_reference(self):
        """Test that factory creates new instances, not references."""
        job_id = "test-job"
        mission_id = "test-mission"
        setup_id = "test-setup"
        version_id = "test-version"

        module1 = ModuleFactory.create_module_instance(
            MockModule, job_id, mission_id, setup_id, version_id
        )
        module2 = ModuleFactory.create_module_instance(
            MockModule, job_id, mission_id, setup_id, version_id
        )

        # Should be different instances
        assert module1 is not module2
        assert id(module1) != id(module2)

    def test_create_module_instance_constructor_error(self):
        """Test handling of module constructor errors."""
        class FailingModule(BaseModule):
            def __init__(self, job_id: str, mission_id: str, setup_id: str, setup_version_id: str):
                raise ValueError("Constructor failed")

        with pytest.raises(TypeError, match="Can't instantiate abstract"):
            ModuleFactory.create_module_instance(
                FailingModule,
                "job-id",
                "mission-id",
                "setup-id",
                "version-id"
            )

    def test_create_module_instance_parameter_validation(self):
        """Test validation of string parameters."""
        # Test empty strings
        with pytest.raises(ValueError, match="job_id cannot be empty"):
            ModuleFactory.create_module_instance(
                MockModule, "", "mission", "setup", "version"
            )

        with pytest.raises(ValueError, match="mission_id cannot be empty"):
            ModuleFactory.create_module_instance(
                MockModule, "job", "", "setup", "version"
            )


class TestQueueFactory:
    """Test QueueFactory for asyncio.Queue creation."""

    def test_create_bounded_queue_default_size(self):
        """Test creating queue with default max size."""
        queue = QueueFactory.create_bounded_queue()

        assert isinstance(queue, asyncio.Queue)
        assert queue.maxsize == 1000
        assert queue.empty()

    def test_create_bounded_queue_custom_size(self):
        """Test creating queue with custom max size."""
        custom_size = 500
        queue = QueueFactory.create_bounded_queue(maxsize=custom_size)

        assert isinstance(queue, asyncio.Queue)
        assert queue.maxsize == custom_size
        assert queue.empty()

    def test_create_bounded_queue_zero_size(self):
        """Test creating queue with zero size (unlimited)."""
        queue = QueueFactory.create_bounded_queue(maxsize=0)

        assert isinstance(queue, asyncio.Queue)
        assert queue.maxsize == 0  # 0 means unlimited
        assert queue.empty()

    def test_create_bounded_queue_negative_size(self):
        """Test that negative size raises error."""
        with pytest.raises(ValueError, match="maxsize must be >= 0"):
            QueueFactory.create_bounded_queue(maxsize=-1)

    @pytest.mark.asyncio
    async def test_create_bounded_queue_capacity_enforcement(self):
        """Test that queue enforces capacity limits."""
        maxsize = 3
        queue = QueueFactory.create_bounded_queue(maxsize=maxsize)

        # Fill the queue
        for i in range(maxsize):
            await queue.put(f"item-{i}")

        assert queue.full()

        # Try to add one more item with timeout
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(queue.put("overflow"), timeout=0.1)

    @pytest.mark.asyncio
    async def test_create_bounded_queue_memory_behavior(self):
        """Test queue memory behavior with many items."""
        queue = QueueFactory.create_bounded_queue(maxsize=100)

        # Add many items
        items = [f"large-item-{i}" * 100 for i in range(50)]  # Large strings
        for item in items:
            await queue.put(item)

        # Verify memory is held
        assert queue.qsize() == 50

        # Clear queue and verify memory can be released
        while not queue.empty():
            await queue.get()

        assert queue.empty()
        assert queue.qsize() == 0

    def test_create_bounded_queue_independence(self):
        """Test that factory creates independent queue instances."""
        queue1 = QueueFactory.create_bounded_queue(maxsize=10)
        queue2 = QueueFactory.create_bounded_queue(maxsize=10)

        # Should be different instances
        assert queue1 is not queue2
        assert id(queue1) != id(queue2)

        # Operations on one shouldn't affect the other
        queue1.put_nowait("item1")
        assert not queue1.empty()
        assert queue2.empty()

    @pytest.mark.asyncio
    async def test_create_bounded_queue_concurrent_access(self):
        """Test queue behavior under concurrent access."""
        queue = QueueFactory.create_bounded_queue(maxsize=10)

        async def producer(n: int):
            for i in range(5):
                await queue.put(f"producer-{n}-item-{i}")
                await asyncio.sleep(0.01)

        async def consumer(n: int):
            items = []
            for _ in range(5):
                item = await queue.get()
                items.append(item)
                await asyncio.sleep(0.01)
            return items

        # Run multiple producers and consumers concurrently
        producers = [producer(i) for i in range(2)]
        consumers = [consumer(i) for i in range(2)]

        results = await asyncio.gather(
            *producers,
            *consumers,
            return_exceptions=True
        )

        # Verify no exceptions occurred
        for result in results:
            assert not isinstance(result, Exception)

        # Queue should be empty after balanced produce/consume
        assert queue.empty()


class TestFactoryIntegration:
    """Test integration between different factories."""

    @pytest.mark.asyncio
    async def test_factories_combined_usage(self):
        """Test using multiple factories together."""
        with patch('digitalkin.core.common.factories.SurrealDBConnection') as mock_conn_class:
            mock_conn = AsyncMock(spec=SurrealDBConnection)
            mock_conn_class.return_value = mock_conn

            # Create resources using factories
            connection = await ConnectionFactory.create_surreal_connection()
            module = ModuleFactory.create_module_instance(
                MockModule, "job-1", "mission-1", "setup-1", "version-1"
            )
            queue = QueueFactory.create_bounded_queue(maxsize=50)

            # Verify all resources created successfully
            assert connection is not None
            assert isinstance(module, MockModule)
            assert isinstance(queue, asyncio.Queue)
            assert queue.maxsize == 50

    @pytest.mark.asyncio
    async def test_factory_error_isolation(self):
        """Test that errors in one factory don't affect others."""
        # Make ConnectionFactory fail
        with patch('digitalkin.core.common.factories.SurrealDBConnection') as mock_conn_class:
            mock_conn_class.side_effect = Exception("Connection failed")

            # ConnectionFactory should fail
            with pytest.raises(Exception, match="Connection failed"):
                await ConnectionFactory.create_surreal_connection()

            # But other factories should still work
            module = ModuleFactory.create_module_instance(
                MockModule, "job-1", "mission-1", "setup-1", "version-1"
            )
            queue = QueueFactory.create_bounded_queue()

            assert isinstance(module, MockModule)
            assert isinstance(queue, asyncio.Queue)