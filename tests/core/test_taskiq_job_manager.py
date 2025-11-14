"""Comprehensive tests for TaskiqJobManager with task manager integration.

Tests cover:
- Task manager integration in workers
- SurrealDB-based status queries
- Distributed signal handling
- Job lifecycle management
- Error handling and cleanup
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest
import pytest_asyncio

from digitalkin.core.job_manager.taskiq_job_manager import TaskiqJobManager
from digitalkin.models.core.task_monitor import TaskStatus
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_models import ServicesMode
from tests.mocks import (
    MockInputModel,
    MockSetupModel,
    SimpleMockModule,
)

# Set timeout for all tests in this file (60 seconds)
pytestmark = pytest.mark.timeout(60)


# ============================================================================
# Fixtures
# ============================================================================
# Note: mock_surreal_connection base is provided by tests.fixtures.core_fixtures


@pytest_asyncio.fixture
async def configured_mock_surreal_connection(mock_surreal_connection: Mock) -> Mock:
    """Configure mock SurrealDB connection with taskiq-specific return values."""
    mock_surreal_connection.select_by_task_id.return_value = {
        "id": "task_123",
        "task_id": "test_job_id",
        "mission_id": "test_mission",
        "status": "running",
    }
    return mock_surreal_connection


@pytest_asyncio.fixture
async def mock_taskiq_broker() -> Mock:
    """Create a mock Taskiq broker."""
    broker = Mock()
    broker.startup = AsyncMock()
    broker.find_task = Mock()
    return broker


@pytest_asyncio.fixture
async def mock_taskiq_task() -> Mock:
    """Create a mock Taskiq task."""
    task = Mock()
    running_task = Mock()
    running_task.task_id = "test_job_id"
    running_task.wait_result = AsyncMock(return_value={"status": "ok"})
    task.kiq = AsyncMock(return_value=running_task)
    return task


@pytest_asyncio.fixture
async def mock_consumer() -> Mock:
    """Create a mock RStream consumer."""
    consumer = Mock()
    consumer.create_stream = AsyncMock()
    consumer.start = AsyncMock()
    consumer.subscribe = AsyncMock()
    consumer.run = AsyncMock()
    consumer.close = AsyncMock()
    return consumer


@pytest_asyncio.fixture
async def taskiq_job_manager(
    configured_mock_surreal_connection: Mock,
    mock_taskiq_broker: Mock,
    mock_consumer: Mock,
) -> TaskiqJobManager:
    """Create a TaskiqJobManager instance with mocked dependencies."""
    with (
        patch(
            "digitalkin.core.task_manager.base_task_manager.SurrealDBConnection",
            return_value=configured_mock_surreal_connection,
        ),
        patch(
            "digitalkin.core.job_manager.taskiq_job_manager.TASKIQ_BROKER",
            mock_taskiq_broker,
        ),
        patch.object(
            TaskiqJobManager,
            "_define_consumer",
            return_value=mock_consumer,
        ),
    ):
        manager = TaskiqJobManager(SimpleMockModule, ServicesMode.LOCAL)
        manager.channel = configured_mock_surreal_connection
        yield manager


# ============================================================================
# Tests: Initialization and Lifecycle
# ============================================================================


@pytest.mark.asyncio
async def test_taskiq_job_manager_initialization():
    """Test TaskiqJobManager initialization."""
    manager = TaskiqJobManager(SimpleMockModule, ServicesMode.LOCAL)

    assert manager.module_class == SimpleMockModule
    assert manager.services_mode == ServicesMode.LOCAL
    assert manager._job_registry == {}
    assert manager.job_queues == {}
    assert manager.max_queue_size == 1000


@pytest.mark.asyncio
async def test_taskiq_job_manager_start(
    taskiq_job_manager: TaskiqJobManager,
    configured_mock_surreal_connection: Mock,
    mock_taskiq_broker: Mock,
    mock_consumer: Mock,
):
    """Test TaskiqJobManager start method."""
    # Mock ConnectionFactory to avoid real connection
    with patch(
        "digitalkin.core.job_manager.taskiq_job_manager.ConnectionFactory.create_surreal_connection",
        new_callable=AsyncMock,
        return_value=configured_mock_surreal_connection,
    ):
        await taskiq_job_manager.start()

    mock_taskiq_broker.startup.assert_called_once()
    mock_consumer.create_stream.assert_called_once()
    mock_consumer.start.assert_called_once()
    mock_consumer.subscribe.assert_called_once()


# ============================================================================
# Tests: Job Creation and Tracking
# ============================================================================


@pytest.mark.asyncio
async def test_create_module_instance_job(
    taskiq_job_manager: TaskiqJobManager,
    mock_taskiq_task: Mock,
    mock_taskiq_broker: Mock,
):
    """Test creating a module instance job."""
    from tests.mocks.models import MockInputTrigger

    mock_taskiq_broker.find_task.return_value = mock_taskiq_task

    input_data = MockInputModel(root=MockInputTrigger(data="test_input"))
    setup_data = MockSetupModel(config="test_config")

    # Mock SimpleMockModule.__init__ to avoid service initialization
    with patch.object(SimpleMockModule, "__init__", return_value=None):
        job_id = await taskiq_job_manager.create_module_instance_job(
            input_data=input_data,
            setup_data=setup_data,
            mission_id="test_mission",
            setup_id="test_setup",
            setup_version_id="v1",
        )

    assert job_id == "test_job_id"
    assert job_id in taskiq_job_manager._job_registry
    # Verify TaskSession was created in manager
    assert job_id in taskiq_job_manager.tasks_sessions
    mock_taskiq_broker.find_task.assert_called_once_with("digitalkin.core.job_manager.taskiq_broker:run_start_module")
    # Verify arguments were passed to worker (7 args total)
    call_args = mock_taskiq_task.kiq.call_args
    assert call_args is not None
    assert (
        len(call_args[0]) == 7
    )  # mission_id, setup_id, setup_version_id, module_class, services_mode, input_data, setup_data


@pytest.mark.asyncio
async def test_create_module_instance_job_task_not_found(
    taskiq_job_manager: TaskiqJobManager,
    mock_taskiq_broker: Mock,
):
    """Test creating a module instance job when task is not found."""
    from tests.mocks.models import MockInputTrigger

    mock_taskiq_broker.find_task.return_value = None

    input_data = MockInputModel(root=MockInputTrigger(data="test_input"))
    setup_data = MockSetupModel(config="test_config")

    with pytest.raises(ValueError, match="Task not found"):
        await taskiq_job_manager.create_module_instance_job(
            input_data=input_data,
            setup_data=setup_data,
            mission_id="test_mission",
            setup_id="test_setup",
            setup_version_id="v1",
        )


@pytest.mark.asyncio
async def test_create_config_setup_instance_job(
    taskiq_job_manager: TaskiqJobManager,
    mock_taskiq_task: Mock,
    mock_taskiq_broker: Mock,
):
    """Test creating a config setup instance job."""
    mock_taskiq_broker.find_task.return_value = mock_taskiq_task

    config_data = MockSetupModel(config="test_config")

    # Mock SimpleMockModule.__init__ to avoid service initialization
    with patch.object(SimpleMockModule, "__init__", return_value=None):
        job_id = await taskiq_job_manager.create_config_setup_instance_job(
            config_setup_data=config_data,
            mission_id="test_mission",
            setup_id="test_setup",
            setup_version_id="v1",
        )

    assert job_id == "test_job_id"
    assert job_id in taskiq_job_manager._job_registry
    # Verify TaskSession was created in manager
    assert job_id in taskiq_job_manager.tasks_sessions
    mock_taskiq_broker.find_task.assert_called_once_with("digitalkin.core.job_manager.taskiq_broker:run_config_module")
    # Verify arguments were passed to worker (6 args total)
    call_args = mock_taskiq_task.kiq.call_args
    assert call_args is not None
    assert (
        len(call_args[0]) == 6
    )  # mission_id, setup_id, setup_version_id, module_class, services_mode, config_setup_data


@pytest.mark.asyncio
async def test_create_config_setup_instance_job_none_data(
    taskiq_job_manager: TaskiqJobManager,
    mock_taskiq_task: Mock,
    mock_taskiq_broker: Mock,
):
    """Test creating a config setup job with None data."""
    mock_taskiq_broker.find_task.return_value = mock_taskiq_task

    with pytest.raises(TypeError, match="config_setup_data must be a valid model"):
        await taskiq_job_manager.create_config_setup_instance_job(
            config_setup_data=None,  # type: ignore
            mission_id="test_mission",
            setup_id="test_setup",
            setup_version_id="v1",
        )


# ============================================================================
# Tests: Status Queries
# ============================================================================


@pytest.mark.asyncio
async def test_get_module_status_running(
    taskiq_job_manager: TaskiqJobManager,
    configured_mock_surreal_connection: Mock,
):
    """Test getting module status when task is running."""
    taskiq_job_manager._job_registry["test_job_id"] = "test_job_id"
    configured_mock_surreal_connection.select_by_task_id.return_value = {
        "id": "task_123",
        "task_id": "test_job_id",
        "status": "running",  # Use lowercase to match TaskStatus enum values
    }

    status = await taskiq_job_manager.get_module_status("test_job_id")

    assert status == TaskStatus.RUNNING
    configured_mock_surreal_connection.select_by_task_id.assert_called_with("tasks", "test_job_id")


@pytest.mark.asyncio
async def test_get_module_status_not_found(
    taskiq_job_manager: TaskiqJobManager,
):
    """Test getting module status when job is not in registry."""
    status = await taskiq_job_manager.get_module_status("unknown_job_id")

    assert status == TaskStatus.FAILED


@pytest.mark.asyncio
async def test_get_module_status_from_heartbeat(
    taskiq_job_manager: TaskiqJobManager,
    configured_mock_surreal_connection: Mock,
):
    """Test getting module status from heartbeat when task record not found."""
    taskiq_job_manager._job_registry["test_job_id"] = "test_job_id"

    # First call returns None (no task record), second call returns heartbeat
    configured_mock_surreal_connection.select_by_task_id.side_effect = [
        None,  # tasks table
        {"id": "hb_123", "task_id": "test_job_id"},  # heartbeats table
    ]

    status = await taskiq_job_manager.get_module_status("test_job_id")

    assert status == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_get_module_status_error(
    taskiq_job_manager: TaskiqJobManager,
    configured_mock_surreal_connection: Mock,
):
    """Test getting module status when an error occurs."""
    taskiq_job_manager._job_registry["test_job_id"] = "test_job_id"
    configured_mock_surreal_connection.select_by_task_id.side_effect = Exception("DB error")

    status = await taskiq_job_manager.get_module_status("test_job_id")

    assert status == TaskStatus.FAILED


# ============================================================================
# Tests: Stop Module
# ============================================================================


@pytest.mark.asyncio
async def test_stop_module(
    taskiq_job_manager: TaskiqJobManager,
    configured_mock_surreal_connection: Mock,
):
    """Test stopping a module using TaskManager."""
    from digitalkin.core.task_manager.task_session import TaskSession

    # Setup job registry and task session
    taskiq_job_manager._job_registry["test_job_id"] = "test_job_id"

    # Create a mock module and task session
    mock_module = Mock(spec=BaseModule)
    mock_module.stop = AsyncMock()
    taskiq_job_manager.tasks_sessions["test_job_id"] = TaskSession(
        "test_job_id", "test_mission", configured_mock_surreal_connection, mock_module
    )

    # Mock cancel_task method
    with patch.object(taskiq_job_manager, "cancel_task", new_callable=AsyncMock) as mock_cancel:
        result = await taskiq_job_manager.stop_module("test_job_id")

        assert result is True
        # Verify TaskManager.cancel_task was called instead of direct SurrealDB update
        mock_cancel.assert_called_once_with("test_job_id", "test_mission")


@pytest.mark.asyncio
async def test_stop_module_not_found(
    taskiq_job_manager: TaskiqJobManager,
):
    """Test stopping a module that is not in registry."""
    result = await taskiq_job_manager.stop_module("unknown_job_id")

    assert result is False


@pytest.mark.asyncio
async def test_stop_module_no_task_session(
    taskiq_job_manager: TaskiqJobManager,
):
    """Test stopping a module when task session is not found."""
    taskiq_job_manager._job_registry["test_job_id"] = "test_job_id"
    # No task session created

    result = await taskiq_job_manager.stop_module("test_job_id")

    assert result is False


@pytest.mark.asyncio
async def test_stop_module_error(
    taskiq_job_manager: TaskiqJobManager,
    configured_mock_surreal_connection: Mock,
):
    """Test stopping a module when an error occurs."""
    from digitalkin.core.task_manager.task_session import TaskSession

    taskiq_job_manager._job_registry["test_job_id"] = "test_job_id"

    # Create a mock module and task session
    mock_module = Mock(spec=BaseModule)
    mock_module.stop = AsyncMock()
    taskiq_job_manager.tasks_sessions["test_job_id"] = TaskSession(
        "test_job_id", "test_mission", configured_mock_surreal_connection, mock_module
    )

    # Mock cancel_task to raise an exception
    with patch.object(taskiq_job_manager, "cancel_task", side_effect=Exception("Cancel error")):
        result = await taskiq_job_manager.stop_module("test_job_id")

        assert result is False


# ============================================================================
# Tests: Stop All Modules
# ============================================================================


@pytest.mark.asyncio
async def test_stop_all_modules(
    taskiq_job_manager: TaskiqJobManager,
    configured_mock_surreal_connection: Mock,
):
    """Test stopping all modules."""
    from digitalkin.core.task_manager.task_session import TaskSession

    # Add multiple jobs to registry with task sessions
    for job_id in ["job_1", "job_2", "job_3"]:
        taskiq_job_manager._job_registry[job_id] = job_id
        mock_module = Mock(spec=BaseModule)
        mock_module.stop = AsyncMock()
        taskiq_job_manager.tasks_sessions[job_id] = TaskSession(
            job_id, "test_mission", configured_mock_surreal_connection, mock_module
        )

    with patch.object(taskiq_job_manager, "cancel_task", new_callable=AsyncMock) as mock_cancel:
        await taskiq_job_manager.stop_all_modules()

        # Verify cancel_task was called for each job
        assert mock_cancel.call_count == 3


@pytest.mark.asyncio
async def test_stop_all_modules_empty_registry(
    taskiq_job_manager: TaskiqJobManager,
):
    """Test stopping all modules when registry is empty."""
    await taskiq_job_manager.stop_all_modules()
    # Should complete without errors


# ============================================================================
# Tests: List Modules
# ============================================================================


@pytest.mark.asyncio
async def test_list_modules(
    taskiq_job_manager: TaskiqJobManager,
    configured_mock_surreal_connection: Mock,
):
    """Test listing all modules."""
    taskiq_job_manager._job_registry["job_1"] = "job_1"
    taskiq_job_manager._job_registry["job_2"] = "job_2"

    configured_mock_surreal_connection.select_by_task_id.return_value = {
        "id": "task_123",
        "task_id": "job_1",
        "mission_id": "test_mission",
        "status": "running",  # Use lowercase to match TaskStatus enum values
    }

    modules = await taskiq_job_manager.list_modules()

    assert len(modules) == 2
    assert "job_1" in modules
    assert "job_2" in modules
    assert modules["job_1"]["name"] == "SimpleMockModule"
    assert modules["job_1"]["class"] == "SimpleMockModule"
    assert modules["job_1"]["status"] == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_list_modules_empty_registry(
    taskiq_job_manager: TaskiqJobManager,
):
    """Test listing modules when registry is empty."""
    modules = await taskiq_job_manager.list_modules()

    assert modules == {}


@pytest.mark.asyncio
async def test_list_modules_with_error(
    taskiq_job_manager: TaskiqJobManager,
    configured_mock_surreal_connection: Mock,
):
    """Test listing modules when an error occurs."""
    taskiq_job_manager._job_registry["job_1"] = "job_1"
    configured_mock_surreal_connection.select_by_task_id.side_effect = Exception("DB error")

    modules = await taskiq_job_manager.list_modules()

    assert len(modules) == 1
    assert modules["job_1"]["status"] == TaskStatus.FAILED
    assert "error" in modules["job_1"]


# ============================================================================
# Tests: Stream Consumer
# ============================================================================


@pytest.mark.asyncio
async def test_generate_stream_consumer(
    taskiq_job_manager: TaskiqJobManager,
):
    """Test generating a stream consumer."""
    job_id = "test_job_id"

    async with taskiq_job_manager.generate_stream_consumer(job_id) as stream:
        # Add some test data to the queue
        queue = taskiq_job_manager.job_queues[job_id]
        await queue.put({"output": "test1"})
        await queue.put({"output": "test2"})

        # Read from the stream with timeout to prevent infinite loop
        outputs = []
        count = 0

        async def read_stream() -> None:
            async for output in stream:
                outputs.append(output)
                nonlocal count
                count += 1
                if count >= 2:
                    break

        try:
            await asyncio.wait_for(read_stream(), timeout=2.0)
        except asyncio.TimeoutError:
            # Timeout is expected if stream doesn't close
            pass

        assert len(outputs) == 2
        assert outputs[0] == {"output": "test1"}
        assert outputs[1] == {"output": "test2"}

    # Verify queue was cleaned up
    assert job_id not in taskiq_job_manager.job_queues


@pytest.mark.asyncio
async def test_generate_config_setup_module_response(
    taskiq_job_manager: TaskiqJobManager,
):
    """Test generating config setup module response."""
    job_id = "test_job_id"

    # Start the response generator in a background task
    async def generate_response():
        return await taskiq_job_manager.generate_config_setup_module_response(job_id)

    response_task = asyncio.create_task(generate_response())

    # Wait a bit for the queue to be created by the method
    await asyncio.sleep(0.1)

    # Put data in the queue that was created by the method
    queue = taskiq_job_manager.job_queues[job_id]
    await queue.put(MockSetupModel(config="configured"))

    # Get the response
    response = await response_task

    assert isinstance(response, MockSetupModel)
    assert response.config == "configured"

    # Verify queue was cleaned up
    assert job_id not in taskiq_job_manager.job_queues
