"""Tests for the BaseModule class."""

import asyncio
from unittest.mock import MagicMock

import pytest

from digitalkin.models.module.module import ModuleStatus, StrategyConfig
from digitalkin.modules._base_module import BaseModule


class MockModule(BaseModule):
    """Concrete implementation of BaseModule for testing."""

    def __init__(
        self,
        strategy_config: StrategyConfig,
        name: str | None = None,
        initialize_error: bool = False,
        run_error: bool = False,
        cleanup_error: bool = False,
    ) -> None:
        """Initialize the test module."""
        super().__init__(strategy_config, name)
        self.initialize_called = False
        self.run_called = False
        self.cleanup_called = False
        self.initialize_error = initialize_error
        self.run_error = run_error
        self.cleanup_error = cleanup_error

    async def _initialize(self) -> None:
        self.initialize_called = True
        if self.initialize_error:
            msg = "Test initialization error"
            raise Exception(msg)

    async def _run(self) -> None:
        self.run_called = True
        if self.run_error:
            msg = "Test run error"
            raise Exception(msg)
        await asyncio.sleep(0.2)  # Simulate some work

    async def _cleanup(self) -> None:
        self.cleanup_called = True
        if self.cleanup_error:
            msg = "Test cleanup error"
            raise Exception(msg)


@pytest.fixture
def mock_strategy_config() -> MagicMock:
    """Creates a mock StrategyConfig for testing.

    Returns:
        MagicMock: The mock StrategyConfig object
    """
    config = MagicMock(spec=StrategyConfig)

    config.storage_strategy = MagicMock(name="storage_strategy")
    config.cost_strategy = MagicMock(name="cost_strategy")
    config.snapshot_strategy = MagicMock(name="snapshot_strategy")
    config.registry_strategy = MagicMock(name="registry_strategy")
    config.filesystem_strategy = MagicMock(name="filesystem_strategy")
    config.agent_strategy = MagicMock(name="agent_strategy")
    config.identity_strategy = MagicMock(name="identity_strategy")
    return config


class TestBaseModule:
    """Tests for the BaseModule class."""

    def test_init_with_default_name(self, mock_strategy_config: MagicMock) -> None:
        """Test module initialization with default name."""
        module = MockModule(mock_strategy_config)
        assert module.name == "MockModule"
        assert module.status == ModuleStatus.CREATED

    def test_init_with_custom_name(self, mock_strategy_config: MagicMock) -> None:
        """Test module initialization with custom name."""
        module = MockModule(mock_strategy_config, name="CustomModuleName")
        assert module.name == "CustomModuleName"
        assert module.status == ModuleStatus.CREATED

    def test_strategy_assignments(self, mock_strategy_config: MagicMock) -> None:
        """Test that strategies are correctly assigned from config."""
        module = MockModule(mock_strategy_config)
        assert module.storage == mock_strategy_config.storage_strategy
        assert module.cost == mock_strategy_config.cost_strategy
        assert module.snapshot == mock_strategy_config.snapshot_strategy
        assert module.registry == mock_strategy_config.registry_strategy
        assert module.filesystem == mock_strategy_config.filesystem_strategy
        assert module.agent == mock_strategy_config.agent_strategy
        assert module.identity == mock_strategy_config.identity_strategy

    def test_status_property(self, mock_strategy_config: MagicMock) -> None:
        """Test the status property returns the correct value."""
        module = MockModule(mock_strategy_config)
        assert module.status == ModuleStatus.CREATED

        # Change internal status and check property reflects it
        module._status = ModuleStatus.RUNNING
        assert module.status == ModuleStatus.RUNNING

    @pytest.mark.asyncio
    async def test_run_lifecycle_success(self, mock_strategy_config: MagicMock) -> None:
        """Test successful _run_lifecycle execution."""
        module = MockModule(mock_strategy_config)
        await module._run_lifecycle()
        assert module.run_called is True

    @pytest.mark.asyncio
    async def test_run_lifecycle_error(self, mock_strategy_config: MagicMock) -> None:
        """Test _run_lifecycle with an error in _run."""
        module = MockModule(mock_strategy_config, run_error=True)

        await module._run_lifecycle()
        assert module.run_called is True
        assert module.status == ModuleStatus.FAILED

    @pytest.mark.asyncio
    async def test_start_success(self, mock_strategy_config: MagicMock) -> None:
        """Test successful module start."""
        module = MockModule(mock_strategy_config)
        await module.start({}, "", int)
        await asyncio.sleep(0.1)  # Allow time for async tasks to run a bit
        assert module.initialize_called is True
        assert module.run_called is True
        assert module.status == ModuleStatus.RUNNING

    @pytest.mark.asyncio
    async def test_start_initialize_error(self, mock_strategy_config: MagicMock) -> None:
        """Test module start with initialization error."""
        module = MockModule(mock_strategy_config, initialize_error=True)
        await module.start({}, "", int)
        await asyncio.sleep(0.1)  # Allow time for async tasks to run a bit
        assert module.initialize_called is True
        assert module.run_called is False
        assert module.status == ModuleStatus.FAILED

    @pytest.mark.asyncio
    async def test_start_run_error(self, mock_strategy_config: MagicMock) -> None:
        """Test module start with run error."""
        module = MockModule(mock_strategy_config, run_error=True)
        await module.start({}, "", int)
        await asyncio.sleep(0.1)  # Allow time for async tasks to run a bit
        assert module.initialize_called is True
        assert module.run_called is True
        assert module.status == ModuleStatus.FAILED

    @pytest.mark.asyncio
    async def test_stop_when_running(self, mock_strategy_config: MagicMock) -> None:
        """Test stopping a running module."""
        module = MockModule(mock_strategy_config)
        module._status = ModuleStatus.RUNNING

        await module.stop()

        assert module.cleanup_called is True
        assert module.status == ModuleStatus.STOPPING

    @pytest.mark.asyncio
    async def test_stop_when_not_running(self, mock_strategy_config: MagicMock) -> None:
        """Test stopping a module that isn't running."""
        module = MockModule(mock_strategy_config)
        # By default status is CREATED

        await module.stop()

        assert module.cleanup_called is False
        assert module.status == ModuleStatus.CREATED

    @pytest.mark.asyncio
    async def test_stop_with_cleanup_error(self, mock_strategy_config: MagicMock) -> None:
        """Test stopping a module with cleanup error."""
        module = MockModule(mock_strategy_config, cleanup_error=True)
        module._status = ModuleStatus.RUNNING

        await module.stop()

        assert module.cleanup_called is True
        assert module.status == ModuleStatus.FAILED

    @pytest.mark.asyncio
    async def test_full_lifecycle(self, mock_strategy_config: MagicMock) -> None:
        """Test a complete module lifecycle from start to stop."""
        module = MockModule(mock_strategy_config)

        await module.start({}, "", int)
        await asyncio.sleep(0.1)  # Allow time for async tasks to run a bit

        assert module.status == ModuleStatus.RUNNING
        assert module.initialize_called is True
        assert module.run_called is True

        await module.stop()
        assert module.status == ModuleStatus.STOPPING
        assert module.cleanup_called is True
