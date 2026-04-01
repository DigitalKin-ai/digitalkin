"""Compliant MockModule implementations for testing.

These modules properly inherit from BaseModule with correct signatures and behavior.
All tests should use these instead of creating custom mock modules.

Usage:
    # Simple no-op mock
    module = SimpleMockModule(
        job_id="test_job",
        mission_id="missions:test",
        setup_id="setup_id",
        setup_version_id="v1",
    )

    # Configurable mock with delays/errors
    module = ConfigurableMockModule(
        job_id="test_job",
        mission_id="missions:test",
        setup_id="setup_id",
        setup_version_id="v1",
        initialize_delay=0.1,
        initialize_error=RuntimeError("Init failed"),
    )
"""

import asyncio
from typing import ClassVar

from digitalkin.models.module.module_context import ModuleContext
from digitalkin.modules._base_module import BaseModule
from digitalkin.services.services_config import ServicesConfig
from digitalkin.services.services_models import ServicesMode, ServicesStrategy
from tests.mocks.models import MockInputModel, MockOutputModel, MockSecretModel, MockSetupModel


class SimpleMockModule(
    BaseModule[
        MockInputModel,
        MockOutputModel,
        MockSetupModel,
        MockSecretModel,
    ]
):
    """Simple no-op mock module for basic testing needs.

    This module:
    - Properly inherits from BaseModule with correct generic parameters
    - Implements all required abstract methods as no-ops
    - Has minimal overhead for fast testing
    - Tracks method calls for assertions

    Use this when you just need a valid module instance without complex behavior.
    """

    name = "SimpleMockModule"
    description = "Simple mock module for basic tests"
    input_format = MockInputModel
    output_format = MockOutputModel
    setup_format = MockSetupModel
    secret_format = MockSecretModel
    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
    services_config_params: ClassVar[dict[str, dict[str, str | None] | None]] = {}
    services_config: ClassVar[ServicesConfig] = ServicesConfig(
        services_config_strategies={}, services_config_params={}, mode=ServicesMode.LOCAL,
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
        """Initialize simple mock module.

        Args:
            job_id: Job identifier
            mission_id: Mission identifier
            setup_id: Setup identifier
            setup_version_id: Setup version identifier
        """
        super().__init__(job_id, mission_id, setup_id, setup_version_id, request_metadata=request_metadata, tool_cache=tool_cache)

        # State tracking for test assertions
        self.initialize_called = False
        self.cleanup_called = False
        self.initialize_count = 0
        self.cleanup_count = 0

    def _init_strategies(self, mission_id: str, setup_id: str, setup_version_id: str) -> dict:
        """Skip service initialization in tests."""
        return {n: None for n in self.services_config.valid_strategy_names()}

    async def initialize(self, context: ModuleContext, setup_data: MockSetupModel) -> None:
        """No-op initialize for testing.

        Args:
            context: Module context with services and callbacks
            setup_data: Module setup configuration
        """
        self.initialize_called = True
        self.initialize_count += 1

    async def cleanup(self) -> None:
        """No-op cleanup for testing."""
        self.cleanup_called = True
        self.cleanup_count += 1


class ConfigurableMockModule(
    BaseModule[
        MockInputModel,
        MockOutputModel,
        MockSetupModel,
        MockSecretModel,
    ]
):
    """Fully configurable mock module for advanced testing.

    This module supports:
    - Configurable delays for async operations
    - Configurable errors to test error handling
    - State tracking for assertions
    - Custom callback behavior

    Use this when you need to test specific scenarios like:
    - Slow initialization/cleanup
    - Error conditions
    - Timing-sensitive operations
    - Complex state transitions

    Example:
        # Test slow initialization
        module = ConfigurableMockModule(
            job_id="test",
            mission_id="missions:test",
            setup_id="setup",
            setup_version_id="v1",
            initialize_delay=2.0,  # 2 second delay
        )

        # Test initialization error
        module = ConfigurableMockModule(
            job_id="test",
            mission_id="missions:test",
            setup_id="setup",
            setup_version_id="v1",
            initialize_error=RuntimeError("Init failed"),
        )
    """

    name = "ConfigurableMockModule"
    description = "Configurable mock module for advanced testing"
    input_format = MockInputModel
    output_format = MockOutputModel
    setup_format = MockSetupModel
    secret_format = MockSecretModel
    services_config_strategies: ClassVar[dict[str, ServicesStrategy | None]] = {}
    services_config_params: ClassVar[dict[str, dict[str, str | None] | None]] = {}
    services_config: ClassVar[ServicesConfig] = ServicesConfig(
        services_config_strategies={}, services_config_params={}, mode=ServicesMode.LOCAL,
    )

    def __init__(
        self,
        job_id: str,
        mission_id: str,
        setup_id: str,
        setup_version_id: str,
        request_metadata: dict[str, str] | None = None,
        tool_cache=None,
        *,
        initialize_delay: float = 0.0,
        initialize_error: Exception | None = None,
        cleanup_delay: float = 0.0,
        cleanup_error: Exception | None = None,
    ) -> None:
        """Initialize configurable mock module.

        Args:
            job_id: Job identifier
            mission_id: Mission identifier
            setup_id: Setup identifier
            setup_version_id: Setup version identifier
            initialize_delay: Delay in seconds before initialize completes
            initialize_error: Exception to raise during initialize
            cleanup_delay: Delay in seconds before cleanup completes
            cleanup_error: Exception to raise during cleanup
        """
        super().__init__(job_id, mission_id, setup_id, setup_version_id, request_metadata=request_metadata, tool_cache=tool_cache)

        # Configuration
        self.initialize_delay = initialize_delay
        self.initialize_error = initialize_error
        self.cleanup_delay = cleanup_delay
        self.cleanup_error = cleanup_error

    def _init_strategies(self, mission_id: str, setup_id: str, setup_version_id: str) -> dict:
        """Skip service initialization in tests."""
        return {n: None for n in self.services_config.valid_strategy_names()}

        # State tracking for test assertions
        self.initialize_called = False
        self.cleanup_called = False
        self.initialize_count = 0
        self.cleanup_count = 0
        self.last_context: ModuleContext | None = None
        self.last_setup_data: MockSetupModel | None = None

    async def initialize(self, context: ModuleContext, setup_data: MockSetupModel) -> None:
        """Configurable initialize with delays and errors.

        Args:
            context: Module context with services and callbacks
            setup_data: Module setup configuration

        Raises:
            Exception: If initialize_error was configured
        """
        self.initialize_called = True
        self.initialize_count += 1
        self.last_context = context
        self.last_setup_data = setup_data

        if self.initialize_delay > 0:
            await asyncio.sleep(self.initialize_delay)

        if self.initialize_error:
            raise self.initialize_error

    async def cleanup(self) -> None:
        """Configurable cleanup with delays and errors.

        Raises:
            Exception: If cleanup_error was configured
        """
        self.cleanup_called = True
        self.cleanup_count += 1

        if self.cleanup_delay > 0:
            await asyncio.sleep(self.cleanup_delay)

        if self.cleanup_error:
            raise self.cleanup_error
