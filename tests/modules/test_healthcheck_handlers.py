"""Tests for the built-in healthcheck triggers."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from zoneinfo import ZoneInfo

import pytest

from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.utility import (
    HealthcheckPingOutput,
    HealthcheckServicesOutput,
    HealthcheckStatusOutput,
    UtilityRegistry,
)
from digitalkin.modules.triggers.healthcheck_ping_trigger import HealthcheckPingTrigger
from digitalkin.modules.triggers.healthcheck_services_trigger import HealthcheckServicesTrigger
from digitalkin.modules.triggers.healthcheck_status_trigger import HealthcheckStatusTrigger


@pytest.fixture
def mock_context() -> ModuleContext:
    """Create a mock ModuleContext for testing.

    Returns:
        ModuleContext: A mock context with configured services and session.
    """
    # Mock all required services (matching healthcheck_services_trigger.py service_names)
    context = MagicMock(spec=ModuleContext)
    context.storage = MagicMock()
    context.cost = MagicMock()
    context.filesystem = MagicMock()
    context.registry = MagicMock()
    context.user_profile = MagicMock()

    # Set up session with required fields
    context.session = SimpleNamespace(
        job_id="test_job_123",
        mission_id="test_mission_456",
        setup_id="test_setup_789",
        setup_version_id="test_setup_version_001",
        timezone=ZoneInfo("Europe/Paris"),
    )

    # Set up timezone property (delegates to session.timezone)
    context.timezone = ZoneInfo("Europe/Paris")

    # Set up callbacks namespace with async send_message
    context.callbacks = SimpleNamespace(send_message=AsyncMock())

    return context


@pytest.fixture
def mock_context_partial_services() -> ModuleContext:
    """Create a mock ModuleContext with some services missing.

    Returns:
        ModuleContext: A mock context with partial services configured.
    """
    context = MagicMock(spec=ModuleContext)

    # Only configure some services (matching healthcheck_services_trigger.py service_names)
    context.storage = MagicMock()
    context.cost = MagicMock()
    context.filesystem = None  # Missing
    context.registry = None  # Missing
    context.user_profile = MagicMock()

    # Set up session
    context.session = SimpleNamespace(
        job_id="test_job_123",
        mission_id="test_mission_456",
        setup_id="test_setup_789",
        setup_version_id="test_setup_version_001",
        timezone=ZoneInfo("Europe/Paris"),
    )

    # Set up timezone property (delegates to session.timezone)
    context.timezone = ZoneInfo("Europe/Paris")

    context.callbacks = SimpleNamespace(send_message=AsyncMock())

    return context


class TestHealthcheckPingTrigger:
    """Tests for the HealthcheckPingTrigger class."""

    def test_protocol_attribute(self) -> None:
        """Test that the trigger has the correct protocol."""
        assert HealthcheckPingTrigger.protocol == "healthcheck_ping"

    @pytest.mark.asyncio
    async def test_handle_returns_pong(self, mock_context: ModuleContext) -> None:
        """Test that handle() sends a pong response.

        Args:
            mock_context: Mock module context.
        """
        trigger = HealthcheckPingTrigger(mock_context)
        await trigger.handle(None, None, mock_context)

        # Verify send_message was called
        mock_context.callbacks.send_message.assert_called_once()

        # Get the output that was sent
        call_args = mock_context.callbacks.send_message.call_args
        output = call_args[0][0]

        assert isinstance(output, HealthcheckPingOutput)
        assert output.status == "pong"

    @pytest.mark.asyncio
    async def test_handle_includes_latency(self, mock_context: ModuleContext) -> None:
        """Test that handle() includes latency measurement.

        Args:
            mock_context: Mock module context.
        """
        trigger = HealthcheckPingTrigger(mock_context)

        await trigger.handle(None, None, mock_context)

        call_args = mock_context.callbacks.send_message.call_args
        output = call_args[0][0]

        # Latency should be calculated and be a non-negative number
        assert output.latency_ms is not None
        assert output.latency_ms >= 0


class TestHealthcheckServicesTrigger:
    """Tests for the HealthcheckServicesTrigger class."""

    def test_protocol_attribute(self) -> None:
        """Test that the trigger has the correct protocol."""
        assert HealthcheckServicesTrigger.protocol == "healthcheck_services"

    @pytest.mark.asyncio
    async def test_handle_all_services_healthy(self, mock_context: ModuleContext) -> None:
        """Test that handle() reports all services as healthy when configured.

        Args:
            mock_context: Mock module context.
        """
        trigger = HealthcheckServicesTrigger(mock_context)
        await trigger.handle(None, None, mock_context)

        mock_context.callbacks.send_message.assert_called_once()
        call_args = mock_context.callbacks.send_message.call_args
        output = call_args[0][0]

        assert isinstance(output, HealthcheckServicesOutput)
        assert output.overall_status == "healthy"
        assert len(output.services) == 5  # All 5 services

        # All services should be healthy
        for service in output.services:
            assert service.status == "healthy"

    @pytest.mark.asyncio
    async def test_handle_partial_services_degraded(self, mock_context_partial_services: ModuleContext) -> None:
        """Test that handle() reports degraded when some services missing.

        Args:
            mock_context_partial_services: Mock context with partial services.
        """
        trigger = HealthcheckServicesTrigger(mock_context_partial_services)
        await trigger.handle(None, None, mock_context_partial_services)

        mock_context_partial_services.callbacks.send_message.assert_called_once()
        call_args = mock_context_partial_services.callbacks.send_message.call_args
        output = call_args[0][0]

        assert isinstance(output, HealthcheckServicesOutput)
        assert output.overall_status == "degraded"

        # Check specific services
        service_map = {s.name: s for s in output.services}
        assert service_map["storage"].status == "healthy"
        assert service_map["cost"].status == "healthy"
        assert service_map["filesystem"].status == "unknown"
        assert service_map["registry"].status == "unknown"
        assert service_map["user_profile"].status == "healthy"

    @pytest.mark.asyncio
    async def test_handle_reports_service_names(self, mock_context: ModuleContext) -> None:
        """Test that handle() reports all expected service names.

        Args:
            mock_context: Mock module context.
        """
        trigger = HealthcheckServicesTrigger(mock_context)
        await trigger.handle(None, None, mock_context)

        call_args = mock_context.callbacks.send_message.call_args
        output = call_args[0][0]

        expected_services = {"storage", "cost", "filesystem", "registry", "user_profile"}
        actual_services = {s.name for s in output.services}

        assert actual_services == expected_services


class TestHealthcheckStatusTrigger:
    """Tests for the HealthcheckStatusTrigger class."""

    def test_protocol_attribute(self) -> None:
        """Test that the trigger has the correct protocol."""
        assert HealthcheckStatusTrigger.protocol == "healthcheck_status"

    @pytest.mark.asyncio
    async def test_handle_returns_module_info(self, mock_context: ModuleContext) -> None:
        """Test that handle() returns comprehensive module info.

        Args:
            mock_context: Mock module context.
        """
        trigger = HealthcheckStatusTrigger(mock_context)
        await trigger.handle(None, None, mock_context)

        mock_context.callbacks.send_message.assert_called_once()
        call_args = mock_context.callbacks.send_message.call_args
        output = call_args[0][0]

        assert isinstance(output, HealthcheckStatusOutput)
        assert output.module_name == "test_setup_789"
        assert output.module_status == "RUNNING"
        assert output.active_jobs >= 1

    @pytest.mark.asyncio
    async def test_handle_includes_uptime(self, mock_context: ModuleContext) -> None:
        """Test that handle() includes uptime.

        Args:
            mock_context: Mock module context.
        """
        trigger = HealthcheckStatusTrigger(mock_context)
        await trigger.handle(None, None, mock_context)

        call_args = mock_context.callbacks.send_message.call_args
        output = call_args[0][0]

        assert output.uptime_seconds is not None
        assert output.uptime_seconds >= 0

    @pytest.mark.asyncio
    async def test_handle_includes_metadata(self, mock_context: ModuleContext) -> None:
        """Test that handle() includes metadata from session.

        Args:
            mock_context: Mock module context.
        """
        trigger = HealthcheckStatusTrigger(mock_context)
        await trigger.handle(None, None, mock_context)

        call_args = mock_context.callbacks.send_message.call_args
        output = call_args[0][0]

        assert "job_id" in output.metadata
        assert output.metadata["job_id"] == "test_job_123"
        assert "mission_id" in output.metadata
        assert output.metadata["mission_id"] == "test_mission_456"


class TestBuiltinTriggersRegistry:
    """Tests for the UtilityRegistry.get_builtin_triggers() registry."""

    def test_all_triggers_registered(self) -> None:
        """Test that all healthcheck triggers are in the registry."""
        builtin_triggers = UtilityRegistry.get_builtin_triggers()
        assert HealthcheckPingTrigger in builtin_triggers
        assert HealthcheckServicesTrigger in builtin_triggers
        assert HealthcheckStatusTrigger in builtin_triggers

    def test_registry_count(self) -> None:
        """Test that the registry has the expected number of triggers."""
        builtin_triggers = UtilityRegistry.get_builtin_triggers()
        assert len(builtin_triggers) == 3

    def test_all_triggers_have_protocol(self) -> None:
        """Test that all triggers have a protocol attribute."""
        builtin_triggers = UtilityRegistry.get_builtin_triggers()
        for trigger_cls in builtin_triggers:
            assert hasattr(trigger_cls, "protocol")
            assert isinstance(trigger_cls.protocol, str)

    def test_protocols_are_unique(self) -> None:
        """Test that all protocols are unique."""
        builtin_triggers = UtilityRegistry.get_builtin_triggers()
        protocols = [t.protocol for t in builtin_triggers]
        assert len(protocols) == len(set(protocols))


class TestUtilityProtocolsIntegration:
    """Integration tests for utility protocols with module system."""

    def test_protocol_class_vars(self) -> None:
        """Test that protocol fields are correctly defined as ClassVars."""
        assert HealthcheckPingOutput().protocol == "healthcheck_ping"
        assert HealthcheckServicesOutput(services=[], overall_status="healthy").protocol == "healthcheck_services"
        assert HealthcheckStatusOutput(module_name="test", module_status="RUNNING").protocol == "healthcheck_status"

    def test_output_models_serializable(self) -> None:
        """Test that output models can be serialized to dict."""
        ping_output = HealthcheckPingOutput(latency_ms=10.5)
        data = ping_output.model_dump()
        assert "status" in data
        assert data["status"] == "pong"
        assert data["latency_ms"] == 10.5

        services_output = HealthcheckServicesOutput(
            services=[],
            overall_status="healthy",
        )
        data = services_output.model_dump()
        assert data["overall_status"] == "healthy"

        status_output = HealthcheckStatusOutput(
            module_name="test",
            module_status="RUNNING",
        )
        data = status_output.model_dump()
        assert data["module_name"] == "test"
        assert data["module_status"] == "RUNNING"
