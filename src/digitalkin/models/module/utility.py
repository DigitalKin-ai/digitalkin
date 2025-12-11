"""Utility protocols for SDK-provided functionality.

These protocols are automatically available to all modules and don't need to be
explicitly included in module output unions.
"""

from typing import Any, ClassVar, Literal

from pydantic import BaseModel, Field

from digitalkin.models.module.module_types import DataTrigger


class UtilityProtocol(DataTrigger):
    """Base class for SDK-provided utility protocols.

    All SDK utility protocols inherit from this class to enable:
    - Easy identification of SDK vs user-defined protocols
    - Auto-injection capability
    - Consistent behavior across the SDK
    """


class EndOfStreamOutput(UtilityProtocol):
    """Signal that the stream has ended."""

    protocol: Literal["end_of_stream"] = "end_of_stream"  # type: ignore


class HealthcheckPingInput(UtilityProtocol):
    """Input for healthcheck ping request."""

    protocol: Literal["healthcheck_ping"] = "healthcheck_ping"  # type: ignore


class HealthcheckPingOutput(UtilityProtocol):
    """Output for healthcheck ping response.

    Simple alive check that returns "pong" status.
    """

    protocol: Literal["healthcheck_ping"] = "healthcheck_ping"  # type: ignore
    status: Literal["pong"] = "pong"
    latency_ms: float | None = Field(
        default=None,
        description="Round-trip latency in milliseconds",
    )


class ServiceHealthStatus(BaseModel):
    """Health status of a single service."""

    name: str = Field(..., description="Name of the service")
    status: Literal["healthy", "unhealthy", "unknown"] = Field(
        ...,
        description="Health status of the service",
    )
    message: str | None = Field(
        default=None,
        description="Optional message about the service status",
    )


class HealthcheckServicesInput(UtilityProtocol):
    """Input for healthcheck services request."""

    protocol: Literal["healthcheck_services"] = "healthcheck_services"  # type: ignore


class HealthcheckServicesOutput(UtilityProtocol):
    """Output for healthcheck services response.

    Reports the health status of all configured services.
    """

    protocol: Literal["healthcheck_services"] = "healthcheck_services"  # type: ignore
    services: list[ServiceHealthStatus] = Field(
        ...,
        description="List of service health statuses",
    )
    overall_status: Literal["healthy", "degraded", "unhealthy"] = Field(
        ...,
        description="Overall health status based on all services",
    )


class HealthcheckStatusInput(UtilityProtocol):
    """Input for healthcheck status request."""

    protocol: Literal["healthcheck_status"] = "healthcheck_status"  # type: ignore


class HealthcheckStatusOutput(UtilityProtocol):
    """Output for healthcheck status response.

    Comprehensive module status including uptime, active jobs, and metadata.
    """

    protocol: Literal["healthcheck_status"] = "healthcheck_status"  # type: ignore
    module_name: str = Field(..., description="Name of the module")
    module_status: str = Field(..., description="Current status of the module")
    uptime_seconds: float | None = Field(
        default=None,
        description="Module uptime in seconds",
    )
    active_jobs: int = Field(
        default=0,
        description="Number of currently active jobs",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Additional metadata about the module",
    )


class UtilityRegistry:
    """Registry for SDK-provided built-in triggers.

    Example:
        builtin_triggers = UtilityRegistry.get_builtin_triggers()
    """

    _builtin_triggers: ClassVar[tuple | None] = None

    @classmethod
    def get_builtin_triggers(cls) -> tuple:
        """Get all SDK-provided built-in trigger handlers.

        Uses lazy loading to avoid circular imports with the modules package.

        Returns:
            Tuple of TriggerHandler subclasses for built-in functionality.
        """
        if cls._builtin_triggers is None:
            from digitalkin.modules.triggers.healthcheck_ping_trigger import HealthcheckPingTrigger  # noqa: PLC0415
            from digitalkin.modules.triggers.healthcheck_services_trigger import (  # noqa: PLC0415
                HealthcheckServicesTrigger,
            )
            from digitalkin.modules.triggers.healthcheck_status_trigger import HealthcheckStatusTrigger  # noqa: PLC0415

            cls._builtin_triggers = (
                HealthcheckPingTrigger,
                HealthcheckServicesTrigger,
                HealthcheckStatusTrigger,
            )
        return cls._builtin_triggers
