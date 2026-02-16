"""Healthcheck services trigger - reports service health."""

from typing import Any, ClassVar, Literal

from digitalkin.mixins import BaseMixin
from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.utility import (
    HealthcheckServicesInput,
    HealthcheckServicesOutput,
    ServiceHealthStatus,
)
from digitalkin.modules.trigger_handler import TriggerHandler


class HealthcheckServicesTrigger(TriggerHandler, BaseMixin):
    """Handler for services healthcheck.

    Reports the health status of all configured services (storage, cost, filesystem, etc.).
    """

    protocol: Literal["healthcheck_services"] = "healthcheck_services"
    description: ClassVar[str] = "Reports health status of all configured services."
    input_format = HealthcheckServicesInput

    def __init__(self, context: ModuleContext) -> None:
        """Initialize the handler."""

    async def handle(
        self,
        input_data: HealthcheckServicesInput,  # Healthcheck needs no input data # noqa: ARG002
        setup_data: Any,  # Module-agnostic setup; healthcheck ignores it # noqa: ARG002
        context: ModuleContext,
    ) -> None:
        """Handle services healthcheck request.

        Args:
            input_data: The input trigger data (unused for healthcheck).
            setup_data: The setup configuration (unused for healthcheck).
            context: The module context.
        """
        services = {
            "storage": context.storage,
            "cost": context.cost,
            "filesystem": context.filesystem,
            "registry": context.registry,
            "user_profile": context.user_profile,
        }
        services_status: list[ServiceHealthStatus] = []

        for name, service in services.items():
            if service is not None:
                services_status.append(ServiceHealthStatus(name=name, status="healthy"))
            else:
                services_status.append(ServiceHealthStatus(name=name, status="unknown", message="Not configured"))

        # Determine overall status
        healthy_count = sum(1 for s in services_status if s.status == "healthy")
        if healthy_count == len(services_status):
            overall_status = "healthy"
        elif healthy_count > 0:
            overall_status = "degraded"
        else:
            overall_status = "unhealthy"

        output = HealthcheckServicesOutput(
            services=services_status,
            overall_status=overall_status,  # type: ignore[arg-type]  # String value matches Literal type at runtime
        )
        await self.send_message(context, output)
