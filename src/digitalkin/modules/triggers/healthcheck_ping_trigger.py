"""Healthcheck ping trigger - simple alive check."""

from datetime import datetime
from typing import Any, ClassVar, Literal

from digitalkin.mixins import BaseMixin
from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.utility import (
    HealthcheckPingInput,
    HealthcheckPingOutput,
)
from digitalkin.modules.trigger_handler import TriggerHandler


class HealthcheckPingTrigger(TriggerHandler, BaseMixin):
    """Handler for simple ping healthcheck.

    Responds immediately with "pong" status to verify the module is responsive.
    """

    protocol: Literal["healthcheck_ping"] = "healthcheck_ping"
    description: ClassVar[str] = "Simple ping healthcheck that responds with pong status."
    input_format = HealthcheckPingInput
    _request_time: datetime

    def __init__(self, context: ModuleContext) -> None:
        """Initialize the handler."""
        super().__init__(context)
        self._request_time = datetime.now(tz=context.session.timezone)

    async def handle(
        self,
        input_data: HealthcheckPingInput,  # Healthcheck needs no input data # noqa: ARG002
        setup_data: Any,  # Module-agnostic setup; healthcheck ignores it # noqa: ARG002
        context: ModuleContext,
    ) -> None:
        """Handle ping healthcheck request.

        Args:
            input_data: The input trigger data (unused for healthcheck).
            setup_data: The setup configuration (unused for healthcheck).
            context: The module context.
        """
        elapsed = datetime.now(tz=context.session.timezone) - self._request_time
        latency_ms = elapsed.total_seconds() * 1000
        output = HealthcheckPingOutput(latency_ms=latency_ms)
        await self.send_message(context, output)
