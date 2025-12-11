"""Healthcheck status trigger - comprehensive module status."""

import time
from typing import Any, ClassVar

from digitalkin.mixins import BaseMixin
from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.utility import (
    HealthcheckStatusInput,
    HealthcheckStatusOutput,
)
from digitalkin.modules.trigger_handler import TriggerHandler


class HealthcheckStatusTrigger(TriggerHandler, BaseMixin):
    """Handler for comprehensive status healthcheck.

    Reports detailed module status including uptime, active jobs, and metadata.
    """

    protocol: ClassVar[str] = "healthcheck_status"
    input_format = HealthcheckStatusInput
    _start_time: ClassVar[float] = time.time()

    def __init__(self, context: ModuleContext) -> None:
        """Initialize the handler."""

    async def handle(
        self,
        input_data: HealthcheckStatusInput,  # noqa: ARG002
        setup_data: Any,  # noqa: ANN401, ARG002
        context: ModuleContext,
    ) -> None:
        """Handle status healthcheck request.

        Args:
            input_data: The input trigger data (unused for healthcheck).
            setup_data: The setup configuration (unused for healthcheck).
            context: The module context.
        """
        output = HealthcheckStatusOutput(
            module_name=context.session.setup_id,
            module_status="RUNNING",
            uptime_seconds=time.time() - self._start_time,
            active_jobs=1,
            metadata={
                "job_id": context.session.job_id,
                "mission_id": context.session.mission_id,
                "setup_version_id": context.session.setup_version_id,
            },
        )
        await self.send_message(context, output)
