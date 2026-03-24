from typing import Literal

from pydantic import Field

from digitalkin.models.module import UtilityProtocol


class HealthcheckPingInputPayload(UtilityProtocol):
    """Input for healthcheck ping request."""

    protocol: Literal["healthcheck_ping"] = "healthcheck_ping"  # type: ignore[misc]


class HealthcheckServicesInputPayload(UtilityProtocol):
    """Input for healthcheck services request."""

    protocol: Literal["healthcheck_services"] = "healthcheck_services"  # type: ignore[misc]


class HealthcheckStatusInputPayload(UtilityProtocol):
    """Input for healthcheck status request."""

    protocol: Literal["healthcheck_status"] = "healthcheck_status"  # type: ignore[misc]


class MessageInputPayload(UtilityProtocol):
    """Input payload for message trigger."""

    protocol: Literal["message"] = "message"
    user_prompt: str = Field(
        ...,
        title="User Prompt",
        description="The prompt provided by the user for processing.",
    )
    file_ids: list[str] | None = Field(
        default=None,
        title="File IDS",
        description="List of files ids to be processed, if any.",
    )
