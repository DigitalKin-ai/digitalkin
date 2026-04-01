"""Output models for the EchoModule."""

from typing import Literal

from pydantic import Field

from digitalkin.models.module import DataModel, DataTrigger


class MessageOutputPayload(DataTrigger):
    """Output payload for message protocol."""

    protocol: Literal["message"] = "message"
    response: str = Field(..., description="The response message")


class EchoOutput(DataModel[MessageOutputPayload]):
    """Unified output model for the EchoModule."""

    root: MessageOutputPayload = Field(..., discriminator="protocol")
