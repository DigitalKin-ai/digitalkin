"""Input models for the EchoModule."""

from typing import Literal

from pydantic import Field

from digitalkin.models.module import DataModel, DataTrigger


class MessageInputPayload(DataTrigger):
    """Input payload for message protocol."""

    protocol: Literal["message"] = "message"
    user_prompt: str = Field(..., description="The user's input prompt")


class EchoInput(DataModel[MessageInputPayload]):
    """Unified input model for the EchoModule."""

    root: MessageInputPayload = Field(..., discriminator="protocol")
