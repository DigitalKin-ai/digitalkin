"""Setup model for the EchoModule."""

from digitalkin.models.module import SetupModel
from pydantic import Field


class EchoSetup(SetupModel):
    """Configuration model for the EchoModule.

    Controls how input text is transformed before streaming back.
    """

    uppercase: bool = Field(default=False, description="Convert output to uppercase")
    repeat: int = Field(default=3, description="Number of output chunks per input")
    delay_ms: int = Field(default=200, description="Milliseconds between chunks")
    prefix: str = Field(default="", description="Prepend to each output chunk")
    reverse: bool = Field(default=False, description="Reverse the text")
