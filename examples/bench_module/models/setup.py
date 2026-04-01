"""Setup model for the EchoModule."""

from pydantic import Field

from digitalkin.models.module import SetupModel


class EchoSetup(SetupModel):
    """Configuration model for the EchoModule.

    Controls how input text is transformed before streaming back.
    """

    uppercase: bool = Field(default=False, description="Convert output to uppercase")
    repeat: int = Field(default=1, description="Number of output chunks per input")
    delay_ms: int = Field(default=0, description="Milliseconds between chunks")
    prefix: str = Field(default="", description="Prepend to each output chunk")
    reverse: bool = Field(default=False, description="Reverse the text")
