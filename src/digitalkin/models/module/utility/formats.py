from pydantic import BaseModel, Field

from digitalkin.models.module import DataModel, SetupModel
from digitalkin.models.module.utility.inputs import MessageInputPayload
from digitalkin.models.module.utility.outputs import MessageOutputPayload


class DefaultSecretFormat(BaseModel):
    """Secret model for API keys and credentials.

    Contains the API key required for the Agno agent.
    """

    API_KEY: str = Field(
        ...,
        description="API key for the Agno agent.",
    )


class DefaultSetupFormat(SetupModel):
    """Setup model defining module configuration parameters."""

    system_prompt: str = Field(
        default="You are a helpful assistant",
        title="System Prompt",
        description="The system prompt used to guide the agent's behavior and responses.",
        json_schema_extra={"ui:widget": "textarea"},
    )


class DefaultInputFormat(DataModel):
    root: MessageInputPayload = Field(
        ...,
        discriminator="protocol",
        title="Root input",
        description="Define the input type (Message or File).",
    )


class DefaultOutputFormat(DataModel):
    """Output model for the Template module with discriminated union."""

    root: MessageOutputPayload = Field(
        ...,
        discriminator="protocol",
        title="Protocol",
        description="Either a message or file response.",
    )
