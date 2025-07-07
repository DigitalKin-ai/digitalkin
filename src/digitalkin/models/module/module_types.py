"""Types for module models."""

from typing import TypeVar

from pydantic import BaseModel


class InputTrigger(BaseModel):
    """Defines the root input model exposing the protocol.

    The mandatory protocol is important to define the module beahvior following the user or agent input.

    Example:
        class MyInput(InputModel):
            root: InputTrigger
            user_define_data: Any

        # Usage
        my_input = MyInput(root=InputTrigger(protocol="message"))
        print(my_input.root.protocol)  # Output: message
    """

    protocol: str


class InputModel(BaseModel):
    """Base definition of input model showing mandatory root fields.

    The Model define the Module Input, usually referring to multiple input type defined by an union.

    Example:
        class ModuleInput(InputModel):
            root: FileInput | MessageInput
    """

    root: InputTrigger


ConfigSetupModelT = TypeVar("ConfigSetupModelT", bound=BaseModel | None)
InputModelT = TypeVar("InputModelT", bound=InputModel)
OutputModelT = TypeVar("OutputModelT", bound=BaseModel)
SetupModelT = TypeVar("SetupModelT", bound=BaseModel)
SecretModelT = TypeVar("SecretModelT", bound=BaseModel)
