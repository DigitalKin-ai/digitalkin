"""Base types for module models."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, ClassVar, Generic, TypeVar

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from digitalkin.models.module.setup_types import SetupModel


class DataTrigger(BaseModel):
    """Defines the root input/output model exposing the protocol.

    The mandatory protocol is important to define the module beahvior following the user or agent input/output.

    Example:
        class MyInput(DataModel):
            root: DataTrigger
            user_define_data: Any

        # Usage
        my_input = MyInput(root=DataTrigger(protocol="message"))
        print(my_input.root.protocol)  # Output: message
    """

    protocol: ClassVar[str]
    created_at: str = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc).isoformat(),
        title="Created At",
        description="Timestamp when the payload was created.",
    )


DataTriggerT = TypeVar("DataTriggerT", bound=DataTrigger)


class DataModel(BaseModel, Generic[DataTriggerT]):
    """Base definition of input/output model showing mandatory root fields.

    The Model define the Module Input/output, usually referring to multiple input/output type defined by an union.

    Example:
        class ModuleInput(DataModel):
            root: FileInput | MessageInput
    """

    root: DataTriggerT
    annotations: dict[str, str] = Field(
        default={},
        title="Annotations",
        description="Additional metadata or annotations related to the output. ex {'role': 'user'}",
    )


InputModelT = TypeVar("InputModelT", bound=DataModel)
OutputModelT = TypeVar("OutputModelT", bound=DataModel)
SecretModelT = TypeVar("SecretModelT", bound=BaseModel)
SetupModelT = TypeVar("SetupModelT", bound="SetupModel")
