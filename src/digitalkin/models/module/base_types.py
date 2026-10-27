"""Base types for module models."""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar

from pydantic import BaseModel, Field, GetJsonSchemaHandler

if TYPE_CHECKING:
    from pydantic.json_schema import JsonSchemaValue
    from pydantic_core import CoreSchema

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

    A trigger shown to users sets ``display_name = "Search documents"``; the label is published on the
    trigger's schema definition, outside ``properties``, so it never reaches the LLM parameters.
    """

    display_name: ClassVar[str | None] = None

    protocol: str
    created_at: str = Field(
        default_factory=lambda: datetime.now(tz=timezone.utc).isoformat(),
        title="Created At",
        description="Timestamp when the payload was created.",
    )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: "CoreSchema", handler: GetJsonSchemaHandler
    ) -> "JsonSchemaValue":
        """Add ``display_name`` to the trigger's own schema when declared.

        Args:
            core_schema: Pydantic core schema of the trigger.
            handler: Pydantic JSON schema handler.

        Returns:
            The trigger JSON schema.
        """
        json_schema = super().__get_pydantic_json_schema__(core_schema, handler)
        if cls.display_name is not None:
            handler.resolve_ref_schema(json_schema)["display_name"] = cls.display_name
        return json_schema


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
