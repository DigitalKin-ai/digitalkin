"""Types for module models."""

from datetime import datetime, timezone
from typing import Any, ClassVar, Generic, TypeVar, cast

from pydantic import BaseModel, ConfigDict, Field, create_model

from digitalkin.logger import logger


class DataTrigger(BaseModel):
    """Defines the root input model exposing the protocol.

    The mandatory protocol is important to define the module beahvior following the user or agent input.

    Example:
        class MyInput(DataModel):
            root: DataTrigger
            user_define_data: Any

        # Usage
        my_input = MyInput(root=DataTrigger(protocol="message"))
        print(my_input.root.protocol)  # Output: message
    """

    protocol: ClassVar[str]
    created_at: str = datetime.now(tz=timezone.utc).isoformat()


DataTriggerT = TypeVar("DataTriggerT", bound=DataTrigger)


class DataModel(BaseModel, Generic[DataTriggerT]):
    """Base definition of input model showing mandatory root fields.

    The Model define the Module Input, usually referring to multiple input type defined by an union.

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


class SetupModel(BaseModel):
    """Base definition of setup model showing mandatory root fields.

    Optionally, the setup model can define a config option in json_schema_extra to be used to initialize the Kin.

    Example:
        class MySetup(SetupModel):
            name: str = Field()
            number: int = Field(..., json_schema_extra={"config": True})
    """

    @classmethod
    def get_clean_model(cls, *, config_fields: bool, hidden_fields: bool) -> type[SetupModelT]:  # type: ignore
        """Dynamically builds and returns a new BaseModel subclass.

        containing only those fields where json_schema_extra["config"] == True.

        Returns:
            Type[BaseModel]: A new BaseModel subclass with the filtered fields.

        Raises:
            ValueError: If both config_fields and hidden_fields are set to True.
        """
        clean_fields: dict[str, Any] = {}
        for name, field_info in cls.model_fields.items():
            extra = getattr(field_info, "json_schema_extra", {}) or {}
            is_config = bool(extra.get("config", False))
            is_hidden = bool(extra.get("hidden", False))

            # Skip config unless explicitly included
            if is_config and not config_fields:
                logger.debug("Skipping '%s' (config-only)", name)
                continue

            # Skip hidden unless explicitly included
            if is_hidden and not hidden_fields:
                logger.debug("Skipping '%s' (hidden-only)", name)
                continue

            clean_fields[name] = (field_info.annotation, field_info)

        # Dynamically create a model e.g. "SetupModel"
        m = create_model(
            f"{cls.__name__}",
            __base__=BaseModel,
            __config__=ConfigDict(arbitrary_types_allowed=True),
            **clean_fields,
        )
        return cast("type[SetupModelT]", m)
