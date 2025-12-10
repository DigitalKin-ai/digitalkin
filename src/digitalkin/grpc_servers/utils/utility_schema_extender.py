"""Utility schema extender for gRPC API responses.

This module extends module schemas with SDK utility protocols for API responses.
"""

from typing import Annotated, Union, get_args, get_origin

from pydantic import Field, create_model

from digitalkin.models.module.module_types import DataModel
from digitalkin.models.module.utility import (
    EndOfStreamOutput,
    HealthcheckPingInput,
    HealthcheckPingOutput,
    HealthcheckServicesInput,
    HealthcheckServicesOutput,
    HealthcheckStatusInput,
    HealthcheckStatusOutput,
)


class UtilitySchemaExtender:
    """Extends module schemas with SDK utility protocols for API responses.

    This class provides methods to create extended Pydantic models that include
    both user-defined protocols and SDK utility protocols in their schemas.
    """

    _output_protocols = (
        EndOfStreamOutput,
        HealthcheckPingOutput,
        HealthcheckServicesOutput,
        HealthcheckStatusOutput,
    )

    _input_protocols = (
        HealthcheckPingInput,
        HealthcheckServicesInput,
        HealthcheckStatusInput,
    )

    @classmethod
    def _extract_union_types(cls, annotation: type) -> tuple:
        """Extract individual types from a Union or Annotated[Union, ...] annotation.

        Returns:
            A tuple of individual types contained in the Union.
        """
        if get_origin(annotation) is Annotated:
            inner_args = get_args(annotation)
            if inner_args:
                return cls._extract_union_types(inner_args[0])
        if get_origin(annotation) is Union:
            return get_args(annotation)
        return (annotation,)

    @classmethod
    def create_extended_output_model(cls, base_model: type[DataModel]) -> type[DataModel]:
        """Create an extended output model that includes utility output protocols.

        Args:
            base_model: The module's output_format class (a DataModel subclass).

        Returns:
            A new DataModel subclass with root typed as Union[original_types, utility_types].
        """
        original_annotation = base_model.model_fields["root"].annotation
        original_types = cls._extract_union_types(original_annotation)
        extended_types = (*original_types, *cls._output_protocols)
        extended_root = Annotated[extended_types, Field(discriminator="protocol")]  # type: ignore[valid-type]
        return create_model(
            f"{base_model.__name__}Utilities",
            __base__=DataModel,
            root=(extended_root, ...),
            annotations=(dict[str, str], Field(default={})),
        )

    @classmethod
    def create_extended_input_model(cls, base_model: type[DataModel]) -> type[DataModel]:
        """Create an extended input model that includes utility input protocols.

        Args:
            base_model: The module's input_format class (a DataModel subclass).

        Returns:
            A new DataModel subclass with root typed as Union[original_types, utility_types].
        """
        original_annotation = base_model.model_fields["root"].annotation
        original_types = cls._extract_union_types(original_annotation)
        extended_types = (*original_types, *cls._input_protocols)
        extended_root = Annotated[extended_types, Field(discriminator="protocol")]  # type: ignore[valid-type]
        return create_model(
            f"{base_model.__name__}Utilities",
            __base__=DataModel,
            root=(extended_root, ...),
            annotations=(dict[str, str], Field(default={})),
        )
