"""ArgParser and Action classes to ease command lines arguments settings."""

import logging
import os
from argparse import Action, ArgumentParser, Namespace
from collections.abc import Sequence
from typing import Any

from digitalkin.logger import logger
from digitalkin.services.services_models import ServicesMode

logger.setLevel(logging.INFO)


class DevelopmentModeMappingAction(Action):
    """ArgParse Action to map an environment variable to a ServicesMode enum."""

    def __init__(
        self,
        env_var: str,
        required: bool = True,  # noqa: FBT001, FBT002
        default: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the DevelopmentModeMappingAction."""
        default = ServicesMode(os.environ.get(env_var, default))

        if required and default:
            required = False
        super().__init__(
            default=default,
            required=required,
            **kwargs,
        )

    def __call__(
        self,
        _parser: ArgumentParser,  # argparse Action.__call__ signature
        namespace: Namespace,
        values: str | Sequence[Any] | None,
        _option_string: str | None = None,  # argparse Action.__call__ signature
    ) -> None:
        """Set the attribute to the corresponding class.

        Raises:
            TypeError: if the value is not a string.
        """
        # Check if the value is a string and convert it to lowercase
        if isinstance(values, str):
            values = values.lower()
        else:
            msg = "values must be a string"
            raise TypeError(msg)
        mode = ServicesMode(values)
        # setattr required by argparse Action API: namespace attributes are dynamic
        namespace.__dict__[self.dest] = mode
