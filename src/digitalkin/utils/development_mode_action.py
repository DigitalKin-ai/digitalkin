"""ArgParser and Action classes to ease command lines arguments settings."""

import logging
from argparse import Action, ArgumentParser, Namespace
from collections.abc import Sequence
from typing import Any

from digitalkin.logger import logger
from digitalkin.models.services.services import ServicesMode
from digitalkin.utils.env_manager import EnvManager

logger.setLevel(logging.INFO)


class DevelopmentModeMappingAction(Action):
    """ArgParse Action defaulting to the environment's ServicesMode.

    The default comes from ``ModuleSettings.services_mode`` (env ``SERVICE_MODE``);
    the command-line flag overrides it.
    """

    def __init__(self, **kwargs: Any) -> None:
        """Initialize the DevelopmentModeMappingAction."""
        kwargs.pop("default", None)
        kwargs.pop("required", None)
        super().__init__(default=EnvManager.services_mode(), required=False, **kwargs)

    def __call__(
        self,
        parser: ArgumentParser,  # argparse Action.__call__ signature # noqa: ARG002
        namespace: Namespace,
        values: str | Sequence[Any] | None,
        option_string: str | None = None,  # argparse Action.__call__ signature # noqa: ARG002
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
