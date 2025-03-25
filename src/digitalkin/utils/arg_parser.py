"""ArgParser and Action classes to ease command lines arguments settings."""

import logging
import os
from argparse import Action, ArgumentParser, Namespace, _HelpAction, _SubParsersAction  # noqa: PLC2701
from collections.abc import Sequence
from typing import Any

from digitalkin.services.service_provider import ServiceProvider

logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger(__name__)


class ArgParser:
    """ArgParse Abstract class to join all argparse argument in the same parser.

    Custom help display to allow multiple parser and subparser help message.

    Examples:
    --------
    Inherit this class in your base class.
    Override '_add_parser_args', '_add_exclusive_args' and '_add_subparser_args'.

    class WindowHandler(ArgParser):

        @staticmethod
        def _add_screen_parser_args(parser) -> None:
            parser.add_argument(
                "-f", "--fps", type=int, default=60, help="Screen FPS", dest="fps"
            )

        def _add_media_parser_args(self, parser) -> None:
            parser.add_argument(
                "-w",
                "--workers",
                type=int,
                default=3,
                help="Number of worker processing media in background",
                dest="media_worker_count"
            )

        def _add_parser_args(self, parser) -> None:
            super()._add_parser_args(parser)
            self._add_screen_parser_args(parser)
            self._add_media_parser_args(parser)

        def __init__(self):
            # init the parser
            super().__init__()
    """

    args: Namespace

    """
        Override methods
    """

    class HelpAction(_HelpAction):
        """."""

        def __call__(
            self,
            parser: ArgumentParser,
            namespace: Namespace,  # noqa: ARG002
            values: str | Sequence[Any] | None,  # noqa: ARG002
            option_string: str | None = None,  # noqa: ARG002
        ) -> None:
            """Override the HelpActions as it doesn't handle subparser well."""
            parser.print_help()
            subparsers_actions = [action for action in parser._actions if isinstance(action, _SubParsersAction)]  # noqa: SLF001
            for subparsers_action in subparsers_actions:
                for choice, subparser in subparsers_action.choices.items():
                    logger.info("Subparser '%s':\n%s", choice, subparser.format_help())
            parser.exit()

    """
        Private methods
    """

    def _add_parser_args(self, parser: ArgumentParser) -> None:
        parser.add_argument("-h", "--help", action=self.HelpAction, help="help usage")

    @staticmethod
    def _add_exclusive_args(parser: ArgumentParser) -> None: ...

    @staticmethod
    def _add_subparser_args(parser: ArgumentParser) -> None: ...

    def __init__(self, prog: str = "PROG") -> None:
        """Create prser and call abstract methods."""
        self.parser = ArgumentParser(prog=prog, conflict_handler="resolve", add_help=False)
        self._add_parser_args(self.parser)
        self._add_exclusive_args(self.parser)
        self._add_subparser_args(self.parser)
        self.args, _ = self.parser.parse_known_args()


class DevelopmentModeMappingAction(Action):
    """."""

    default: ServiceProvider | None
    class_mapping: dict[str, ServiceProvider]

    def __init__(
        self,
        env_var: str,
        class_mapping: dict[str, ServiceProvider],
        required: bool = True,  # noqa: FBT001, FBT002
        default: str | None = None,
        **kwargs: dict[str, Any],
    ) -> None:
        """."""
        default = class_mapping.get(os.environ.get(env_var, default), None)  # type: ignore
        if default is None:
            logger.error("Invalid default value: %s, for the Service Provider in the module", default)

        if required and default:
            required = False
        self.class_mapping = class_mapping
        super().__init__(default=default, required=required, **kwargs)  # type: ignore

    def __call__(
        self,
        parser: ArgumentParser,
        namespace: Namespace,
        values: str | Sequence[Any] | None,
        option_string: str | None = None,  # noqa: ARG002
    ) -> None:
        """Set the attribute to the corresponding class."""
        if values not in self.class_mapping:
            logger.error("Invalid mode: %s, dest: %s not set!", values, self.dest)
            parser.error(f"Invalid mode: {values}")

        values = self.class_mapping[values]  # type: ignore
        setattr(namespace, self.dest, values)
