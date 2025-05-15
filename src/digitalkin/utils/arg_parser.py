"""ArgParser and Action classes to ease command lines arguments settings."""

import logging
from argparse import ArgumentParser, Namespace, _HelpAction, _SubParsersAction  # noqa: PLC2701
from collections.abc import Sequence
from typing import Any

from digitalkin.logger import logger

logger.setLevel(logging.INFO)


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

    def _add_exclusive_args(self, parser: ArgumentParser) -> None: ...

    def _add_subparser_args(self, parser: ArgumentParser) -> None: ...

    def __init__(self, prog: str = "PROG") -> None:
        """Create prser and call abstract methods."""
        self.parser = ArgumentParser(prog=prog, conflict_handler="resolve", add_help=False)
        self._add_parser_args(self.parser)
        self._add_exclusive_args(self.parser)
        self._add_subparser_args(self.parser)
        self.args, _ = self.parser.parse_known_args()
