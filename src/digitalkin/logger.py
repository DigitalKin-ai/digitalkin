"""This module sets up a logger."""

import logging
import sys
from typing import ClassVar


class ColorFormatter(logging.Formatter):
    """Color formatter for logging."""

    grey = "\x1b[38;20m"
    green = "\x1b[32;20m"
    blue = "\x1b[34;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"  # type: ignore

    FORMATS: ClassVar[dict[int, str]] = {
        logging.DEBUG: grey + format + reset + "\n",  # type: ignore
        logging.INFO: green + format + reset + "\n",  # type: ignore
        logging.WARNING: yellow + format + reset + "\n",  # type: ignore
        logging.ERROR: red + format + reset + "\n",  # type: ignore
        logging.CRITICAL: bold_red + format + reset + "\n",  # type: ignore
    }

    def format(self, record: logging.LogRecord) -> str:  # type: ignore
        """Format the log record.

        Args:
            record: The log record to format.

        Returns:
            str: The formatted log record.
        """
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    datefmt="%Y-%m-%d %H:%M:%S",
)

logging.getLogger("grpc").setLevel(logging.DEBUG)
logging.getLogger("asyncio").setLevel(logging.DEBUG)


logger = logging.getLogger("digitalkin")

if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)

    ch.setFormatter(ColorFormatter())

    logger.addHandler(ch)
    logger.propagate = False
