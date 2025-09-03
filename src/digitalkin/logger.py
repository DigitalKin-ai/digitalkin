"""This module sets up a logger."""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, ClassVar


class ColorJSONFormatter(logging.Formatter):
    """Color JSON formatter for development (pretty-printed with colors)."""

    def __init__(self, *, is_production: bool = False) -> None:
        """Initialize the formatter.

        Args:
            is_production: Whether the application is running in production.
        """
        self.is_production = is_production
        super().__init__()

    grey = "\x1b[38;20m"
    green = "\x1b[32;20m"
    blue = "\x1b[34;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    COLORS: ClassVar[dict[int, str]] = {
        logging.DEBUG: grey,
        logging.INFO: green,
        logging.WARNING: yellow,
        logging.ERROR: red,
        logging.CRITICAL: bold_red,
    }

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as colored JSON for development.

        Args:
            record: The log record to format.

        Returns:
            str: The colored JSON formatted log record.
        """
        log_obj: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "logger": record.name,
            "message": record.getMessage(),
            "location": f"{record.filename}:{record.lineno}",
            "function": record.funcName,
        }

        # Add exception info if present
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        # Add any extra fields
        skip_attrs = {
            "name",
            "msg",
            "args",
            "created",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "message",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "thread",
            "threadName",
            "exc_info",
            "exc_text",
            "stack_info",
        }

        extras = {key: value for key, value in record.__dict__.items() if key not in skip_attrs}

        if extras:
            log_obj["extra"] = extras

        # Pretty print with color
        color = self.COLORS.get(record.levelno, self.grey)
        if self.is_production:
            json_str = json.dumps(log_obj, default=str, separators=(",", ":"))
        else:
            json_str = json.dumps(log_obj, indent=2, default=str)
            json_str = json_str.replace("\\n", "\n")
        return f"{color}{json_str}{self.reset}"


logging.basicConfig(
    level=logging.DEBUG,
    stream=sys.stdout,
    datefmt="%Y-%m-%d %H:%M:%S",
)

logging.getLogger("grpc").setLevel(logging.DEBUG)
logging.getLogger("asyncio").setLevel(logging.DEBUG)


logger = logging.getLogger("digitalkin")
is_production = os.getenv("RAILWAY_SERVICE_NAME") is not None

if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(ColorJSONFormatter(is_production=is_production))

    logger.addHandler(ch)
    logger.propagate = False
