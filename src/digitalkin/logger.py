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
            "message": record.getMessage(),
            "module": record.module,
            "location": f"{record.pathname}:{record.lineno}:{record.funcName}",
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
            log_obj["message"] = f"{color}{log_obj.get('message', '')}{self.reset}"
            return json.dumps(log_obj, default=str, separators=(",", ":"))
        json_str = json.dumps(log_obj, indent=2, default=str)
        json_str = json_str.replace("\\n", "\n")
        return f"{color}{json_str}{self.reset}"


def setup_logger(
    name: str,
    level: int = logging.INFO,
    additional_loggers: dict[str, int] | None = None,
    *,
    is_production: bool | None = None,
    configure_root: bool = True,
) -> logging.Logger:
    """Set up a logger with the ColorJSONFormatter.

    Args:
        name: Name of the logger to create
        level: Logging level (default: logging.INFO)
        is_production: Whether running in production. If None, checks RAILWAY_SERVICE_NAME env var
        configure_root: Whether to configure root logger (default: True)
        additional_loggers: Dict of additional logger names and their levels to configure

    Returns:
        logging.Logger: Configured logger instance
    """
    # Determine if we're in production
    if is_production is None:
        is_production = os.getenv("RAILWAY_SERVICE_NAME") is not None

    # Configure root logger if requested
    if configure_root:
        logging.basicConfig(
            level=logging.DEBUG,
            stream=sys.stdout,
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    # Configure additional loggers
    if additional_loggers:
        for logger_name, logger_level in additional_loggers.items():
            logging.getLogger(logger_name).setLevel(logger_level)

    # Create and configure the main logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    # Only add handler if not already configured
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(level)
        ch.setFormatter(ColorJSONFormatter(is_production=is_production))
        logger.addHandler(ch)
        logger.propagate = False

    return logger


logger = setup_logger(
    "digitalkin",
    level=logging.INFO,
    additional_loggers={
        "grpc": logging.DEBUG,
        "asyncio": logging.DEBUG,
    },
)
