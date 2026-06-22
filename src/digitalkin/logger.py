"""This module sets up a logger."""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, ClassVar

from digitalkin.grpc_servers.interceptors.request_ids import RequestContext
from digitalkin.models.settings.log import get_logging_settings


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
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

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

        color = self.COLORS.get(record.levelno, self.grey)
        if self.is_production:
            log_obj["message"] = f"{color}{log_obj.get('message', '')}{self.reset}"
            return json.dumps(log_obj, default=str, separators=(",", ":"))
        json_str = json.dumps(log_obj, indent=2, default=str)
        json_str = json_str.replace("\\n", "\n")
        return f"{color}{json_str}{self.reset}"


class PlainJSONFormatter(logging.Formatter):
    """Plain JSON formatter for log files (no ANSI colors, compact JSON)."""

    def format(self, record: logging.LogRecord) -> str:
        """Format the log record as compact JSON for file output.

        Args:
            record: The log record to format.

        Returns:
            str: The compact JSON formatted log record.
        """
        log_obj: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname.lower(),
            "message": record.getMessage(),
            "module": record.module,
            "location": f"{record.pathname}:{record.lineno}:{record.funcName}",
        }
        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

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

        return json.dumps(log_obj, default=str, separators=(",", ":"))


class RequestIdLogFilter(logging.Filter):
    """Inject ambient request IDs (task/setup/mission) onto every log record."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: PLR6301
        """Add ambient IDs to the record if present.

        Uses ``setdefault`` so an explicit ``extra=`` at the call site wins.

        Args:
            record: The log record to enrich.

        Returns:
            True — never drops records.
        """
        for key, value in RequestContext.current().items():
            record.__dict__.setdefault(key, value)
        return True


class LoggerFactory:
    """Build configured loggers with JSON formatters and optional file output."""

    LEVEL_NAMES: ClassVar[dict[str, int]] = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    @staticmethod
    def add_file_handler(logger: logging.Logger) -> None:
        """Add a rotating file handler to a logger if ``DIGITALKIN_LOG_DIR`` is set.

        Only creates log files when the environment variable is explicitly set
        and points to an existing directory.  Attaches a :class:`RotatingFileHandler`
        (10 MB, 5 backups) with :class:`PlainJSONFormatter` at DEBUG level.

        Args:
            logger: The logger to attach the file handler to.
        """
        settings = get_logging_settings()
        log_dir = settings.dir
        if not log_dir or not os.path.isdir(log_dir):
            return
        if any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
            # Low: idempotent — repeated setup_logger() calls must not stack handlers.
            logger.debug(
                "[VALIDATE Low-loghandler] file handler already attached, skipping"
            )  # TODO(validate): remove after prod validation
            return

        log_file = settings.file or os.path.join(log_dir, f"{logger.name}.log")
        fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5)
        fh.setLevel(LoggerFactory.LEVEL_NAMES.get(settings.file_level.upper(), logging.DEBUG))
        fh.setFormatter(PlainJSONFormatter())
        fh.addFilter(RequestIdLogFilter())
        logger.addHandler(fh)

    @staticmethod
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
        if is_production is None:
            is_production = get_logging_settings().railway_service_name is not None

        if configure_root:
            logging.basicConfig(
                level=logging.WARNING,
                stream=sys.stdout,
                datefmt="%Y-%m-%d %H:%M:%S",
            )

        if additional_loggers:
            for logger_name, logger_level in additional_loggers.items():
                logging.getLogger(logger_name).setLevel(logger_level)

        logger = logging.getLogger(name)
        logger.setLevel(level)
        if not logger.handlers:
            ch = logging.StreamHandler()
            ch.setLevel(level)
            ch.setFormatter(ColorJSONFormatter(is_production=is_production))
            ch.addFilter(RequestIdLogFilter())
            logger.addHandler(ch)
            logger.propagate = False

        LoggerFactory.add_file_handler(logger)

        return logger


logger = LoggerFactory.setup_logger(
    "digitalkin",
    level=LoggerFactory.LEVEL_NAMES.get(get_logging_settings().level.upper(), logging.INFO),
)

# Backwards-compatible re-exports for downstream that imported these directly
# (e.g. ``archetype_ada/logger.py``). Aliases to the staticmethods, identical behaviour.
setup_logger = LoggerFactory.setup_logger
add_file_handler = LoggerFactory.add_file_handler
