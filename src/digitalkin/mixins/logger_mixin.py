"""Logger Mixin to ease and merge every logs."""

from digitalkin.models.module.module_context import ModuleContext


class LoggerMixin:
    """Mixin providing callback operations through the callbacks strategy.

    This mixin wraps callback strategy calls to provide a cleaner API
    for logging and messaging in trigger handlers.
    """

    @staticmethod
    def log_debug(context: ModuleContext, message: str) -> None:
        """Log debug message using the callbacks strategy.

        Args:
            context: Module context containing the callbacks strategy
            message: Debug message to log
        """
        return context.callbacks.logger.debug(message, extra=context.session.current_ids())

    @staticmethod
    def log_info(context: ModuleContext, message: str) -> None:
        """Log info message using the callbacks strategy.

        Args:
            context: Module context containing the callbacks strategy
            message: Info message to log
        """
        return context.callbacks.logger.info(message, extra=context.session.current_ids())

    @staticmethod
    def log_warning(context: ModuleContext, message: str) -> None:
        """Log warning message using the callbacks strategy.

        Args:
            context: Module context containing the callbacks strategy
            message: Warning message to log
        """
        return context.callbacks.logger.warning(message, extra=context.session.current_ids())

    @staticmethod
    def log_error(context: ModuleContext, message: str) -> None:
        """Log error message using the callbacks strategy.

        Args:
            context: Module context containing the callbacks strategy
            message: Error message to log
        """
        return context.callbacks.logger.error(message, extra=context.session.current_ids())
