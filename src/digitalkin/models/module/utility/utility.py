"""Utility protocols for SDK-provided functionality.

These protocols are automatically available to all modules and don't need to be
explicitly included in module output unions.
"""

from typing import ClassVar

from digitalkin.models.module.base_types import DataTrigger


class UtilityProtocol(DataTrigger):
    """Base class for SDK-provided utility protocols.

    All SDK utility protocols inherit from this class to enable:
    - Easy identification of SDK vs user-defined protocols
    - Auto-injection capability
    - Consistent behavior across the SDK
    """


class UtilityRegistry:
    """Registry for SDK-provided built-in triggers.

    Example:
        builtin_triggers = UtilityRegistry.get_builtin_triggers()
    """

    _builtin_triggers: ClassVar[tuple | None] = None

    @classmethod
    def get_builtin_triggers(cls) -> tuple:
        """Get all SDK-provided built-in trigger handlers.

        Uses lazy loading to avoid circular imports with the modules package.

        Returns:
            Tuple of TriggerHandler subclasses for built-in functionality.
        """
        if cls._builtin_triggers is None:
            from digitalkin.modules.triggers.healthcheck_ping_trigger import (
                HealthcheckPingTrigger,
            )  # Lazy import to avoid circular dependency
            from digitalkin.modules.triggers.healthcheck_services_trigger import (
                HealthcheckServicesTrigger,
            )  # Lazy import to avoid circular dependency
            from digitalkin.modules.triggers.healthcheck_status_trigger import (
                HealthcheckStatusTrigger,
            )  # Lazy import to avoid circular dependency
            from digitalkin.modules.triggers.message_trigger import (
                MessageTrigger,
            )  # Lazy import to avoid circular dependency

            cls._builtin_triggers = (
                HealthcheckPingTrigger,
                HealthcheckServicesTrigger,
                HealthcheckStatusTrigger,
                MessageTrigger,
            )
        return cls._builtin_triggers
