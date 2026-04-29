"""User callback to send a message from the Trigger.

.. deprecated::
    Use :class:`digitalkin.mixins.agui_mixin.AgUiMixin` instead.
"""

import warnings
from typing import Any, Generic

from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.module_types import OutputModelT


class UserMessageMixin(Generic[OutputModelT]):
    """Mixin providing callback operations through the callbacks.

    .. deprecated::
        Use :class:`digitalkin.mixins.agui_mixin.AgUiMixin` instead.
    """

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Deprecated warning."""
        super().__init_subclass__(**kwargs)
        warnings.warn(
            f"{cls.__name__} inherits from UserMessageMixin which is deprecated. Use AgUiMixin.send_message instead.",
            DeprecationWarning,
            stacklevel=2,
        )

    @staticmethod
    async def send_message(context: ModuleContext, output: OutputModelT) -> None:
        """Send a message using the callbacks strategy.

        Args:
            context: Module context containing the callbacks strategy.
            output: Message to send with the Module defined output Type.
        """
        await context.callbacks.send_message(output)
