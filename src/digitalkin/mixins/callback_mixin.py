"""User callback to send a message from the Trigger."""

from typing import Generic

from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.module_types import OutputModelT


class UserMessageMixin(Generic[OutputModelT]):
    """Mixin providing callback operations through the callbacks .

    This mixin wraps callback strategy calls to provide a cleaner API
    for direct messaging in trigger handlers.
    """

    @staticmethod
    async def send_message(context: ModuleContext, output: OutputModelT) -> None:
        """Send a message using the callbacks strategy.

        Args:
            context: Module context containing the callbacks strategy.
            output: Message to send with the Module defined output Type.
        """
        await context.callbacks.send_message(output)
