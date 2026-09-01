"""Message trigger handler for the EchoModule."""

import asyncio
from typing import ClassVar, Literal

from echo_module import EchoToolModule
from models.input import MessageInputPayload
from models.output import MessageOutputPayload
from models.setup import EchoSetup

from digitalkin.models.module import ModuleContext
from digitalkin.modules.trigger_handler import TriggerHandler


@EchoToolModule.register
class MessageTrigger(TriggerHandler):
    """Handles message protocol inputs — transforms and streams output chunks."""

    protocol: Literal["message"] = "message"
    description: ClassVar[str] = "Echo input text with optional transforms (uppercase, prefix, reverse, repeat)."
    input_format = MessageInputPayload
    output_format = MessageOutputPayload

    def __init__(self, context: ModuleContext) -> None:
        """Initialize the message trigger.

        Args:
            context: The module context.
        """
        self.enable_log = True

    async def handle(
        self,
        input_data: MessageInputPayload,
        setup_data: EchoSetup,
        context: ModuleContext,
    ) -> None:
        """Transform input and stream output chunks.

        Args:
            input_data: The input data payload.
            setup_data: The setup configuration.
            context: The module context.
        """
        text = input_data.user_prompt
        repeat = setup_data.repeat
        delay_s = setup_data.delay_ms / 1000

        for i in range(repeat):
            result = text
            if setup_data.reverse:
                result = result[::-1]
            if setup_data.uppercase:
                result = result.upper()
            if setup_data.prefix:
                result = f"{setup_data.prefix}{result}"
            chunk = f"[{i + 1}/{repeat}] {result}"

            output = MessageOutputPayload(response=chunk)
            await self.send_message(context, output)

            if i < repeat - 1 and delay_s > 0:
                await asyncio.sleep(delay_s)
