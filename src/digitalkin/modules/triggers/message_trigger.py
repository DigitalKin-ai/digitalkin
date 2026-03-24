from typing import ClassVar, Literal

from digitalkin.logger import logger
from digitalkin.mixins import BaseMixin
from digitalkin.models.module.module_context import ModuleContext
from digitalkin.models.module.utility.default import DefaultChatHistory
from digitalkin.models.module.utility.formats import DefaultInputFormat, DefaultOutputFormat, DefaultSetupFormat
from digitalkin.models.module.utility.inputs import MessageInputPayload
from digitalkin.models.module.utility.outputs import MessageOutputPayload
from digitalkin.models.services import BaseRole
from digitalkin.modules.trigger_handler import TriggerHandler


class MessageTrigger(TriggerHandler, BaseMixin):
    """Message trigger - processes user messages with Agno agent.

    Sends the user prompt to an Agno Agent backed by OpenAI,
    then returns the agent's response.
    """

    protocol: Literal["message"] = "message"
    description: ClassVar[str] = "Process user messages with Agno agent."
    input_format = DefaultInputFormat
    output_format = DefaultOutputFormat

    async def handle(
        self,
        input_data: MessageInputPayload,
        setup_format: DefaultSetupFormat,  # noqa: ARG002
        context: ModuleContext,
    ) -> None:
        """Handle incoming message by invoking the Agno agent.

        Args:
            input_data: Input payload with user_prompt
            setup_format: Setup configuration (system_prompt applied at init)
            context: Module context with services and agent state
        """
        logger.info("MessageTrigger received: %s", input_data.user_prompt)
        message_stack: str = input_data.user_prompt

        # Invoke the Agno agent
        if input_data.file_ids:
            prefix = (
                "The following document has been successfully processed and indexed into "
                "the knowledge base. Inform the user what file_id is."
            )
            message_stack = f"{prefix}{message_stack}"
        run_response = await context.state.agent.arun(message_stack)
        response_text = run_response.content if run_response and run_response.content else "No response generated."

        # Update chat history in storage
        chat_history_record = None
        try:
            chat_history_record = await context.storage.read("chat_history", "session")
            chat_history = (
                DefaultChatHistory.model_validate(chat_history_record.data)
                if chat_history_record
                else DefaultChatHistory(messages=[])
            )
        except Exception:
            chat_history = DefaultChatHistory(messages=[])

        chat_history.messages.append(f"USER: {input_data.user_prompt}")
        chat_history.messages.append(f"ASSISTANT: {response_text}")

        try:
            if chat_history_record:
                await context.storage.update("chat_history", "session", chat_history.model_dump())
            else:
                await context.storage.store("chat_history", "session", chat_history.model_dump(), data_type="OUTPUT")
        except Exception:
            await context.storage.store("chat_history", "session", chat_history.model_dump(), data_type="OUTPUT")

        # Track cost
        await context.cost.add("api_call", "api_call", 1)

        # Send response
        output_payload = MessageOutputPayload(user_response=response_text)
        await self.send_message(
            context=context,
            output=self.output_format(root=output_payload, annotations={"role": BaseRole.ASSISTANT}),
        )

        logger.info("MessageTrigger sent response")
