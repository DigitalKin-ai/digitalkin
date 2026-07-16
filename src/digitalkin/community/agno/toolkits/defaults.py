"""One-call assembler for the default DigitalKin toolkits."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from digitalkin.community.agno.toolkits.chat_history import ChatHistoryTools
from digitalkin.community.agno.toolkits.registry import RegistryTools
from digitalkin.community.agno.toolkits.setup import SetupTools
from digitalkin.community.agno.toolkits.tool_loader import ToolLoaderTools
from digitalkin.community.agno.toolkits.user_profile import UserProfileTools

if TYPE_CHECKING:
    from collections.abc import Callable

    from agno.tools import Toolkit

    from digitalkin.models.module.module_context import ModuleContext


class DefaultToolkits:
    """Assemble the default DigitalKin toolkits (chat history, user profile, registry).

    Two-phase usage — ChatHistoryTools needs the constructed Agent/Team::

        tools = DefaultToolkits.build(context, session_id=sid)
        agent = Agent(tools=AguiTools.make_tools_factory(tools), cache_callables=False, ...)
        DefaultToolkits.bind_host(tools, agent)

    In team mode call :meth:`bind_host` again with the ``Team`` so history reads
    target the team session rather than the bare head agent.
    """

    @staticmethod
    def build(context: ModuleContext, session_id: str | None = None) -> list[Toolkit]:
        """Build the default toolkits from a module context.

        Includes SetupTools only when ``context.setup`` is wired (the servicer's shared
        setup service). ToolLoaderTools is always added and bound to the returned list so
        dynamically-loaded tools land in the exact list the agent's factory splats.

        Args:
            context: The module context carrying the services and (optional) setup service.
            session_id: The Agno session whose chat history should be readable.

        Returns:
            [ChatHistoryTools, UserProfileTools, RegistryTools, (SetupTools?), ToolLoaderTools].
        """
        tools: list[Toolkit] = [
            ChatHistoryTools(session_id=session_id, context=context),
            UserProfileTools(context.user_profile, context=context),
            RegistryTools(context.registry, context=context),
        ]
        if context.setup is not None:
            tools.append(SetupTools(context.setup, context=context))
        loader = ToolLoaderTools(context=context)
        tools.append(loader)
        loader.bind_tools(tools)
        return tools

    @staticmethod
    def bind_host(tools: list[Any] | Callable[..., list[Any]] | None, host: Any) -> None:
        """Late-bind the constructed Agent/Team into ChatHistoryTools (delegates).

        Args:
            tools: The tools list (or a make_tools_factory callable) containing the toolkits.
            host: The constructed Agent or Team to bind as the history source.
        """
        ChatHistoryTools.bind_host(tools, host)
