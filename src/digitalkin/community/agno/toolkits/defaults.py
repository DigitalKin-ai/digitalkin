"""One-call assembler for the default DigitalKin toolkits."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from digitalkin.community.agno.toolkits.chat_history import ChatHistoryTools
from digitalkin.community.agno.toolkits.registry.kins.kit import KinsManager
from digitalkin.community.agno.toolkits.registry.loader.kit import LoadManager
from digitalkin.community.agno.toolkits.registry.services.kit import ServicesManager
from digitalkin.community.agno.toolkits.registry.tools.kit import ToolsManager
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

        Includes the three registry managers (Tools / Services / Kins) only when
        ``context.setup`` is wired (they combine setup CRUD with registry search).
        LoadManager is always added and bound to the returned list so dynamically-loaded
        tools land in the exact list the agent's factory splats.

        Args:
            context: The module context carrying the services and (optional) setup service.
            session_id: The Agno session whose chat history should be readable.

        Returns:
            [ChatHistoryTools, UserProfileTools,
             (ToolsManager, ServicesManager, KinsManager)?, LoadManager].
        """
        tools: list[Toolkit] = [
            ChatHistoryTools(session_id=session_id, context=context),
            UserProfileTools(context.user_profile, context=context),
        ]
        if context.setup is not None:
            tools.extend((
                ToolsManager(context.setup, context.registry, context=context),
                ServicesManager(context.setup, context.registry, context=context),
                KinsManager(context.setup, context.registry, context=context),
            ))
        loader = LoadManager(context=context)
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
